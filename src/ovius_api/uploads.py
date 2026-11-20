"""Phase 2 resumable multipart upload, heartbeat, and ingest-state orchestration service."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import logging
import os
import re
import time
from typing import Any, Callable
import uuid

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from ovius_api.repository import get_dynamodb_resource, json_safe

logger = logging.getLogger(__name__)

FRESH_HEARTBEAT_MAX_AGE_S = 90
SESSION_TTL_SECONDS = 30 * 86400
DEFAULT_PRESIGN_EXPIRES_S = 1800
MAX_PRESIGN_EXPIRES_S = 1800
MIN_PRESIGN_EXPIRES_S = 60
MAX_RECORDINGS_PER_SESSION = 50
MAX_CHUNKS_PER_RECORDING = 100
_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{2,128}$")

_S3_CLIENT_CONFIG = Config(
    signature_version="s3v4",
    s3={"addressing_style": "virtual"},
    retries={"total_max_attempts": 10, "mode": "adaptive"},
)
_SQS_CLIENT_CONFIG = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
_BATCH_CLIENT_CONFIG = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
_EC2_CLIENT_CONFIG = Config(
    connect_timeout=3,
    read_timeout=5,
    retries={"total_max_attempts": 1, "mode": "standard"},
)

_CACHED_S3_CLIENT: Any | None = None
_CACHED_SQS_CLIENT: Any | None = None
_CACHED_BATCH_CLIENT: Any | None = None
_CACHED_EC2_CLIENT: Any | None = None


class UploadValidationError(ValueError):
    """Raised when an upload control request payload fails schema or state validation."""


class UploadConflictError(ValueError):
    """Raised when an upload session or resource already exists and cannot be overwritten."""


class UploadNotFoundError(LookupError):
    """Raised when a referenced upload session, recording, or chunk does not exist."""


def _aws_region() -> str:
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-3"


def get_s3_client() -> Any:
    """Return a cached S3 client configured for virtual-hosted SigV4 pre-signing."""
    global _CACHED_S3_CLIENT  # noqa: PLW0603
    if _CACHED_S3_CLIENT is None:
        _CACHED_S3_CLIENT = boto3.client("s3", region_name=_aws_region(), config=_S3_CLIENT_CONFIG)
    return _CACHED_S3_CLIENT


def get_sqs_client() -> Any:
    """Return a cached SQS client reused across warm Lambda invocations."""
    global _CACHED_SQS_CLIENT  # noqa: PLW0603
    if _CACHED_SQS_CLIENT is None:
        _CACHED_SQS_CLIENT = boto3.client("sqs", region_name=_aws_region(), config=_SQS_CLIENT_CONFIG)
    return _CACHED_SQS_CLIENT


def get_batch_client() -> Any:
    """Return a cached AWS Batch client reused across warm Lambda invocations."""
    global _CACHED_BATCH_CLIENT  # noqa: PLW0603
    if _CACHED_BATCH_CLIENT is None:
        _CACHED_BATCH_CLIENT = boto3.client(
            "batch", region_name=_aws_region(), config=_BATCH_CLIENT_CONFIG
        )
    return _CACHED_BATCH_CLIENT


def get_ec2_client() -> Any:
    """Return a cached EC2 client reused across warm Lambda invocations."""
    global _CACHED_EC2_CLIENT  # noqa: PLW0603
    if _CACHED_EC2_CLIENT is None:
        worker_region = os.environ.get("WORKER_REGION") or _aws_region()
        _CACHED_EC2_CLIENT = boto3.client(
            "ec2", region_name=worker_region, config=_EC2_CLIENT_CONFIG
        )
    return _CACHED_EC2_CLIENT


def _utc_iso(epoch_s: int | float) -> str:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).isoformat()


def _parse_worker_instance_ids(raw: str | list[str] | tuple[str, ...] | None) -> list[str]:
    """Parse a comma-separated string or sequence of EC2 instance IDs into an ordered unique list."""
    if raw is None:
        return []
    tokens = raw.split(",") if isinstance(raw, str) else list(raw)
    result: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        cleaned = str(token).strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            result.append(cleaned)
    return result


def _extract_ec2_error_code(exc: Exception) -> str:
    """Return AWS Error.Code for ClientError or exception class name for BotoCoreError."""
    if isinstance(exc, ClientError):
        return str(exc.response.get("Error", {}).get("Code", "Unknown"))
    return type(exc).__name__


def _describe_worker_states(ec2: Any, instance_ids: list[str]) -> dict[str, str]:
    """Batch-describe EC2 worker instances and map each instance ID to its state name."""
    desc = ec2.describe_instances(InstanceIds=instance_ids)
    state_by_id: dict[str, str] = {}
    for res in desc.get("Reservations", []):
        for inst in res.get("Instances", []):
            iid = str(inst.get("InstanceId") or "")
            if iid:
                state_by_id[iid] = str(inst["State"]["Name"])
    return state_by_id


def _require_id(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise UploadValidationError(f"{field_name} must be a string")
    cleaned = value.strip()
    if not _ID_PATTERN.match(cleaned):
        raise UploadValidationError(
            f"{field_name} must be 2-128 chars matching [A-Za-z0-9._-]"
        )
    return cleaned


def _require_non_empty_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise UploadValidationError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_utc_timestamp(value: Any, field_name: str = "timestamp") -> str:
    """Validate an ISO-8601 timestamp and normalize it to canonical UTC (+00:00)."""
    text = _require_non_empty_str(value, field_name)
    normalized = f"{text[:-1]}+00:00" if text.endswith(("Z", "z")) else text
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise UploadValidationError(f"{field_name} must be a valid ISO-8601 datetime: {text!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def _derive_session_status(rec_items: list[dict[str, Any]], fallback: str = "UPLOADING") -> str:
    """Derive live session_status from constituent recording upload/process states."""
    if not rec_items:
        return fallback
    upload_states = [str(r.get("upload_status") or "UPLOADING") for r in rec_items]
    proc_states = [str(r.get("process_status") or "WAITING_FOR_UPLOAD") for r in rec_items]

    if all(p == "COMPLETED" for p in proc_states):
        return "COMPLETED"
    if all(p == "ABORTED" or u == "ABORTED" for u, p in zip(upload_states, proc_states)):
        return "ABORTED"
    terminal = {"COMPLETED", "FAILED", "ABORTED"}
    if all(p in terminal for p in proc_states):
        if all(p == "FAILED" for p in proc_states):
            return "FAILED"
        return "COMPLETED_WITH_ERRORS"
    if any(p in {"PROCESSING_CHUNK", "PROCESSING"} for p in proc_states):
        return "PROCESSING"
    if any(u == "UPLOADING" for u in upload_states):
        return "UPLOADING"
    if any(u == "STALLED" for u in upload_states):
        return "STALLED"
    if any(p == "QUEUED" or u == "READY" for u, p in zip(upload_states, proc_states)):
        return "QUEUED"
    return fallback


def _require_positive_float(value: Any, field_name: str) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError) as exc:
        raise UploadValidationError(f"{field_name} must be a number") from exc
    if num <= 0.0:
        raise UploadValidationError(f"{field_name} must be > 0")
    return num


def _require_non_negative_float(value: Any, field_name: str) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError) as exc:
        raise UploadValidationError(f"{field_name} must be a number") from exc
    if num < 0.0:
        raise UploadValidationError(f"{field_name} must be >= 0")
    return num


def _require_int(value: Any, field_name: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    if isinstance(value, bool):
        raise UploadValidationError(f"{field_name} must be an integer")
    try:
        num = int(value)
    except (TypeError, ValueError) as exc:
        raise UploadValidationError(f"{field_name} must be an integer") from exc
    if num < minimum or (maximum is not None and num > maximum):
        bound = f">= {minimum}" if maximum is None else f"between {minimum} and {maximum}"
        raise UploadValidationError(f"{field_name} must be {bound}")
    return num


class UploadControlService:
    """Orchestrates upload sessions, resumable S3 multipart pre-signing, and ingest state."""

    def __init__(
        self,
        *,
        ddb_resource: Any | None = None,
        s3_client: Any | None = None,
        sqs_client: Any | None = None,
        batch_client: Any | None = None,
        ec2_client: Any | None = None,
        ingest_table_name: str | None = None,
        bucket_name: str | None = None,
        ready_queue_name: str | None = None,
        ready_queue_url: str | None = None,
        batch_job_queue: str | None = None,
        batch_job_definition: str | None = None,
        auto_submit_batch: bool | None = None,
        worker_instance_id: str | None = None,
        worker_instance_ids: list[str] | tuple[str, ...] | str | None = None,
        auto_start_ec2: bool | None = None,
        now_fn: Callable[[], float] | None = None,
    ) -> None:
        self._ddb = ddb_resource or get_dynamodb_resource()
        self._s3 = s3_client or get_s3_client()
        self._sqs = sqs_client or get_sqs_client()
        self._batch = batch_client
        self._ec2 = ec2_client
        self.ingest_table_name = ingest_table_name or os.environ.get(
            "INGEST_STATE_TABLE", "ovius-ingest-state"
        )
        self.bucket_name = (
            bucket_name
            or os.environ.get("PIPELINE_S3_BUCKET")
            or os.environ.get("ANALYTICS_S3_BUCKET")
            or "ovius-pipeline-apse3-610331148578"
        )
        self.ready_queue_name = ready_queue_name or os.environ.get(
            "READY_QUEUE_NAME", "ovius-ready-recordings"
        )
        self._ready_queue_url = ready_queue_url or os.environ.get("READY_QUEUE_URL")
        self.batch_job_queue = batch_job_queue or os.environ.get("BATCH_JOB_QUEUE")
        self.batch_job_definition = batch_job_definition or os.environ.get("BATCH_JOB_DEFINITION")
        if auto_submit_batch is None:
            self.auto_submit_batch = (
                os.environ.get("AUTO_SUBMIT_BATCH_JOBS", "false").strip().lower() == "true"
            )
        else:
            self.auto_submit_batch = auto_submit_batch
        raw_worker_ids: str | list[str] | tuple[str, ...] | None
        if worker_instance_ids is not None:
            raw_worker_ids = worker_instance_ids
        elif worker_instance_id is not None:
            raw_worker_ids = worker_instance_id
        else:
            raw_worker_ids = (
                os.environ.get("WORKER_INSTANCE_IDS", "")
                or os.environ.get("WORKER_INSTANCE_ID", "")
            )
        self.worker_instance_ids = _parse_worker_instance_ids(raw_worker_ids)
        self.worker_instance_id = self.worker_instance_ids[0] if self.worker_instance_ids else ""
        if auto_start_ec2 is None:
            self.auto_start_ec2 = (
                os.environ.get("AUTO_START_EC2_WORKER", "false").strip().lower() == "true"
            )
        else:
            self.auto_start_ec2 = auto_start_ec2
        self._now = now_fn or time.time
        self.ingest_table = self._ddb.Table(self.ingest_table_name)

    def _resolve_ready_queue_url(self) -> str | None:
        if self._ready_queue_url:
            return self._ready_queue_url
        try:
            resp = self._sqs.get_queue_url(QueueName=self.ready_queue_name)
            self._ready_queue_url = resp["QueueUrl"]
            return self._ready_queue_url
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"}:
                logger.info("Ready queue %s not provisioned yet; skipping SQS enqueue", self.ready_queue_name)
                return None
            raise

    def _enqueue_ready_recording(self, payload: dict[str, Any]) -> bool:
        import json

        queue_url = self._resolve_ready_queue_url()
        if not queue_url:
            return False
        self._sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(payload, separators=(",", ":")))
        return True

    def create_session(self, body: dict[str, Any]) -> dict[str, Any]:
        """Register a multi-recording upload session and expected chunks in ovius-ingest-state."""
        if not isinstance(body, dict):
            raise UploadValidationError("Request body must be a JSON object")

        raw_site_id = body.get("site_id")
        site_id = _require_id(
            "open_pit_mine_01" if raw_site_id is None else raw_site_id, "site_id"
        )
        upload_id = (
            _require_id(body["upload_id"], "upload_id")
            if body.get("upload_id") is not None
            else f"upl_{uuid.uuid4().hex[:16]}"
        )

        raw_recordings = body.get("recordings")
        if not isinstance(raw_recordings, list) or not raw_recordings:
            raise UploadValidationError("recordings must be a non-empty list")
        if len(raw_recordings) > MAX_RECORDINGS_PER_SESSION:
            raise UploadValidationError(
                f"recordings count must be <= {MAX_RECORDINGS_PER_SESSION}"
            )

        now_epoch = int(self._now())
        now_iso = _utc_iso(now_epoch)
        expires_at = now_epoch + SESSION_TTL_SECONDS

        seen_rec_ids: set[str] = set()
        session_items: list[dict[str, Any]] = []
        response_recordings: list[dict[str, Any]] = []

        for raw_rec in raw_recordings:
            if not isinstance(raw_rec, dict):
                raise UploadValidationError("Each recording entry must be an object")

            rec_id = _require_id(raw_rec.get("recording_id"), "recording_id")
            if rec_id in seen_rec_ids:
                raise UploadValidationError(f"Duplicate recording_id in session: {rec_id}")
            seen_rec_ids.add(rec_id)

            camera_id = _require_id(raw_rec.get("camera_id"), "camera_id")
            timestamp = _require_utc_timestamp(raw_rec.get("timestamp"), "timestamp")
            duration_s = _require_positive_float(raw_rec.get("duration_s"), "duration_s")
            fps = _require_positive_float(raw_rec.get("fps"), "fps")
            raw_res = raw_rec.get("resolution")
            resolution = _require_non_empty_str(
                "1920x1080" if raw_res is None else raw_res, "resolution"
            )

            raw_chunks = raw_rec.get("chunks")
            if not isinstance(raw_chunks, list) or not raw_chunks:
                raise UploadValidationError(f"Recording {rec_id} chunks must be a non-empty list")
            if len(raw_chunks) > MAX_CHUNKS_PER_RECORDING:
                raise UploadValidationError(
                    f"Recording {rec_id} chunks count must be <= {MAX_CHUNKS_PER_RECORDING}"
                )

            seen_chunk_indices: set[int] = set()
            chunk_items: list[dict[str, Any]] = []
            response_chunks: list[dict[str, Any]] = []
            total_bytes = 0

            for raw_chunk in raw_chunks:
                if not isinstance(raw_chunk, dict):
                    raise UploadValidationError("Each chunk entry must be an object")
                chunk_idx = _require_int(raw_chunk.get("chunk_index"), "chunk_index", minimum=0)
                if chunk_idx in seen_chunk_indices:
                    raise UploadValidationError(
                        f"Duplicate chunk_index {chunk_idx} in recording {rec_id}"
                    )
                seen_chunk_indices.add(chunk_idx)

                start_offset_s = _require_non_negative_float(
                    raw_chunk.get("start_offset_s"), "start_offset_s"
                )
                chunk_duration_s = _require_positive_float(raw_chunk.get("duration_s"), "duration_s")
                byte_size = _require_int(raw_chunk.get("byte_size"), "byte_size", minimum=1)
                total_bytes += byte_size

                s3_key = f"uploads/{site_id}/{upload_id}/{rec_id}/chunk_{chunk_idx:02d}.mp4"
                chunk_item: dict[str, Any] = {
                    "pk": f"REC#{rec_id}",
                    "sk": f"CHUNK#{chunk_idx:04d}",
                    "upload_id": upload_id,
                    "recording_id": rec_id,
                    "site_id": site_id,
                    "chunk_index": chunk_idx,
                    "s3_key": s3_key,
                    "start_offset_s": Decimal(str(start_offset_s)),
                    "duration_s": Decimal(str(chunk_duration_s)),
                    "byte_size": byte_size,
                    "upload_status": "PENDING",
                    "process_status": "PENDING",
                    "updated_at": now_iso,
                    "expires_at": expires_at,
                }
                chunk_items.append(chunk_item)
                response_chunks.append(
                    {
                        "chunk_index": chunk_idx,
                        "s3_key": s3_key,
                        "start_offset_s": start_offset_s,
                        "duration_s": chunk_duration_s,
                        "byte_size": byte_size,
                        "upload_status": "PENDING",
                    }
                )

            rec_item: dict[str, Any] = {
                "pk": f"SESSION#{upload_id}",
                "sk": f"REC#{rec_id}",
                "upload_id": upload_id,
                "recording_id": rec_id,
                "site_id": site_id,
                "camera_id": camera_id,
                "timestamp": timestamp,
                "duration_s": Decimal(str(duration_s)),
                "fps": Decimal(str(fps)),
                "resolution": resolution,
                "upload_status": "UPLOADING",
                "uploaded_chunks": 0,
                "expected_chunks": len(chunk_items),
                "uploaded_bytes": 0,
                "total_bytes": total_bytes,
                "upload_eta_s": Decimal("999999"),
                "last_heartbeat_at": now_epoch,
                "GSI1PK": "ACTIVE_UPLOAD",
                "GSI1SK": Decimal("999999"),
                "process_status": "WAITING_FOR_UPLOAD",
                "last_completed_chunk_index": -1,
                "next_cycle_seq": 0,
                "next_segment_seq": 0,
                "processed_chunks": 0,
                "active_chunk_index": 0,
                "active_chunk_frame_progress_pct": Decimal("0.0"),
                "cycles_extracted_so_far": 0,
                "updated_at": now_iso,
                "expires_at": expires_at,
            }
            if raw_rec.get("location_tag") is not None:
                rec_item["location_tag"] = _require_non_empty_str(
                    raw_rec["location_tag"], "location_tag"
                )
            if raw_rec.get("filename") is not None:
                rec_item["filename"] = _require_non_empty_str(raw_rec["filename"], "filename")
            if raw_rec.get("tags") is not None:
                raw_tags = raw_rec["tags"]
                if not isinstance(raw_tags, list) or not all(isinstance(t, str) for t in raw_tags):
                    raise UploadValidationError("tags must be a list of strings")
                rec_item["tags"] = list(raw_tags)

            session_items.append(rec_item)
            session_items.extend(chunk_items)

            response_chunks.sort(key=lambda c: c["chunk_index"])
            response_recordings.append(
                {
                    "recording_id": rec_id,
                    "camera_id": camera_id,
                    "upload_status": "UPLOADING",
                    "expected_chunks": len(chunk_items),
                    "total_bytes": total_bytes,
                    "chunks": response_chunks,
                }
            )

        meta_item: dict[str, Any] = {
            "pk": f"SESSION#{upload_id}",
            "sk": "META",
            "upload_id": upload_id,
            "site_id": site_id,
            "session_status": "UPLOADING",
            "smoothed_upload_bps": Decimal("0"),
            "last_heartbeat_at": now_epoch,
            "total_recordings": len(response_recordings),
            "completed_recordings": 0,
            "created_at": now_iso,
            "updated_at": now_iso,
            "expires_at": expires_at,
        }

        try:
            self.ingest_table.put_item(
                Item=meta_item,
                ConditionExpression="attribute_not_exists(pk)",
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                raise UploadConflictError(f"Upload session {upload_id} already exists") from exc
            raise

        with self.ingest_table.batch_writer() as writer:
            for item in session_items:
                writer.put_item(Item=item)

        return {
            "upload_id": upload_id,
            "site_id": site_id,
            "session_status": "UPLOADING",
            "s3_bucket": self.bucket_name,
            "total_recordings": len(response_recordings),
            "recordings": response_recordings,
        }

    def presign_chunk_parts(self, body: dict[str, Any]) -> dict[str, Any]:
        """Lazily create or resume an S3 multipart upload and mint part-level pre-signed PUT URLs."""
        if not isinstance(body, dict):
            raise UploadValidationError("Request body must be a JSON object")

        upload_id = _require_id(body.get("upload_id"), "upload_id")
        recording_id = _require_id(body.get("recording_id"), "recording_id")
        chunk_index = _require_int(body.get("chunk_index"), "chunk_index", minimum=0)
        raw_expires_in = body.get("expires_in")
        expires_in = (
            DEFAULT_PRESIGN_EXPIRES_S
            if raw_expires_in is None
            else _require_int(
                raw_expires_in,
                "expires_in",
                minimum=MIN_PRESIGN_EXPIRES_S,
                maximum=MAX_PRESIGN_EXPIRES_S,
            )
        )

        raw_parts = body.get("part_numbers")
        if not isinstance(raw_parts, list) or not raw_parts:
            raise UploadValidationError("part_numbers must be a non-empty list")
        part_numbers = list(
            dict.fromkeys(_require_int(p, "part_number", minimum=1, maximum=10000) for p in raw_parts)
        )

        req_s3_upload_id = body.get("s3_upload_id")
        if req_s3_upload_id is not None:
            req_s3_upload_id = _require_non_empty_str(req_s3_upload_id, "s3_upload_id")

        chunk_key = {"pk": f"REC#{recording_id}", "sk": f"CHUNK#{chunk_index:04d}"}
        chunk_item = self.ingest_table.get_item(Key=chunk_key).get("Item")
        if not chunk_item:
            raise UploadNotFoundError(
                f"Chunk {chunk_index} for recording {recording_id} is not registered"
            )
        if chunk_item.get("upload_id") != upload_id:
            raise UploadValidationError("upload_id does not match registered chunk")
        if chunk_item.get("upload_status") == "ABORTED":
            raise UploadValidationError("Chunk upload has been aborted")
        if chunk_item.get("upload_status") == "UPLOADED":
            raise UploadValidationError("Chunk upload is already completed")

        s3_key = str(chunk_item["s3_key"])
        active_s3_upload_id = chunk_item.get("s3_upload_id")
        if active_s3_upload_id and req_s3_upload_id and req_s3_upload_id != active_s3_upload_id:
            raise UploadValidationError("s3_upload_id does not match active multipart upload")

        now_iso = _utc_iso(int(self._now()))
        if not active_s3_upload_id:
            created = self._s3.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                ContentType="video/mp4",
            )
            created_id = str(created["UploadId"])
            try:
                self.ingest_table.update_item(
                    Key=chunk_key,
                    UpdateExpression="SET s3_upload_id = :uid, updated_at = :now",
                    ConditionExpression="attribute_not_exists(s3_upload_id)",
                    ExpressionAttributeValues={":uid": created_id, ":now": now_iso},
                )
                active_s3_upload_id = created_id
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                    raise
                self._s3.abort_multipart_upload(
                    Bucket=self.bucket_name, Key=s3_key, UploadId=created_id
                )
                refreshed = self.ingest_table.get_item(Key=chunk_key).get("Item") or {}
                active_s3_upload_id = refreshed.get("s3_upload_id")
                if not active_s3_upload_id:
                    raise

        presigned_parts = [
            {
                "part_number": part_no,
                "url": self._s3.generate_presigned_url(
                    "upload_part",
                    Params={
                        "Bucket": self.bucket_name,
                        "Key": s3_key,
                        "UploadId": str(active_s3_upload_id),
                        "PartNumber": part_no,
                    },
                    ExpiresIn=expires_in,
                    HttpMethod="PUT",
                ),
            }
            for part_no in part_numbers
        ]

        return {
            "upload_id": upload_id,
            "recording_id": recording_id,
            "chunk_index": chunk_index,
            "s3_bucket": self.bucket_name,
            "s3_key": s3_key,
            "s3_upload_id": str(active_s3_upload_id),
            "expires_in": expires_in,
            "presigned_parts": presigned_parts,
        }

    def update_heartbeat(self, upload_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Update client upload progress and refresh sparse GSI_ACTIVE_UPLOADS ETAs."""
        clean_upload_id = _require_id(upload_id, "upload_id")
        if not isinstance(body, dict):
            raise UploadValidationError("Request body must be a JSON object")

        bps = _require_non_negative_float(body.get("smoothed_upload_bps"), "smoothed_upload_bps")
        raw_recs = body.get("recordings", [])
        if not isinstance(raw_recs, list):
            raise UploadValidationError("recordings must be a list")

        session_pk = f"SESSION#{clean_upload_id}"
        session_items = self.ingest_table.query(
            KeyConditionExpression=Key("pk").eq(session_pk)
        ).get("Items", [])
        if not session_items:
            raise UploadNotFoundError(f"Upload session {clean_upload_id} not found")

        rec_map = {
            str(item["recording_id"]): item
            for item in session_items
            if str(item["sk"]).startswith("REC#")
        }

        now_epoch = int(self._now())
        now_iso = _utc_iso(now_epoch)

        self.ingest_table.update_item(
            Key={"pk": session_pk, "sk": "META"},
            UpdateExpression="SET smoothed_upload_bps = :bps, last_heartbeat_at = :hb, updated_at = :now",
            ExpressionAttributeValues={
                ":bps": Decimal(str(round(bps, 2))),
                ":hb": now_epoch,
                ":now": now_iso,
            },
        )

        updated_recordings: list[dict[str, Any]] = []
        effective_bps = max(bps, 1.0)
        for entry in raw_recs:
            if not isinstance(entry, dict):
                raise UploadValidationError("Each heartbeat recording entry must be an object")
            rec_id = _require_id(entry.get("recording_id"), "recording_id")
            uploaded_bytes = _require_int(entry.get("uploaded_bytes"), "uploaded_bytes", minimum=0)

            existing = rec_map.get(rec_id)
            if existing is None:
                raise UploadNotFoundError(
                    f"Recording {rec_id} does not belong to upload session {clean_upload_id}"
                )

            total_bytes = int(existing.get("total_bytes", 0))
            remaining_bytes = max(0, total_bytes - uploaded_bytes)
            eta_s = max(0, int(round(remaining_bytes / effective_bps)))

            try:
                self.ingest_table.update_item(
                    Key={"pk": session_pk, "sk": f"REC#{rec_id}"},
                    UpdateExpression=(
                        "SET uploaded_bytes = :ub, upload_eta_s = :eta, "
                        "GSI1PK = :gpk, GSI1SK = :eta, last_heartbeat_at = :hb, updated_at = :now"
                    ),
                    ConditionExpression="upload_status = :uploading",
                    ExpressionAttributeValues={
                        ":ub": uploaded_bytes,
                        ":eta": Decimal(eta_s),
                        ":gpk": "ACTIVE_UPLOAD",
                        ":hb": now_epoch,
                        ":now": now_iso,
                        ":uploading": "UPLOADING",
                    },
                )
                updated_recordings.append(
                    {
                        "recording_id": rec_id,
                        "upload_status": "UPLOADING",
                        "uploaded_bytes": uploaded_bytes,
                        "upload_eta_s": eta_s,
                    }
                )
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                    raise
                updated_recordings.append(
                    {
                        "recording_id": rec_id,
                        "upload_status": str(existing.get("upload_status", "UNKNOWN")),
                        "uploaded_bytes": int(existing.get("uploaded_bytes", 0)),
                        "upload_eta_s": int(existing.get("upload_eta_s", 0)),
                    }
                )

        return {
            "upload_id": clean_upload_id,
            "last_heartbeat_at": now_epoch,
            "smoothed_upload_bps": bps,
            "recordings": updated_recordings,
        }

    def complete_chunk(self, body: dict[str, Any]) -> dict[str, Any]:
        """Synchronously complete S3 multipart upload and atomically advance chunk/recording state."""
        if not isinstance(body, dict):
            raise UploadValidationError("Request body must be a JSON object")

        upload_id = _require_id(body.get("upload_id"), "upload_id")
        recording_id = _require_id(body.get("recording_id"), "recording_id")
        chunk_index = _require_int(body.get("chunk_index"), "chunk_index", minimum=0)
        s3_upload_id = _require_non_empty_str(body.get("s3_upload_id"), "s3_upload_id")

        raw_parts = body.get("parts")
        if not isinstance(raw_parts, list) or not raw_parts:
            raise UploadValidationError("parts must be a non-empty list")

        normalized_parts: list[dict[str, Any]] = []
        for raw_part in raw_parts:
            if not isinstance(raw_part, dict):
                raise UploadValidationError("Each part entry must be an object")
            part_no = _require_int(raw_part.get("PartNumber"), "PartNumber", minimum=1, maximum=10000)
            etag = _require_non_empty_str(raw_part.get("ETag"), "ETag")
            normalized_parts.append({"PartNumber": part_no, "ETag": etag})
        normalized_parts.sort(key=lambda p: p["PartNumber"])

        chunk_pk = f"REC#{recording_id}"
        chunk_sk = f"CHUNK#{chunk_index:04d}"
        chunk_item = self.ingest_table.get_item(Key={"pk": chunk_pk, "sk": chunk_sk}).get("Item")
        if not chunk_item:
            raise UploadNotFoundError(
                f"Chunk {chunk_index} for recording {recording_id} is not registered"
            )
        if chunk_item.get("upload_id") != upload_id:
            raise UploadValidationError("upload_id does not match registered chunk")
        if chunk_item.get("upload_status") == "ABORTED":
            raise UploadValidationError("Chunk upload has been aborted")
        if chunk_item.get("s3_upload_id") and chunk_item["s3_upload_id"] != s3_upload_id:
            raise UploadValidationError("s3_upload_id does not match registered chunk")

        s3_key = str(chunk_item["s3_key"])
        try:
            self._s3.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=s3_upload_id,
                MultipartUpload={"Parts": normalized_parts},
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "NoSuchUpload":
                # Retry resilience: verify object already materialized in S3 before treating as idempotent.
                self._s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            else:
                raise

        now_epoch = int(self._now())
        now_iso = _utc_iso(now_epoch)
        session_pk = f"SESSION#{upload_id}"
        rec_sk = f"REC#{recording_id}"
        return self.transition_chunk_uploaded(
            upload_id=upload_id,
            recording_id=recording_id,
            chunk_index=chunk_index,
            s3_key=s3_key,
            now_epoch=now_epoch,
            now_iso=now_iso,
            chunk_pk=chunk_pk,
            chunk_sk=chunk_sk,
            session_pk=session_pk,
            rec_sk=rec_sk,
        )

    def try_claim_worker_lock_and_submit_batch(self, recording_id: str) -> dict[str, Any]:
        """Atomically claim WORKER#LOCK if idle/exiting/expired and optionally submit an AWS Batch job."""
        if not (self.auto_submit_batch and self.batch_job_queue and self.batch_job_definition):
            return {"claimed": False, "batch_submitted": False, "reason": "auto_submit_disabled"}

        now_epoch = int(self._now())
        now_iso = _utc_iso(now_epoch)
        lease_expires_at = now_epoch + 300
        lock_key = {"pk": "WORKER#LOCK", "sk": "PRIMARY"}

        try:
            self.ingest_table.update_item(
                Key=lock_key,
                UpdateExpression=(
                    "SET #st = :starting, active_recording_id = :rec, "
                    "lease_expires_at = :lease, updated_at = :now_iso"
                ),
                ConditionExpression=(
                    "attribute_not_exists(#st) OR #st IN (:idle, :exiting) "
                    "OR lease_expires_at < :now_epoch"
                ),
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":starting": "STARTING",
                    ":idle": "IDLE",
                    ":exiting": "EXITING",
                    ":rec": recording_id,
                    ":lease": lease_expires_at,
                    ":now_epoch": now_epoch,
                    ":now_iso": now_iso,
                },
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return {"claimed": False, "batch_submitted": False}
            raise

        batch_client = self._batch or get_batch_client()
        job_name = re.sub(r"[^A-Za-z0-9_-]", "-", f"ovius-{recording_id}")[:128]
        try:
            resp = batch_client.submit_job(
                jobName=job_name,
                jobQueue=self.batch_job_queue,
                jobDefinition=self.batch_job_definition,
                containerOverrides={
                    "environment": [
                        {"name": "TRIGGER_RECORDING_ID", "value": recording_id},
                    ]
                },
            )
            job_id = str(resp["jobId"])
            self.ingest_table.update_item(
                Key=lock_key,
                UpdateExpression="SET batch_job_id = :jid, updated_at = :now_iso",
                ExpressionAttributeValues={":jid": job_id, ":now_iso": now_iso},
            )
            return {"claimed": True, "batch_submitted": True, "batch_job_id": job_id}
        except Exception:  # noqa: BLE001
            logger.exception("Failed to submit AWS Batch job for %s; releasing WORKER#LOCK", recording_id)
            try:
                self.ingest_table.update_item(
                    Key=lock_key,
                    UpdateExpression="SET #st = :idle, lease_expires_at = :zero, updated_at = :now_iso",
                    ExpressionAttributeNames={"#st": "status"},
                    ExpressionAttributeValues={
                        ":idle": "IDLE",
                        ":zero": 0,
                        ":now_iso": _utc_iso(int(self._now())),
                    },
                )
            except ClientError:
                pass
            raise

    def try_start_ec2_worker(self, recording_id: str) -> dict[str, Any]:
        """Wake the persistent stopped GPU EC2 worker with multi-AZ failover on capacity errors."""
        if not (self.auto_start_ec2 and self.worker_instance_ids):
            return {"claimed": False, "ec2_started": False, "reason": "auto_start_ec2_disabled"}

        ec2 = self._ec2 or get_ec2_client()
        try:
            state_by_id = _describe_worker_states(ec2, self.worker_instance_ids)
        except (ClientError, BotoCoreError) as exc:
            code = _extract_ec2_error_code(exc)
            logger.warning(
                "EC2 describe_instances failed for %s (%s)", self.worker_instance_ids, code
            )
            return {
                "claimed": False,
                "ec2_started": False,
                "instance_id": self.worker_instance_id,
                "attempted_instance_ids": list(self.worker_instance_ids),
                "retry_in_watchdog": True,
                "error_code": code,
            }

        if not state_by_id:
            return {
                "claimed": False,
                "ec2_started": False,
                "instance_id": self.worker_instance_id,
                "reason": "instance_not_found",
            }

        for iid in self.worker_instance_ids:
            state = state_by_id.get(iid)
            if state in {"running", "pending"}:
                return {
                    "claimed": False,
                    "ec2_started": False,
                    "instance_id": iid,
                    "instance_state": state,
                    "reason": "already_active",
                }

        stopped_ids = [iid for iid in self.worker_instance_ids if state_by_id.get(iid) == "stopped"]
        if not stopped_ids:
            first_id = next(
                (iid for iid in self.worker_instance_ids if iid in state_by_id),
                self.worker_instance_id,
            )
            first_state = state_by_id.get(first_id, "unknown")
            return {
                "claimed": False,
                "ec2_started": False,
                "instance_id": first_id,
                "instance_state": first_state,
                "retry_in_watchdog": True,
                "reason": f"instance_{first_state}",
            }

        now_epoch = int(self._now())
        now_iso = _utc_iso(now_epoch)
        lock_key = {"pk": "WORKER#LOCK", "sk": "PRIMARY"}
        primary_candidate = stopped_ids[0]
        try:
            self.ingest_table.update_item(
                Key=lock_key,
                UpdateExpression=(
                    "SET #st = :starting, active_recording_id = :rec, "
                    "worker_instance_id = :iid, lease_expires_at = :lease, updated_at = :now_iso"
                ),
                ConditionExpression=(
                    "attribute_not_exists(#st) OR #st IN (:idle, :exiting) "
                    "OR lease_expires_at < :now_epoch"
                ),
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":starting": "STARTING",
                    ":idle": "IDLE",
                    ":exiting": "EXITING",
                    ":rec": recording_id,
                    ":iid": primary_candidate,
                    ":lease": now_epoch + 300,
                    ":now_epoch": now_epoch,
                    ":now_iso": now_iso,
                },
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return {
                    "claimed": False,
                    "ec2_started": False,
                    "instance_id": primary_candidate,
                    "reason": "lock_busy",
                }
            raise

        attempts: list[dict[str, str]] = []
        last_error_code = "Unknown"
        for candidate_id in stopped_ids:
            try:
                ec2.start_instances(InstanceIds=[candidate_id])
                if candidate_id != primary_candidate:
                    try:
                        self.ingest_table.update_item(
                            Key=lock_key,
                            UpdateExpression="SET worker_instance_id = :iid, updated_at = :now_iso",
                            ExpressionAttributeValues={
                                ":iid": candidate_id,
                                ":now_iso": _utc_iso(int(self._now())),
                            },
                        )
                    except ClientError:
                        pass
                return {
                    "claimed": True,
                    "ec2_started": True,
                    "instance_id": candidate_id,
                    "instance_state": "pending",
                    "failover_used": candidate_id != primary_candidate,
                    "attempts": attempts,
                }
            except (ClientError, BotoCoreError) as exc:
                last_error_code = _extract_ec2_error_code(exc)
                attempts.append({"instance_id": candidate_id, "error_code": last_error_code})
                logger.warning(
                    "EC2 start_instances deferred for %s (%s); trying next AZ standby if available",
                    candidate_id,
                    last_error_code,
                )

        try:
            self.ingest_table.update_item(
                Key=lock_key,
                UpdateExpression="SET #st = :idle, lease_expires_at = :zero, updated_at = :now_iso",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":idle": "IDLE",
                    ":zero": 0,
                    ":now_iso": _utc_iso(int(self._now())),
                },
            )
        except ClientError:
            pass
        return {
            "claimed": False,
            "ec2_started": False,
            "instance_id": stopped_ids[-1],
            "retry_in_watchdog": True,
            "error_code": last_error_code,
            "attempts": attempts,
        }

    def transition_chunk_uploaded(
        self,
        *,
        upload_id: str,
        recording_id: str,
        chunk_index: int,
        s3_key: str,
        now_epoch: int | None = None,
        now_iso: str | None = None,
        chunk_pk: str | None = None,
        chunk_sk: str | None = None,
        session_pk: str | None = None,
        rec_sk: str | None = None,
    ) -> dict[str, Any]:
        """Idempotently record chunk completion and transition UPLOADING -> READY at most once."""
        resolved_epoch = int(self._now()) if now_epoch is None else now_epoch
        resolved_iso = _utc_iso(resolved_epoch) if now_iso is None else now_iso
        resolved_chunk_pk = chunk_pk or f"REC#{recording_id}"
        resolved_chunk_sk = chunk_sk or f"CHUNK#{chunk_index:04d}"
        resolved_session_pk = session_pk or f"SESSION#{upload_id}"
        resolved_rec_sk = rec_sk or f"REC#{recording_id}"

        chunk_newly_completed = True
        try:
            self._ddb.meta.client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": self.ingest_table_name,
                            "Key": {"pk": resolved_chunk_pk, "sk": resolved_chunk_sk},
                            "UpdateExpression": (
                                "SET upload_status = :uploaded, completed_at = :now, updated_at = :now"
                            ),
                            "ConditionExpression": (
                                "attribute_not_exists(upload_status) OR upload_status <> :uploaded"
                            ),
                            "ExpressionAttributeValues": {
                                ":uploaded": "UPLOADED",
                                ":now": resolved_iso,
                            },
                        }
                    },
                    {
                        "Update": {
                            "TableName": self.ingest_table_name,
                            "Key": {"pk": resolved_session_pk, "sk": resolved_rec_sk},
                            "UpdateExpression": "ADD uploaded_chunks :one SET updated_at = :now",
                            "ExpressionAttributeValues": {
                                ":one": 1,
                                ":now": resolved_iso,
                            },
                        }
                    },
                ]
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "TransactionCanceledException":
                raise
            chunk_newly_completed = False

        rec_item = self.ingest_table.get_item(
            Key={"pk": resolved_session_pk, "sk": resolved_rec_sk}
        ).get("Item")
        if not rec_item:
            raise UploadNotFoundError(f"Recording {recording_id} not found in session {upload_id}")

        uploaded_chunks = int(rec_item.get("uploaded_chunks", 0))
        expected_chunks = int(rec_item.get("expected_chunks", 0))
        upload_status = str(rec_item.get("upload_status", "UPLOADING"))
        transitioned_to_ready = False
        enqueued_sqs = False
        worker_lock: dict[str, Any] = {"claimed": False, "batch_submitted": False}

        if uploaded_chunks >= expected_chunks and upload_status == "UPLOADING":
            try:
                self.ingest_table.update_item(
                    Key={"pk": resolved_session_pk, "sk": resolved_rec_sk},
                    UpdateExpression=(
                        "SET upload_status = :ready, process_status = :queued, "
                        "upload_eta_s = :zero, updated_at = :now REMOVE GSI1PK, GSI1SK"
                    ),
                    ConditionExpression="upload_status = :uploading AND uploaded_chunks >= expected_chunks",
                    ExpressionAttributeValues={
                        ":ready": "READY",
                        ":queued": "QUEUED",
                        ":zero": Decimal("0"),
                        ":now": resolved_iso,
                        ":uploading": "UPLOADING",
                    },
                )
                transitioned_to_ready = True
                upload_status = "READY"
                self.ingest_table.update_item(
                    Key={"pk": resolved_session_pk, "sk": "META"},
                    UpdateExpression="ADD completed_recordings :one SET updated_at = :now",
                    ExpressionAttributeValues={":one": 1, ":now": resolved_iso},
                )
                enqueued_sqs = self._enqueue_ready_recording(
                    {
                        "upload_id": upload_id,
                        "recording_id": recording_id,
                        "site_id": str(rec_item.get("site_id", "open_pit_mine_01")),
                    }
                )
                if enqueued_sqs:
                    if self.auto_start_ec2 and self.worker_instance_id:
                        worker_lock = self.try_start_ec2_worker(recording_id)
                    else:
                        worker_lock = self.try_claim_worker_lock_and_submit_batch(recording_id)
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                    raise

        return {
            "upload_id": upload_id,
            "recording_id": recording_id,
            "chunk_index": chunk_index,
            "s3_bucket": self.bucket_name,
            "s3_key": s3_key,
            "chunk_upload_status": "UPLOADED",
            "upload_status": upload_status,
            "uploaded_chunks": uploaded_chunks,
            "expected_chunks": expected_chunks,
            "idempotent_replay": not chunk_newly_completed,
            "transitioned_to_ready": transitioned_to_ready,
            "enqueued_sqs": enqueued_sqs,
            "worker_lock": worker_lock,
        }

    def _query_min_active_upload_eta(self, now_epoch: int) -> int | None:
        """Query sparse GSI_ACTIVE_UPLOADS strictly without Limit=1 to avoid stale-heartbeat masking."""
        fresh_cutoff = now_epoch - FRESH_HEARTBEAT_MAX_AGE_S
        query_kwargs: dict[str, Any] = {
            "IndexName": "GSI_ACTIVE_UPLOADS",
            "KeyConditionExpression": Key("GSI1PK").eq("ACTIVE_UPLOAD"),
            "ScanIndexForward": True,
            "FilterExpression": Attr("last_heartbeat_at").gte(fresh_cutoff),
        }
        while True:
            resp = self.ingest_table.query(**query_kwargs)
            items = resp.get("Items", [])
            if items:
                return int(items[0]["upload_eta_s"])
            lek = resp.get("LastEvaluatedKey")
            if not lek:
                return None
            query_kwargs["ExclusiveStartKey"] = lek

    def get_session_status(self, upload_id: str) -> dict[str, Any]:
        """Return single-partition session progress, worker lock status, and global min(ETA)."""
        clean_upload_id = _require_id(upload_id, "upload_id")
        session_pk = f"SESSION#{clean_upload_id}"
        items = self.ingest_table.query(KeyConditionExpression=Key("pk").eq(session_pk)).get(
            "Items", []
        )
        if not items:
            raise UploadNotFoundError(f"Upload session {clean_upload_id} not found")

        meta: dict[str, Any] | None = None
        rec_items: list[dict[str, Any]] = []
        for item in items:
            if item.get("sk") == "META":
                meta = item
            elif str(item.get("sk", "")).startswith("REC#"):
                rec_items.append(item)

        if meta is None:
            raise UploadNotFoundError(f"Upload session {clean_upload_id} metadata not found")

        worker_item = self.ingest_table.get_item(
            Key={"pk": "WORKER#LOCK", "sk": "PRIMARY"}
        ).get("Item")
        worker_status = str(worker_item.get("status", "IDLE")) if worker_item else "IDLE"
        worker_instance_id = (
            str(worker_item.get("worker_instance_id", "")) if worker_item else ""
        ) or self.worker_instance_id
        candidate_ids = _parse_worker_instance_ids(
            ([worker_instance_id] if worker_instance_id else []) + list(self.worker_instance_ids)
        )
        worker_instance_state: str | None = None
        if candidate_ids and (self._ec2 is not None or self.auto_start_ec2):
            try:
                ec2 = self._ec2 or get_ec2_client()
                state_by_id = _describe_worker_states(ec2, candidate_ids)
                active_id = next(
                    (iid for iid in candidate_ids if state_by_id.get(iid) in {"running", "pending"}),
                    None,
                )
                if active_id is not None:
                    worker_instance_id = active_id
                    worker_instance_state = state_by_id[active_id]
                elif worker_instance_id and worker_instance_id in state_by_id:
                    worker_instance_state = state_by_id[worker_instance_id]
                else:
                    first_known = next((iid for iid in candidate_ids if iid in state_by_id), None)
                    if first_known is not None:
                        worker_instance_id = first_known
                        worker_instance_state = state_by_id[first_known]
            except Exception:  # noqa: BLE001
                worker_instance_state = "unknown"

        now_epoch = int(self._now())
        min_eta_s = self._query_min_active_upload_eta(now_epoch)

        rec_items.sort(key=lambda r: str(r.get("recording_id", "")))
        recordings_out = [
            json_safe(
                {
                    "recording_id": r["recording_id"],
                    "filename": r.get("filename"),
                    "camera_id": r.get("camera_id"),
                    "location_tag": r.get("location_tag"),
                    "tags": r.get("tags", []),
                    "upload_status": r.get("upload_status"),
                    "uploaded_chunks": r.get("uploaded_chunks", 0),
                    "expected_chunks": r.get("expected_chunks", 0),
                    "uploaded_bytes": r.get("uploaded_bytes", 0),
                    "total_bytes": r.get("total_bytes", 0),
                    "upload_eta_s": r.get("upload_eta_s"),
                    "process_status": r.get("process_status"),
                    "process_stage": r.get("process_stage"),
                    "processed_chunks": r.get("processed_chunks", 0),
                    "active_chunk_index": r.get("active_chunk_index", 0),
                    "active_chunk_frame_progress_pct": r.get("active_chunk_frame_progress_pct", 0.0),
                    "cycles_extracted_so_far": r.get("cycles_extracted_so_far", 0),
                    "segments_extracted_so_far": r.get("segments_extracted_so_far", 0),
                    "last_error": r.get("last_error"),
                    "updated_at": r.get("updated_at"),
                }
            )
            for r in rec_items
        ]

        derived_status = _derive_session_status(
            rec_items, fallback=str(meta.get("session_status") or "UPLOADING")
        )
        return json_safe(
            {
                "upload_id": clean_upload_id,
                "site_id": meta.get("site_id"),
                "session_status": derived_status,
                "worker_status": worker_status,
                "worker_instance_id": worker_instance_id or None,
                "worker_instance_state": worker_instance_state,
                "min_remaining_upload_eta_s": min_eta_s,
                "total_recordings": meta.get("total_recordings", len(recordings_out)),
                "completed_recordings": meta.get("completed_recordings", 0),
                "recordings": recordings_out,
            }
        )

    def abort_recording(self, recording_id: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        """Abort active S3 multipart uploads for a recording and evict it from GSI_ACTIVE_UPLOADS."""
        clean_rec_id = _require_id(recording_id, "recording_id")
        if body is not None and not isinstance(body, dict):
            raise UploadValidationError("Request body must be a JSON object")

        chunk_items = self.ingest_table.query(
            KeyConditionExpression=Key("pk").eq(f"REC#{clean_rec_id}")
        ).get("Items", [])
        if not chunk_items:
            raise UploadNotFoundError(f"Recording {clean_rec_id} not found in ingest state")

        resolved_upload_id = (
            _require_id(body["upload_id"], "upload_id")
            if body and body.get("upload_id") is not None
            else str(chunk_items[0].get("upload_id", ""))
        )
        if not resolved_upload_id:
            raise UploadValidationError("Unable to resolve upload_id for recording")

        now_iso = _utc_iso(int(self._now()))
        aborted_chunks: list[int] = []
        for chunk in chunk_items:
            if not str(chunk.get("sk", "")).startswith("CHUNK#"):
                continue
            s3_upload_id = chunk.get("s3_upload_id")
            if s3_upload_id and chunk.get("upload_status") != "UPLOADED":
                try:
                    self._s3.abort_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=str(chunk["s3_key"]),
                        UploadId=str(s3_upload_id),
                    )
                except ClientError as exc:
                    if exc.response.get("Error", {}).get("Code") != "NoSuchUpload":
                        raise
            self.ingest_table.update_item(
                Key={"pk": chunk["pk"], "sk": chunk["sk"]},
                UpdateExpression="SET upload_status = :aborted, process_status = :aborted, updated_at = :now",
                ExpressionAttributeValues={":aborted": "ABORTED", ":now": now_iso},
            )
            aborted_chunks.append(int(chunk["chunk_index"]))

        self.ingest_table.update_item(
            Key={"pk": f"SESSION#{resolved_upload_id}", "sk": f"REC#{clean_rec_id}"},
            UpdateExpression=(
                "SET upload_status = :aborted, process_status = :aborted, updated_at = :now "
                "REMOVE GSI1PK, GSI1SK"
            ),
            ConditionExpression="attribute_exists(pk)",
            ExpressionAttributeValues={":aborted": "ABORTED", ":now": now_iso},
        )

        aborted_chunks.sort()
        return {
            "upload_id": resolved_upload_id,
            "recording_id": clean_rec_id,
            "upload_status": "ABORTED",
            "aborted_chunks": aborted_chunks,
        }
