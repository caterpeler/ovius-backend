"""Background EventBridge orchestrator for S3 upload fallback and stale upload sweeps."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
import re
from typing import Any
import urllib.parse

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from ovius_api.uploads import UploadControlService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_STALE_AFTER_S = 900
_CHUNK_KEY_RE = re.compile(r"^uploads/([^/]+)/([^/]+)/([^/]+)/chunk_(\d+)\.mp4$")


def _utc_iso(epoch_s: int | float) -> str:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).isoformat()


def sweep_stale_uploads(
    service: UploadControlService,
    *,
    stale_after_s: int = DEFAULT_STALE_AFTER_S,
) -> dict[str, Any]:
    """Query sparse GSI_ACTIVE_UPLOADS (without Limit=1) and mark stalled uploads."""
    now_epoch = int(service._now())
    cutoff = now_epoch - stale_after_s
    now_iso = _utc_iso(now_epoch)

    stale_items: list[dict[str, Any]] = []
    query_kwargs: dict[str, Any] = {
        "IndexName": "GSI_ACTIVE_UPLOADS",
        "KeyConditionExpression": Key("GSI1PK").eq("ACTIVE_UPLOAD"),
        "ScanIndexForward": True,
        "FilterExpression": Attr("last_heartbeat_at").lt(cutoff),
    }
    while True:
        resp = service.ingest_table.query(**query_kwargs)
        stale_items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        query_kwargs["ExclusiveStartKey"] = lek

    stalled_ids: list[str] = []
    for item in stale_items:
        rec_id = str(item.get("recording_id", ""))
        try:
            service.ingest_table.update_item(
                Key={"pk": item["pk"], "sk": item["sk"]},
                UpdateExpression="SET upload_status = :stalled, updated_at = :now REMOVE GSI1PK, GSI1SK",
                ConditionExpression="upload_status = :uploading AND last_heartbeat_at < :cutoff",
                ExpressionAttributeValues={
                    ":stalled": "STALLED",
                    ":uploading": "UPLOADING",
                    ":cutoff": cutoff,
                    ":now": now_iso,
                },
            )
            stalled_ids.append(rec_id)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise

    return {
        "mode": "stale_watchdog",
        "cutoff_epoch": cutoff,
        "scanned_stale_count": len(stale_items),
        "stalled_count": len(stalled_ids),
        "stalled_recording_ids": stalled_ids,
        "ec2_watchdog": retry_queued_ec2_worker(service),
    }


def retry_queued_ec2_worker(service: UploadControlService) -> dict[str, Any]:
    """Retry waking the persistent EC2 GPU worker if SQS still holds queued READY recordings."""
    if not (service.auto_start_ec2 and service.worker_instance_id):
        return {"checked": False, "reason": "auto_start_ec2_disabled"}
    queue_url = service._resolve_ready_queue_url()
    if not queue_url:
        return {"checked": False, "reason": "queue_not_found"}

    attrs = service._sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    ).get("Attributes", {})
    queued = int(attrs.get("ApproximateNumberOfMessages", "0"))
    in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", "0"))
    if queued <= 0:
        return {
            "checked": True,
            "queued_messages": queued,
            "in_flight_messages": in_flight,
            "wake_attempted": False,
        }
    wake = service.try_start_ec2_worker("watchdog-retry")
    return {
        "checked": True,
        "queued_messages": queued,
        "in_flight_messages": in_flight,
        "wake_attempted": True,
        "wake_result": wake,
    }


def handle_s3_object_created(
    event: dict[str, Any],
    service: UploadControlService,
) -> dict[str, Any]:
    """Idempotently finalize chunk state when an S3 Object Created event arrives under uploads/."""
    detail = event.get("detail") or {}
    bucket_name = detail.get("bucket", {}).get("name")
    if bucket_name and bucket_name != service.bucket_name:
        return {
            "mode": "s3_object_created",
            "status": "ignored",
            "reason": f"bucket mismatch: {bucket_name}",
        }

    raw_key = str(detail.get("object", {}).get("key") or "")
    s3_key = urllib.parse.unquote_plus(raw_key)
    match = _CHUNK_KEY_RE.match(s3_key)
    if not match:
        return {
            "mode": "s3_object_created",
            "status": "ignored",
            "reason": f"non-chunk key: {s3_key}",
        }

    _, upload_id, recording_id, chunk_idx_str = match.groups()
    chunk_index = int(chunk_idx_str)
    chunk_pk = f"REC#{recording_id}"
    chunk_sk = f"CHUNK#{chunk_index:04d}"

    chunk_item = service.ingest_table.get_item(Key={"pk": chunk_pk, "sk": chunk_sk}).get("Item")
    if not chunk_item:
        return {
            "mode": "s3_object_created",
            "status": "ignored",
            "reason": f"unregistered chunk {chunk_pk}/{chunk_sk}",
        }
    if chunk_item.get("upload_id") != upload_id:
        return {
            "mode": "s3_object_created",
            "status": "ignored",
            "reason": "upload_id mismatch",
        }
    if chunk_item.get("upload_status") == "ABORTED":
        return {
            "mode": "s3_object_created",
            "status": "ignored",
            "reason": "chunk aborted",
        }

    result = service.transition_chunk_uploaded(
        upload_id=upload_id,
        recording_id=recording_id,
        chunk_index=chunk_index,
        s3_key=s3_key,
        chunk_pk=chunk_pk,
        chunk_sk=chunk_sk,
        session_pk=f"SESSION#{upload_id}",
        rec_sk=f"REC#{recording_id}",
    )
    return {"mode": "s3_object_created", "status": "processed", **result}


def lambda_handler(
    event: dict[str, Any],
    context: Any = None,
    *,
    service: UploadControlService | None = None,
) -> dict[str, Any]:
    """Dispatch EventBridge S3 Object Created fallback events and 5-minute watchdog sweeps."""
    svc = service or UploadControlService()
    source = str(event.get("source") or "")
    detail_type = str(event.get("detail-type") or "")

    if source == "aws.s3" or detail_type == "Object Created":
        return handle_s3_object_created(event, svc)

    if source == "aws.events" or detail_type == "Scheduled Event" or event.get("action") == "stale_watchdog":
        default_stale = int(os.environ.get("STALE_UPLOAD_TIMEOUT_S", str(DEFAULT_STALE_AFTER_S)))
        raw_stale = event.get("stale_after_s", default_stale)
        stale_after_s = max(0, int(raw_stale))
        return sweep_stale_uploads(svc, stale_after_s=stale_after_s)

    return {"mode": "unknown", "status": "ignored", "source": source, "detail_type": detail_type}
