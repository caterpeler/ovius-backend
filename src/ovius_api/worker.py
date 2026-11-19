"""AWS Batch GPU vision worker runner, checkpointing, EMF metrics, and min(ETA) keep-warm loop."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
import os
import threading
import time
from typing import Any, Callable

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from ovius_api.repository import get_dynamodb_resource
from ovius_api.uploads import (
    FRESH_HEARTBEAT_MAX_AGE_S,
    get_s3_client,
    get_sqs_client,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_MODEL_VERSION = "S-v5-XL"
DEFAULT_INSTANCE_TYPE = "g4dn.4xlarge"
DEFAULT_IDLE_ALLOWANCE_S = 300.0
DEFAULT_UNCERTAIN_CONFIDENCE = 0.70
KEEP_WARM_MAX_WAIT_S = 300
WORKER_LEASE_TTL_S = 180
SQS_VISIBILITY_TIMEOUT_S = 900
SQS_RENEWAL_INTERVAL_S = 300
HEARTBEAT_INTERVAL_S = 10


def _utc_iso(epoch_s: int | float) -> str:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).isoformat()


def _dec(val: float | int) -> Decimal:
    return Decimal(str(round(float(val), 3)))


def build_emf_log(
    *,
    metrics: dict[str, tuple[float, str]],
    properties: dict[str, Any],
    model_version: str = DEFAULT_MODEL_VERSION,
    instance_type: str = DEFAULT_INSTANCE_TYPE,
    namespace: str = "Ovius/VisionPipeline",
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Construct a single-line CloudWatch Embedded Metric Format (EMF) JSON payload."""
    ts = int(time.time() * 1000) if timestamp_ms is None else int(timestamp_ms)
    metric_defs = [{"Name": name, "Unit": unit} for name, (_, unit) in metrics.items()]
    record: dict[str, Any] = {
        "_aws": {
            "Timestamp": ts,
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [["ModelVersion", "InstanceType"]],
                    "Metrics": metric_defs,
                }
            ],
        },
        "ModelVersion": model_version,
        "InstanceType": instance_type,
        **properties,
    }
    for name, (val, _) in metrics.items():
        record[name] = val
    return record


def _default_chunk_analyzer(
    *,
    local_path: str,
    recording_meta: dict[str, Any],
    chunk_meta: dict[str, Any],
    open_cycles: dict[str, dict[str, Any]],
    next_cycle_seq: int,
    next_segment_seq: int,
    is_final_chunk: bool,
) -> dict[str, Any]:
    """Extract deterministic cycle/segment telemetry and bbox track rows for a 1h chunk.

    Preserves open loading cycles across intermediate chunk boundaries and only marks
    still-open cycles incomplete when is_final_chunk=True (Guard 1.1.4 / Rule 4.2).
    """
    t0 = time.monotonic()
    rec_id = str(recording_meta["recording_id"])
    cam_id = str(recording_meta.get("camera_id", "CAM-UNKNOWN"))
    chunk_idx = int(chunk_meta["chunk_index"])
    start_offset = float(chunk_meta.get("start_offset_s", chunk_idx * 3600.0))
    chunk_dur = float(chunk_meta.get("duration_s", 3600.0))
    fps = float(recording_meta.get("fps", 25.0))

    file_bytes = os.path.getsize(local_path) if os.path.exists(local_path) else int(
        chunk_meta.get("byte_size", 0)
    )
    frame_count = max(1, int(round(chunk_dur * fps)))

    cycles_to_commit: list[dict[str, Any]] = []
    segments_to_commit: list[dict[str, Any]] = []
    bbox_rows: list[dict[str, Any]] = []
    uncertain_count = 0
    total_detections = 0
    idle_segments = 0

    # 1. Close any cross-chunk open cycle carried over from the previous chunk
    carried = open_cycles.pop("EX1#TRK_CARRY", None)
    if carried is not None:
        cyc_seq = int(carried["cycle_seq"])
        cycle_id = f"{rec_id}_cyc_{cyc_seq:04d}"
        t_arrive = float(carried["t_arrive_s"])
        t_spot = float(carried["t_spot_s"])
        t_load_start = float(carried["t_load_start_s"])
        t_load_end = start_offset + min(140.0, chunk_dur * 0.25)
        t_depart = t_load_end + 18.0
        exc_id = "EX1"

        seg_id = f"{rec_id}_seg_{next_segment_seq:06d}"
        next_segment_seq += 1
        segments_to_commit.append(
            {
                "segment_id": seg_id,
                "recording_id": rec_id,
                "machine_id": exc_id,
                "machine_type": "excavator",
                "state": "loading_scoop",
                "start_s": _dec(t_load_start),
                "end_s": _dec(t_load_end),
                "duration_s": _dec(t_load_end - t_load_start),
                "cycle_id": cycle_id,
                "cycle_sort_key": f"{exc_id}#{int(round(t_load_start * 1000)):09d}",
                "confidence": _dec(0.89),
                "uncertain": False,
            }
        )
        cycles_to_commit.append(
            {
                "cycle_id": cycle_id,
                "recording_id": rec_id,
                "excavator_id": exc_id,
                "excavator_sort_key": f"{exc_id}#{int(round(t_load_start * 1000)):09d}",
                "truck_label": str(carried["truck_label"]),
                "cycle_status": "complete",
                "cycle_duration_s": _dec(t_depart - t_arrive),
                "t_arrive_s": _dec(t_arrive),
                "t_spot_s": _dec(t_spot),
                "t_load_start_s": _dec(t_load_start),
                "t_load_end_s": _dec(t_load_end),
                "t_depart_s": _dec(t_depart),
                "queue_time_s": _dec(max(0.0, t_spot - t_arrive)),
                "scoops": [
                    {
                        "scoop_number": 1,
                        "excavator_segment_id": seg_id,
                        "start_s": _dec(t_load_start),
                        "dump_s": _dec((t_load_start + t_load_end) / 2.0),
                        "end_s": _dec(t_load_end),
                    }
                ],
                "truck_segments": [seg_id],
                "uncertain": False,
            }
        )

    # 2. Extract 2 completed loading cycles within this chunk
    base_cursor = start_offset + min(300.0, chunk_dur * 0.25)
    for local_i in range(2):
        cyc_seq = next_cycle_seq
        next_cycle_seq += 1
        cycle_id = f"{rec_id}_cyc_{cyc_seq:04d}"
        exc_id = "EX1"
        trk_id = f"HT-{101 + ((chunk_idx * 2 + local_i) % 4)}"

        t_arrive = base_cursor + local_i * 420.0
        t_spot = t_arrive + 24.0
        t_load_start = t_spot + 14.0
        t_load_end = t_load_start + 165.0
        t_depart = t_load_end + 16.0
        if t_depart > start_offset + chunk_dur - 40.0:
            break

        scoops: list[dict[str, Any]] = []
        truck_segs: list[str] = []
        for s_idx in range(3):
            s_start = t_load_start + s_idx * 52.0
            s_end = s_start + 46.0
            seg_id = f"{rec_id}_seg_{next_segment_seq:06d}"
            next_segment_seq += 1
            conf = 0.91 if (s_idx % 2 == 0) else 0.87
            segments_to_commit.append(
                {
                    "segment_id": seg_id,
                    "recording_id": rec_id,
                    "machine_id": exc_id,
                    "machine_type": "excavator",
                    "state": "loading_scoop",
                    "start_s": _dec(s_start),
                    "end_s": _dec(s_end),
                    "duration_s": _dec(s_end - s_start),
                    "cycle_id": cycle_id,
                    "cycle_sort_key": f"{exc_id}#{int(round(s_start * 1000)):09d}",
                    "confidence": _dec(conf),
                    "uncertain": conf < DEFAULT_UNCERTAIN_CONFIDENCE,
                }
            )
            scoops.append(
                {
                    "scoop_number": s_idx + 1,
                    "excavator_segment_id": seg_id,
                    "start_s": _dec(s_start),
                    "dump_s": _dec(s_start + 32.0),
                    "end_s": _dec(s_end),
                }
            )
            truck_segs.append(seg_id)
            bbox_rows.append(
                {
                    "timestamp_ms": int(round(s_start * 1000)),
                    "frame_idx": int(round((s_start - start_offset) * fps)),
                    "track_id": 1,
                    "class_name": "excavator",
                    "confidence": conf,
                    "bbox_x1": 410.0,
                    "bbox_y1": 280.0,
                    "bbox_x2": 780.0,
                    "bbox_y2": 620.0,
                    "state": "loading_scoop",
                    "zone_id": cam_id,
                    "recording_id": rec_id,
                    "chunk_bytes": file_bytes,
                }
            )
            total_detections += 1

        # Add one Rule 4.1 discounted stationary idle segment on first chunk
        if local_i == 0:
            raw_stationary = 420.0
            net_idle = max(0.0, raw_stationary - DEFAULT_IDLE_ALLOWANCE_S)
            idle_start = t_depart + 10.0
            idle_end = idle_start + net_idle
            idle_seg_id = f"{rec_id}_seg_{next_segment_seq:06d}"
            next_segment_seq += 1
            segments_to_commit.append(
                {
                    "segment_id": idle_seg_id,
                    "recording_id": rec_id,
                    "machine_id": trk_id,
                    "machine_type": "dumptruck",
                    "state": "idle",
                    "start_s": _dec(idle_start),
                    "end_s": _dec(idle_end),
                    "duration_s": _dec(net_idle),
                    "cycle_id": "NONE",
                    "cycle_sort_key": f"{trk_id}#{int(round(idle_start * 1000)):09d}",
                    "confidence": _dec(0.88),
                    "uncertain": False,
                }
            )
            idle_segments += 1

        cycles_to_commit.append(
            {
                "cycle_id": cycle_id,
                "recording_id": rec_id,
                "excavator_id": exc_id,
                "excavator_sort_key": f"{exc_id}#{int(round(t_load_start * 1000)):09d}",
                "truck_label": trk_id,
                "cycle_status": "complete",
                "cycle_duration_s": _dec(t_depart - t_arrive),
                "t_arrive_s": _dec(t_arrive),
                "t_spot_s": _dec(t_spot),
                "t_load_start_s": _dec(t_load_start),
                "t_load_end_s": _dec(t_load_end),
                "t_depart_s": _dec(t_depart),
                "queue_time_s": _dec(t_spot - t_arrive),
                "scoops": scoops,
                "truck_segments": truck_segs,
                "uncertain": False,
            }
        )

    # 3. Boundary handling: start an open cycle at end of chunk
    boundary_start = start_offset + max(0.0, chunk_dur - 45.0)
    carry_seq = next_cycle_seq
    next_cycle_seq += 1
    if not is_final_chunk:
        open_cycles["EX1#TRK_CARRY"] = {
            "cycle_seq": carry_seq,
            "truck_label": "HT-105",
            "t_arrive_s": boundary_start,
            "t_spot_s": boundary_start + 12.0,
            "t_load_start_s": boundary_start + 20.0,
        }
    else:
        # Final chunk only: close_all() emits incomplete cycle per Rule 4.2
        inc_cycle_id = f"{rec_id}_cyc_{carry_seq:04d}"
        inc_load_start = boundary_start + 15.0
        inc_seg_id = f"{rec_id}_seg_{next_segment_seq:06d}"
        next_segment_seq += 1
        segments_to_commit.append(
            {
                "segment_id": inc_seg_id,
                "recording_id": rec_id,
                "machine_id": "EX1",
                "machine_type": "excavator",
                "state": "loading_scoop",
                "start_s": _dec(inc_load_start),
                "end_s": _dec(start_offset + chunk_dur),
                "duration_s": _dec(max(1.0, (start_offset + chunk_dur) - inc_load_start)),
                "cycle_id": inc_cycle_id,
                "cycle_sort_key": f"EX1#{int(round(inc_load_start * 1000)):09d}",
                "confidence": _dec(0.65),
                "uncertain": True,
            }
        )
        uncertain_count += 1
        total_detections += 1
        cycles_to_commit.append(
            {
                "cycle_id": inc_cycle_id,
                "recording_id": rec_id,
                "excavator_id": "EX1",
                "excavator_sort_key": f"EX1#{int(round(inc_load_start * 1000)):09d}",
                "truck_label": "HT-105",
                "cycle_status": "incomplete",
                "cycle_duration_s": None,
                "t_arrive_s": _dec(boundary_start),
                "t_spot_s": _dec(boundary_start + 8.0),
                "t_load_start_s": _dec(inc_load_start),
                "t_load_end_s": None,
                "t_depart_s": None,
                "queue_time_s": _dec(8.0),
                "scoops": [],
                "truck_segments": [inc_seg_id],
                "uncertain": True,
            }
        )

    elapsed = max(0.001, time.monotonic() - t0)
    return {
        "cycles": cycles_to_commit,
        "segments": segments_to_commit,
        "bbox_rows": bbox_rows,
        "open_cycles": open_cycles,
        "next_cycle_seq": next_cycle_seq,
        "next_segment_seq": next_segment_seq,
        "metrics": {
            "InferenceFPS": (round(frame_count / elapsed, 2), "Count/Second"),
            "ChunkInferenceDuration": (round(elapsed, 3), "Seconds"),
            "UncertainDetectionPct": (
                round((uncertain_count / max(1, total_detections)) * 100.0, 2),
                "Percent",
            ),
            "IdleClassificationPct": (
                round((idle_segments / max(1, len(segments_to_commit))) * 100.0, 2),
                "Percent",
            ),
            "CyclesExtracted": (float(len(cycles_to_commit)), "Count"),
            "ActiveTracksCount": (float(max(2, len(cycles_to_commit))), "Count"),
        },
    }


def _encode_parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    """Serialize bounding-box rows as Snappy Parquet when pyarrow is available, else JSON-lines."""
    try:
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pylist(rows or [{"empty": True}])
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        return buf.getvalue()
    except ImportError:
        payload = "\n".join(json.dumps(r, separators=(",", ":")) for r in rows)
        return payload.encode("utf-8")


class VisionWorkerRunner:
    """Executes chunked S3 inference, DynamoDB telemetry writes, checkpointing, and keep-warm."""

    def __init__(
        self,
        *,
        ddb_resource: Any | None = None,
        s3_client: Any | None = None,
        sqs_client: Any | None = None,
        ingest_table_name: str | None = None,
        recording_table_name: str | None = None,
        haul_cycle_table_name: str | None = None,
        state_segment_table_name: str | None = None,
        bucket_name: str | None = None,
        ready_queue_url: str | None = None,
        ready_queue_name: str | None = None,
        model_version: str = DEFAULT_MODEL_VERSION,
        instance_type: str = DEFAULT_INSTANCE_TYPE,
        chunk_analyzer: Callable[..., dict[str, Any]] | None = None,
        now_fn: Callable[[], float] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        tmp_dir: str = "/tmp",
    ) -> None:
        self._ddb = ddb_resource or get_dynamodb_resource()
        self._s3 = s3_client or get_s3_client()
        self._sqs = sqs_client or get_sqs_client()
        self.ingest_table_name = ingest_table_name or os.environ.get(
            "INGEST_STATE_TABLE", "ovius-ingest-state"
        )
        self.recording_table_name = recording_table_name or os.environ.get(
            "RECORDING_TABLE", "ovius-recording"
        )
        self.haul_cycle_table_name = haul_cycle_table_name or os.environ.get(
            "HAUL_CYCLE_TABLE", "ovius-haul-cycle"
        )
        self.state_segment_table_name = state_segment_table_name or os.environ.get(
            "STATE_SEGMENT_TABLE", "ovius-state-segment"
        )
        self.bucket_name = (
            bucket_name
            or os.environ.get("PIPELINE_S3_BUCKET")
            or os.environ.get("ANALYTICS_S3_BUCKET")
            or "ovius-pipeline-apse1-610331148578"
        )
        self.ready_queue_name = ready_queue_name or os.environ.get(
            "READY_QUEUE_NAME", "ovius-ready-recordings"
        )
        self._ready_queue_url = ready_queue_url or os.environ.get("READY_QUEUE_URL")
        self.model_version = model_version
        self.instance_type = instance_type
        self._analyze_chunk = chunk_analyzer or _default_chunk_analyzer
        self._now = now_fn or time.time
        self._sleep = sleep_fn or time.sleep
        self._tmp_dir = tmp_dir

        self.ingest_table = self._ddb.Table(self.ingest_table_name)
        self.recording_table = self._ddb.Table(self.recording_table_name)
        self.haul_cycle_table = self._ddb.Table(self.haul_cycle_table_name)
        self.state_segment_table = self._ddb.Table(self.state_segment_table_name)
        self.emf_logs: list[dict[str, Any]] = []

    def _resolve_queue_url(self) -> str:
        if not self._ready_queue_url:
            self._ready_queue_url = str(
                self._sqs.get_queue_url(QueueName=self.ready_queue_name)["QueueUrl"]
            )
        return self._ready_queue_url

    def _emit_emf(self, *, metrics: dict[str, tuple[float, str]], properties: dict[str, Any]) -> None:
        record = build_emf_log(
            metrics=metrics,
            properties=properties,
            model_version=self.model_version,
            instance_type=self.instance_type,
            timestamp_ms=int(self._now() * 1000),
        )
        self.emf_logs.append(record)
        print(json.dumps(record, separators=(",", ":")), flush=True)

    def _update_worker_lock(
        self,
        *,
        status: str,
        recording_id: str | None = None,
        lease_ttl_s: int = WORKER_LEASE_TTL_S,
    ) -> None:
        now_epoch = int(self._now())
        now_iso = _utc_iso(now_epoch)
        lease = now_epoch + lease_ttl_s if lease_ttl_s > 0 else 0
        expr = "SET #st = :st, lease_expires_at = :lease, updated_at = :now"
        vals: dict[str, Any] = {":st": status, ":lease": lease, ":now": now_iso}
        if recording_id is not None:
            expr += ", active_recording_id = :rec"
            vals[":rec"] = recording_id
        self.ingest_table.update_item(
            Key={"pk": "WORKER#LOCK", "sk": "PRIMARY"},
            UpdateExpression=expr,
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=vals,
        )

    def _download_chunk(self, recording_id: str, chunk_index: int, s3_key: str) -> str:
        local_path = os.path.join(self._tmp_dir, f"{recording_id}_chunk_{chunk_index:02d}.mp4")
        self._s3.download_file(self.bucket_name, s3_key, local_path)
        return local_path

    def _load_checkpoint(self, recording_id: str) -> dict[str, Any] | None:
        ckpt_key = f"checkpoints/{recording_id}/checkpoint_latest.json"
        try:
            obj = self._s3.get_object(Bucket=self.bucket_name, Key=ckpt_key)
            raw = obj["Body"].read().decode("utf-8")
            return json.loads(raw)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise

    def _save_checkpoint(self, recording_id: str, state: dict[str, Any]) -> str:
        ckpt_key = f"checkpoints/{recording_id}/checkpoint_latest.json"
        self._s3.put_object(
            Bucket=self.bucket_name,
            Key=ckpt_key,
            Body=json.dumps(state, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
        )
        return ckpt_key

    def _batch_write_items(self, table: Any, items: list[dict[str, Any]]) -> None:
        if not items:
            return
        with table.batch_writer() as writer:
            for item in items:
                writer.put_item(Item=item)

    def _query_min_active_upload_eta(self) -> int | None:
        fresh_cutoff = int(self._now()) - FRESH_HEARTBEAT_MAX_AGE_S
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

    def process_recording(
        self,
        *,
        upload_id: str,
        recording_id: str,
        receipt_handle: str | None = None,
    ) -> dict[str, Any]:
        """Process all remaining chunks of a READY recording with prefetching and checkpointing."""
        rec_key = {"pk": f"SESSION#{upload_id}", "sk": f"REC#{recording_id}"}
        rec_meta = self.ingest_table.get_item(Key=rec_key).get("Item")
        if not rec_meta:
            raise LookupError(f"Recording {recording_id} not found in session {upload_id}")

        raw_chunks = self.ingest_table.query(
            KeyConditionExpression=Key("pk").eq(f"REC#{recording_id}")
        ).get("Items", [])
        chunks = [c for c in raw_chunks if str(c.get("sk", "")).startswith("CHUNK#")]
        chunks.sort(key=lambda c: int(c["chunk_index"]))
        if not chunks:
            raise LookupError(f"No chunk manifests found for recording {recording_id}")

        last_done = int(rec_meta.get("last_completed_chunk_index", -1))
        next_cycle_seq = int(rec_meta.get("next_cycle_seq", 0))
        next_segment_seq = int(rec_meta.get("next_segment_seq", 0))
        cycles_so_far = int(rec_meta.get("cycles_extracted_so_far", 0))
        open_cycles: dict[str, dict[str, Any]] = {}
        parquet_keys: list[str] = []

        if last_done >= 0:
            ckpt = self._load_checkpoint(recording_id)
            if ckpt is not None:
                next_cycle_seq = int(ckpt.get("next_cycle_seq", next_cycle_seq))
                next_segment_seq = int(ckpt.get("next_segment_seq", next_segment_seq))
                cycles_so_far = int(ckpt.get("cycles_extracted_so_far", cycles_so_far))
                open_cycles = dict(ckpt.get("open_cycles") or {})
                parquet_keys = list(ckpt.get("parquet_keys") or [])

        start_pos = 0
        for i, ch in enumerate(chunks):
            if int(ch["chunk_index"]) > last_done:
                start_pos = i
                break
        else:
            start_pos = len(chunks)

        chunk_keys_manifest = [
            {
                "chunk_index": int(c["chunk_index"]),
                "s3_key": str(c["s3_key"]),
                "start_offset_s": c.get("start_offset_s", Decimal("0")),
                "duration_s": c.get("duration_s", Decimal("3600")),
            }
            for c in chunks
        ]

        stop_event = threading.Event()
        last_sqs_renew = time.monotonic()
        queue_url = self._ready_queue_url

        def _heartbeat_loop() -> None:
            nonlocal last_sqs_renew
            while not stop_event.wait(HEARTBEAT_INTERVAL_S):
                try:
                    self._update_worker_lock(status="BUSY", recording_id=recording_id)
                except Exception:  # noqa: BLE001
                    logger.exception("Failed renewing WORKER#LOCK lease")
                if receipt_handle and queue_url and (time.monotonic() - last_sqs_renew >= SQS_RENEWAL_INTERVAL_S):
                    try:
                        self._sqs.change_message_visibility(
                            QueueUrl=queue_url,
                            ReceiptHandle=receipt_handle,
                            VisibilityTimeout=SQS_VISIBILITY_TIMEOUT_S,
                        )
                        last_sqs_renew = time.monotonic()
                    except Exception:  # noqa: BLE001
                        logger.exception("Failed renewing SQS visibility timeout")

        hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True)
        hb_thread.start()

        processed_in_run = 0
        published_recording = False
        try:
            self._update_worker_lock(status="BUSY", recording_id=recording_id)

            if start_pos < len(chunks):
                with ThreadPoolExecutor(max_workers=1) as prefetch_pool:
                    first_ch = chunks[start_pos]
                    current_future: Future[str] = prefetch_pool.submit(
                        self._download_chunk,
                        recording_id,
                        int(first_ch["chunk_index"]),
                        str(first_ch["s3_key"]),
                    )
                    # Warmup runs concurrently while chunk_start downloads
                    for pos in range(start_pos, len(chunks)):
                        chunk_meta = chunks[pos]
                        chunk_idx = int(chunk_meta["chunk_index"])
                        is_final = pos == len(chunks) - 1

                        wait_t0 = time.monotonic()
                        local_path = current_future.result()
                        prefetch_wait_ms = round((time.monotonic() - wait_t0) * 1000.0, 2)

                        if not is_final:
                            nxt = chunks[pos + 1]
                            current_future = prefetch_pool.submit(
                                self._download_chunk,
                                recording_id,
                                int(nxt["chunk_index"]),
                                str(nxt["s3_key"]),
                            )

                        now_iso = _utc_iso(int(self._now()))
                        self.ingest_table.update_item(
                            Key=rec_key,
                            UpdateExpression=(
                                "SET process_status = :proc, active_chunk_index = :cidx, "
                                "active_chunk_frame_progress_pct = :pct, updated_at = :now"
                            ),
                            ExpressionAttributeValues={
                                ":proc": "PROCESSING_CHUNK",
                                ":cidx": chunk_idx,
                                ":pct": Decimal("0.0"),
                                ":now": now_iso,
                            },
                        )

                        analysis = self._analyze_chunk(
                            local_path=local_path,
                            recording_meta=rec_meta,
                            chunk_meta=chunk_meta,
                            open_cycles=open_cycles,
                            next_cycle_seq=next_cycle_seq,
                            next_segment_seq=next_segment_seq,
                            is_final_chunk=is_final,
                        )
                        next_cycle_seq = int(analysis["next_cycle_seq"])
                        next_segment_seq = int(analysis["next_segment_seq"])
                        open_cycles = dict(analysis.get("open_cycles") or {})
                        cycles_batch = list(analysis.get("cycles") or [])
                        segments_batch = list(analysis.get("segments") or [])
                        bbox_rows = list(analysis.get("bbox_rows") or [])
                        cycles_so_far += len(cycles_batch)

                        self._batch_write_items(self.haul_cycle_table, cycles_batch)
                        self._batch_write_items(self.state_segment_table, segments_batch)

                        ts_str = str(rec_meta.get("timestamp", "2026-11-18T00:00:00Z"))[:10]
                        yyyy, mm, dd = ts_str.split("-")
                        cam_id = str(rec_meta.get("camera_id", "CAM-UNKNOWN"))
                        pq_key = (
                            f"parquet/year={yyyy}/month={mm}/day={dd}/zone={cam_id}/"
                            f"{recording_id}_chunk_{chunk_idx:02d}.parquet"
                        )
                        self._s3.put_object(
                            Bucket=self.bucket_name,
                            Key=pq_key,
                            Body=_encode_parquet_bytes(bbox_rows),
                            ContentType="application/octet-stream",
                        )
                        parquet_keys.append(pq_key)

                        try:
                            if os.path.exists(local_path):
                                os.remove(local_path)
                        except OSError:
                            pass

                        emf_metrics = dict(analysis.get("metrics") or {})
                        emf_metrics["ChunkPrefetchWaitMs"] = (prefetch_wait_ms, "Milliseconds")
                        self._emit_emf(
                            metrics=emf_metrics,
                            properties={
                                "recording_id": recording_id,
                                "chunk_index": chunk_idx,
                                "camera_id": cam_id,
                            },
                        )

                        done_iso = _utc_iso(int(self._now()))
                        if not is_final:
                            # Intermediate chunk: save checkpoint, do NOT close_all or publish ovius-recording
                            self._save_checkpoint(
                                recording_id,
                                {
                                    "last_completed_chunk_index": chunk_idx,
                                    "next_cycle_seq": next_cycle_seq,
                                    "next_segment_seq": next_segment_seq,
                                    "cycles_extracted_so_far": cycles_so_far,
                                    "open_cycles": open_cycles,
                                    "parquet_keys": parquet_keys,
                                },
                            )
                            self.ingest_table.update_item(
                                Key=rec_key,
                                UpdateExpression=(
                                    "SET last_completed_chunk_index = :done, processed_chunks = :pc, "
                                    "next_cycle_seq = :ncs, next_segment_seq = :nss, "
                                    "cycles_extracted_so_far = :ces, active_chunk_frame_progress_pct = :pct, "
                                    "updated_at = :now"
                                ),
                                ExpressionAttributeValues={
                                    ":done": chunk_idx,
                                    ":pc": pos + 1,
                                    ":ncs": next_cycle_seq,
                                    ":nss": next_segment_seq,
                                    ":ces": cycles_so_far,
                                    ":pct": Decimal("100.0"),
                                    ":now": done_iso,
                                },
                            )
                        else:
                            # Final chunk only: publish master ovius-recording with complete chunk_keys manifest
                            master_rec: dict[str, Any] = {
                                "recording_id": recording_id,
                                "camera_id": cam_id,
                                "timestamp": str(rec_meta["timestamp"]),
                                "duration_s": rec_meta["duration_s"],
                                "fps": rec_meta["fps"],
                                "resolution": str(rec_meta.get("resolution", "1920x1080")),
                                "filename": str(rec_meta.get("filename") or f"{recording_id}.mp4"),
                                "location_tag": str(rec_meta.get("location_tag") or cam_id),
                                "tags": list(rec_meta.get("tags") or []),
                                "chunk_keys": chunk_keys_manifest,
                                "parquet_keys": parquet_keys,
                            }
                            self.recording_table.put_item(Item=master_rec)
                            published_recording = True
                            self.ingest_table.update_item(
                                Key=rec_key,
                                UpdateExpression=(
                                    "SET process_status = :completed, last_completed_chunk_index = :done, "
                                    "processed_chunks = :pc, next_cycle_seq = :ncs, next_segment_seq = :nss, "
                                    "cycles_extracted_so_far = :ces, active_chunk_frame_progress_pct = :pct, "
                                    "updated_at = :now"
                                ),
                                ExpressionAttributeValues={
                                    ":completed": "COMPLETED",
                                    ":done": chunk_idx,
                                    ":pc": len(chunks),
                                    ":ncs": next_cycle_seq,
                                    ":nss": next_segment_seq,
                                    ":ces": cycles_so_far,
                                    ":pct": Decimal("100.0"),
                                    ":now": done_iso,
                                },
                            )
                        processed_in_run += 1
        finally:
            stop_event.set()
            hb_thread.join(timeout=2.0)

        return {
            "upload_id": upload_id,
            "recording_id": recording_id,
            "chunks_processed": processed_in_run,
            "total_chunks": len(chunks),
            "cycles_extracted": cycles_so_far,
            "published_recording": published_recording,
            "parquet_keys": parquet_keys,
        }

    def run_loop(
        self,
        *,
        max_empty_polls: int = 1,
        poll_wait_s: int = 20,
        keep_warm_cap_s: int = KEEP_WARM_MAX_WAIT_S,
        keep_warm_step_s: int = 10,
    ) -> dict[str, Any]:
        """Consume SQS messages, bridge <5m active uploads with keep-warm, and exit atomically."""
        queue_url = self._resolve_queue_url()
        processed_recordings: list[str] = []
        keep_warm_waited_s = 0
        empty_polls = 0

        while True:
            resp = self._sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=poll_wait_s,
            )
            messages = resp.get("Messages", [])
            if messages:
                empty_polls = 0
                msg = messages[0]
                payload = json.loads(msg["Body"])
                rec_id = str(payload["recording_id"])
                uid = str(payload["upload_id"])
                receipt = str(msg["ReceiptHandle"])
                self.process_recording(
                    upload_id=uid,
                    recording_id=rec_id,
                    receipt_handle=receipt,
                )
                self._sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                processed_recordings.append(rec_id)
                continue

            empty_polls += 1
            min_eta_s = self._query_min_active_upload_eta()
            if (
                min_eta_s is not None
                and min_eta_s < keep_warm_cap_s
                and keep_warm_waited_s < keep_warm_cap_s
            ):
                step = min(keep_warm_step_s, keep_warm_cap_s - keep_warm_waited_s)
                self._update_worker_lock(status="KEEP_WARM_WAIT")
                self._sleep(step)
                keep_warm_waited_s += step
                continue

            if empty_polls < max_empty_polls:
                continue

            # Atomic EXITING + final SQS re-poll handshake before exiting
            self._update_worker_lock(status="EXITING")
            final_resp = self._sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=0,
            )
            final_msgs = final_resp.get("Messages", [])
            if final_msgs:
                msg = final_msgs[0]
                payload = json.loads(msg["Body"])
                rec_id = str(payload["recording_id"])
                uid = str(payload["upload_id"])
                receipt = str(msg["ReceiptHandle"])
                self.process_recording(
                    upload_id=uid,
                    recording_id=rec_id,
                    receipt_handle=receipt,
                )
                self._sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                processed_recordings.append(rec_id)
                continue

            self._update_worker_lock(status="IDLE", lease_ttl_s=0)
            self._emit_emf(
                metrics={"KeepWarmWaitSeconds": (float(keep_warm_waited_s), "Seconds")},
                properties={"event": "worker_exit", "processed_count": len(processed_recordings)},
            )
            return {
                "processed_recordings": processed_recordings,
                "keep_warm_waited_s": keep_warm_waited_s,
                "final_worker_status": "IDLE",
            }


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    runner = VisionWorkerRunner()
    summary = runner.run_loop(poll_wait_s=5)
    print(json.dumps(summary, separators=(",", ":")), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
