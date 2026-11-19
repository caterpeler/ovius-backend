"""Single-EC2 GPU systemd worker connecting S3/SQS (ap-southeast-1) to PTZ S-v5-XL pipeline (ap-southeast-3)."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import io
import json
import logging
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from typing import Any, Callable

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config
from botocore.exceptions import ClientError

from ovius_api.worker import (
    DEFAULT_MODEL_VERSION,
    FRESH_HEARTBEAT_MAX_AGE_S,
    HEARTBEAT_INTERVAL_S,
    KEEP_WARM_MAX_WAIT_S,
    SQS_RENEWAL_INTERVAL_S,
    SQS_VISIBILITY_TIMEOUT_S,
    WORKER_LEASE_TTL_S,
    build_emf_log,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_DATA_REGION = "ap-southeast-3"
DEFAULT_BUCKET = "ovius-pipeline-apse3-610331148578"
DEFAULT_QUEUE_NAME = "ovius-ready-recordings"
DEFAULT_PIPELINE_DIR = "/opt/pipeline"
DEFAULT_CODE_BUNDLE_KEY = "pipeline/ptz_code_bundle.tar.gz"
DEFAULT_MODEL_PREFIX = "models/s-v5-xl"

ARCHIVE_CSVS = [
    "boxes",
    "frame_states",
    "state_segments",
    "events",
    "loadings",
    "units",
    "periods",
    "push_cycles",
]

REQUIRED_INDEX_KEY_TYPES: dict[str, dict[str, Any]] = {
    "recording": {"camera_id": str, "timestamp": str},
    "haul_cycle": {
        "recording_id": str,
        "excavator_sort_key": str,
        "t_load_start_s": (int, Decimal),
    },
    "state_segment": {
        "recording_id": str,
        "cycle_id": str,
        "cycle_sort_key": str,
        "start_s": (int, Decimal),
    },
}


_VIDEO_HEADER_RE = re.compile(
    r"\[video\]\s+vfps=(?P<vfps>[0-9.]+)\s+\|\s+frame_skip_every=(?P<every>\d+)\s+\|\s+"
    r"analysed_fps=(?P<afps>[0-9.]+)\s+\|\s+total_frames=(?P<total>\d+)\s+\|\s+examined_frames=(?P<examined>\d+)"
)
_FRAME_PROGRESS_RE = re.compile(
    r"(?P<video_s>[0-9.]+)\s+s of video\s+\|\s+(?P<count>\d+)\s+frames\s+\|\s+(?P<rate>[0-9.]+)\s+frames/s"
)


def parse_pipeline_progress_line(line: str, state: dict[str, Any]) -> bool:
    """Parse [video] header or frame_pass progress line into mutable state; return True on progress tick."""
    m_hdr = _VIDEO_HEADER_RE.search(line)
    if m_hdr:
        state["vfps"] = float(m_hdr.group("vfps"))
        state["every"] = int(m_hdr.group("every"))
        state["analysed_fps"] = float(m_hdr.group("afps"))
        state["total_frames"] = int(m_hdr.group("total"))
        state["examined_frames"] = int(m_hdr.group("examined"))
        if state["vfps"] > 0 and state["total_frames"] > 0:
            state["duration_s"] = state["total_frames"] / state["vfps"]
        return True

    m_prog = _FRAME_PROGRESS_RE.search(line)
    if m_prog:
        video_s = float(m_prog.group("video_s"))
        count = int(m_prog.group("count"))
        rate = float(m_prog.group("rate"))
        state["video_s"] = video_s
        state["frames_done"] = count
        state["proc_fps"] = rate
        analysed_fps = float(state.get("analysed_fps") or 0.0)
        if analysed_fps <= 0.0 and video_s > 0.0:
            analysed_fps = count / video_s
        state["rtf"] = round(analysed_fps / max(0.01, rate), 3) if analysed_fps > 0.0 else None
        dur_s = float(state.get("duration_s") or 0.0)
        examined = int(state.get("examined_frames") or 0)
        frac = 0.0
        if examined > 0:
            frac = min(1.0, max(0.0, count / examined))
        elif dur_s > 0.0:
            frac = min(1.0, max(0.0, video_s / dur_s))
        state["frac"] = frac
        state["pct"] = round(35.0 + 53.0 * frac, 1)
        if examined > 0 and rate > 0.0:
            rem_s = max(0.0, (examined - count) / rate)
            state["eta_s"] = int(round(rem_s))
        elif dur_s > 0.0 and state.get("rtf"):
            state["eta_s"] = int(round(max(0.0, dur_s - video_s) * float(state["rtf"])))
        return True
    return False


def format_live_stage_label(state: dict[str, Any]) -> str:
    """Format concise human-readable progress string with skip stride, FPS, live RTF, and ETA."""
    every = state.get("every")
    done = int(state.get("frames_done") or 0)
    examined = int(state.get("examined_frames") or 0)
    video_s = float(state.get("video_s") or 0.0)
    dur_s = float(state.get("duration_s") or 0.0)
    rate = float(state.get("proc_fps") or 0.0)
    rtf = state.get("rtf")
    eta_s = state.get("eta_s")

    skip_txt = f"skip={every}" if every else "skip=auto"
    if done == 0 and examined > 0:
        return f"Loaded video ({dur_s:.0f}s, {skip_txt}, {examined:,} frames to examine) — warming TensorRT..."
    parts = [f"Frame {done:,}/{examined:,}" if examined > 0 else f"Frame {done:,}"]
    if dur_s > 0:
        parts.append(f"({video_s:.0f}s/{dur_s:.0f}s, {skip_txt})")
    else:
        parts.append(f"({video_s:.0f}s, {skip_txt})")
    if rate > 0:
        parts.append(f"| {rate:.1f} fps")
    if rtf is not None:
        parts.append(f"| RTF {rtf:.2f}x")
    if eta_s is not None:
        mins, secs = divmod(int(eta_s), 60)
        parts.append(f"| ETA {mins}m {secs:02d}s" if mins else f"| ETA {secs}s")
    return " ".join(parts)


def _utc_iso(epoch_s: int | float) -> str:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).isoformat()


def _normalize_utc_iso(ts: str) -> str:
    """Normalize any ISO-8601 timestamp string (Z, naive, or offset) to canonical UTC +00:00."""
    text = str(ts or "").strip()
    if not text:
        return text
    normalized = f"{text[:-1]}+00:00" if text.endswith(("Z", "z")) else text
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def _sanitize_ddb_item(item: dict[str, Any]) -> dict[str, Any]:
    """Remove empty-string attributes and convert floats recursively to Decimal for DynamoDB."""
    out: dict[str, Any] = {}
    for k, v in item.items():
        if v == "":
            continue
        if isinstance(v, float):
            out[k] = Decimal(str(round(v, 4)))
        elif isinstance(v, dict):
            out[k] = _sanitize_ddb_item(v)
        elif isinstance(v, list):
            out[k] = [
                _sanitize_ddb_item(x)
                if isinstance(x, dict)
                else (Decimal(str(round(x, 4))) if isinstance(x, float) else x)
                for x in v
            ]
        else:
            out[k] = v
    return out


def _rewrite_recording_id_in_items(
    *,
    tables: dict[str, list[dict[str, Any]]],
    target_recording_id: str,
    rec_meta: dict[str, Any],
    chunk_keys_manifest: list[dict[str, Any]],
    parquet_keys: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Normalize recording/cycle/segment IDs and attach upload metadata, chunk_keys, and parquet_keys."""
    rec_list = tables.get("recording") or []
    rec = dict(rec_list[0]) if rec_list else {}
    orig_rid = str(rec.get("recording_id") or "")

    rec["recording_id"] = target_recording_id
    for field in ("camera_id", "timestamp", "location_tag", "filename"):
        val = rec_meta.get(field)
        if val:
            rec[field] = str(val)
    if rec.get("timestamp"):
        rec["timestamp"] = _normalize_utc_iso(str(rec["timestamp"]))
    if not rec.get("filename"):
        rec["filename"] = f"{target_recording_id}.mp4"
    tags = list(rec.get("tags") or [])
    for t in rec_meta.get("tags") or []:
        if t not in tags:
            tags.append(str(t))
    rec["tags"] = tags
    rec["chunk_keys"] = chunk_keys_manifest
    rec["parquet_keys"] = parquet_keys

    def _swap_prefix(val: Any) -> Any:
        if isinstance(val, str) and orig_rid and orig_rid != target_recording_id:
            if val.startswith(f"{orig_rid}-") or val.startswith(f"{orig_rid}#"):
                return target_recording_id + val[len(orig_rid) :]
        return val

    cycles_out: list[dict[str, Any]] = []
    for raw_c in tables.get("haul_cycle") or []:
        c = dict(raw_c)
        c["recording_id"] = target_recording_id
        c["cycle_id"] = _swap_prefix(c.get("cycle_id"))
        if isinstance(c.get("scoops"), list):
            new_scoops = []
            for sc in c["scoops"]:
                sc_copy = dict(sc)
                if "excavator_segment_id" in sc_copy:
                    sc_copy["excavator_segment_id"] = _swap_prefix(sc_copy["excavator_segment_id"])
                new_scoops.append(sc_copy)
            c["scoops"] = new_scoops
        if isinstance(c.get("truck_segments"), list):
            c["truck_segments"] = [_swap_prefix(seg_id) for seg_id in c["truck_segments"]]
        cycles_out.append(_sanitize_ddb_item(c))

    segs_out: list[dict[str, Any]] = []
    for raw_s in tables.get("state_segment") or []:
        s = dict(raw_s)
        s["recording_id"] = target_recording_id
        s["segment_id"] = _swap_prefix(s.get("segment_id"))
        raw_cid = s.get("cycle_id")
        if raw_cid is None or str(raw_cid).strip() == "":
            # Match seeded DynamoDB contract (cycle_id="NONE" on idle/face_prep segments)
            # without writing cycle_sort_key so sparse cycle-id-index avoids hot partitions.
            s["cycle_id"] = "NONE"
        else:
            s["cycle_id"] = _swap_prefix(raw_cid)
        if "cycle_sort_key" in s:
            s["cycle_sort_key"] = _swap_prefix(s.get("cycle_sort_key"))
        segs_out.append(_sanitize_ddb_item(s))

    return {
        "recording": [_sanitize_ddb_item(rec)],
        "haul_cycle": cycles_out,
        "state_segment": segs_out,
    }


def _csv_to_snappy_parquet_bytes(csv_path: Path) -> bytes:
    """Serialize CSV to Snappy Parquet when pyarrow is available, else UTF-8 JSON-lines fallback."""
    try:
        import pandas as pd

        df = pd.read_csv(csv_path)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, compression="snappy")
        return buf.getvalue()
    except ImportError:
        import csv

        with csv_path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        return "\n".join(json.dumps(r, separators=(",", ":")) for r in rows).encode("utf-8")


class Ec2PipelineWorker:
    """Runs on a persistent g5 EC2 host (ap-southeast-3) polling SQS/S3/DynamoDB (ap-southeast-1)."""

    def __init__(
        self,
        *,
        data_region: str | None = None,
        bucket_name: str | None = None,
        ready_queue_url: str | None = None,
        ready_queue_name: str | None = None,
        pipeline_dir: str | Path | None = None,
        code_bundle_key: str | None = None,
        model_prefix: str | None = None,
        instance_type: str | None = None,
        ddb_resource: Any | None = None,
        s3_client: Any | None = None,
        sqs_client: Any | None = None,
        run_cmd_fn: Callable[..., subprocess.CompletedProcess[str]] | None = None,
        shutdown_fn: Callable[[], None] | None = None,
        now_fn: Callable[[], float] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        tmp_dir: str | Path | None = None,
        auto_shutdown: bool | None = None,
    ) -> None:
        self.data_region = data_region or os.environ.get("DATA_REGION", DEFAULT_DATA_REGION)
        self.bucket_name = bucket_name or os.environ.get("PIPELINE_S3_BUCKET", DEFAULT_BUCKET)
        self.ready_queue_name = ready_queue_name or os.environ.get("READY_QUEUE_NAME", DEFAULT_QUEUE_NAME)
        self._ready_queue_url = ready_queue_url or os.environ.get("READY_QUEUE_URL")
        self.pipeline_dir = Path(pipeline_dir or os.environ.get("PIPELINE_DIR", DEFAULT_PIPELINE_DIR))
        self.code_bundle_key = code_bundle_key or os.environ.get("CODE_BUNDLE_KEY", DEFAULT_CODE_BUNDLE_KEY)
        self.model_prefix = (model_prefix or os.environ.get("MODEL_PREFIX", DEFAULT_MODEL_PREFIX)).strip("/")
        self.instance_type = instance_type or os.environ.get("WORKER_INSTANCE_TYPE", "g5.xlarge")
        self.tmp_dir = Path(tmp_dir or os.environ.get("WORKER_TMP_DIR", "/tmp"))
        if auto_shutdown is None:
            self.auto_shutdown = os.environ.get("AUTO_SHUTDOWN", "false").strip().lower() == "true"
        else:
            self.auto_shutdown = auto_shutdown

        cfg = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
        sess = boto3.Session(region_name=self.data_region)
        self._ddb = ddb_resource or sess.resource("dynamodb", config=cfg)
        self._s3 = s3_client or sess.client("s3", config=cfg)
        self._sqs = sqs_client or sess.client("sqs", config=cfg)

        self.recording_table = self._ddb.Table(os.environ.get("RECORDING_TABLE", "ovius-recording"))
        self.haul_cycle_table = self._ddb.Table(os.environ.get("HAUL_CYCLE_TABLE", "ovius-haul-cycle"))
        self.state_segment_table = self._ddb.Table(os.environ.get("STATE_SEGMENT_TABLE", "ovius-state-segment"))
        self.ingest_table = self._ddb.Table(os.environ.get("INGEST_STATE_TABLE", "ovius-ingest-state"))

        self._has_custom_run_cmd = run_cmd_fn is not None
        self._run_cmd = run_cmd_fn or self._default_run_cmd
        self._shutdown = shutdown_fn or self._default_shutdown
        self._now = now_fn or time.time
        self._sleep = sleep_fn or time.sleep
        self.emf_logs: list[dict[str, Any]] = []

    @staticmethod
    def _default_run_cmd(
        cmd: list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(cmd, env=env, cwd=cwd, check=True, text=True, capture_output=True)

    @staticmethod
    def _default_shutdown() -> None:
        logger.info("Idle timeout reached; initiating OS shutdown (InstanceInitiatedShutdownBehavior=stop)")
        subprocess.run(["sudo", "shutdown", "-h", "now"], check=False)

    def _resolve_queue_url(self) -> str:
        if self._ready_queue_url:
            return self._ready_queue_url
        resp = self._sqs.get_queue_url(QueueName=self.ready_queue_name)
        self._ready_queue_url = str(resp["QueueUrl"])
        return self._ready_queue_url

    def _emit_emf(self, *, metrics: dict[str, tuple[float, str]], properties: dict[str, Any]) -> None:
        payload = build_emf_log(
            metrics=metrics,
            properties=properties,
            model_version=DEFAULT_MODEL_VERSION,
            instance_type=self.instance_type,
        )
        self.emf_logs.append(payload)
        print(json.dumps(payload, separators=(",", ":")), flush=True)

    def _update_worker_lock(
        self,
        *,
        status: str,
        recording_id: str | None = None,
        lease_ttl_s: int = WORKER_LEASE_TTL_S,
    ) -> None:
        now = int(self._now())
        expr = "SET #st = :st, lease_expires_at = :lease, updated_at = :now"
        vals: dict[str, Any] = {
            ":st": status,
            ":lease": now + lease_ttl_s,
            ":now": _utc_iso(now),
        }
        if recording_id is not None:
            expr += ", active_recording_id = :rec"
            vals[":rec"] = recording_id
        self.ingest_table.update_item(
            Key={"pk": "WORKER#LOCK", "sk": "PRIMARY"},
            UpdateExpression=expr,
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=vals,
        )

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

    def sync_pipeline_assets_from_s3(self) -> dict[str, Any]:
        """ETag-checked hot-sync of lightweight PTZ code bundle and S-v5-XL optimized model files."""
        self.pipeline_dir.mkdir(parents=True, exist_ok=True)
        bundle_dir = self.pipeline_dir / "bundle"
        models_dir = bundle_dir / "models"
        updated_files: list[str] = []

        # 1. Sync lightweight code bundle (<200 KB)
        code_etag_mark = self.pipeline_dir / ".ptz_code_bundle.etag"
        try:
            head = self._s3.head_object(Bucket=self.bucket_name, Key=self.code_bundle_key)
            remote_etag = str(head["ETag"])
            if not (bundle_dir.is_dir() and code_etag_mark.exists() and code_etag_mark.read_text().strip() == remote_etag):
                part = self.pipeline_dir / ".ptz_code_bundle.tar.gz.part"
                self._s3.download_file(self.bucket_name, self.code_bundle_key, str(part))
                staging = self.pipeline_dir / "bundle.new"
                shutil.rmtree(staging, ignore_errors=True)
                staging.mkdir(parents=True, exist_ok=True)
                with tarfile.open(part, "r:gz") as tf:
                    tf.extractall(staging)
                extracted_bundle = staging / "bundle" if (staging / "bundle").is_dir() else staging
                # Preserve existing cached models across code-only swaps
                if models_dir.is_dir():
                    target_models = extracted_bundle / "models"
                    shutil.rmtree(target_models, ignore_errors=True)
                    shutil.move(str(models_dir), str(target_models))
                shutil.rmtree(bundle_dir, ignore_errors=True)
                extracted_bundle.rename(bundle_dir)
                shutil.rmtree(staging, ignore_errors=True)
                part.unlink(missing_ok=True)
                code_etag_mark.write_text(remote_etag)
                updated_files.append(self.code_bundle_key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey"}:
                raise

        # 2. Sync model manifest + preferred artifacts (TensorRT engine -> ONNX -> FP16 safetensors -> config)
        models_dir.mkdir(parents=True, exist_ok=True)
        manifest_key = f"{self.model_prefix}/manifest.json"
        try:
            m_head = self._s3.head_object(Bucket=self.bucket_name, Key=manifest_key)
            m_etag = str(m_head["ETag"])
            m_mark = models_dir / ".manifest.etag"
            manifest_path = models_dir / "manifest.json"
            if not (manifest_path.exists() and m_mark.exists() and m_mark.read_text().strip() == m_etag):
                self._s3.download_file(self.bucket_name, manifest_key, str(manifest_path))
                m_mark.write_text(m_etag)
                updated_files.append(manifest_key)

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            artifacts = manifest.get("artifacts") or {}
            # Always sync dfine_config + preferred runtime artifact (TRT engine + one FP16 fallback)
            wanted_keys = ["dfine_config", "tensorrt_fp16_sm86", "tensorrt_fp16_sm75"]
            if "ema_fp16_safetensors" in artifacts:
                wanted_keys.append("ema_fp16_safetensors")
            elif "ema_fp16_pth" in artifacts:
                wanted_keys.append("ema_fp16_pth")
            for art_name in wanted_keys:
                art = artifacts.get(art_name)
                if not isinstance(art, dict) or not art.get("file"):
                    continue
                fname = str(art["file"])
                s3_key = f"{self.model_prefix}/{fname}"
                dst = models_dir / fname
                etag_file = models_dir / f".{fname}.etag"
                a_head = self._s3.head_object(Bucket=self.bucket_name, Key=s3_key)
                a_etag = str(a_head["ETag"])
                if not (dst.exists() and etag_file.exists() and etag_file.read_text().strip() == a_etag):
                    self._s3.download_file(self.bucket_name, s3_key, str(dst))
                    etag_file.write_text(a_etag)
                    updated_files.append(s3_key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey"}:
                raise

        return {"updated_files": updated_files, "bundle_dir": str(bundle_dir)}

    def _set_recording_stage(
        self,
        *,
        rec_key: dict[str, str],
        process_status: str,
        stage_label: str,
        pct: float,
        active_chunk_index: int = 0,
        last_error: str | None = None,
    ) -> None:
        expr = (
            "SET process_status = :proc, process_stage = :stage, "
            "active_chunk_index = :cidx, active_chunk_frame_progress_pct = :pct, updated_at = :now"
        )
        vals: dict[str, Any] = {
            ":proc": process_status,
            ":stage": stage_label,
            ":cidx": active_chunk_index,
            ":pct": Decimal(str(round(pct, 1))),
            ":now": _utc_iso(int(self._now())),
        }
        if last_error is not None:
            expr += ", last_error = :err"
            vals[":err"] = last_error[:500]
        self.ingest_table.update_item(
            Key=rec_key,
            UpdateExpression=expr,
            ExpressionAttributeValues=vals,
        )

    def _download_and_join_chunks(
        self,
        *,
        recording_id: str,
        chunks: list[dict[str, Any]],
        work_dir: Path,
    ) -> tuple[Path, float]:
        t0 = time.monotonic()
        local_chunks: list[Path] = []
        for ch in chunks:
            idx = int(ch["chunk_index"])
            s3_key = str(ch["s3_key"])
            dst = work_dir / f"{recording_id}_chunk_{idx:02d}.mp4"
            self._s3.download_file(self.bucket_name, s3_key, str(dst))
            local_chunks.append(dst)

        joined_path = work_dir / f"{recording_id}.mp4"
        if len(local_chunks) == 1:
            local_chunks[0].rename(joined_path)
        else:
            concat_txt = work_dir / "concat.txt"
            concat_txt.write_text(
                "".join(f"file '{p.as_posix()}'\n" for p in local_chunks),
                encoding="utf-8",
            )
            self._run_cmd(
                [
                    "ffmpeg",
                    "-y",
                    "-loglevel",
                    "error",
                    "-f",
                    "concat",
                    "-safe",
                    "0",
                    "-i",
                    str(concat_txt),
                    "-c",
                    "copy",
                    str(joined_path),
                ]
            )
            for p in local_chunks:
                p.unlink(missing_ok=True)
        download_ms = round((time.monotonic() - t0) * 1000.0, 2)
        return joined_path, download_ms

    def _batch_write_items(self, table: Any, items: list[dict[str, Any]]) -> None:
        if not items:
            return
        with table.batch_writer() as writer:
            for item in items:
                writer.put_item(Item=item)

    def _upload_parquet_outputs(
        self,
        *,
        out_dir: Path,
        recording_id: str,
        camera_id: str,
        timestamp: str,
    ) -> list[str]:
        norm_ts = _normalize_utc_iso(timestamp)
        ts_date = norm_ts[:10] if len(norm_ts) >= 10 and norm_ts[4] == "-" else "2026-11-19"
        yyyy, mm, dd = ts_date.split("-")
        parquet_keys: list[str] = []

        boxes_bytes: bytes | None = None
        boxes_csv = out_dir / "boxes.csv"
        if boxes_csv.exists():
            boxes_bytes = _csv_to_snappy_parquet_bytes(boxes_csv)
            boxes_key = f"parquet/year={yyyy}/month={mm}/day={dd}/zone={camera_id}/{recording_id}_chunk_00.parquet"
            self._s3.put_object(
                Bucket=self.bucket_name,
                Key=boxes_key,
                Body=boxes_bytes,
                ContentType="application/octet-stream",
            )
            parquet_keys.append(boxes_key)

        for name in ARCHIVE_CSVS:
            csv_path = out_dir / f"{name}.csv"
            if csv_path.exists():
                arch_key = f"parquet/archive/{name}/recording_id={recording_id}/part.parquet"
                body = (
                    boxes_bytes
                    if name == "boxes" and boxes_bytes is not None
                    else _csv_to_snappy_parquet_bytes(csv_path)
                )
                self._s3.put_object(
                    Bucket=self.bucket_name,
                    Key=arch_key,
                    Body=body,
                    ContentType="application/octet-stream",
                )
        return parquet_keys

    def _run_pipeline_with_progress(
        self,
        *,
        cmd: list[str],
        env: dict[str, str],
        cwd: str,
        rec_key: dict[str, str],
        expected_duration_s: float,
    ) -> dict[str, Any]:
        """Execute run_pipeline.py while streaming live frame progress, RTF, and ETA to DynamoDB."""
        if self._has_custom_run_cmd:
            self._run_cmd(cmd, env=env, cwd=cwd)
            return {}

        state: dict[str, Any] = {"duration_s": expected_duration_s}
        tail_lines: list[str] = []
        last_push = 0.0

        proc = subprocess.Popen(
            cmd,
            env=env,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if line:
                print(line, flush=True)
                tail_lines.append(line)
                if len(tail_lines) > 25:
                    tail_lines.pop(0)
            if parse_pipeline_progress_line(line, state):
                now_m = time.monotonic()
                if now_m - last_push >= 6.0 or state.get("frames_done", 0) == 0:
                    last_push = now_m
                    try:
                        self._set_recording_stage(
                            rec_key=rec_key,
                            process_status="PROCESSING_CHUNK",
                            stage_label=format_live_stage_label(state),
                            pct=float(state.get("pct", 35.0)),
                        )
                    except Exception:  # noqa: BLE001
                        logger.warning("Failed updating live frame progress in ovius-ingest-state")

        rc = proc.wait()
        if rc != 0:
            snippet = " | ".join(tail_lines[-6:])
            raise RuntimeError(f"run_pipeline.py exited {rc}: {snippet}")
        return state

    def process_recording(
        self,
        *,
        upload_id: str,
        recording_id: str,
        receipt_handle: str | None = None,
    ) -> dict[str, Any]:
        """Download/join all chunks for a READY recording, run PTZ S-v5-XL pipeline, and publish telemetry."""
        rec_key = {"pk": f"SESSION#{upload_id}", "sk": f"REC#{recording_id}"}
        rec_meta = self.ingest_table.get_item(Key=rec_key).get("Item")
        if not rec_meta:
            raise LookupError(f"Recording {recording_id} not found in session {upload_id}")
        if rec_meta.get("upload_status") == "ABORTED" or rec_meta.get("process_status") == "ABORTED":
            logger.info("Skipping aborted recording %s in session %s", recording_id, upload_id)
            return {
                "upload_id": upload_id,
                "recording_id": recording_id,
                "skipped": True,
                "reason": "aborted",
            }

        raw_chunks = self.ingest_table.query(
            KeyConditionExpression=Key("pk").eq(f"REC#{recording_id}")
        ).get("Items", [])
        chunks = [c for c in raw_chunks if str(c.get("sk", "")).startswith("CHUNK#")]
        chunks.sort(key=lambda c: int(c["chunk_index"]))
        if not chunks:
            raise LookupError(f"No chunk manifests found for recording {recording_id}")

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

        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        work_dir = Path(tempfile.mkdtemp(prefix=f"ovius_rec_{recording_id}_", dir=str(self.tmp_dir)))
        try:
            self._update_worker_lock(status="BUSY", recording_id=recording_id)
            self._set_recording_stage(
                rec_key=rec_key,
                process_status="PROCESSING_CHUNK",
                stage_label="Syncing PTZ bundle & TensorRT S-v5-XL engine from S3",
                pct=5.0,
            )
            sync_info = self.sync_pipeline_assets_from_s3()

            self._set_recording_stage(
                rec_key=rec_key,
                process_status="PROCESSING_CHUNK",
                stage_label=f"Downloading & joining {len(chunks)} chunk(s) from S3",
                pct=15.0,
            )
            video_path, download_ms = self._download_and_join_chunks(
                recording_id=recording_id,
                chunks=chunks,
                work_dir=work_dir,
            )

            self._set_recording_stage(
                rec_key=rec_key,
                process_status="PROCESSING_CHUNK",
                stage_label="Running S-v5-XL TensorRT FP16 detection, ByteTrack & heuristics",
                pct=35.0,
            )
            out_root = work_dir / "pipeline_out"
            out_root.mkdir(parents=True, exist_ok=True)
            venv_python = self.pipeline_dir / "venv" / "bin" / "python"
            python_bin = str(venv_python) if venv_python.exists() else sys.executable
            run_pipeline_py = self.pipeline_dir / "bundle" / "run_pipeline.py"

            env = dict(os.environ)
            env.pop("LD_LIBRARY_PATH", None)
            cmd = [
                python_bin,
                str(run_pipeline_py),
                str(video_path),
                "--out",
                str(out_root),
                "--no-cache",
            ]
            ts_str = str(rec_meta.get("timestamp") or "")
            if ts_str:
                cmd.extend(["--start", ts_str])

            t_infer = time.monotonic()
            expected_dur = float(rec_meta.get("duration_s") or 0.0)
            pipe_stats = self._run_pipeline_with_progress(
                cmd=cmd,
                env=env,
                cwd=str(self.pipeline_dir / "bundle"),
                rec_key=rec_key,
                expected_duration_s=expected_dur,
            )
            infer_s = round(max(0.001, time.monotonic() - t_infer), 3)
            measured_rtf = (
                round(infer_s / expected_dur, 3)
                if expected_dur > 0
                else pipe_stats.get("rtf")
            )

            self._set_recording_stage(
                rec_key=rec_key,
                process_status="PROCESSING_CHUNK",
                stage_label="Uploading Snappy Parquet & publishing DynamoDB telemetry",
                pct=90.0,
            )

            # Locate pipeline output directory (<recording_id>__<pipeline_name>/)
            candidates = sorted(p for p in out_root.iterdir() if p.is_dir())
            if not candidates:
                raise RuntimeError(f"No pipeline output folder created under {out_root}")
            out_dir = candidates[0]

            raw_tables: dict[str, list[dict[str, Any]]] = {}
            for tbl_key in ("recording", "haul_cycle", "state_segment"):
                p_json = out_dir / f"{tbl_key}.json"
                raw_tables[tbl_key] = (
                    json.loads(p_json.read_text(encoding="utf-8"), parse_float=Decimal)
                    if p_json.exists()
                    else []
                )

            cam_id = str(rec_meta.get("camera_id") or "CAM-UNKNOWN")
            parquet_keys = self._upload_parquet_outputs(
                out_dir=out_dir,
                recording_id=recording_id,
                camera_id=cam_id,
                timestamp=ts_str,
            )
            normalized = _rewrite_recording_id_in_items(
                tables=raw_tables,
                target_recording_id=recording_id,
                rec_meta=rec_meta,
                chunk_keys_manifest=chunk_keys_manifest,
                parquet_keys=parquet_keys,
            )

            # Publish child tables first, master ovius-recording LAST (Guard 1.1.4)
            self._batch_write_items(self.haul_cycle_table, normalized["haul_cycle"])
            self._batch_write_items(self.state_segment_table, normalized["state_segment"])
            self.recording_table.put_item(Item=normalized["recording"][0])

            cycles_count = len(normalized["haul_cycle"])
            segments_count = len(normalized["state_segment"])
            self.ingest_table.update_item(
                Key=rec_key,
                UpdateExpression=(
                    "SET process_status = :completed, process_stage = :stage, "
                    "last_completed_chunk_index = :done, "
                    "processed_chunks = :pc, cycles_extracted_so_far = :ces, "
                    "segments_extracted_so_far = :ses, "
                    "active_chunk_frame_progress_pct = :pct, updated_at = :now"
                ),
                ExpressionAttributeValues={
                    ":completed": "COMPLETED",
                    ":stage": (
                        f"Completed in {infer_s:.1f}s (RTF {measured_rtf:.2f}x, skip={pipe_stats.get('every', 'auto')}) "
                        f"— {cycles_count} cycles, {segments_count} segments live"
                        if measured_rtf is not None
                        else f"Completed in {infer_s:.1f}s — {cycles_count} cycles, {segments_count} segments live"
                    ),
                    ":done": len(chunks) - 1,
                    ":pc": len(chunks),
                    ":ces": cycles_count,
                    ":ses": segments_count,
                    ":pct": Decimal("100.0"),
                    ":now": _utc_iso(int(self._now())),
                },
            )

            self._emit_emf(
                metrics={
                    "ChunkPrefetchWaitMs": (download_ms, "Milliseconds"),
                    "ChunkInferenceDuration": (infer_s, "Seconds"),
                    "CyclesExtracted": (float(cycles_count), "Count"),
                },
                properties={
                    "recording_id": recording_id,
                    "camera_id": cam_id,
                    "chunks_joined": len(chunks),
                    "segments_extracted": segments_count,
                },
            )

            return {
                "upload_id": upload_id,
                "recording_id": recording_id,
                "chunks_joined": len(chunks),
                "cycles_extracted": cycles_count,
                "segments_extracted": segments_count,
                "parquet_keys": parquet_keys,
                "infer_seconds": infer_s,
                "download_ms": download_ms,
                "synced_assets": sync_info["updated_files"],
            }
        except Exception as exc:
            try:
                self._set_recording_stage(
                    rec_key=rec_key,
                    process_status="FAILED",
                    stage_label="Pipeline processing failed",
                    pct=0.0,
                    last_error=str(exc),
                )
            except Exception:  # noqa: BLE001
                pass
            raise
        finally:
            stop_event.set()
            hb_thread.join(timeout=2.0)
            shutil.rmtree(work_dir, ignore_errors=True)

    def _handle_sqs_message(
        self,
        *,
        queue_url: str,
        msg: dict[str, Any],
        processed_recordings: list[str],
        failed_recordings: list[str],
    ) -> None:
        """Process a single SQS message safely; leave failed messages for SQS DLQ retries."""
        receipt = str(msg["ReceiptHandle"])
        try:
            payload = json.loads(msg["Body"])
            rec_id = str(payload["recording_id"])
            uid = str(payload["upload_id"])
        except Exception:  # noqa: BLE001
            logger.exception("Malformed SQS message body; deleting unparseable message")
            try:
                self._sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            except Exception:  # noqa: BLE001
                logger.exception("Failed deleting malformed SQS message")
            return

        try:
            res = self.process_recording(upload_id=uid, recording_id=rec_id, receipt_handle=receipt)
            self._sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            if not res.get("skipped"):
                processed_recordings.append(rec_id)
        except Exception:  # noqa: BLE001
            logger.exception(
                "Recording %s (session %s) failed processing; leaving message for SQS DLQ retry",
                rec_id,
                uid,
            )
            failed_recordings.append(rec_id)

    def run_loop(
        self,
        *,
        max_empty_polls: int = 2,
        poll_wait_s: int = 10,
        keep_warm_cap_s: int = KEEP_WARM_MAX_WAIT_S,
        keep_warm_step_s: int = 10,
    ) -> dict[str, Any]:
        """Consume SQS messages, bridge <5m uploads via keep-warm, exit atomically, and optionally stop EC2."""
        processed_recordings: list[str] = []
        failed_recordings: list[str] = []
        keep_warm_waited_s = 0
        empty_polls = 0
        clean_exit = False

        try:
            queue_url = self._resolve_queue_url()
            while True:
                resp = self._sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=poll_wait_s,
                )
                messages = resp.get("Messages", [])
                if messages:
                    empty_polls = 0
                    self._handle_sqs_message(
                        queue_url=queue_url,
                        msg=messages[0],
                        processed_recordings=processed_recordings,
                        failed_recordings=failed_recordings,
                    )
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

                self._update_worker_lock(status="EXITING")
                final_resp = self._sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=0,
                )
                final_msgs = final_resp.get("Messages", [])
                if final_msgs:
                    self._handle_sqs_message(
                        queue_url=queue_url,
                        msg=final_msgs[0],
                        processed_recordings=processed_recordings,
                        failed_recordings=failed_recordings,
                    )
                    continue

                self._update_worker_lock(status="IDLE", lease_ttl_s=0)
                self._emit_emf(
                    metrics={"KeepWarmWaitSeconds": (float(keep_warm_waited_s), "Seconds")},
                    properties={
                        "event": "worker_exit",
                        "processed_count": len(processed_recordings),
                        "failed_count": len(failed_recordings),
                    },
                )
                clean_exit = True
                if self.auto_shutdown:
                    self._shutdown()
                return {
                    "processed_recordings": processed_recordings,
                    "failed_recordings": failed_recordings,
                    "keep_warm_waited_s": keep_warm_waited_s,
                    "final_worker_status": "IDLE",
                    "shutdown_triggered": self.auto_shutdown,
                }
        finally:
            if not clean_exit:
                try:
                    self._update_worker_lock(status="IDLE", lease_ttl_s=0)
                except Exception:  # noqa: BLE001
                    logger.exception("Failed resetting WORKER#LOCK to IDLE during worker teardown")
                if self.auto_shutdown:
                    self._shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(description="Ovius EC2 GPU PTZ S-v5-XL pipeline worker daemon.")
    parser.add_argument("--data-region", default=os.environ.get("DATA_REGION", DEFAULT_DATA_REGION))
    parser.add_argument("--bucket", default=os.environ.get("PIPELINE_S3_BUCKET", DEFAULT_BUCKET))
    parser.add_argument("--pipeline-dir", default=os.environ.get("PIPELINE_DIR", DEFAULT_PIPELINE_DIR))
    parser.add_argument("--poll-wait-s", type=int, default=10)
    parser.add_argument("--max-empty-polls", type=int, default=2)
    parser.add_argument("--sync-only", action="store_true")
    parser.add_argument("--auto-shutdown", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    worker = Ec2PipelineWorker(
        data_region=args.data_region,
        bucket_name=args.bucket,
        pipeline_dir=args.pipeline_dir,
        auto_shutdown=args.auto_shutdown or os.environ.get("AUTO_SHUTDOWN", "false").lower() == "true",
    )
    if args.sync_only:
        print(json.dumps(worker.sync_pipeline_assets_from_s3(), indent=2), flush=True)
        return 0
    summary = worker.run_loop(max_empty_polls=args.max_empty_polls, poll_wait_s=args.poll_wait_s)
    print(json.dumps(summary, separators=(",", ":")), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
