"""Unit tests for build_ptz_code_bundle.py and Ec2PipelineWorker (Phase 2)."""

from __future__ import annotations

from decimal import Decimal
import io
import json
from pathlib import Path
import py_compile
import subprocess
import sys
import tarfile
from typing import Any

from botocore.exceptions import ClientError

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPTS_DIR = str(_REPO_ROOT / "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import build_ptz_code_bundle as builder  # noqa: E402
from ovius_api.ec2_pipeline_worker import (  # noqa: E402
    Ec2PipelineWorker,
    format_live_stage_label,
    parse_pipeline_progress_line,
)


def _not_found() -> ClientError:
    return ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")


class _BatchWriter:
    def __init__(self, table: "_MemTable") -> None:
        self._table = table

    def __enter__(self) -> "_BatchWriter":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def put_item(self, *, Item: dict[str, Any]) -> None:
        self._table.put_item(Item=Item)


class _MemTable:
    def __init__(self, name: str, pk_fields: tuple[str, ...]) -> None:
        self.name = name
        self._pk_fields = pk_fields
        self.items: dict[tuple[Any, ...], dict[str, Any]] = {}
        self.put_order: list[str] = []

    def _kt(self, item: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(item[k] for k in self._pk_fields)

    def put_item(self, *, Item: dict[str, Any]) -> dict[str, Any]:
        self.items[self._kt(Item)] = dict(Item)
        self.put_order.append(self.name)
        return {}

    def get_item(self, *, Key: dict[str, Any]) -> dict[str, Any]:
        it = self.items.get(self._kt(Key))
        return {"Item": dict(it)} if it is not None else {}

    def batch_writer(self) -> _BatchWriter:
        return _BatchWriter(self)

    def query(self, **kwargs: Any) -> dict[str, Any]:
        if kwargs.get("IndexName") == "GSI_ACTIVE_UPLOADS":
            active = [dict(it) for it in self.items.values() if it.get("GSI1PK") == "ACTIVE_UPLOAD"]
            active.sort(key=lambda it: float(it.get("GSI1SK", 999999)))
            return {"Items": active}
        vals = kwargs["KeyConditionExpression"].get_expression()["values"]
        pk_val = vals[1]
        matched = [dict(it) for (pk, _), it in self.items.items() if pk == pk_val]
        matched.sort(key=lambda it: str(it["sk"]))
        return {"Items": matched}

    def update_item(
        self,
        *,
        Key: dict[str, Any],
        UpdateExpression: str,
        ExpressionAttributeNames: dict[str, str] | None = None,
        ExpressionAttributeValues: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        _ = ExpressionAttributeNames
        kt = self._kt(Key)
        item = dict(self.items.get(kt, Key))
        vals = ExpressionAttributeValues or {}
        if "#st = :st" in UpdateExpression:
            item["status"] = vals[":st"]
            item["lease_expires_at"] = vals[":lease"]
            if ":rec" in vals:
                item["active_recording_id"] = vals[":rec"]
        elif ":completed" in vals:
            item["process_status"] = vals[":completed"]
            item["last_completed_chunk_index"] = vals[":done"]
            item["processed_chunks"] = vals[":pc"]
            item["cycles_extracted_so_far"] = vals[":ces"]
        elif ":proc" in vals:
            item["process_status"] = vals[":proc"]
        self.items[kt] = item
        return {}


class _MemDDB:
    def __init__(self) -> None:
        self.recording = _MemTable("ovius-recording", ("recording_id",))
        self.cycle = _MemTable("ovius-haul-cycle", ("cycle_id",))
        self.segment = _MemTable("ovius-state-segment", ("segment_id",))
        self.ingest = _MemTable("ovius-ingest-state", ("pk", "sk"))
        self._map = {
            "ovius-recording": self.recording,
            "ovius-haul-cycle": self.cycle,
            "ovius-state-segment": self.segment,
            "ovius-ingest-state": self.ingest,
        }

    def Table(self, name: str) -> _MemTable:
        return self._map[name]


class _MemS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, str]] = {}
        self.downloads: list[str] = []

    def seed(self, bucket: str, key: str, data: bytes, etag: str = '"e1"') -> None:
        self.objects[(bucket, key)] = (data, etag)

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if (Bucket, Key) not in self.objects:
            raise _not_found()
        data, etag = self.objects[(Bucket, Key)]
        return {"ETag": etag, "ContentLength": len(data)}

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        if (bucket, key) not in self.objects:
            raise _not_found()
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_bytes(self.objects[(bucket, key)][0])
        self.downloads.append(key)

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **_kw: Any) -> dict[str, Any]:
        self.objects[(Bucket, Key)] = (Body, '"put"')
        return {"ETag": '"put"'}


class _MemSQS:
    def __init__(self, batches: list[list[dict[str, Any]]]) -> None:
        self._batches = list(batches)
        self.deleted: list[str] = []

    def receive_message(self, **_kw: Any) -> dict[str, Any]:
        if self._batches:
            return {"Messages": self._batches.pop(0)}
        return {}

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> dict[str, Any]:
        _ = QueueUrl
        self.deleted.append(ReceiptHandle)
        return {}


def test_patch_extracted_bundle_removes_drawn_and_upgrades_sv5xl(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    (bundle / "models").mkdir(parents=True)
    (bundle / "models" / "S-v3-dfinex-wide.dfine.json").write_text("{}")
    (bundle / "pipeline.json").write_text(
        json.dumps(
            {
                "name": "PTZ tracker + heuristic state",
                "nodes": [
                    {"id": "video", "kind": "video", "params": {"fps": 7.0}},
                    {"id": "detect", "kind": "detect", "params": {"model": "old", "min_confidence": 0.55}},
                    {"id": "drawn", "kind": "annotated_video", "params": {"file": "annotated.mp4"}},
                    {"id": "python-1", "kind": "python", "params": {}},
                ],
            }
        )
    )
    (bundle / "detector-detect.json").write_text(
        json.dumps(
            {
                "name": "S-v3-dfinex-wide",
                "classes": ["excavator", "dumptruck", "bulldozer"],
                "steps": [
                    {
                        "model": "models/S-v3-dfinex-wide.pth",
                        "preprocess": {"code": [{"name": "v3 Bandpass-Chroma CLAHE", "source": "def preprocess(i): return i"}]},
                        "rules": {"min_confidence": 0.55},
                    }
                ],
            }
        )
    )
    (bundle / "run_pipeline.py").write_text(
        'from types import SimpleNamespace\n'
        'def node(*_a, **_kw): return lambda f: f\n'
        'def node_ctx(ctx, n):\n'
        '    frames = ctx.frames\n'
        '    path = lambda name: name\n'
        '    return SimpleNamespace(video=ctx.video, out=ctx.out, frames=frames, fps=frames.vfps / frames.every, video_fps=frames.vfps,\n'
        '                           every=frames.every, node=n["id"], path=path)\n'
        '@node("video")\n'
        'def video(ctx, p):\n'
        '    pass\n\n'
        '# ---- frame by frame ----\n'
        'def relink(ctx, p, tracks, views):\n'
        '    span = None\n'
        '    frames = ctx.frames\n'
        '    mask = None\n'
        + builder.RELINK_OLD_SCAN
        + "\n"
    )
    (bundle / "run_workflow.py").write_text(
        'import json, os, sys\nfrom pathlib import Path\nimport numpy as np\nMAX_DETECTIONS = 300\n\n'
        'class DFineModel:\n    pass\n\n'
        'class Workflow:\n'
        '    def _load(self, models, file, device):\n'
        '        models[file] = DFineModel(file, device) if file.suffix == ".pth" else Model(file, device)\n'
    )
    (bundle / "requirements.txt").write_text("numpy\nimageio-ffmpeg\nultralytics>=8.4,<9\n")

    info = builder.patch_extracted_bundle(tmp_path)
    assert info["removed_nodes"] == ["drawn"]
    assert info["remaining_node_count"] == 3
    assert not (bundle / "models" / "S-v3-dfinex-wide.dfine.json").exists()

    pip = json.loads((bundle / "pipeline.json").read_text())
    assert all(n["id"] != "drawn" for n in pip["nodes"])
    vid_node = next(n for n in pip["nodes"] if n["id"] == "video")
    assert vid_node["params"]["fps"] == 3.0
    det_node = next(n for n in pip["nodes"] if n["id"] == "detect")
    assert det_node["params"]["model"] == builder.SV5XL_MODEL_ID
    assert det_node["params"]["class_map"] == {"negative_vehicle": None}
    assert det_node["params"]["class_confidence"] == {"excavator": 0.56, "dumptruck": 0.56}

    det_json = json.loads((bundle / "detector-detect.json").read_text())
    assert det_json["steps"][0]["preprocess"]["code"][0]["name"] == "v3 Bandpass-Chroma CLAHE (GPU)"
    assert "_gauss_blur_gpu" in det_json["steps"][0]["preprocess"]["code"][0]["source"]

    rp_src = (bundle / "run_pipeline.py").read_text()
    assert "start=ctx.start" in rp_src
    assert "frame_skip_every=" in rp_src
    assert "ThreadPoolExecutor" in rp_src
    assert "CAP_PROP_POS_FRAMES" in rp_src
    assert "if len(span) > 1 and wanted:" in rp_src
    assert "imageio-ffmpeg" not in (bundle / "requirements.txt").read_text()

    # Patched Python files must compile cleanly
    py_compile.compile(str(bundle / "run_workflow.py"), doraise=True)
    py_compile.compile(str(bundle / "run_pipeline.py"), doraise=True)


def test_parse_pipeline_progress_line_computes_skip_rtf_pct_and_eta() -> None:
    state: dict[str, Any] = {}
    assert parse_pipeline_progress_line(
        "[video] vfps=24.000 | frame_skip_every=8 | analysed_fps=3.000 | total_frames=7200 | examined_frames=900",
        state,
    )
    assert state["every"] == 8
    assert state["examined_frames"] == 900
    assert state["duration_s"] == 300.0
    assert "skip=8" in format_live_stage_label(state)

    assert parse_pipeline_progress_line(" 150.0 s of video | 450 frames | 18.00 frames/s", state)
    assert state["pct"] == 61.5
    assert state["rtf"] == 0.167
    assert state["eta_s"] == 25
    label = format_live_stage_label(state)
    assert "Frame 450/900" in label
    assert "skip=8" in label
    assert "RTF 0.17x" in label
    assert "ETA 25s" in label


def test_ec2_pipeline_worker_multichunk_sync_publish_and_auto_shutdown(tmp_path: Path) -> None:
    ddb = _MemDDB()
    s3 = _MemS3()
    bucket = "ovius-pipeline-apse1-610331148578"

    # Build tiny valid ptz_code_bundle.tar.gz in memory
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        data = b"print('ok')\n"
        ti = tarfile.TarInfo("bundle/run_pipeline.py")
        ti.size = len(data)
        tf.addfile(ti, io.BytesIO(data))
    s3.seed(bucket, "pipeline/ptz_code_bundle.tar.gz", buf.getvalue(), etag='"code-v1"')
    s3.seed(
        bucket,
        "models/s-v5-xl/manifest.json",
        json.dumps(
            {
                "artifacts": {
                    "dfine_config": {"file": "s_v5_xl.dfine.json"},
                    "tensorrt_fp16_sm86": {"file": "s_v5_xl_1920x1088_fp16_sm86.engine"},
                    "ema_fp16_safetensors": {"file": "s_v5_xl_ema_fp16.safetensors"},
                }
            }
        ).encode(),
        etag='"man-v1"',
    )
    s3.seed(bucket, "models/s-v5-xl/s_v5_xl.dfine.json", b"{}", etag='"cfg-v1"')
    s3.seed(bucket, "models/s-v5-xl/s_v5_xl_1920x1088_fp16_sm86.engine", b"TRT", etag='"eng-v1"')
    s3.seed(bucket, "models/s-v5-xl/s_v5_xl_ema_fp16.safetensors", b"ST", etag='"st-v1"')
    s3.seed(bucket, "uploads/site/upl1/rec_e2e/chunk_00.mp4", b"MP4-CHUNK-0")
    s3.seed(bucket, "uploads/site/upl1/rec_e2e/chunk_01.mp4", b"MP4-CHUNK-1")

    ddb.ingest.put_item(
        Item={
            "pk": "SESSION#upl1",
            "sk": "REC#rec_e2e",
            "recording_id": "rec_e2e",
            "camera_id": "CAM-G5-01",
            "timestamp": "2026-11-19T08:00:00Z",
            "location_tag": "Section A",
            "tags": ["pit:north"],
            "filename": "morning_shift.mp4",
        }
    )
    ddb.ingest.put_item(
        Item={
            "pk": "REC#rec_e2e",
            "sk": "CHUNK#0000",
            "chunk_index": 0,
            "s3_key": "uploads/site/upl1/rec_e2e/chunk_00.mp4",
            "start_offset_s": Decimal("0"),
            "duration_s": Decimal("3600"),
        }
    )
    ddb.ingest.put_item(
        Item={
            "pk": "REC#rec_e2e",
            "sk": "CHUNK#0001",
            "chunk_index": 1,
            "s3_key": "uploads/site/upl1/rec_e2e/chunk_01.mp4",
            "start_offset_s": Decimal("3600"),
            "duration_s": Decimal("3600"),
        }
    )

    cmds_seen: list[list[str]] = []
    shutdown_called: list[bool] = []

    def _fake_cmd(cmd: list[str], **_kw: Any) -> subprocess.CompletedProcess[str]:
        cmds_seen.append(cmd)
        if cmd[0] == "ffmpeg":
            Path(cmd[-1]).write_bytes(b"JOINED-MP4")
        else:
            out_idx = cmd.index("--out") + 1
            out_dir = Path(cmd[out_idx]) / "rec_e2e__PTZ_tracker"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "recording.json").write_text(
                json.dumps([{"recording_id": "wrong_stem", "duration_s": 7200.5, "fps": 25.0, "resolution": "1920x1080"}])
            )
            (out_dir / "haul_cycle.json").write_text(
                json.dumps(
                    [
                        {
                            "recording_id": "wrong_stem",
                            "cycle_id": "wrong_stem-T001",
                            "excavator_id": "E1",
                            "excavator_sort_key": "E1#000120000",
                            "truck_label": "T12",
                            "cycle_status": "complete",
                            "t_load_start_s": 120.5,
                            "empty_attr": "",
                            "truck_segments": ["wrong_stem#T12#000100000"],
                            "scoops": [{"scoop_number": 1, "excavator_segment_id": "wrong_stem#E1#000120000", "start_s": 120.5, "dump_s": 132.0, "end_s": 144.0}],
                        }
                    ]
                )
            )
            (out_dir / "state_segment.json").write_text(
                json.dumps(
                    [
                        {
                            "segment_id": "wrong_stem#E1#000120000",
                            "recording_id": "wrong_stem",
                            "machine_id": "E1",
                            "state": "loading_scoop",
                            "start_s": 120.5,
                            "end_s": 144.0,
                            "cycle_id": "wrong_stem-T001",
                            "cycle_sort_key": "wrong_stem-T001#000120000",
                        },
                        {
                            "segment_id": "wrong_stem#E1#000010000",
                            "recording_id": "wrong_stem",
                            "machine_id": "E1",
                            "state": "idle",
                            "start_s": 10.0,
                            "end_s": 50.0,
                            "cycle_id": "",
                            "cycle_sort_key": "",
                        },
                    ]
                )
            )
            (out_dir / "boxes.csv").write_text("frame,id,cls,conf,x1,y1,x2,y2,t\n0,1,excavator,0.9,10,20,100,200,0.0\n")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    sqs = _MemSQS(
        batches=[
            [{"Body": json.dumps({"upload_id": "upl1", "recording_id": "rec_e2e"}), "ReceiptHandle": "rh-1"}],
            [],
            [],
        ]
    )

    worker = Ec2PipelineWorker(
        bucket_name=bucket,
        ready_queue_url="https://sqs.ap-southeast-1.amazonaws.com/610331148578/ovius-ready-recordings",
        pipeline_dir=tmp_path / "opt_pipeline",
        ddb_resource=ddb,
        s3_client=s3,
        sqs_client=sqs,
        run_cmd_fn=_fake_cmd,
        shutdown_fn=lambda: shutdown_called.append(True),
        tmp_dir=tmp_path / "scratch",
        auto_shutdown=True,
    )
    (tmp_path / "scratch").mkdir()

    summary = worker.run_loop(max_empty_polls=1, poll_wait_s=0)
    assert summary["processed_recordings"] == ["rec_e2e"]
    assert summary["final_worker_status"] == "IDLE"
    assert summary["shutdown_triggered"] is True
    assert shutdown_called == [True]
    assert sqs.deleted == ["rh-1"]

    # Verified lossless ffmpeg concat ran for 2 chunks + run_pipeline.py ran once
    assert cmds_seen[0][0] == "ffmpeg"
    assert any("run_pipeline.py" in a for a in cmds_seen[1])

    # Verified recording/cycle/segment normalized IDs, UTC +00:00 timestamp, truck_segments, cycle_id="NONE"
    rec_row = ddb.recording.items[("rec_e2e",)]
    assert rec_row["camera_id"] == "CAM-G5-01"
    assert rec_row["timestamp"] == "2026-11-19T08:00:00+00:00"
    assert len(rec_row["chunk_keys"]) == 2
    assert rec_row["parquet_keys"] == [
        "parquet/year=2026/month=11/day=19/zone=CAM-G5-01/rec_e2e_chunk_00.parquet"
    ]
    assert ("rec_e2e-T001",) in ddb.cycle.items
    cyc = ddb.cycle.items[("rec_e2e-T001",)]
    assert "empty_attr" not in cyc
    assert cyc["truck_segments"] == ["rec_e2e#T12#000100000"]
    assert cyc["scoops"][0]["excavator_segment_id"] == "rec_e2e#E1#000120000"
    assert ("rec_e2e#E1#000120000",) in ddb.segment.items
    idle_seg = ddb.segment.items[("rec_e2e#E1#000010000",)]
    assert idle_seg["cycle_id"] == "NONE"
    assert "cycle_sort_key" not in idle_seg
    assert ddb.ingest.items[("SESSION#upl1", "REC#rec_e2e")]["process_status"] == "COMPLETED"

    # Verified S3 parquet written (binary PAR1 when pyarrow installed, else valid JSON-lines)
    pq_bytes, _ = s3.objects[(bucket, "parquet/year=2026/month=11/day=19/zone=CAM-G5-01/rec_e2e_chunk_00.parquet")]
    assert pq_bytes[:4] == b"PAR1" or json.loads(pq_bytes.decode("utf-8").splitlines()[0])["cls"] == "excavator"

    # Second ETag sync skips re-downloading unchanged bundle & model files
    prev_downloads = len(s3.downloads)
    sync2 = worker.sync_pipeline_assets_from_s3()
    assert sync2["updated_files"] == []
    assert len(s3.downloads) == prev_downloads


def test_worker_aborted_short_circuit_and_poison_pill_recovery(tmp_path: Path, monkeypatch: Any) -> None:
    ddb = _MemDDB()
    s3 = _MemS3()
    bucket = "ovius-pipeline-apse3-610331148578"
    s3.seed(bucket, "uploads/site/upl1/rec_poison/chunk_00.mp4", b"BAD")
    s3.seed(bucket, "uploads/site/upl1/rec_good/chunk_00.mp4", b"OK")

    # 1. Aborted recording
    ddb.ingest.put_item(
        Item={
            "pk": "SESSION#upl1",
            "sk": "REC#rec_aborted",
            "recording_id": "rec_aborted",
            "upload_status": "ABORTED",
            "process_status": "ABORTED",
        }
    )
    # 2. Poison pill recording (run_pipeline will raise)
    ddb.ingest.put_item(
        Item={
            "pk": "SESSION#upl1",
            "sk": "REC#rec_poison",
            "recording_id": "rec_poison",
            "camera_id": "CAM-1",
            "timestamp": "2026-11-19T08:00:00Z",
        }
    )
    ddb.ingest.put_item(
        Item={
            "pk": "REC#rec_poison",
            "sk": "CHUNK#0000",
            "chunk_index": 0,
            "s3_key": "uploads/site/upl1/rec_poison/chunk_00.mp4",
        }
    )
    # 3. Healthy recording after the poison pill
    ddb.ingest.put_item(
        Item={
            "pk": "SESSION#upl1",
            "sk": "REC#rec_good",
            "recording_id": "rec_good",
            "camera_id": "CAM-1",
            "timestamp": "2026-11-19T08:00:00Z",
        }
    )
    ddb.ingest.put_item(
        Item={
            "pk": "REC#rec_good",
            "sk": "CHUNK#0000",
            "chunk_index": 0,
            "s3_key": "uploads/site/upl1/rec_good/chunk_00.mp4",
        }
    )

    encode_calls: list[str] = []
    import ovius_api.ec2_pipeline_worker as worker_mod

    orig_encode = worker_mod._csv_to_snappy_parquet_bytes

    def _counting_encode(p: Path) -> bytes:
        encode_calls.append(p.name)
        return orig_encode(p)

    monkeypatch.setattr(worker_mod, "_csv_to_snappy_parquet_bytes", _counting_encode)

    def _cmd(cmd: list[str], **_kw: Any) -> subprocess.CompletedProcess[str]:
        if any("rec_poison" in arg for arg in cmd):
            raise RuntimeError("corrupt moov atom")
        out_dir = Path(cmd[cmd.index("--out") + 1]) / "rec_good__PTZ"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "recording.json").write_text(json.dumps([{"recording_id": "rec_good", "duration_s": 10.0}]))
        (out_dir / "haul_cycle.json").write_text("[]")
        (out_dir / "state_segment.json").write_text("[]")
        (out_dir / "boxes.csv").write_text("frame,id,cls,conf,x1,y1,x2,y2,t\n0,1,excavator,0.9,1,2,3,4,0.0\n")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    sqs = _MemSQS(
        batches=[
            [{"Body": json.dumps({"upload_id": "upl1", "recording_id": "rec_aborted"}), "ReceiptHandle": "rh-abort"}],
            [{"Body": json.dumps({"upload_id": "upl1", "recording_id": "rec_poison"}), "ReceiptHandle": "rh-poison"}],
            [{"Body": json.dumps({"upload_id": "upl1", "recording_id": "rec_good"}), "ReceiptHandle": "rh-good"}],
            [],
            [],
        ]
    )
    shutdown_called: list[bool] = []
    worker = Ec2PipelineWorker(
        bucket_name=bucket,
        ready_queue_url="https://sqs.ap-southeast-3.amazonaws.com/610331148578/ovius-ready-recordings",
        pipeline_dir=tmp_path / "opt",
        ddb_resource=ddb,
        s3_client=s3,
        sqs_client=sqs,
        run_cmd_fn=_cmd,
        shutdown_fn=lambda: shutdown_called.append(True),
        tmp_dir=tmp_path / "scratch_auto_created",
        auto_shutdown=True,
    )

    summary = worker.run_loop(max_empty_polls=1, poll_wait_s=0)
    assert summary["processed_recordings"] == ["rec_good"]
    assert summary["failed_recordings"] == ["rec_poison"]
    assert summary["final_worker_status"] == "IDLE"
    assert shutdown_called == [True]
    # Aborted and healthy messages deleted; poison message left for SQS DLQ retry
    assert sqs.deleted == ["rh-abort", "rh-good"]
    assert ddb.ingest.items[("SESSION#upl1", "REC#rec_poison")]["process_status"] == "FAILED"
    assert ddb.ingest.items[("WORKER#LOCK", "PRIMARY")]["status"] == "IDLE"
    # boxes.csv encoded only ONCE across both primary + archive Parquet uploads
    assert encode_calls.count("boxes.csv") == 1

