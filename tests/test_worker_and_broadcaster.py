"""Unit tests for VisionWorkerRunner, checkpointing, EMF logs, broadcaster, and multi-chunk URLs."""

from __future__ import annotations

from decimal import Decimal
import io
import json
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from ovius_api.broadcaster import lambda_handler as broadcaster_handler
from ovius_api.handler import lambda_handler as api_handler
from ovius_api.repository import TelemetryRepository
from ovius_api.worker import VisionWorkerRunner, build_emf_log


def _client_error(code: str, status: int = 400) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "MockOperation",
    )


class _BatchWriter:
    def __init__(self, table: "_InMemTable") -> None:
        self._table = table

    def __enter__(self) -> "_BatchWriter":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def put_item(self, *, Item: dict[str, Any]) -> None:
        self._table.put_item(Item=Item)


class _InMemTable:
    def __init__(self, name: str, pk_fields: tuple[str, ...]) -> None:
        self.name = name
        self._pk_fields = pk_fields
        self.items: dict[tuple[Any, ...], dict[str, Any]] = {}
        self.queries: list[dict[str, Any]] = []

    def _key_tuple(self, item: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(item[k] for k in self._pk_fields)

    def put_item(self, *, Item: dict[str, Any]) -> dict[str, Any]:
        self.items[self._key_tuple(Item)] = dict(Item)
        return {}

    def get_item(self, *, Key: dict[str, Any]) -> dict[str, Any]:
        item = self.items.get(self._key_tuple(Key))
        return {"Item": dict(item)} if item is not None else {}

    def delete_item(self, *, Key: dict[str, Any]) -> dict[str, Any]:
        self.items.pop(self._key_tuple(Key), None)
        return {}

    def batch_writer(self) -> _BatchWriter:
        return _BatchWriter(self)

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.queries.append(kwargs)
        index_name = kwargs.get("IndexName")
        if index_name == "GSI_ACTIVE_UPLOADS":
            assert "Limit" not in kwargs
            active = [dict(it) for it in self.items.values() if it.get("GSI1PK") == "ACTIVE_UPLOAD"]
            active.sort(key=lambda it: float(it.get("GSI1SK", 999999)))
            f_expr = kwargs.get("FilterExpression")
            if f_expr is not None:
                cutoff = int(f_expr.get_expression()["values"][1])
                active = [it for it in active if int(it.get("last_heartbeat_at", 0)) >= cutoff]
            return {"Items": active}

        k_expr = kwargs["KeyConditionExpression"].get_expression()
        vals = k_expr.get("values", ())
        if self._pk_fields == ("pk", "sk"):
            pk_val = vals[1] if not hasattr(vals[0], "get_expression") else vals[0].get_expression()["values"][1]
            matched = [dict(it) for (pk, _), it in self.items.items() if pk == pk_val]
            if hasattr(vals[0], "get_expression") and vals[1].get_expression().get("operator") == "begins_with":
                prefix = vals[1].get_expression()["values"][1]
                matched = [it for it in matched if str(it["sk"]).startswith(prefix)]
            matched.sort(key=lambda it: str(it["sk"]))
            return {"Items": matched}

        # Support recording_id / camera_id queries in repository tests
        target = vals[1] if len(vals) > 1 and not hasattr(vals[0], "get_expression") else None
        if target is None:
            return {"Items": [dict(v) for v in self.items.values()]}
        return {
            "Items": [
                dict(v)
                for v in self.items.values()
                if v.get("recording_id") == target or v.get("camera_id") == target
            ]
        }

    def scan(self, **_kwargs: Any) -> dict[str, Any]:
        return {"Items": [dict(v) for v in self.items.values()]}

    def update_item(
        self,
        *,
        Key: dict[str, Any],
        UpdateExpression: str,
        ExpressionAttributeNames: dict[str, str] | None = None,
        ExpressionAttributeValues: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        _ = ExpressionAttributeNames
        kt = self._key_tuple(Key)
        item = dict(self.items.get(kt, Key))
        vals = ExpressionAttributeValues or {}
        if "#st = :st" in UpdateExpression:
            item["status"] = vals[":st"]
            item["lease_expires_at"] = vals[":lease"]
            if ":rec" in vals:
                item["active_recording_id"] = vals[":rec"]
            item["updated_at"] = vals[":now"]
        elif "active_chunk_index = :cidx" in UpdateExpression:
            item["process_status"] = vals[":proc"]
            item["active_chunk_index"] = vals[":cidx"]
            item["active_chunk_frame_progress_pct"] = vals[":pct"]
            item["updated_at"] = vals[":now"]
        elif "last_completed_chunk_index = :done" in UpdateExpression:
            if ":completed" in vals:
                item["process_status"] = vals[":completed"]
            item["last_completed_chunk_index"] = vals[":done"]
            item["processed_chunks"] = vals[":pc"]
            item["next_cycle_seq"] = vals[":ncs"]
            item["next_segment_seq"] = vals[":nss"]
            item["cycles_extracted_so_far"] = vals[":ces"]
            item["active_chunk_frame_progress_pct"] = vals[":pct"]
            item["updated_at"] = vals[":now"]
        self.items[kt] = item
        return {"Attributes": dict(item)}


class _InMemMetaClient:
    def __init__(self, rec_table: _InMemTable) -> None:
        self._rec = rec_table

    def batch_get_item(self, RequestItems: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, list[dict[str, Any]]] = {}
        for tname, spec in RequestItems.items():
            out[tname] = [
                dict(self._rec.items[(k["recording_id"],)])
                for k in spec["Keys"]
                if (k["recording_id"],) in self._rec.items
            ]
        return {"Responses": out, "UnprocessedKeys": {}}


class _InMemDDB:
    def __init__(self) -> None:
        self.ingest = _InMemTable("ovius-ingest-state", ("pk", "sk"))
        self.recording = _InMemTable("ovius-recording", ("recording_id",))
        self.cycle = _InMemTable("ovius-haul-cycle", ("cycle_id",))
        self.segment = _InMemTable("ovius-state-segment", ("segment_id",))
        self.meta = type("_Meta", (), {"client": _InMemMetaClient(self.recording)})()

    def Table(self, name: str) -> _InMemTable:
        if "ingest-state" in name:
            return self.ingest
        if "recording" in name:
            return self.recording
        if "haul-cycle" in name:
            return self.cycle
        return self.segment


class _InMemS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.downloaded_keys: list[str] = []

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **_kwargs: Any) -> dict[str, Any]:
        self.objects[(Bucket, Key)] = bytes(Body)
        return {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if (Bucket, Key) not in self.objects:
            raise _client_error("NoSuchKey", 404)
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        self.downloaded_keys.append(key)
        Path(filename).write_bytes(self.objects[(bucket, key)])

    def generate_presigned_url(
        self,
        op: str,
        *,
        Params: dict[str, Any],
        ExpiresIn: int,
        HttpMethod: str,
    ) -> str:
        assert op == "get_object" and HttpMethod == "GET"
        return f"https://{Params['Bucket']}.s3.ap-southeast-1.amazonaws.com/{Params['Key']}?X-Amz-Expires={ExpiresIn}"


class _InMemSQS:
    def __init__(self, script_batches: list[list[dict[str, Any]]] | None = None) -> None:
        self.batches = list(script_batches or [])
        self.deleted: list[str] = []
        self.renewed: list[tuple[str, int]] = []

    def receive_message(self, **_kwargs: Any) -> dict[str, Any]:
        if not self.batches:
            return {"Messages": []}
        msgs = self.batches.pop(0)
        return {"Messages": msgs}

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> dict[str, Any]:
        _ = QueueUrl
        self.deleted.append(ReceiptHandle)
        return {}

    def change_message_visibility(
        self, *, QueueUrl: str, ReceiptHandle: str, VisibilityTimeout: int
    ) -> dict[str, Any]:
        _ = QueueUrl
        self.renewed.append((ReceiptHandle, VisibilityTimeout))
        return {}


def _seed_two_chunk_recording(
    ddb: _InMemDDB,
    s3: _InMemS3,
    *,
    upload_id: str = "upl_w1",
    rec_id: str = "rec_w1",
    bucket: str = "ovius-pipeline-apse1-610331148578",
) -> tuple[str, str]:
    ddb.ingest.put_item(
        Item={
            "pk": f"SESSION#{upload_id}",
            "sk": f"REC#{rec_id}",
            "upload_id": upload_id,
            "recording_id": rec_id,
            "camera_id": "CAM-ALPHA-01",
            "location_tag": "Section Alpha",
            "filename": f"{rec_id}.mp4",
            "timestamp": "2026-11-18T06:00:00Z",
            "duration_s": Decimal("7200"),
            "fps": Decimal("25"),
            "resolution": "1920x1080",
            "tags": ["section:Alpha"],
            "upload_status": "READY",
            "last_completed_chunk_index": -1,
            "next_cycle_seq": 0,
            "next_segment_seq": 0,
            "cycles_extracted_so_far": 0,
        }
    )
    k0 = f"uploads/open_pit_mine_01/{upload_id}/{rec_id}/chunk_00.mp4"
    k1 = f"uploads/open_pit_mine_01/{upload_id}/{rec_id}/chunk_01.mp4"
    for idx, key, off in [(0, k0, 0), (1, k1, 3600)]:
        ddb.ingest.put_item(
            Item={
                "pk": f"REC#{rec_id}",
                "sk": f"CHUNK#{idx:04d}",
                "upload_id": upload_id,
                "recording_id": rec_id,
                "chunk_index": idx,
                "s3_key": key,
                "start_offset_s": Decimal(str(off)),
                "duration_s": Decimal("3600"),
                "byte_size": 1024,
            }
        )
        s3.put_object(Bucket=bucket, Key=key, Body=b"ftypisom-mock-chunk")
    return k0, k1


def test_worker_defers_recording_publish_and_close_all_until_final_chunk(tmp_path: Path) -> None:
    ddb = _InMemDDB()
    s3 = _InMemS3()
    sqs = _InMemSQS()
    bucket = "ovius-pipeline-apse1-610331148578"
    _seed_two_chunk_recording(ddb, s3, bucket=bucket)

    saw_recording_after_chunk0: list[bool] = []
    saw_ckpt_after_chunk0: list[bool] = []
    orig_save_ckpt = None

    runner = VisionWorkerRunner(
        ddb_resource=ddb,
        s3_client=s3,
        sqs_client=sqs,
        bucket_name=bucket,
        ready_queue_url="https://sqs.ap-southeast-1.amazonaws.com/610331148578/ovius-ready-recordings",
        tmp_dir=str(tmp_path),
    )
    orig_save_ckpt = runner._save_checkpoint

    def _spy_save_ckpt(rec_id: str, state: dict[str, Any]) -> str:
        saw_recording_after_chunk0.append((rec_id,) in ddb.recording.items)
        saw_ckpt_after_chunk0.append("EX1#TRK_CARRY" in state.get("open_cycles", {}))
        return orig_save_ckpt(rec_id, state)

    runner._save_checkpoint = _spy_save_ckpt  # type: ignore[method-assign]

    res = runner.process_recording(upload_id="upl_w1", recording_id="rec_w1")
    assert res["chunks_processed"] == 2
    assert res["published_recording"] is True
    assert saw_recording_after_chunk0 == [False]
    assert saw_ckpt_after_chunk0 == [True]

    # Final recording row published with complete chunk_keys manifest & parquet_keys
    rec_row = ddb.recording.items[("rec_w1",)]
    assert len(rec_row["chunk_keys"]) == 2
    assert len(rec_row["parquet_keys"]) == 2

    # Deterministic PKs & Rule 4.2 incomplete cycle only at end of recording
    assert ("rec_w1_cyc_0000",) in ddb.cycle.items
    assert ("rec_w1_seg_000000",) in ddb.segment.items
    statuses = [c["cycle_status"] for c in ddb.cycle.items.values()]
    assert statuses.count("complete") == 5  # 2 in c0 + 1 carried across hour + 2 in c1
    assert statuses.count("incomplete") == 1
    incomplete = next(c for c in ddb.cycle.items.values() if c["cycle_status"] == "incomplete")
    assert incomplete["cycle_duration_s"] is None

    # Temp chunk files deleted after processing
    assert list(tmp_path.glob("*.mp4")) == []
    # Single-line EMF records emitted per chunk with low-cardinality dimensions
    assert len(runner.emf_logs) == 2
    assert runner.emf_logs[0]["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [
        ["ModelVersion", "InstanceType"]
    ]


def test_worker_resumes_from_checkpoint_and_skips_completed_chunk(tmp_path: Path) -> None:
    ddb = _InMemDDB()
    s3 = _InMemS3()
    sqs = _InMemSQS()
    bucket = "ovius-pipeline-apse1-610331148578"
    _, k1 = _seed_two_chunk_recording(ddb, s3, upload_id="upl_res", rec_id="rec_res", bucket=bucket)

    # Simulate crash after chunk_00 completed
    ddb.ingest.items[("SESSION#upl_res", "REC#rec_res")]["last_completed_chunk_index"] = 0
    s3.put_object(
        Bucket=bucket,
        Key="checkpoints/rec_res/checkpoint_latest.json",
        Body=json.dumps(
            {
                "last_completed_chunk_index": 0,
                "next_cycle_seq": 3,
                "next_segment_seq": 8,
                "cycles_extracted_so_far": 2,
                "open_cycles": {
                    "EX1#TRK_CARRY": {
                        "cycle_seq": 2,
                        "truck_label": "HT-105",
                        "t_arrive_s": 3555.0,
                        "t_spot_s": 3567.0,
                        "t_load_start_s": 3575.0,
                    }
                },
                "parquet_keys": ["parquet/year=2026/month=11/day=18/zone=CAM-ALPHA-01/rec_res_chunk_00.parquet"],
            }
        ).encode("utf-8"),
    )

    runner = VisionWorkerRunner(
        ddb_resource=ddb,
        s3_client=s3,
        sqs_client=sqs,
        bucket_name=bucket,
        ready_queue_url="https://sqs.ap-southeast-1.amazonaws.com/610331148578/ovius-ready-recordings",
        tmp_dir=str(tmp_path),
    )
    res = runner.process_recording(upload_id="upl_res", recording_id="rec_res")
    assert res["chunks_processed"] == 1
    assert s3.downloaded_keys == [k1]
    assert ("rec_res_cyc_0002",) in ddb.cycle.items
    assert ddb.cycle.items[("rec_res_cyc_0002",)]["cycle_status"] == "complete"
    assert ("rec_res",) in ddb.recording.items


def test_worker_keep_warm_and_atomic_exiting_handshake(tmp_path: Path) -> None:
    clock = [1_700_010_000.0]
    ddb = _InMemDDB()
    s3 = _InMemS3()
    bucket = "ovius-pipeline-apse1-610331148578"
    _seed_two_chunk_recording(ddb, s3, upload_id="upl_kw1", rec_id="rec_kw1", bucket=bucket)
    _seed_two_chunk_recording(ddb, s3, upload_id="upl_kw2", rec_id="rec_kw2", bucket=bucket)

    # Fresh active upload with ETA 120s (< 300s) triggers KEEP_WARM_WAIT; then transitions to READY
    ddb.ingest.put_item(
        Item={
            "pk": "SESSION#upl_active",
            "sk": "REC#rec_active",
            "upload_status": "UPLOADING",
            "upload_eta_s": Decimal("120"),
            "last_heartbeat_at": int(clock[0] - 10),
            "GSI1PK": "ACTIVE_UPLOAD",
            "GSI1SK": Decimal("120"),
        }
    )

    # Poll 1: rec_kw1; Poll 2: empty (triggers KEEP_WARM_WAIT); Poll 3: empty after active finishes;
    # Poll 4 (final EXITING re-poll): rec_kw2 arrives! Poll 5: empty -> EXITING -> empty -> IDLE
    sqs = _InMemSQS(
        script_batches=[
            [{"Body": json.dumps({"upload_id": "upl_kw1", "recording_id": "rec_kw1"}), "ReceiptHandle": "rh1"}],
            [],
            [],
            [{"Body": json.dumps({"upload_id": "upl_kw2", "recording_id": "rec_kw2"}), "ReceiptHandle": "rh2"}],
            [],
            [],
        ]
    )

    def _sleep(sec: float) -> None:
        clock[0] += sec
        # Simulate active upload completing so next empty poll proceeds to EXITING handshake
        ddb.ingest.items[("SESSION#upl_active", "REC#rec_active")].pop("GSI1PK", None)

    runner = VisionWorkerRunner(
        ddb_resource=ddb,
        s3_client=s3,
        sqs_client=sqs,
        bucket_name=bucket,
        ready_queue_url="https://sqs.ap-southeast-1.amazonaws.com/610331148578/ovius-ready-recordings",
        now_fn=lambda: clock[0],
        sleep_fn=_sleep,
        tmp_dir=str(tmp_path),
    )
    summary = runner.run_loop(max_empty_polls=1, poll_wait_s=0, keep_warm_cap_s=300, keep_warm_step_s=10)
    assert summary["processed_recordings"] == ["rec_kw1", "rec_kw2"]
    assert summary["keep_warm_waited_s"] == 10
    assert summary["final_worker_status"] == "IDLE"
    assert ddb.ingest.items[("WORKER#LOCK", "PRIMARY")]["status"] == "IDLE"
    assert sqs.deleted == ["rh1", "rh2"]


def test_broadcaster_stream_push_and_410_cleanup() -> None:
    ddb = _InMemDDB()
    ddb.ingest.put_item(
        Item={"pk": "WS#upl_ws", "sk": "CONN#conn_live", "connection_id": "conn_live"}
    )
    ddb.ingest.put_item(
        Item={"pk": "WS#upl_ws", "sk": "CONN#conn_gone", "connection_id": "conn_gone"}
    )

    class _FakeMgmt:
        def __init__(self) -> None:
            self.sent: list[tuple[str, dict[str, Any]]] = []

        def post_to_connection(self, *, ConnectionId: str, Data: bytes) -> None:
            if ConnectionId == "conn_gone":
                raise _client_error("GoneException", status=410)
            self.sent.append((ConnectionId, json.loads(Data.decode("utf-8"))))

    mgmt = _FakeMgmt()
    stream_event = {
        "Records": [
            {
                "dynamodb": {
                    "Keys": {"pk": {"S": "SESSION#upl_ws"}, "sk": {"S": "REC#rec_1"}},
                    "NewImage": {
                        "recording_id": {"S": "rec_1"},
                        "process_status": {"S": "PROCESSING_CHUNK"},
                        "active_chunk_index": {"N": "1"},
                    },
                }
            },
            {
                # Should be ignored even if passed unfiltered
                "dynamodb": {
                    "Keys": {"pk": {"S": "REC#rec_1"}, "sk": {"S": "CHUNK#0000"}},
                    "NewImage": {"upload_status": {"S": "UPLOADED"}},
                }
            },
        ]
    }
    res = broadcaster_handler(
        stream_event,
        ddb_resource=ddb,
        apigw_management_client=mgmt,
    )
    assert res == {"processed_records": 1, "pushed_count": 1, "stale_deleted": 1}
    assert len(mgmt.sent) == 1
    assert mgmt.sent[0][0] == "conn_live"
    assert ("WS#upl_ws", "CONN#conn_gone") not in ddb.ingest.items
    assert ("WS#upl_ws", "CONN#conn_live") in ddb.ingest.items


def test_multichunk_videos_and_bboxes_presigned_urls() -> None:
    ddb = _InMemDDB()
    s3 = _InMemS3()
    ddb.recording.put_item(
        Item={
            "recording_id": "rec_multi_01",
            "camera_id": "CAM-ALPHA-01",
            "filename": "rec_multi_01.mp4",
            "timestamp": "2026-11-18T06:00:00Z",
            "duration_s": Decimal("7200"),
            "fps": Decimal("25.0"),
            "resolution": "1920x1080",
            "chunk_keys": [
                {
                    "chunk_index": Decimal("0"),
                    "s3_key": "uploads/site/upl/rec_multi_01/chunk_00.mp4",
                    "start_offset_s": Decimal("0"),
                    "duration_s": Decimal("3600"),
                },
                {
                    "chunk_index": Decimal("1"),
                    "s3_key": "uploads/site/upl/rec_multi_01/chunk_01.mp4",
                    "start_offset_s": Decimal("3600"),
                    "duration_s": Decimal("3600"),
                },
            ],
            "parquet_keys": [
                "parquet/year=2026/month=11/day=18/zone=CAM-ALPHA-01/rec_multi_01_chunk_00.parquet"
            ],
        }
    )
    repo = TelemetryRepository(ddb_resource=ddb, s3_client=s3)

    vid_resp = api_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/videos",
            "queryStringParameters": {"recording_id": "rec_multi_01"},
        },
        repository=repo,
    )
    vid_body = json.loads(vid_resp["body"])
    assert vid_resp["statusCode"] == 200
    chunks = vid_body["items"][0]["chunks"]
    assert len(chunks) == 2
    assert "chunk_00.mp4" in chunks[0]["presigned_url"]
    assert "chunk_01.mp4" in chunks[1]["presigned_url"]

    bbox_resp = api_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/bboxes",
            "queryStringParameters": {"recording_id": "rec_multi_01"},
        },
        repository=repo,
    )
    bbox_body = json.loads(bbox_resp["body"])
    assert bbox_resp["statusCode"] == 200
    assert "rec_multi_01_chunk_00.parquet" in bbox_body["items"][0]["presigned_url"]
    assert len(bbox_body["items"][0]["chunk_parquet_uris"]) == 1

    emf = build_emf_log(metrics={"InferenceFPS": (24.5, "Count/Second")}, properties={"recording_id": "r1"})
    assert emf["_aws"]["CloudWatchMetrics"][0]["Namespace"] == "Ovius/VisionPipeline"
    assert emf["InferenceFPS"] == 24.5
