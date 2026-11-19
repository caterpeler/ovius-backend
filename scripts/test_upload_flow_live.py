"""End-to-end live verification of Phase 2 resumable upload endpoints with mock MP4 chunks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import ssl
import struct
import sys
import time
import urllib.error
import urllib.request
import uuid

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError

_SRC_DIR = str(Path(__file__).resolve().parents[1] / "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from ovius_api.worker import VisionWorkerRunner

PART_MIN_BYTES = 5 * 1024 * 1024  # S3 requires >= 5 MiB for all non-final multipart parts


def _ssl_context() -> ssl.SSLContext:
    mac_ca = Path("/etc/ssl/cert.pem")
    if mac_ca.exists():
        return ssl.create_default_context(cafile=str(mac_ca))
    return ssl.create_default_context()


_SSL_CONTEXT = _ssl_context()


def _mp4_box(box_type: bytes, payload: bytes) -> bytes:
    assert len(box_type) == 4
    return struct.pack(">I", 8 + len(payload)) + box_type + payload


def build_mock_mp4(total_bytes: int, seed_label: str) -> bytes:
    """Synthesize a deterministic ISO BMFF (.mp4) byte stream of exact length total_bytes."""
    ftyp = _mp4_box(b"ftyp", b"isom\x00\x00\x02\x00isomiso2mp41")
    moov = _mp4_box(b"moov", _mp4_box(b"udta", seed_label.encode("ascii")))
    header = ftyp + moov
    if total_bytes <= len(header) + 8:
        raise ValueError(f"total_bytes={total_bytes} too small for MP4 box header")
    mdat_payload_len = total_bytes - len(header) - 8
    pattern = (f"OVIUS-MOCK-MP4:{seed_label}|").encode("ascii")
    repeats, rem = divmod(mdat_payload_len, len(pattern))
    mdat_payload = (pattern * repeats) + pattern[:rem]
    data = header + _mp4_box(b"mdat", mdat_payload)
    assert len(data) == total_bytes
    return data


def _api_json(
    url: str,
    *,
    method: str,
    payload: dict[str, object] | None = None,
) -> tuple[int, dict[str, str], dict[str, object]]:
    body_bytes = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body_bytes, method=method)
    if payload is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30, context=_SSL_CONTEXT) as resp:
            raw = resp.read().decode("utf-8")
            headers = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status, headers, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        headers = {k.lower(): v for k, v in exc.headers.items()}
        return exc.code, headers, json.loads(raw) if raw else {}


def _put_s3_part(presigned_url: str, data: bytes) -> str:
    req = urllib.request.Request(presigned_url, data=data, method="PUT")
    with urllib.request.urlopen(req, timeout=120, context=_SSL_CONTEXT) as resp:
        assert resp.status in {200, 204}, f"Unexpected S3 PUT status: {resp.status}"
        etag = resp.headers.get("ETag") or resp.headers.get("etag")
        assert etag, "Missing ETag header from S3 UploadPart response"
        return etag


def _get_presigned_bytes(presigned_url: str) -> bytes:
    req = urllib.request.Request(presigned_url, method="GET")
    with urllib.request.urlopen(req, timeout=120, context=_SSL_CONTEXT) as resp:
        assert resp.status == 200, f"Unexpected S3 GET status: {resp.status}"
        return resp.read()


def _query_recording_children(table: object, recording_id: str) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    kwargs: dict[str, object] = {
        "IndexName": "recording-id-index",
        "KeyConditionExpression": Key("recording_id").eq(recording_id),
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def _cleanup_test_artifacts(
    *,
    s3: object,
    ddb_table: object,
    ddb_resource: object | None,
    recording_table_name: str,
    cycle_table_name: str,
    segment_table_name: str,
    bucket: str,
    s3_keys: list[str],
    upload_ids: list[str],
    recording_ids: list[str],
) -> None:
    extra_s3_keys: set[str] = set(s3_keys)

    if ddb_resource is not None:
        rec_tbl = ddb_resource.Table(recording_table_name)
        cyc_tbl = ddb_resource.Table(cycle_table_name)
        seg_tbl = ddb_resource.Table(segment_table_name)
        for rid in recording_ids:
            try:
                rec_item = rec_tbl.get_item(Key={"recording_id": rid}).get("Item")
                if rec_item:
                    for pk_key in rec_item.get("parquet_keys", []):
                        extra_s3_keys.add(str(pk_key))
                    rec_tbl.delete_item(Key={"recording_id": rid})
            except ClientError:
                pass
            try:
                for cyc in _query_recording_children(cyc_tbl, rid):
                    cyc_tbl.delete_item(Key={"cycle_id": cyc["cycle_id"]})
            except ClientError:
                pass
            try:
                for seg in _query_recording_children(seg_tbl, rid):
                    seg_tbl.delete_item(Key={"segment_id": seg["segment_id"]})
            except ClientError:
                pass

    for rid in recording_ids:
        try:
            listed = s3.list_objects_v2(Bucket=bucket, Prefix=f"checkpoints/{rid}/")
            for obj in listed.get("Contents", []):
                extra_s3_keys.add(str(obj["Key"]))
        except ClientError:
            pass

    pks = [f"SESSION#{uid}" for uid in upload_ids] + [f"REC#{rid}" for rid in recording_ids]
    for pk in pks:
        try:
            items = ddb_table.query(KeyConditionExpression=Key("pk").eq(pk)).get("Items", [])
            for item in items:
                s3_uid = item.get("s3_upload_id")
                s3_key = item.get("s3_key")
                if s3_uid and s3_key:
                    try:
                        s3.abort_multipart_upload(
                            Bucket=bucket, Key=str(s3_key), UploadId=str(s3_uid)
                        )
                    except ClientError:
                        pass
                for pk_key in item.get("parquet_keys", []):
                    extra_s3_keys.add(str(pk_key))
                ddb_table.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})
        except ClientError:
            pass

    try:
        lock_item = ddb_table.get_item(Key={"pk": "WORKER#LOCK", "sk": "PRIMARY"}).get("Item")
        if lock_item and lock_item.get("active_recording_id") in set(recording_ids):
            ddb_table.delete_item(Key={"pk": "WORKER#LOCK", "sk": "PRIMARY"})
    except ClientError:
        pass

    for key in sorted(extra_s3_keys):
        try:
            s3.delete_object(Bucket=bucket, Key=key)
        except ClientError:
            pass


def run_live_upload_flow(
    *,
    cf_base_url: str,
    apigw_base_url: str,
    s3: object,
    ddb_table: object,
    ddb_resource: object | None,
    sqs: object | None,
    lambda_client: object | None,
    ready_queue_url: str | None,
    orchestrator_fn_name: str | None,
    bucket_name: str,
    recording_table_name: str = "ovius-recording",
    cycle_table_name: str = "ovius-haul-cycle",
    segment_table_name: str = "ovius-state-segment",
) -> dict[str, object]:
    """Exercise full mock MP4 session, resumable presign, heartbeat, complete, SQS, and orchestrator flow."""
    run_id = uuid.uuid4().hex[:8]
    upload_id = f"upl_live_{run_id}"
    rec_ready_id = f"rec_live_ok_{run_id}"
    rec_abort_id = f"rec_live_ab_{run_id}"
    rec_stale_id = f"rec_live_st_{run_id}"
    apigw_upload_id = f"upl_apigw_{run_id}"
    apigw_rec_id = f"rec_apigw_{run_id}"
    test_camera_id = f"CAM-TEST-{run_id}"

    created_s3_keys: list[str] = []
    created_upload_ids = [upload_id, apigw_upload_id]
    created_rec_ids = [rec_ready_id, rec_abort_id, rec_stale_id, apigw_rec_id]

    chunk0_bytes = build_mock_mp4(262_144, f"{rec_ready_id}:chunk00")  # 256 KiB single part
    chunk1_part1 = build_mock_mp4(PART_MIN_BYTES, f"{rec_ready_id}:chunk01:p1")  # 5 MiB part 1
    chunk1_part2 = b"OVIUS-MOCK-TAIL-" * 4096  # 64 KiB part 2
    chunk1_total_size = len(chunk1_part1) + len(chunk1_part2)
    abort_chunk_bytes = build_mock_mp4(131_072, f"{rec_abort_id}:chunk00")

    base = cf_base_url.rstrip("/")
    apigw_base = apigw_base_url.rstrip("/")

    try:
        # 1. Create 2-recording session via CloudFront (tests CloudFront POST -> APIGW -> Lambda -> DDB)
        s_create, _, body_create = _api_json(
            f"{base}/uploads/sessions",
            method="POST",
            payload={
                "upload_id": upload_id,
                "site_id": "open_pit_mine_01",
                "recordings": [
                    {
                        "recording_id": rec_ready_id,
                        "camera_id": test_camera_id,
                        "location_tag": "Section Alpha - Mock Test",
                        "timestamp": "2026-11-18T08:00:00Z",
                        "duration_s": 7200,
                        "fps": 25.0,
                        "resolution": "1920x1080",
                        "chunks": [
                            {
                                "chunk_index": 0,
                                "start_offset_s": 0,
                                "duration_s": 3600,
                                "byte_size": len(chunk0_bytes),
                            },
                            {
                                "chunk_index": 1,
                                "start_offset_s": 3600,
                                "duration_s": 3600,
                                "byte_size": chunk1_total_size,
                            },
                        ],
                    },
                    {
                        "recording_id": rec_abort_id,
                        "camera_id": "CAM-BETA-01",
                        "timestamp": "2026-11-18T08:00:00Z",
                        "duration_s": 3600,
                        "fps": 25.0,
                        "chunks": [
                            {
                                "chunk_index": 0,
                                "start_offset_s": 0,
                                "duration_s": 3600,
                                "byte_size": len(abort_chunk_bytes),
                            }
                        ],
                    },
                ],
            },
        )
        assert s_create == 201, f"Session create failed: {s_create} {body_create}"
        for rec in body_create["recordings"]:
            for ch in rec["chunks"]:
                created_s3_keys.append(str(ch["s3_key"]))

        # 2. Heartbeat PUT + Status GET
        s_hb, _, body_hb = _api_json(
            f"{base}/uploads/sessions/{upload_id}/heartbeat",
            method="PUT",
            payload={
                "smoothed_upload_bps": 500_000,
                "recordings": [
                    {"recording_id": rec_ready_id, "uploaded_bytes": 1_000_000},
                    {"recording_id": rec_abort_id, "uploaded_bytes": 32_768},
                ],
            },
        )
        assert s_hb == 200, f"Heartbeat PUT failed: {s_hb} {body_hb}"

        s_stat1, _, body_stat1 = _api_json(f"{base}/uploads/sessions/{upload_id}", method="GET")
        assert s_stat1 == 200, f"Status GET failed: {s_stat1} {body_stat1}"
        assert body_stat1["min_remaining_upload_eta_s"] is not None

        # 3. Single-part upload & complete for chunk_00
        s_p0, _, body_p0 = _api_json(
            f"{base}/uploads/chunks/presign",
            method="POST",
            payload={
                "upload_id": upload_id,
                "recording_id": rec_ready_id,
                "chunk_index": 0,
                "part_numbers": [1],
            },
        )
        assert s_p0 == 200, f"Presign chunk 0 failed: {s_p0} {body_p0}"
        etag_0_1 = _put_s3_part(str(body_p0["presigned_parts"][0]["url"]), chunk0_bytes)

        s_c0, _, body_c0 = _api_json(
            f"{base}/uploads/chunks/complete",
            method="POST",
            payload={
                "upload_id": upload_id,
                "recording_id": rec_ready_id,
                "chunk_index": 0,
                "s3_upload_id": body_p0["s3_upload_id"],
                "parts": [{"PartNumber": 1, "ETag": etag_0_1}],
            },
        )
        assert s_c0 == 200 and body_c0["upload_status"] == "UPLOADING", f"Complete c0 failed: {s_c0} {body_c0}"

        # 4. Multi-part resumable presign (Part 1, then re-entrant Part 2 with s3_upload_id) on chunk_01
        s_p1a, _, body_p1a = _api_json(
            f"{base}/uploads/chunks/presign",
            method="POST",
            payload={
                "upload_id": upload_id,
                "recording_id": rec_ready_id,
                "chunk_index": 1,
                "part_numbers": [1],
            },
        )
        assert s_p1a == 200, f"Presign chunk 1 part 1 failed: {s_p1a} {body_p1a}"
        s3_uid_1 = str(body_p1a["s3_upload_id"])
        etag_1_1 = _put_s3_part(str(body_p1a["presigned_parts"][0]["url"]), chunk1_part1)

        s_p1b, _, body_p1b = _api_json(
            f"{base}/uploads/chunks/presign",
            method="POST",
            payload={
                "upload_id": upload_id,
                "recording_id": rec_ready_id,
                "chunk_index": 1,
                "s3_upload_id": s3_uid_1,
                "part_numbers": [2],
                "expires_in": 900,
            },
        )
        assert s_p1b == 200 and body_p1b["s3_upload_id"] == s3_uid_1, f"Resumed presign failed: {body_p1b}"
        etag_1_2 = _put_s3_part(str(body_p1b["presigned_parts"][0]["url"]), chunk1_part2)

        complete_c1_payload = {
            "upload_id": upload_id,
            "recording_id": rec_ready_id,
            "chunk_index": 1,
            "s3_upload_id": s3_uid_1,
            "parts": [
                {"PartNumber": 1, "ETag": etag_1_1},
                {"PartNumber": 2, "ETag": etag_1_2},
            ],
        }
        s_c1, _, body_c1 = _api_json(
            f"{base}/uploads/chunks/complete",
            method="POST",
            payload=complete_c1_payload,
        )
        assert s_c1 == 200, f"Complete chunk 1 failed: {s_c1} {body_c1}"
        assert body_c1["upload_status"] == "READY"
        assert body_c1["transitioned_to_ready"] is True
        if ready_queue_url:
            assert body_c1["enqueued_sqs"] is True, f"Expected enqueued_sqs=True, got {body_c1}"

        # Verify S3 object bytes exist with exact size
        head_c1 = s3.head_object(Bucket=bucket_name, Key=str(body_c1["s3_key"]))
        assert head_c1["ContentLength"] == chunk1_total_size

        # Duplicate /chunks/complete idempotency check
        s_c1_dup, _, body_c1_dup = _api_json(
            f"{base}/uploads/chunks/complete",
            method="POST",
            payload=complete_c1_payload,
        )
        assert s_c1_dup == 200 and body_c1_dup["idempotent_replay"] is True
        assert body_c1_dup["transitioned_to_ready"] is False

        # SQS message stays untouched in ovius-ready-recordings until consumed cleanly in Step 5b
        sqs_verified = False

        # Verify orchestrator Lambda (S3 Object Created fallback replay + Stale Upload Watchdog sweep)
        orchestrator_s3_replay_ok = False
        orchestrator_watchdog_ok = False
        if lambda_client is not None and orchestrator_fn_name:
            s3_evt = {
                "source": "aws.s3",
                "detail-type": "Object Created",
                "detail": {
                    "bucket": {"name": bucket_name},
                    "object": {"key": str(body_c1["s3_key"])},
                },
            }
            inv_s3 = lambda_client.invoke(
                FunctionName=orchestrator_fn_name,
                Payload=json.dumps(s3_evt).encode("utf-8"),
            )
            assert inv_s3.get("FunctionError") is None, f"Orchestrator S3 error: {inv_s3}"
            s3_res = json.loads(inv_s3["Payload"].read().decode("utf-8"))
            assert s3_res["status"] == "processed" and s3_res["idempotent_replay"] is True
            assert s3_res["transitioned_to_ready"] is False
            orchestrator_s3_replay_ok = True

            # Insert a synthetic stale recording and invoke the 5-minute watchdog sweep
            stale_hb = int(time.time()) - 1800
            ddb_table.put_item(
                Item={
                    "pk": f"SESSION#{upload_id}",
                    "sk": f"REC#{rec_stale_id}",
                    "upload_id": upload_id,
                    "recording_id": rec_stale_id,
                    "upload_status": "UPLOADING",
                    "last_heartbeat_at": stale_hb,
                    "GSI1PK": "ACTIVE_UPLOAD",
                    "GSI1SK": 60,
                    "expires_at": int(time.time()) + 3600,
                }
            )
            wd_evt = {"source": "aws.events", "detail-type": "Scheduled Event", "action": "stale_watchdog", "stale_after_s": 900}
            inv_wd = lambda_client.invoke(
                FunctionName=orchestrator_fn_name,
                Payload=json.dumps(wd_evt).encode("utf-8"),
            )
            assert inv_wd.get("FunctionError") is None, f"Orchestrator watchdog error: {inv_wd}"
            wd_res = json.loads(inv_wd["Payload"].read().decode("utf-8"))
            assert rec_stale_id in wd_res["stalled_recording_ids"], f"Watchdog missed stale rec: {wd_res}"
            stale_after = ddb_table.get_item(Key={"pk": f"SESSION#{upload_id}", "sk": f"REC#{rec_stale_id}"})["Item"]
            assert stale_after["upload_status"] == "STALLED"
            assert "GSI1PK" not in stale_after
            orchestrator_watchdog_ok = True

        # 5. Presign + upload part + abort second recording
        s_pa, _, body_pa = _api_json(
            f"{base}/uploads/chunks/presign",
            method="POST",
            payload={
                "upload_id": upload_id,
                "recording_id": rec_abort_id,
                "chunk_index": 0,
                "part_numbers": [1],
            },
        )
        assert s_pa == 200
        _put_s3_part(str(body_pa["presigned_parts"][0]["url"]), abort_chunk_bytes)

        s_ab, _, body_ab = _api_json(
            f"{base}/uploads/recordings/{rec_abort_id}/abort",
            method="POST",
            payload={"upload_id": upload_id},
        )
        assert s_ab == 200 and body_ab["upload_status"] == "ABORTED"

        # 5b. Run VisionWorkerRunner locally against live SQS -> S3 -> DynamoDB telemetry + Parquet
        worker_summary: dict[str, object] = {}
        worker_cycles_count = 0
        worker_segments_count = 0
        videos_presigned_chunks = 0
        bboxes_presigned_ok = False
        metrics_cycle_count = 0
        if ddb_resource is not None and ready_queue_url:
            runner = VisionWorkerRunner(
                ddb_resource=ddb_resource,
                s3_client=s3,
                sqs_client=sqs,
                bucket_name=bucket_name,
                ready_queue_url=ready_queue_url,
            )
            worker_summary = runner.run_loop(
                max_empty_polls=3,
                poll_wait_s=2,
                keep_warm_cap_s=30,
                keep_warm_step_s=1,
            )
            assert rec_ready_id in worker_summary["processed_recordings"], (
                f"Worker did not process {rec_ready_id}: {worker_summary}"
            )
            assert worker_summary["final_worker_status"] == "IDLE"
            sqs_verified = True

            # Verify DynamoDB telemetry tables and Parquet outputs
            rec_item = (
                ddb_resource.Table(recording_table_name)
                .get_item(Key={"recording_id": rec_ready_id})
                .get("Item")
            )
            assert rec_item is not None, f"Missing ovius-recording row for {rec_ready_id}"
            assert len(rec_item.get("chunk_keys", [])) == 2
            assert len(rec_item.get("parquet_keys", [])) == 2
            created_s3_keys.extend(str(k) for k in rec_item.get("parquet_keys", []))

            cycles = _query_recording_children(
                ddb_resource.Table(cycle_table_name), rec_ready_id
            )
            segments = _query_recording_children(
                ddb_resource.Table(segment_table_name), rec_ready_id
            )
            worker_cycles_count = len(cycles)
            worker_segments_count = len(segments)
            assert worker_cycles_count >= 5, f"Expected >=5 cycles, got {worker_cycles_count}"
            assert worker_segments_count >= 10, f"Expected >=10 segments, got {worker_segments_count}"
            assert any(c.get("cycle_status") == "incomplete" for c in cycles)

            ingest_after = ddb_table.get_item(
                Key={"pk": f"SESSION#{upload_id}", "sk": f"REC#{rec_ready_id}"}
            )["Item"]
            assert ingest_after.get("process_status") == "COMPLETED"
            assert int(ingest_after.get("last_completed_chunk_index", -1)) == 1

            # Verify CloudFront /api/v1/metrics, /metrics/cycles, /videos, /bboxes serve new recording
            s_m, _, body_m = _api_json(
                f"{base}/metrics?start_date=2026-11-18T00:00:00Z&end_date=2026-11-18T23:59:59Z&camera_id={test_camera_id}&include_segments=true",
                method="GET",
            )
            assert s_m == 200, f"/metrics failed: {s_m} {body_m}"
            rec_rows = body_m.get("recordings", [])
            assert any(r.get("recording_id") == rec_ready_id for r in rec_rows), (
                f"/metrics missing {rec_ready_id}: {body_m}"
            )
            metrics_cycle_count = len(body_m.get("haul_cycles", []))
            assert metrics_cycle_count == worker_cycles_count, (
                f"Expected {worker_cycles_count} haul_cycles, got {metrics_cycle_count} (keys={list(body_m.keys())})"
            )
            assert len(body_m.get("state_segments", [])) == worker_segments_count

            s_v, _, body_v = _api_json(f"{base}/videos?recording_id={rec_ready_id}", method="GET")
            assert s_v == 200, f"/videos failed: {s_v} {body_v}"
            v_chunks = body_v["items"][0]["chunks"]
            videos_presigned_chunks = len(v_chunks)
            assert videos_presigned_chunks == 2
            dl_c0 = _get_presigned_bytes(str(v_chunks[0]["presigned_url"]))
            assert dl_c0 == chunk0_bytes, f"Downloaded chunk0 bytes mismatch: {len(dl_c0)} != {len(chunk0_bytes)}"

            s_b, _, body_b = _api_json(f"{base}/bboxes?recording_id={rec_ready_id}", method="GET")
            assert s_b == 200, f"/bboxes failed: {s_b} {body_b}"
            assert len(body_b["items"][0]["chunk_parquet_uris"]) == 2
            dl_pq = _get_presigned_bytes(str(body_b["items"][0]["presigned_url"]))
            if dl_pq[:4] == b"PAR1":
                bboxes_presigned_ok = True
            else:
                first_row = json.loads(dl_pq.decode("utf-8").splitlines()[0])
                assert first_row.get("recording_id") == rec_ready_id, f"Unexpected bbox payload: {first_row}"
                bboxes_presigned_ok = True

        # 6. Verify direct API Gateway route parity too
        s_gw, _, body_gw = _api_json(
            f"{apigw_base}/uploads/sessions",
            method="POST",
            payload={
                "upload_id": apigw_upload_id,
                "site_id": "open_pit_mine_01",
                "recordings": [
                    {
                        "recording_id": apigw_rec_id,
                        "camera_id": "CAM-ALPHA-01",
                        "timestamp": "2026-11-18T08:00:00Z",
                        "duration_s": 3600,
                        "fps": 25.0,
                        "chunks": [
                            {
                                "chunk_index": 0,
                                "start_offset_s": 0,
                                "duration_s": 3600,
                                "byte_size": 1024,
                            }
                        ],
                    }
                ],
            },
        )
        assert s_gw == 201, f"Direct API Gateway POST failed: {s_gw} {body_gw}"
        s_gw_stat, _, _ = _api_json(f"{apigw_base}/uploads/sessions/{apigw_upload_id}", method="GET")
        assert s_gw_stat == 200

        return {
            "cloudfront_upload_flow_ok": True,
            "apigw_upload_flow_ok": True,
            "ready_recording_id": rec_ready_id,
            "test_camera_id": test_camera_id,
            "chunk0_bytes": len(chunk0_bytes),
            "chunk1_bytes": chunk1_total_size,
            "chunk1_resumable_presign_ok": True,
            "chunk1_idempotent_replay_ok": True,
            "aborted_recording_id": rec_abort_id,
            "s3_verified_content_length": head_c1["ContentLength"],
            "enqueued_sqs": body_c1["enqueued_sqs"],
            "sqs_verified": sqs_verified,
            "orchestrator_s3_replay_ok": orchestrator_s3_replay_ok,
            "orchestrator_watchdog_ok": orchestrator_watchdog_ok,
            "worker_processed_recordings": worker_summary.get("processed_recordings", []),
            "worker_cycles_count": worker_cycles_count,
            "worker_segments_count": worker_segments_count,
            "metrics_cycle_count": metrics_cycle_count,
            "videos_presigned_chunks": videos_presigned_chunks,
            "bboxes_presigned_ok": bboxes_presigned_ok,
        }
    finally:
        _cleanup_test_artifacts(
            s3=s3,
            ddb_table=ddb_table,
            ddb_resource=ddb_resource,
            recording_table_name=recording_table_name,
            cycle_table_name=cycle_table_name,
            segment_table_name=segment_table_name,
            bucket=bucket_name,
            s3_keys=created_s3_keys,
            upload_ids=created_upload_ids,
            recording_ids=created_rec_ids,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Live mock MP4 upload flow verifier.")
    parser.add_argument("--stack-name", default="ovius-api-backend")
    parser.add_argument("--compute-stack-name", default="ovius-ingest-compute")
    parser.add_argument("--ingest-table", default="ovius-ingest-state")
    parser.add_argument("--recording-table", default="ovius-recording")
    parser.add_argument("--cycle-table", default="ovius-haul-cycle")
    parser.add_argument("--segment-table", default="ovius-state-segment")
    parser.add_argument("--bucket-name", default="ovius-pipeline-apse1-610331148578")
    parser.add_argument("--profile", default="610331148578_Project8Learner")
    parser.add_argument("--region", default="ap-southeast-1")
    args = parser.parse_args()

    cfg = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    cfn = session.client("cloudformation", config=cfg)
    s3 = session.client("s3", config=cfg)
    sqs = session.client("sqs", config=cfg)
    lambda_client = session.client("lambda", config=cfg)
    ddb = session.resource("dynamodb", config=cfg)
    ddb_table = ddb.Table(args.ingest_table)

    stack = cfn.describe_stacks(StackName=args.stack_name)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}

    ready_queue_url: str | None = None
    orchestrator_fn_name: str | None = None
    try:
        c_stack = cfn.describe_stacks(StackName=args.compute_stack_name)["Stacks"][0]
        c_outputs = {o["OutputKey"]: o["OutputValue"] for o in c_stack.get("Outputs", [])}
        ready_queue_url = c_outputs.get("ReadyQueueUrl")
        orchestrator_fn_name = c_outputs.get("OrchestratorFunctionName")
    except ClientError:
        pass

    summary = run_live_upload_flow(
        cf_base_url=outputs["CloudFrontBaseUrl"],
        apigw_base_url=outputs["ApiGatewayUrl"],
        s3=s3,
        ddb_table=ddb_table,
        ddb_resource=ddb,
        sqs=sqs,
        lambda_client=lambda_client,
        ready_queue_url=ready_queue_url,
        orchestrator_fn_name=orchestrator_fn_name,
        bucket_name=args.bucket_name,
        recording_table_name=args.recording_table,
        cycle_table_name=args.cycle_table,
        segment_table_name=args.segment_table,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
