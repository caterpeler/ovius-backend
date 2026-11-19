"""Ingest 3-table DynamoDB seed JSONL into AWS DynamoDB tables via concurrent BatchWriteItem.

Reads mock_data/dynamodb_items.jsonl in streaming fashion using parse_float=Decimal
(required by DynamoDB Number attributes), dispatches 25-item BatchWriteItem requests
via a bounded worker pool with adaptive retries and UnprocessedItems backoff, and
supports atomic line-watermark checkpoints for safe resumption.

Run:
  uv run --python 3.11 --with boto3 python3 scripts/ingest_dynamodb.py \
    --profile 610331148578_Project8Learner --region ap-southeast-1 --table-prefix ovius
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import random
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
_LOCAL_JSONL = ROOT / "mock_data" / "dynamodb_items.jsonl"
_UPSTREAM_JSONL = ROOT.parent / "AIMLC4_Playground" / "mock_data" / "dynamodb_items.jsonl"
DEFAULT_JSONL = _LOCAL_JSONL if _LOCAL_JSONL.exists() else _UPSTREAM_JSONL
DEFAULT_CHECKPOINT = ROOT / ".tmp_ingest_checkpoint.json"
BATCH_SIZE = 25


class IngestStats:
    """Thread-safe container tracking uploaded items and retries across tables."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {"recording": 0, "haul_cycle": 0, "state_segment": 0}
        self.unprocessed_retries = 0
        self.throttle_retries = 0
        self.start_time = time.monotonic()


def build_table_map(table_prefix: str) -> dict[str, str]:
    """Map logical JSONL table names to provisioned physical DynamoDB table names."""
    return {
        "recording": f"{table_prefix}-recording",
        "haul_cycle": f"{table_prefix}-haul-cycle",
        "state_segment": f"{table_prefix}-state-segment",
    }


def load_checkpoint(checkpoint_path: Path | None) -> int:
    """Return the committed 0-based JSONL line watermark if a checkpoint file exists."""
    if checkpoint_path is None or not checkpoint_path.exists():
        return 0
    data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    return int(data.get("committed_lines", 0))


def save_checkpoint(checkpoint_path: Path | None, committed_lines: int, stats: IngestStats) -> None:
    """Atomically persist ingestion progress to disk."""
    if checkpoint_path is None:
        return
    tmp = checkpoint_path.with_suffix(".tmp")
    payload = {
        "committed_lines": committed_lines,
        "counts": stats.counts,
        "unprocessed_retries": stats.unprocessed_retries,
        "throttle_retries": stats.throttle_retries,
        "updated_at_epoch": round(time.time(), 2),
    }
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    tmp.replace(checkpoint_path)


def serialize_put_request(
    serializer: TypeSerializer,
    item: dict[str, Any],
) -> dict[str, Any]:
    """Convert a Decimal-decoded Python dict into a DynamoDB PutRequest map."""
    marshalled = {k: serializer.serialize(v) for k, v in item.items()}
    return {"PutRequest": {"Item": marshalled}}


def write_batch_with_retry(
    client: Any,
    table_name: str,
    requests: list[dict[str, Any]],
    max_attempts: int = 12,
) -> tuple[int, int]:
    """Send a single 1..25-item BatchWriteItem call, retrying UnprocessedItems with backoff.

    Returns:
        Tuple of (unprocessed_retry_count, throttle_retry_count).
    """
    pending = {table_name: requests}
    unprocessed_retries = 0
    throttle_retries = 0
    rng = random.Random()

    for attempt in range(max_attempts):
        try:
            resp = client.batch_write_item(RequestItems=pending)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            retryable = code in {
                "ProvisionedThroughputExceededException",
                "ThrottlingException",
                "RequestLimitExceeded",
                "InternalServerError",
                "ServiceUnavailable",
            }
            if not retryable or attempt == max_attempts - 1:
                raise
            throttle_retries += 1
            delay = min(15.0, (0.08 * (2**attempt)) + rng.uniform(0.0, 0.08))
            time.sleep(delay)
            continue

        unproc = resp.get("UnprocessedItems") or {}
        if not unproc or not unproc.get(table_name):
            return unprocessed_retries, throttle_retries

        unprocessed_retries += 1
        pending = unproc
        if attempt == max_attempts - 1:
            raise RuntimeError(
                f"Exceeded {max_attempts} retries with {len(pending[table_name])} UnprocessedItems on {table_name}"
            )
        delay = min(10.0, (0.05 * (2**attempt)) + rng.uniform(0.0, 0.05))
        time.sleep(delay)

    return unprocessed_retries, throttle_retries


def wait_for_tables_active(client: Any, table_names: list[str]) -> None:
    """Wait until all target DynamoDB tables and their GSIs are ACTIVE."""
    waiter = client.get_waiter("table_exists")
    for tname in table_names:
        waiter.wait(TableName=tname, WaiterConfig={"Delay": 5, "MaxAttempts": 60})
        while True:
            desc = client.describe_table(TableName=tname)["Table"]
            tbl_active = desc.get("TableStatus") == "ACTIVE"
            gsis = desc.get("GlobalSecondaryIndexes") or []
            gsis_active = all(g.get("IndexStatus") == "ACTIVE" for g in gsis)
            if tbl_active and gsis_active:
                break
            time.sleep(3)


def verify_live_queries(
    client: Any,
    table_map: dict[str, str],
    sample_recording_id: str,
    sample_camera_id: str,
    sample_cycle_id: str,
    sample_segment_id: str,
) -> dict[str, Any]:
    """Run direct GetItem and GSI Query smoke checks against live tables."""
    rec_resp = client.get_item(
        TableName=table_map["recording"],
        Key={"recording_id": {"S": sample_recording_id}},
    )
    cam_resp = client.query(
        TableName=table_map["recording"],
        IndexName="camera-id-timestamp-index",
        KeyConditionExpression="camera_id = :c",
        ExpressionAttributeValues={":c": {"S": sample_camera_id}},
        Select="COUNT",
    )
    cyc_get = client.get_item(
        TableName=table_map["haul_cycle"],
        Key={"cycle_id": {"S": sample_cycle_id}},
    )
    cyc_rec_q = client.query(
        TableName=table_map["haul_cycle"],
        IndexName="recording-id-index",
        KeyConditionExpression="recording_id = :r",
        ExpressionAttributeValues={":r": {"S": sample_recording_id}},
        Select="COUNT",
    )
    seg_get = client.get_item(
        TableName=table_map["state_segment"],
        Key={"segment_id": {"S": sample_segment_id}},
    )
    seg_cyc_q = client.query(
        TableName=table_map["state_segment"],
        IndexName="cycle-id-index",
        KeyConditionExpression="cycle_id = :c",
        ExpressionAttributeValues={":c": {"S": sample_cycle_id}},
        Select="COUNT",
    )

    return {
        "recording_get_found": "Item" in rec_resp,
        "camera_90d_query_count": cam_resp["Count"],
        "haul_cycle_get_found": "Item" in cyc_get,
        "haul_cycle_recording_query_count": cyc_rec_q["Count"],
        "state_segment_get_found": "Item" in seg_get,
        "state_segment_cycle_query_count": seg_cyc_q["Count"],
    }


def ingest_jsonl(
    jsonl_path: Path,
    profile_name: str,
    region_name: str,
    table_prefix: str,
    workers: int,
    target_tables: set[str],
    checkpoint_path: Path | None,
    reset_checkpoint: bool,
    dry_run: bool,
    max_lines: int | None,
) -> dict[str, Any]:
    """Stream JSONL records and ingest into DynamoDB using bounded concurrency."""
    table_map = build_table_map(table_prefix)
    serializer = TypeSerializer()
    stats = IngestStats()

    if reset_checkpoint and checkpoint_path is not None and checkpoint_path.exists():
        checkpoint_path.unlink()

    start_after_line = load_checkpoint(checkpoint_path)
    if start_after_line > 0:
        print(f"Resuming from checkpoint line {start_after_line:,}...")

    client = None
    if not dry_run:
        session = boto3.Session(profile_name=profile_name, region_name=region_name)
        cfg = Config(
            retries={"total_max_attempts": 10, "mode": "adaptive"},
            connect_timeout=5,
            read_timeout=30,
            max_pool_connections=max(32, workers * 2),
        )
        client = session.client("dynamodb", config=cfg)
        active_physical_tables = [table_map[t] for t in sorted(target_tables)]
        wait_for_tables_active(client, active_physical_tables)

    buffers: dict[str, list[dict[str, Any]]] = {t: [] for t in target_tables}
    buffer_lines: dict[str, list[int]] = {t: [] for t in target_tables}

    max_in_flight = max(workers * 4, 32)
    in_flight: dict[concurrent.futures.Future[tuple[int, int]], tuple[str, int, int]] = {}
    committed_watermark = start_after_line
    highest_seen_line = start_after_line
    last_log_time = stats.start_time

    sample_ids: dict[str, str] = {}

    def _drain_completed(wait_all: bool = False) -> None:
        nonlocal committed_watermark, last_log_time
        if not in_flight:
            return
        when = (
            concurrent.futures.ALL_COMPLETED
            if wait_all
            else concurrent.futures.FIRST_COMPLETED
        )
        done, _ = concurrent.futures.wait(in_flight.keys(), return_when=when)
        for fut in done:
            tbl, batch_len, _min_line_no = in_flight.pop(fut)
            unproc_r, thr_r = fut.result()
            stats.counts[tbl] += batch_len
            stats.unprocessed_retries += unproc_r
            stats.throttle_retries += thr_r

        pending_mins = [meta[2] for meta in in_flight.values()]
        for b_lines in buffer_lines.values():
            if b_lines:
                pending_mins.append(b_lines[0])
        if pending_mins:
            committed_watermark = max(committed_watermark, min(pending_mins) - 1)
        else:
            committed_watermark = max(committed_watermark, highest_seen_line)

        now = time.monotonic()
        if wait_all or (now - last_log_time >= 20.0):
            elapsed = max(0.001, now - stats.start_time)
            tot = sum(stats.counts.values())
            rate = tot / elapsed
            print(
                f"Progress [{elapsed:6.1f}s]: total={tot:,} ({rate:,.0f} items/s) | "
                f"rec={stats.counts['recording']:,} cyc={stats.counts['haul_cycle']:,} "
                f"seg={stats.counts['state_segment']:,} | "
                f"unproc_retries={stats.unprocessed_retries} thr_retries={stats.throttle_retries}",
                flush=True,
            )
            if not dry_run and max_lines is None:
                save_checkpoint(checkpoint_path, committed_watermark, stats)
            last_log_time = now

    def _submit_batch(
        executor: concurrent.futures.ThreadPoolExecutor,
        tbl: str,
    ) -> None:
        batch = buffers[tbl]
        if not batch:
            return
        min_ln = buffer_lines[tbl][0]
        buffers[tbl] = []
        buffer_lines[tbl] = []
        if dry_run:
            stats.counts[tbl] += len(batch)
            return
        while len(in_flight) >= max_in_flight:
            _drain_completed(wait_all=False)
        fut = executor.submit(write_batch_with_retry, client, table_map[tbl], batch)
        in_flight[fut] = (tbl, len(batch), min_ln)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        with jsonl_path.open("r", encoding="utf-8") as fh:
            for line_idx, raw_line in enumerate(fh, start=1):
                if max_lines is not None and line_idx > max_lines:
                    break
                highest_seen_line = line_idx
                if not raw_line.strip():
                    continue
                if line_idx <= start_after_line and len(sample_ids) >= 4:
                    continue
                row = json.loads(raw_line, parse_float=Decimal)
                tbl = row["table"]
                item = row["item"]

                if tbl == "recording" and "recording_id" not in sample_ids:
                    sample_ids["recording_id"] = item["recording_id"]
                    sample_ids["camera_id"] = item["camera_id"]
                elif tbl == "haul_cycle" and "cycle_id" not in sample_ids:
                    sample_ids["cycle_id"] = item["cycle_id"]
                elif (
                    tbl == "state_segment"
                    and "segment_id" not in sample_ids
                    and item.get("cycle_id") == sample_ids.get("cycle_id")
                ):
                    sample_ids["segment_id"] = item["segment_id"]

                if line_idx <= start_after_line:
                    continue
                if tbl not in target_tables:
                    continue

                buffers[tbl].append(serialize_put_request(serializer, item))
                buffer_lines[tbl].append(line_idx)
                if len(buffers[tbl]) >= BATCH_SIZE:
                    _submit_batch(executor, tbl)

        for tbl in list(target_tables):
            _submit_batch(executor, tbl)
        _drain_completed(wait_all=True)

    elapsed = round(time.monotonic() - stats.start_time, 2)
    if checkpoint_path is not None and checkpoint_path.exists() and max_lines is None and not dry_run:
        checkpoint_path.unlink()

    verification: dict[str, Any] = {}
    if not dry_run and client is not None and {"recording", "haul_cycle", "state_segment"}.issubset(target_tables):
        verification = verify_live_queries(
            client=client,
            table_map=table_map,
            sample_recording_id=sample_ids["recording_id"],
            sample_camera_id=sample_ids["camera_id"],
            sample_cycle_id=sample_ids["cycle_id"],
            sample_segment_id=sample_ids["segment_id"],
        )

    return {
        "dry_run": dry_run,
        "region": region_name,
        "profile": profile_name,
        "tables": {k: table_map[k] for k in sorted(target_tables)},
        "uploaded_counts": stats.counts,
        "total_uploaded": sum(stats.counts.values()),
        "unprocessed_retries": stats.unprocessed_retries,
        "throttle_retries": stats.throttle_retries,
        "elapsed_s": elapsed,
        "verification": verification,
    }


def main() -> int:
    """CLI entrypoint for DynamoDB seed ingestion."""
    parser = argparse.ArgumentParser(description="Stream-ingest DynamoDB JSONL seed items into AWS DynamoDB tables.")
    parser.add_argument("--jsonl", type=Path, default=DEFAULT_JSONL, help="Path to dynamodb_items.jsonl")
    parser.add_argument("--profile", default="610331148578_Project8Learner", help="AWS CLI/SDK profile name")
    parser.add_argument("--region", default="ap-southeast-1", help="Target AWS region")
    parser.add_argument("--table-prefix", default="ovius", help="Table name prefix matching CloudFormation stack")
    parser.add_argument("--workers", type=int, default=16, help="Concurrent BatchWriteItem worker threads")
    parser.add_argument(
        "--tables",
        default="recording,haul_cycle,state_segment",
        help="Comma-separated logical tables to ingest",
    )
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT, help="Checkpoint file path")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Ignore/delete any prior checkpoint")
    parser.add_argument("--dry-run", action="store_true", help="Parse and batch without writing to AWS")
    parser.add_argument("--max-lines", type=int, default=None, help="Optional line cap for smoke testing")
    args = parser.parse_args()

    target_tables = {t.strip() for t in args.tables.split(",") if t.strip()}
    try:
        summary = ingest_jsonl(
            jsonl_path=args.jsonl,
            profile_name=args.profile,
            region_name=args.region,
            table_prefix=args.table_prefix,
            workers=args.workers,
            target_tables=target_tables,
            checkpoint_path=args.checkpoint,
            reset_checkpoint=args.reset_checkpoint,
            dry_run=args.dry_run,
            max_lines=args.max_lines,
        )
        print(json.dumps(summary, indent=2))
        return 0
    except ClientError as exc:
        print(f"AWS ClientError: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Ingestion failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
