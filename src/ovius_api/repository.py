"""Pure DynamoDB raw-telemetry repository (zero server-side math)."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import json
import os
from typing import Any, Callable

import boto3
from boto3.dynamodb.conditions import Attr, ConditionBase, Key
from botocore.config import Config

from ovius_api.filters import (
    FilterValidationError,
    QueryFilters,
    encode_cursor,
)

MAX_PAYLOAD_BYTES = 4_200_000
DYNAMO_CHUNK_CAP = 500
MAX_QUERY_WORKERS = 11

_DEFAULT_RESOURCE_CONFIG = Config(
    retries={"total_max_attempts": 10, "mode": "adaptive"},
)

_CACHED_DDB_RESOURCE: Any | None = None


def get_dynamodb_resource() -> Any:
    """Return a cached DynamoDB ServiceResource reused across warm Lambda invocations."""
    global _CACHED_DDB_RESOURCE  # noqa: PLW0603
    if _CACHED_DDB_RESOURCE is None:
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-3"
        _CACHED_DDB_RESOURCE = boto3.resource(
            "dynamodb",
            region_name=region,
            config=_DEFAULT_RESOURCE_CONFIG,
        )
    return _CACHED_DDB_RESOURCE


def json_safe(value: Any) -> Any:
    """Convert DynamoDB Decimals and nested containers to JSON-serializable primitives."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(f"Non-finite Decimal value encountered: {value!r}")
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    return value


def _estimate_bytes(item: Any) -> int:
    return len(json.dumps(item, separators=(",", ":"), allow_nan=False).encode("utf-8"))


def _table_names() -> tuple[str, str, str]:
    rec_tbl = os.environ.get("RECORDING_TABLE", "ovius-recording")
    cyc_tbl = os.environ.get("HAUL_CYCLE_TABLE", "ovius-haul-cycle")
    seg_tbl = os.environ.get("STATE_SEGMENT_TABLE", "ovius-state-segment")
    return rec_tbl, cyc_tbl, seg_tbl


def _query_all_pages(table: Any, **kwargs: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    params = dict(kwargs)
    while True:
        resp = table.query(**params)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        params["ExclusiveStartKey"] = lek
    return items


class TelemetryRepository:
    """Pure data-access facade over ovius-recording, ovius-haul-cycle, and ovius-state-segment."""

    def __init__(self, ddb_resource: Any | None = None) -> None:
        self._ddb = ddb_resource or get_dynamodb_resource()
        rec_name, cyc_name, seg_name = _table_names()
        self.recording_table = self._ddb.Table(rec_name)
        self.haul_cycle_table = self._ddb.Table(cyc_name)
        self.state_segment_table = self._ddb.Table(seg_name)

    def resolve_recordings(self, filters: QueryFilters) -> list[dict[str, Any]]:
        """Fetch raw recording items matching recording_id, camera_id*, and datetime range."""
        items: list[dict[str, Any]] = []

        if filters.recording_ids:
            client = self._ddb.meta.client
            table_name = self.recording_table.name
            for start_idx in range(0, len(filters.recording_ids), 100):
                batch_ids = filters.recording_ids[start_idx : start_idx + 100]
                request_items: dict[str, Any] = {
                    table_name: {"Keys": [{"recording_id": rid} for rid in batch_ids]}
                }
                while request_items:
                    resp = client.batch_get_item(RequestItems=request_items)
                    raw_rows = resp.get("Responses", {}).get(table_name, [])
                    items.extend(raw_rows)
                    unprocessed = resp.get("UnprocessedKeys", {})
                    request_items = unprocessed if unprocessed else {}

            items = self._filter_recordings_in_memory(items, filters)
        elif filters.camera_ids:
            assert filters.start_time is not None and filters.end_time is not None
            start_ts = filters.start_time
            end_ts = filters.end_time

            def _fetch_cam(cam_id: str) -> list[dict[str, Any]]:
                return _query_all_pages(
                    self.recording_table,
                    IndexName="camera-id-timestamp-index",
                    KeyConditionExpression=(
                        Key("camera_id").eq(cam_id)
                        & Key("timestamp").between(start_ts, end_ts)
                    ),
                )

            if len(filters.camera_ids) == 1:
                items = _fetch_cam(filters.camera_ids[0])
            else:
                workers = min(MAX_QUERY_WORKERS, len(filters.camera_ids))
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    for cam_rows in pool.map(_fetch_cam, filters.camera_ids):
                        items.extend(cam_rows)
        else:
            assert filters.start_time is not None and filters.end_time is not None
            scan_kwargs: dict[str, Any] = {
                "FilterExpression": Attr("timestamp").between(
                    filters.start_time,
                    filters.end_time,
                )
            }
            while True:
                resp = self.recording_table.scan(**scan_kwargs)
                items.extend(resp.get("Items", []))
                lek = resp.get("LastEvaluatedKey")
                if not lek:
                    break
                scan_kwargs["ExclusiveStartKey"] = lek

        normalized = [json_safe(row) for row in items]
        normalized.sort(key=lambda r: (str(r.get("timestamp", "")), str(r.get("recording_id", ""))))
        return normalized

    @staticmethod
    def _filter_recordings_in_memory(
        items: list[dict[str, Any]],
        filters: QueryFilters,
    ) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        allowed_cams = set(filters.camera_ids) if filters.camera_ids else None
        for row in items:
            if allowed_cams is not None and row.get("camera_id") not in allowed_cams:
                continue
            ts = str(row.get("timestamp", ""))
            if filters.start_time is not None and ts < filters.start_time:
                continue
            if filters.end_time is not None and ts > filters.end_time:
                continue
            filtered.append(row)
        return filtered

    def list_recordings_page(self, filters: QueryFilters) -> dict[str, Any]:
        """Return paginated raw recording items."""
        offset = 0
        if filters.next_token:
            raw_offset = filters.next_token.get("offset", 0)
            if not isinstance(raw_offset, int) or raw_offset < 0:
                raise FilterValidationError("Invalid recording pagination offset")
            offset = raw_offset

        all_recordings = self.resolve_recordings(filters)
        selected: list[dict[str, Any]] = []
        used_bytes = 0
        next_idx = offset

        for idx in range(offset, len(all_recordings)):
            if len(selected) >= filters.limit:
                break
            item = all_recordings[idx]
            item_bytes = _estimate_bytes(item) + 1
            if selected and used_bytes + item_bytes > MAX_PAYLOAD_BYTES:
                break
            selected.append(item)
            used_bytes += item_bytes
            next_idx = idx + 1

        next_token = (
            encode_cursor({"offset": next_idx}) if next_idx < len(all_recordings) else None
        )
        return {
            "recordings": selected,
            "pagination": {
                "count": len(selected),
                "total_matched_recordings": len(all_recordings),
                "next_token": next_token,
            },
        }

    def _paginate_child_table(
        self,
        *,
        table: Any,
        index_name: str,
        build_key_condition: Callable[[str], ConditionBase],
        recording_ids: list[str],
        start_rec_idx: int,
        start_lek: dict[str, Any] | None,
        max_items: int,
        max_bytes: int,
    ) -> tuple[list[dict[str, Any]], int, int | None, dict[str, Any] | None]:
        """Shared paginated reader across sorted recording_ids with payload byte guard."""
        collected: list[dict[str, Any]] = []
        used_bytes = 0

        if start_rec_idx >= len(recording_ids) or max_items <= 0:
            return collected, used_bytes, None, None

        for idx in range(start_rec_idx, len(recording_ids)):
            rec_id = recording_ids[idx]
            lek = start_lek if idx == start_rec_idx else None

            while True:
                remaining = max_items - len(collected)
                if remaining <= 0 or (collected and used_bytes >= max_bytes):
                    return collected, used_bytes, idx, lek

                query_kwargs: dict[str, Any] = {
                    "IndexName": index_name,
                    "KeyConditionExpression": build_key_condition(rec_id),
                    "Limit": min(remaining, DYNAMO_CHUNK_CAP),
                }
                if lek is not None:
                    query_kwargs["ExclusiveStartKey"] = lek

                resp = table.query(**query_kwargs)
                raw_chunk = resp.get("Items", [])
                lek = resp.get("LastEvaluatedKey")

                for raw_item in raw_chunk:
                    item = json_safe(raw_item)
                    collected.append(item)
                    used_bytes += _estimate_bytes(item) + 1

                if not lek:
                    break
                if len(collected) >= max_items or used_bytes >= max_bytes:
                    return collected, used_bytes, idx, lek

            if len(collected) >= max_items or used_bytes >= max_bytes:
                if idx + 1 < len(recording_ids):
                    return collected, used_bytes, idx + 1, None
                return collected, used_bytes, None, None

        return collected, used_bytes, None, None

    def _cycle_query_spec(self, filters: QueryFilters) -> tuple[str, Callable[[str], ConditionBase]]:
        start_dec = Decimal(str(filters.tod_start_s))
        end_dec = Decimal(str(filters.tod_end_s))
        if filters.excavator_id:
            start_ms = int(round(filters.tod_start_s * 1000))
            end_ms = int(round(filters.tod_end_s * 1000))
            low = f"{filters.excavator_id}#{start_ms:09d}"
            high = f"{filters.excavator_id}#{end_ms:09d}"
            return (
                "recording-excavator-index",
                lambda rec_id: Key("recording_id").eq(rec_id)
                & Key("excavator_sort_key").between(low, high),
            )
        return (
            "recording-id-index",
            lambda rec_id: Key("recording_id").eq(rec_id)
            & Key("t_load_start_s").between(start_dec, end_dec),
        )

    def _segment_query_spec(self, filters: QueryFilters) -> tuple[str, Callable[[str], ConditionBase]]:
        start_dec = Decimal(str(filters.tod_start_s))
        end_dec = Decimal(str(filters.tod_end_s))
        return (
            "recording-id-index",
            lambda rec_id: Key("recording_id").eq(rec_id)
            & Key("start_s").between(start_dec, end_dec),
        )

    def list_cycles_page(self, filters: QueryFilters) -> dict[str, Any]:
        """Fetch paginated raw haul_cycle items matching the filters."""
        recordings = self.resolve_recordings(filters)
        rec_ids = [str(r["recording_id"]) for r in recordings]

        rec_idx = 0
        lek: dict[str, Any] | None = None
        if filters.next_token:
            rec_idx = int(filters.next_token.get("rec_idx", 0))
            lek = filters.next_token.get("lek")

        index_name, build_key = self._cycle_query_spec(filters)
        cycles, _, next_idx, next_lek = self._paginate_child_table(
            table=self.haul_cycle_table,
            index_name=index_name,
            build_key_condition=build_key,
            recording_ids=rec_ids,
            start_rec_idx=rec_idx,
            start_lek=lek,
            max_items=filters.limit,
            max_bytes=MAX_PAYLOAD_BYTES,
        )

        next_token = (
            encode_cursor({"rec_idx": next_idx, "lek": next_lek})
            if next_idx is not None
            else None
        )
        return {
            "haul_cycles": cycles,
            "pagination": {
                "count": len(cycles),
                "matched_recordings": len(rec_ids),
                "next_token": next_token,
            },
        }

    def list_segments_page(self, filters: QueryFilters) -> dict[str, Any]:
        """Fetch paginated raw state_segment items by cycle_id or recording filters."""
        if filters.cycle_id:
            lek = filters.next_token.get("lek") if filters.next_token else None
            segments: list[dict[str, Any]] = []
            used_bytes = 0

            while True:
                remaining = filters.limit - len(segments)
                if remaining <= 0 or (segments and used_bytes >= MAX_PAYLOAD_BYTES):
                    break
                query_kwargs: dict[str, Any] = {
                    "IndexName": "cycle-id-index",
                    "KeyConditionExpression": Key("cycle_id").eq(filters.cycle_id),
                    "Limit": min(remaining, DYNAMO_CHUNK_CAP),
                }
                if lek is not None:
                    query_kwargs["ExclusiveStartKey"] = lek
                resp = self.state_segment_table.query(**query_kwargs)
                for raw_item in resp.get("Items", []):
                    item = json_safe(raw_item)
                    segments.append(item)
                    used_bytes += _estimate_bytes(item) + 1
                lek = resp.get("LastEvaluatedKey")
                if not lek:
                    break

            next_token = encode_cursor({"lek": lek}) if lek is not None else None
            return {
                "state_segments": segments,
                "pagination": {
                    "count": len(segments),
                    "next_token": next_token,
                },
            }

        recordings = self.resolve_recordings(filters)
        rec_ids = [str(r["recording_id"]) for r in recordings]
        rec_idx = 0
        lek = None
        if filters.next_token:
            rec_idx = int(filters.next_token.get("rec_idx", 0))
            lek = filters.next_token.get("lek")

        index_name, build_key = self._segment_query_spec(filters)
        segments, _, next_idx, next_lek = self._paginate_child_table(
            table=self.state_segment_table,
            index_name=index_name,
            build_key_condition=build_key,
            recording_ids=rec_ids,
            start_rec_idx=rec_idx,
            start_lek=lek,
            max_items=filters.limit,
            max_bytes=MAX_PAYLOAD_BYTES,
        )

        next_token = (
            encode_cursor({"rec_idx": next_idx, "lek": next_lek})
            if next_idx is not None
            else None
        )
        return {
            "state_segments": segments,
            "pagination": {
                "count": len(segments),
                "matched_recordings": len(rec_ids),
                "next_token": next_token,
            },
        }

    def get_dashboard_bundle(self, filters: QueryFilters) -> dict[str, Any]:
        """Return composite raw telemetry bundle (recordings, haul_cycles, optional state_segments)."""
        recordings = self.resolve_recordings(filters)
        rec_ids = [str(r["recording_id"]) for r in recordings]

        if filters.next_token is None:
            out_recordings = recordings
            budget_bytes = max(512_000, MAX_PAYLOAD_BYTES - _estimate_bytes(recordings))
            stage = "cycles"
            rec_idx = 0
            lek: dict[str, Any] | None = None
        else:
            out_recordings = []
            budget_bytes = MAX_PAYLOAD_BYTES
            stage = str(filters.next_token.get("stage", "cycles"))
            if stage not in {"cycles", "segments"}:
                raise FilterValidationError(f"Unsupported cursor stage: {stage!r}")
            rec_idx = int(filters.next_token.get("rec_idx", 0))
            lek = filters.next_token.get("lek")

        haul_cycles: list[dict[str, Any]] = []
        state_segments: list[dict[str, Any]] = []
        next_token: str | None = None
        remaining_items = filters.limit
        used_bytes = 0

        if stage == "cycles":
            cyc_index, cyc_key = self._cycle_query_spec(filters)
            haul_cycles, cyc_bytes, cyc_next_idx, cyc_next_lek = self._paginate_child_table(
                table=self.haul_cycle_table,
                index_name=cyc_index,
                build_key_condition=cyc_key,
                recording_ids=rec_ids,
                start_rec_idx=rec_idx,
                start_lek=lek,
                max_items=remaining_items,
                max_bytes=budget_bytes,
            )
            used_bytes += cyc_bytes
            remaining_items -= len(haul_cycles)
            if cyc_next_idx is not None:
                next_token = encode_cursor(
                    {"stage": "cycles", "rec_idx": cyc_next_idx, "lek": cyc_next_lek}
                )
            elif filters.include_segments:
                stage = "segments"
                rec_idx = 0
                lek = None

        if stage == "segments" and filters.include_segments and next_token is None:
            rem_bytes = budget_bytes - used_bytes
            if remaining_items > 0 and rem_bytes > 100_000:
                seg_index, seg_key = self._segment_query_spec(filters)
                state_segments, _, seg_next_idx, seg_next_lek = self._paginate_child_table(
                    table=self.state_segment_table,
                    index_name=seg_index,
                    build_key_condition=seg_key,
                    recording_ids=rec_ids,
                    start_rec_idx=rec_idx,
                    start_lek=lek,
                    max_items=remaining_items,
                    max_bytes=rem_bytes,
                )
                if seg_next_idx is not None:
                    next_token = encode_cursor(
                        {"stage": "segments", "rec_idx": seg_next_idx, "lek": seg_next_lek}
                    )
            else:
                next_token = encode_cursor({"stage": "segments", "rec_idx": rec_idx, "lek": lek})

        return {
            "recordings": out_recordings,
            "haul_cycles": haul_cycles,
            "state_segments": state_segments if filters.include_segments else [],
            "pagination": {
                "recordings_count": len(out_recordings),
                "haul_cycles_count": len(haul_cycles),
                "state_segments_count": len(state_segments),
                "matched_recordings": len(rec_ids),
                "next_token": next_token,
            },
        }

    def list_bboxes_template(self, filters: QueryFilters) -> dict[str, Any]:
        """Template endpoint returning future S3 bounding-box track pointers for matched recordings."""
        page = self.list_recordings_page(filters)
        bucket = os.environ.get("ANALYTICS_S3_BUCKET", "ovius-cctv-analytics-apse3")
        items = [
            {
                "recording_id": rec["recording_id"],
                "camera_id": rec.get("camera_id"),
                "timestamp": rec.get("timestamp"),
                "bbox_artifact_uri": f"s3://{bucket}/bboxes/{rec['recording_id']}.parquet",
                "presigned_url": None,
            }
            for rec in page["recordings"]
        ]
        return {
            "query_type": "bboxes",
            "status": "template",
            "items": items,
            "pagination": page["pagination"],
        }

    def list_videos_template(self, filters: QueryFilters) -> dict[str, Any]:
        """Template endpoint returning future CloudFront video stream pointers for matched recordings."""
        page = self.list_recordings_page(filters)
        items = [
            {
                "recording_id": rec["recording_id"],
                "camera_id": rec.get("camera_id"),
                "filename": rec.get("filename"),
                "timestamp": rec.get("timestamp"),
                "duration_s": rec.get("duration_s"),
                "fps": rec.get("fps"),
                "resolution": rec.get("resolution"),
                "stream_url": f"/videos/{rec.get('camera_id')}/{rec.get('filename')}",
                "signed_cookies_required": True,
            }
            for rec in page["recordings"]
        ]
        return {
            "query_type": "videos",
            "status": "template",
            "items": items,
            "pagination": page["pagination"],
        }
