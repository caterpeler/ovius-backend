"""Query filter parsing and cursor helpers for Ovius data access API."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
from typing import Any

DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_TOD_START_S = 0.0
DEFAULT_TOD_END_S = 43200.0
DEFAULT_LIMIT = 1000
MAX_LIMIT = 5000
OPEN_END_ISO = "9999-12-31T23:59:59+00:00"


class FilterValidationError(ValueError):
    """Raised when an incoming API query parameter or cursor is invalid."""


@dataclass(frozen=True)
class QueryFilters:
    """Validated query parameters shared across Ovius API endpoints."""

    start_time: str | None
    end_time: str | None
    camera_ids: tuple[str, ...] | None
    recording_ids: tuple[str, ...] | None
    cycle_id: str | None
    excavator_id: str | None
    tod_start_s: float
    tod_end_s: float
    limit: int
    include_segments: bool
    next_token: dict[str, Any] | None

    def to_public_dict(self) -> dict[str, Any]:
        """Serialize resolved filters for echo in API responses."""
        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "camera_id": list(self.camera_ids) if self.camera_ids else ["*"],
            "recording_id": list(self.recording_ids) if self.recording_ids else None,
            "cycle_id": self.cycle_id,
            "excavator_id": self.excavator_id,
            "tod_start_s": self.tod_start_s,
            "tod_end_s": self.tod_end_s,
            "limit": self.limit,
        }


def encode_cursor(payload: dict[str, Any]) -> str:
    """Encode pagination state as a URL-safe base64 token."""
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(token: str) -> dict[str, Any]:
    """Decode and validate an opaque pagination cursor token."""
    try:
        padded = token + "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
        obj = json.loads(decoded.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise FilterValidationError("Invalid next_token cursor") from exc
    if not isinstance(obj, dict):
        raise FilterValidationError("Invalid next_token payload shape")
    return obj


def _parse_iso_datetime(value: str, *, is_end: bool) -> datetime:
    text = value.strip()
    if not text:
        raise FilterValidationError("Empty datetime filter value")

    if len(text) == 10:
        try:
            parsed_date = date.fromisoformat(text)
        except ValueError as exc:
            raise FilterValidationError(f"Invalid date format: {value!r}") from exc
        if is_end:
            return datetime(
                parsed_date.year,
                parsed_date.month,
                parsed_date.day,
                23,
                59,
                59,
                tzinfo=timezone.utc,
            )
        return datetime(
            parsed_date.year,
            parsed_date.month,
            parsed_date.day,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        )

    normalized = f"{text[:-1]}+00:00" if text.endswith("Z") else text
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise FilterValidationError(f"Invalid ISO-8601 datetime: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_param_list(event: dict[str, Any], key: str) -> list[str]:
    values: list[str] = []
    multi = event.get("multiValueQueryStringParameters") or {}
    single = event.get("queryStringParameters") or {}

    raw_items: list[str] = []
    if key in multi and multi[key]:
        raw_items.extend(str(item) for item in multi[key] if item is not None)
    elif key in single and single[key] is not None:
        raw_items.append(str(single[key]))

    for raw in raw_items:
        for part in raw.split(","):
            cleaned = part.strip()
            if cleaned:
                values.append(cleaned)
    return values


def _extract_single(event: dict[str, Any], key: str) -> str | None:
    vals = _extract_param_list(event, key)
    return vals[0] if vals else None


def _parse_float(raw: str | None, *, default: float, name: str) -> float:
    if raw is None:
        return default
    try:
        val = float(raw)
    except ValueError as exc:
        raise FilterValidationError(f"Invalid numeric parameter {name}: {raw!r}") from exc
    if val < 0.0:
        raise FilterValidationError(f"Parameter {name} must be >= 0.0")
    return val


def _parse_bool(raw: str | None, *, default: bool = False) -> bool:
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise FilterValidationError(f"Invalid boolean flag: {raw!r}")


def parse_query_filters(
    event: dict[str, Any],
    *,
    now: datetime | None = None,
) -> QueryFilters:
    """Parse and validate API Gateway query parameters into QueryFilters."""
    rec_ids = _extract_param_list(event, "recording_id")
    cycle_id = _extract_single(event, "cycle_id")
    excavator_id = _extract_single(event, "excavator_id")

    raw_cameras = _extract_param_list(event, "camera_id")
    if not raw_cameras or "*" in raw_cameras:
        camera_ids: tuple[str, ...] | None = None
    else:
        # Deduplicate while preserving deterministic order
        camera_ids = tuple(dict.fromkeys(raw_cameras))

    raw_start = _extract_single(event, "start_time")
    raw_end = _extract_single(event, "end_time")

    start_iso: str | None
    end_iso: str | None

    if raw_start is not None or raw_end is not None:
        if raw_start is not None and raw_end is not None:
            start_dt = _parse_iso_datetime(raw_start, is_end=False)
            end_dt = _parse_iso_datetime(raw_end, is_end=True)
        elif raw_start is not None:
            start_dt = _parse_iso_datetime(raw_start, is_end=False)
            end_dt = _parse_iso_datetime(OPEN_END_ISO, is_end=True)
        else:
            assert raw_end is not None
            end_dt = _parse_iso_datetime(raw_end, is_end=True)
            start_dt = (end_dt - timedelta(days=DEFAULT_LOOKBACK_DAYS)).replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )

        if start_dt > end_dt:
            raise FilterValidationError("start_time must be <= end_time")
        start_iso = start_dt.isoformat()
        end_iso = end_dt.isoformat()
    elif rec_ids or cycle_id:
        start_iso = None
        end_iso = None
    else:
        ref_now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        end_dt = ref_now.replace(hour=23, minute=59, second=59, microsecond=0)
        start_dt = (end_dt - timedelta(days=DEFAULT_LOOKBACK_DAYS)).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        start_iso = start_dt.isoformat()
        end_iso = end_dt.isoformat()

    tod_start_s = _parse_float(
        _extract_single(event, "tod_start_s"),
        default=DEFAULT_TOD_START_S,
        name="tod_start_s",
    )
    tod_end_s = _parse_float(
        _extract_single(event, "tod_end_s"),
        default=DEFAULT_TOD_END_S,
        name="tod_end_s",
    )
    if tod_start_s > tod_end_s:
        raise FilterValidationError("tod_start_s must be <= tod_end_s")

    raw_limit = _extract_single(event, "limit")
    if raw_limit is None:
        limit = DEFAULT_LIMIT
    else:
        try:
            limit = int(raw_limit)
        except ValueError as exc:
            raise FilterValidationError(f"Invalid limit parameter: {raw_limit!r}") from exc
        if limit < 1 or limit > MAX_LIMIT:
            raise FilterValidationError(f"limit must be between 1 and {MAX_LIMIT}")

    include_segments = _parse_bool(
        _extract_single(event, "include_segments"),
        default=False,
    )

    raw_token = _extract_single(event, "next_token")
    cursor = decode_cursor(raw_token) if raw_token is not None else None

    return QueryFilters(
        start_time=start_iso,
        end_time=end_iso,
        camera_ids=camera_ids,
        recording_ids=tuple(dict.fromkeys(rec_ids)) if rec_ids else None,
        cycle_id=cycle_id,
        excavator_id=excavator_id,
        tod_start_s=tod_start_s,
        tod_end_s=tod_end_s,
        limit=limit,
        include_segments=include_segments,
        next_token=cursor,
    )
