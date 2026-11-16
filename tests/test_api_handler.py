"""Unit tests for Ovius API filter parsing, pagination, Decimal conversion, and routes."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
from typing import Any

import pytest

from ovius_api.filters import FilterValidationError, parse_query_filters
from ovius_api.handler import lambda_handler
from ovius_api.repository import MAX_PAYLOAD_BYTES, TelemetryRepository, json_safe


class _FakeRecordingTable:
    name = "ovius-recording"

    def __init__(self, recordings: list[dict[str, Any]]) -> None:
        self.recordings = recordings
        self.queries: list[dict[str, Any]] = []
        self.scans: list[dict[str, Any]] = []

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.queries.append(kwargs)
        return {"Items": self.recordings}

    def scan(self, **kwargs: Any) -> dict[str, Any]:
        self.scans.append(kwargs)
        return {"Items": self.recordings}


class _FakeChildTable:
    def __init__(self, name: str, pages_by_key: dict[str, list[list[dict[str, Any]]]]) -> None:
        self.name = name
        self.pages_by_key = pages_by_key
        self.queries: list[dict[str, Any]] = []

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.queries.append(kwargs)
        esk = kwargs.get("ExclusiveStartKey")
        rec_key = esk["key"] if esk else self._extract_key(kwargs)
        page_idx = int(esk["page"]) if esk else 0
        pages = self.pages_by_key.get(rec_key, [[]])
        items = pages[page_idx] if page_idx < len(pages) else []
        out: dict[str, Any] = {"Items": items}
        if page_idx + 1 < len(pages):
            out["LastEvaluatedKey"] = {"key": rec_key, "page": page_idx + 1}
        return out

    @staticmethod
    def _extract_key(kwargs: Any) -> str:
        # Inspect recorded condition tree cleanly by checking internal expression values
        cond = kwargs["KeyConditionExpression"]
        expr = cond.get_expression()
        for val in expr["values"]:
            if isinstance(val, str) and not val.startswith("E"):
                return val
        return str(expr["values"][0])


class _FakeMetaClient:
    def __init__(self, recordings: list[dict[str, Any]]) -> None:
        self.by_id = {r["recording_id"]: r for r in recordings}

    def batch_get_item(self, RequestItems: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, list[dict[str, Any]]] = {}
        for tbl_name, spec in RequestItems.items():
            out[tbl_name] = [
                self.by_id[k["recording_id"]]
                for k in spec["Keys"]
                if k["recording_id"] in self.by_id
            ]
        return {"Responses": out, "UnprocessedKeys": {}}


class _FakeDynamoMeta:
    def __init__(self, recordings: list[dict[str, Any]]) -> None:
        self.client = _FakeMetaClient(recordings)


class _FakeDynamoResource:
    def __init__(
        self,
        recordings: list[dict[str, Any]],
        cycles_by_rec: dict[str, list[list[dict[str, Any]]]],
        segs_by_key: dict[str, list[list[dict[str, Any]]]],
    ) -> None:
        self.meta = _FakeDynamoMeta(recordings)
        self.rec_table = _FakeRecordingTable(recordings)
        self.cyc_table = _FakeChildTable("ovius-haul-cycle", cycles_by_rec)
        self.seg_table = _FakeChildTable("ovius-state-segment", segs_by_key)

    def Table(self, name: str) -> Any:
        if "recording" in name:
            return self.rec_table
        if "haul-cycle" in name:
            return self.cyc_table
        return self.seg_table


def _sample_recordings() -> list[dict[str, Any]]:
    return [
        {
            "recording_id": "mock_sample_1_d001",
            "camera_id": "CAM-ALPHA-01",
            "filename": "2026-08-19_mock_sample_1.mp4",
            "timestamp": "2026-08-19T06:00:00+00:00",
            "duration_s": Decimal("43200.0"),
            "fps": Decimal("25.0"),
            "resolution": "1920x1080",
        },
        {
            "recording_id": "mock_sample_2_d001",
            "camera_id": "CAM-BETA-01",
            "filename": "2026-08-19_mock_sample_2.mp4",
            "timestamp": "2026-08-19T06:00:00+00:00",
            "duration_s": Decimal("43200.0"),
            "fps": Decimal("25.0"),
            "resolution": "1920x1080",
        },
    ]


def test_parse_filters_date_range_and_camera_wildcard() -> None:
    filters = parse_query_filters(
        {
            "queryStringParameters": {
                "start_time": "2026-11-01",
                "end_time": "2026-11-15",
                "camera_id": "*",
            }
        }
    )
    assert filters.start_time == "2026-11-01T00:00:00+00:00"
    assert filters.end_time == "2026-11-15T23:59:59+00:00"
    assert filters.camera_ids is None
    assert filters.tod_start_s == 0.0
    assert filters.tod_end_s == 43200.0


def test_parse_filters_normalizes_z_and_multi_value_cameras() -> None:
    filters = parse_query_filters(
        {
            "queryStringParameters": {
                "start_time": "2026-11-16T06:00:00Z",
                "end_time": "2026-11-16T18:00:00Z",
                "tod_start_s": "3600",
                "tod_end_s": "7200",
            },
            "multiValueQueryStringParameters": {
                "camera_id": ["CAM-ALPHA-01", "CAM-BETA-01,CAM-ALPHA-01"],
            },
        }
    )
    assert filters.start_time == "2026-11-16T06:00:00+00:00"
    assert filters.end_time == "2026-11-16T18:00:00+00:00"
    assert filters.camera_ids == ("CAM-ALPHA-01", "CAM-BETA-01")
    assert filters.tod_start_s == 3600.0
    assert filters.tod_end_s == 7200.0


def test_parse_filters_default_7d_window() -> None:
    fixed_now = datetime(2026, 11, 16, 10, 0, 0, tzinfo=timezone.utc)
    filters = parse_query_filters({"queryStringParameters": {}}, now=fixed_now)
    assert filters.start_time == "2026-11-09T00:00:00+00:00"
    assert filters.end_time == "2026-11-16T23:59:59+00:00"


@pytest.mark.parametrize(
    "params",
    [
        {"start_time": "2026-11-16", "end_time": "2026-11-10"},
        {"start_time": "not-a-date"},
        {"tod_start_s": "5000", "tod_end_s": "1000"},
        {"limit": "99999"},
        {"next_token": "not_valid_base64!!"},
    ],
)
def test_parse_filters_validation_errors(params: dict[str, str]) -> None:
    with pytest.raises(FilterValidationError):
        parse_query_filters({"queryStringParameters": params})


def test_json_safe_converts_decimals() -> None:
    converted = json_safe({"scoop_number": Decimal("2"), "t_arrive_s": Decimal("12.35")})
    assert isinstance(converted["scoop_number"], int)
    assert converted["scoop_number"] == 2
    assert isinstance(converted["t_arrive_s"], float)
    assert converted["t_arrive_s"] == 12.35


def test_handler_routes_metrics_cycles_segments_and_templates() -> None:
    recs = _sample_recordings()
    cycles = {
        "mock_sample_1_d001": [
            [{"recording_id": "mock_sample_1_d001", "cycle_id": "c1", "t_load_start_s": Decimal("200.5")}],
            [{"recording_id": "mock_sample_1_d001", "cycle_id": "c2", "t_load_start_s": Decimal("500.0")}],
        ],
        "mock_sample_2_d001": [
            [{"recording_id": "mock_sample_2_d001", "cycle_id": "c3", "t_load_start_s": Decimal("300.0")}],
        ],
    }
    segs = {
        "mock_sample_1_d001": [
            [{"recording_id": "mock_sample_1_d001", "segment_id": "s1", "start_s": Decimal("10.0")}]
        ],
        "c1": [
            [{"cycle_id": "c1", "segment_id": "s1", "state": "loading_scoop", "scoop_number": Decimal("1")}]
        ],
    }
    repo = TelemetryRepository(ddb_resource=_FakeDynamoResource(recs, cycles, segs))

    # Page 1 of /api/v1/metrics (limit=1 returns first cycle + next_token + recordings)
    resp1 = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics",
            "queryStringParameters": {
                "start_time": "2026-08-19",
                "end_time": "2026-08-19",
                "camera_id": "CAM-ALPHA-01,CAM-BETA-01",
                "limit": "1",
            },
        },
        repository=repo,
    )
    assert resp1["statusCode"] == 200
    assert resp1["headers"]["Access-Control-Allow-Origin"] == "*"
    body1 = json.loads(resp1["body"])
    assert len(body1["recordings"]) == 2
    assert [c["cycle_id"] for c in body1["haul_cycles"]] == ["c1"]
    assert body1["pagination"]["next_token"] is not None

    # Page 2 via next_token
    resp2 = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics",
            "queryStringParameters": {
                "start_time": "2026-08-19",
                "end_time": "2026-08-19",
                "camera_id": "CAM-ALPHA-01,CAM-BETA-01",
                "limit": "10",
                "next_token": body1["pagination"]["next_token"],
            },
        },
        repository=repo,
    )
    body2 = json.loads(resp2["body"])
    assert body2["recordings"] == []
    assert [c["cycle_id"] for c in body2["haul_cycles"]] == ["c2", "c3"]
    assert body2["pagination"]["next_token"] is None

    # Segments by cycle_id
    resp_seg = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics/segments",
            "queryStringParameters": {"cycle_id": "c1"},
        },
        repository=repo,
    )
    body_seg = json.loads(resp_seg["body"])
    assert resp_seg["statusCode"] == 200
    assert body_seg["state_segments"][0]["scoop_number"] == 1

    # BBoxes and Videos templates
    resp_bbox = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/bboxes",
            "queryStringParameters": {"recording_id": "mock_sample_1_d001"},
        },
        repository=repo,
    )
    body_bbox = json.loads(resp_bbox["body"])
    assert body_bbox["query_type"] == "bboxes"
    assert body_bbox["status"] == "template"
    assert body_bbox["items"][0]["bbox_artifact_uri"].endswith("mock_sample_1_d001.parquet")

    resp_vid = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/videos",
            "queryStringParameters": {"recording_id": "mock_sample_1_d001"},
        },
        repository=repo,
    )
    body_vid = json.loads(resp_vid["body"])
    assert body_vid["query_type"] == "videos"
    assert body_vid["items"][0]["stream_url"] == "/videos/CAM-ALPHA-01/2026-08-19_mock_sample_1.mp4"


def test_handler_options_404_405_and_byte_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    recs = _sample_recordings()
    cycles = {
        "mock_sample_1_d001": [
            [{"recording_id": "mock_sample_1_d001", "cycle_id": f"c{i}", "payload": "x" * 600}]
            for i in range(4)
        ],
    }
    repo = TelemetryRepository(ddb_resource=_FakeDynamoResource(recs[:1], cycles, {}))
    monkeypatch.setattr("ovius_api.repository.MAX_PAYLOAD_BYTES", 500)

    resp_cycles = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics/cycles",
            "queryStringParameters": {"recording_id": "mock_sample_1_d001", "limit": "50"},
        },
        repository=repo,
    )
    body = json.loads(resp_cycles["body"])
    assert len(body["haul_cycles"]) == 1
    assert body["pagination"]["next_token"] is not None

    opt = lambda_handler({"httpMethod": "OPTIONS", "path": "/api/v1/metrics"}, repository=repo)
    assert opt["statusCode"] == 204
    assert "x-api-key" in opt["headers"]["Access-Control-Allow-Headers"]

    not_found = lambda_handler({"httpMethod": "GET", "path": "/api/v1/unknown"}, repository=repo)
    assert not_found["statusCode"] == 404

    post_resp = lambda_handler({"httpMethod": "POST", "path": "/api/v1/metrics"}, repository=repo)
    assert post_resp["statusCode"] == 405
