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


def _extract_condition_strings(cond: Any) -> list[str]:
    expr = cond.get_expression()
    found: list[str] = []
    for val in expr.get("values", ()):
        if hasattr(val, "get_expression"):
            found.extend(_extract_condition_strings(val))
        elif isinstance(val, str):
            found.append(val)
    return found


class _FakeRecordingTable:
    name = "ovius-recording"

    def __init__(self, recordings: list[dict[str, Any]]) -> None:
        self.recordings = recordings
        self.queries: list[dict[str, Any]] = []
        self.scans: list[dict[str, Any]] = []

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.queries.append(kwargs)
        strs = _extract_condition_strings(kwargs["KeyConditionExpression"])
        cam_id = next((s for s in strs if s.startswith("CAM-")), None)
        matched = [r for r in self.recordings if cam_id is None or r["camera_id"] == cam_id]
        return {"Items": matched}

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
        strs = _extract_condition_strings(kwargs["KeyConditionExpression"])
        for val in strs:
            if not val.startswith("E"):
                return val
        return strs[0]


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
            "location_tag": "Section Alpha - Wide Overview Bench",
            "tags": ["section:Alpha", "day:2026-08-19"],
            "timestamp": "2026-08-19T06:00:00+00:00",
            "duration_s": Decimal("43200.0"),
            "fps": Decimal("25.0"),
            "resolution": "1920x1080",
        },
        {
            "recording_id": "mock_sample_2_d001",
            "camera_id": "CAM-BETA-01",
            "filename": "2026-08-19_mock_sample_2.mp4",
            "location_tag": "Section Beta - Queue Bottleneck Bench",
            "tags": ["section:Beta", "day:2026-08-19"],
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


def test_cursor_round_trips_decimal_last_evaluated_key() -> None:
    from ovius_api.filters import decode_cursor, encode_cursor

    lek = {"recording_id": "r1", "cycle_id": "c1", "t_load_start_s": Decimal("1234.56")}
    token = encode_cursor({"rec_idx": 0, "lek": lek})
    decoded = decode_cursor(token)
    assert decoded["rec_idx"] == 0
    assert isinstance(decoded["lek"]["t_load_start_s"], Decimal)
    assert decoded["lek"]["t_load_start_s"] == Decimal("1234.56")


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

    # Composite /api/v1/metrics with include_segments=true and wildcard camera_id
    resp_both = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics",
            "queryStringParameters": {
                "recording_id": "mock_sample_1_d001",
                "include_segments": "true",
                "limit": "10",
            },
        },
        repository=repo,
    )
    assert resp_both["statusCode"] == 200
    body_both = json.loads(resp_both["body"])
    assert len(body_both["haul_cycles"]) == 2
    assert len(body_both["state_segments"]) == 1
    assert body_both["pagination"]["next_token"] is None

    # excavator_id filter routes to recording-excavator-index
    resp_exc = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics/cycles",
            "queryStringParameters": {
                "recording_id": "mock_sample_1_d001",
                "excavator_id": "E1",
                "tod_start_s": "100",
                "tod_end_s": "500",
            },
        },
        repository=repo,
    )
    assert resp_exc["statusCode"] == 200
    assert repo._ddb.cyc_table.queries[-1]["IndexName"] == "recording-excavator-index"


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


def test_efficiency_filters_projection_and_wave_parallel_truncation() -> None:
    recs = _sample_recordings()
    # Multi-item single chunk per recording so wave-parallelism & mid-chunk lek_builder are both exercised
    cycles = {
        "mock_sample_1_d001": [
            [
                {
                    "recording_id": "mock_sample_1_d001",
                    "cycle_id": "c1",
                    "t_load_start_s": Decimal("100.0"),
                },
                {
                    "recording_id": "mock_sample_1_d001",
                    "cycle_id": "c2",
                    "t_load_start_s": Decimal("200.0"),
                },
            ]
        ],
        "mock_sample_2_d001": [
            [
                {
                    "recording_id": "mock_sample_2_d001",
                    "cycle_id": "c3",
                    "t_load_start_s": Decimal("300.0"),
                },
                {
                    "recording_id": "mock_sample_2_d001",
                    "cycle_id": "c4",
                    "t_load_start_s": Decimal("400.0"),
                },
            ]
        ],
    }
    segs = {
        "mock_sample_1_d001": [
            [
                {
                    "segment_id": "s1",
                    "recording_id": "mock_sample_1_d001",
                    "start_s": Decimal("10.0"),
                    "state": "idle",
                }
            ]
        ]
    }
    repo = TelemetryRepository(ddb_resource=_FakeDynamoResource(recs, cycles, segs))

    # 1. location_tag / tag / filename selector filtering on recordings
    r_tag = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics/recordings",
            "queryStringParameters": {
                "start_time": "2026-08-19",
                "end_time": "2026-08-19",
                "tag": "section:Beta",
            },
        },
        repository=repo,
    )
    assert [r["recording_id"] for r in json.loads(r_tag["body"])["recordings"]] == ["mock_sample_2_d001"]

    r_loc = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics/recordings",
            "queryStringParameters": {
                "start_time": "2026-08-19",
                "end_time": "2026-08-19",
                "location_tag": "Section Alpha - Wide Overview Bench",
                "filename": "2026-08-19_mock_sample_1.mp4",
            },
        },
        repository=repo,
    )
    assert [r["recording_id"] for r in json.loads(r_loc["body"])["recordings"]] == ["mock_sample_1_d001"]

    # 2. include_scoops=false adds DynamoDB ProjectionExpression & max limit 15000 is allowed
    r_proj = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics/cycles",
            "queryStringParameters": {
                "start_time": "2026-08-19",
                "end_time": "2026-08-19",
                "camera_id": "*",
                "include_scoops": "false",
                "limit": "15000",
            },
        },
        repository=repo,
    )
    assert r_proj["statusCode"] == 200
    body_proj = json.loads(r_proj["body"])
    assert [c["cycle_id"] for c in body_proj["haul_cycles"]] == ["c1", "c2", "c3", "c4"]
    assert "ProjectionExpression" in repo._ddb.cyc_table.queries[-1]
    assert "scoops" not in repo._ddb.cyc_table.queries[-1]["ProjectionExpression"]

    # 3. Wave-parallel partial limit (limit=3 trims inside second recording cleanly via lek_builder)
    r_trim = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics/cycles",
            "queryStringParameters": {
                "start_time": "2026-08-19",
                "end_time": "2026-08-19",
                "camera_id": "*",
                "limit": "3",
            },
        },
        repository=repo,
    )
    body_trim = json.loads(r_trim["body"])
    assert [c["cycle_id"] for c in body_trim["haul_cycles"]] == ["c1", "c2", "c3"]
    assert body_trim["pagination"]["next_token"] is not None

    # 4. state filter attaches FilterExpression cleanly on segments
    r_state = lambda_handler(
        {
            "httpMethod": "GET",
            "path": "/api/v1/metrics/segments",
            "queryStringParameters": {
                "recording_id": "mock_sample_1_d001",
                "state": "idle,out_of_frame",
            },
        },
        repository=repo,
    )
    assert r_state["statusCode"] == 200
    assert "FilterExpression" in repo._ddb.seg_table.queries[-1]

    # 5. limit > 15000 rejected cleanly
    with pytest.raises(FilterValidationError):
        parse_query_filters({"queryStringParameters": {"limit": "15001"}})
