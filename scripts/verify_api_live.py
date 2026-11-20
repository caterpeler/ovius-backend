"""End-to-end live verification script for deployed Ovius API Gateway & CloudFront endpoints."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def _ssl_context() -> ssl.SSLContext:
    mac_ca = Path("/etc/ssl/cert.pem")
    if mac_ca.exists():
        return ssl.create_default_context(cafile=str(mac_ca))
    return ssl.create_default_context()


_SSL_CONTEXT = _ssl_context()


def http_request(url: str, *, method: str = "GET") -> tuple[int, dict[str, str], dict[str, object]]:
    """Send an HTTP request and parse status, headers, and optional JSON payload."""
    req = urllib.request.Request(url, method=method)
    if method == "OPTIONS":
        req.add_header("Origin", "http://localhost:3000")
        req.add_header("Access-Control-Request-Method", "GET")
    try:
        with urllib.request.urlopen(req, timeout=30, context=_SSL_CONTEXT) as resp:
            raw = resp.read().decode("utf-8")
            headers = {k.lower(): v for k, v in resp.headers.items()}
            payload = json.loads(raw) if raw else {}
            return resp.status, headers, payload
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        headers = {k.lower(): v for k, v in exc.headers.items()}
        payload = json.loads(raw) if raw else {}
        return exc.code, headers, payload


def verify_base_url(label: str, base_url: str) -> dict[str, object]:
    """Run full route and filter smoke tests against a target base URL."""
    base = base_url.rstrip("/")
    results: dict[str, object] = {"base_url": base}

    # 1. Recordings by date range + multi-camera filter
    s1, h1, p1 = http_request(
        f"{base}/metrics/recordings?start_time=2026-11-10&end_time=2026-11-16&camera_id=CAM-ALPHA-01,CAM-BETA-01"
    )
    assert s1 == 200, f"[{label}] recordings failed: {s1} {p1}"
    assert "access-control-allow-origin" in h1
    recs = p1["recordings"]
    assert len(recs) == 14, f"[{label}] expected 14 recordings, got {len(recs)}"
    results["recordings_multi_cam_7d_count"] = len(recs)

    sample_rec_id = str(recs[0]["recording_id"])

    # 2. Recordings wildcard camera_id=* for single day
    s2, _, p2 = http_request(
        f"{base}/metrics/recordings?start_time=2026-11-15&end_time=2026-11-15&camera_id=*"
    )
    assert s2 == 200, f"[{label}] wildcard recordings failed: {s2} {p2}"
    assert len(p2["recordings"]) == 11, f"[{label}] expected 11 cameras, got {len(p2['recordings'])}"
    results["recordings_wildcard_day_count"] = len(p2["recordings"])

    # 3. Cycles with intra-day offset and pagination cursor
    s3, _, p3 = http_request(
        f"{base}/metrics/cycles?recording_id={urllib.parse.quote(sample_rec_id)}&tod_start_s=0&tod_end_s=7200&limit=5"
    )
    assert s3 == 200, f"[{label}] cycles p1 failed: {s3} {p3}"
    assert len(p3["haul_cycles"]) == 5
    next_tok = p3["pagination"]["next_token"]
    assert next_tok, f"[{label}] expected next_token for cycles"
    sample_cycle_id = str(p3["haul_cycles"][0]["cycle_id"])

    s3b, _, p3b = http_request(
        f"{base}/metrics/cycles?recording_id={urllib.parse.quote(sample_rec_id)}&tod_start_s=0&tod_end_s=7200&limit=5&next_token={urllib.parse.quote(next_tok)}"
    )
    assert s3b == 200, f"[{label}] cycles p2 failed: {s3b} {p3b}"
    page1_ids = {c["cycle_id"] for c in p3["haul_cycles"]}
    page2_ids = {c["cycle_id"] for c in p3b["haul_cycles"]}
    assert page1_ids.isdisjoint(page2_ids), f"[{label}] duplicate cycle IDs across pages"
    results["cycles_paginated_checked"] = len(page1_ids) + len(page2_ids)

    # 4. State segments by cycle_id
    s4, _, p4 = http_request(
        f"{base}/metrics/segments?cycle_id={urllib.parse.quote(sample_cycle_id)}"
    )
    assert s4 == 200 and len(p4["state_segments"]) > 0, f"[{label}] segments failed: {s4} {p4}"
    results["segments_for_cycle_count"] = len(p4["state_segments"])

    # 5. Composite /metrics bundle + query-efficiency options (include_scoops=false & state=idle)
    s5, _, p5 = http_request(
        f"{base}/metrics?start_time=2026-11-15&end_time=2026-11-15&camera_id=CAM-ALPHA-01&include_segments=true&limit=20"
    )
    assert s5 == 200, f"[{label}] composite /metrics failed: {s5} {p5}"
    assert len(p5["recordings"]) == 1
    assert len(p5["haul_cycles"]) == 20
    results["composite_metrics_ok"] = True

    s5b, _, p5b = http_request(
        f"{base}/metrics/cycles?start_time=2026-11-15&end_time=2026-11-15&camera_id=*&include_scoops=false&limit=15000"
    )
    assert s5b == 200, f"[{label}] projected cycles failed: {s5b} {p5b}"
    assert len(p5b["haul_cycles"]) > 1000
    assert "scoops" not in p5b["haul_cycles"][0]
    results["projected_day_cycles_all_cams_count"] = len(p5b["haul_cycles"])

    s5c, _, p5c = http_request(
        f"{base}/metrics/segments?recording_id={urllib.parse.quote(sample_rec_id)}&state=idle&limit=2000"
    )
    assert s5c == 200, f"[{label}] idle-filtered segments failed: {s5c} {p5c}"
    assert len(p5c["state_segments"]) > 0
    assert all(seg["state"] == "idle" for seg in p5c["state_segments"])
    results["filtered_idle_segments_count"] = len(p5c["state_segments"])

    s5d, _, p5d = http_request(
        f"{base}/metrics/recordings?start_time=2026-11-15&end_time=2026-11-15&tag=section:Alpha"
    )
    assert s5d == 200 and len(p5d["recordings"]) == 3
    results["tag_filtered_recordings_count"] = len(p5d["recordings"])

    # 6. Bounding Boxes & Videos template endpoints
    s6, _, p6 = http_request(
        f"{base}/bboxes?start_time=2026-11-15&end_time=2026-11-15&camera_id=CAM-ALPHA-01"
    )
    assert s6 == 200 and p6.get("status") == "template" and len(p6["items"]) == 1

    s7, _, p7 = http_request(
        f"{base}/videos?start_time=2026-11-15&end_time=2026-11-15&camera_id=*"
    )
    assert s7 == 200 and p7.get("status") == "template" and len(p7["items"]) == 11
    results["templates_ok"] = True

    # 7. CORS OPTIONS preflight & 400 validation check
    s8, h8, _ = http_request(f"{base}/metrics", method="OPTIONS")
    assert s8 == 204 and "x-api-key" in h8.get("access-control-allow-headers", "").lower()

    s9, _, _ = http_request(f"{base}/metrics?start_time=2026-11-16&end_time=2026-11-01")
    assert s9 == 400
    results["options_and_validation_ok"] = True

    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify live Ovius API Gateway & CloudFront endpoints.")
    parser.add_argument("--stack-name", default="ovius-api-backend")
    parser.add_argument("--profile", default="610331148578_Project8Learner")
    parser.add_argument("--region", default="ap-southeast-3")
    args = parser.parse_args()

    cfg = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    cfn = session.client("cloudformation", config=cfg)

    try:
        stack = cfn.describe_stacks(StackName=args.stack_name)["Stacks"][0]
    except ClientError as exc:
        print(f"Failed to describe stack {args.stack_name}: {exc}", file=sys.stderr)
        return 1

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    apigw_url = outputs["ApiGatewayUrl"]
    cf_url = outputs["CloudFrontBaseUrl"]

    summary = {
        "api_gateway": verify_base_url("api_gateway", apigw_url),
        "cloudfront": verify_base_url("cloudfront", cf_url),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
