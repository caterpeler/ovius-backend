"""AWS Lambda proxy handler for the Ovius frontend data access API."""

from __future__ import annotations

import json
import logging
from typing import Any

from botocore.exceptions import ClientError

from ovius_api.filters import FilterValidationError, parse_query_filters
from ovius_api.repository import TelemetryRepository

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization,x-api-key",
    "Cache-Control": "no-store",
}


def _response(status_code: int, body: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": dict(_CORS_HEADERS),
        "body": "" if body is None else json.dumps(body, separators=(",", ":"), allow_nan=False),
    }


def _extract_method_and_path(event: dict[str, Any]) -> tuple[str, str]:
    method = (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method")
        or "GET"
    ).upper()

    raw_path = str(event.get("path") or event.get("rawPath") or "/")
    stage = event.get("requestContext", {}).get("stage")
    if stage and raw_path.startswith(f"/{stage}/"):
        raw_path = raw_path[len(stage) + 1 :]
    elif stage and raw_path == f"/{stage}":
        raw_path = "/"

    normalized_path = raw_path.rstrip("/") or "/"
    return method, normalized_path


def lambda_handler(
    event: dict[str, Any],
    context: Any = None,
    *,
    repository: TelemetryRepository | None = None,
) -> dict[str, Any]:
    """Handle API Gateway REST/HTTP proxy requests for Ovius telemetry endpoints."""
    method, path = _extract_method_and_path(event)
    if method == "OPTIONS":
        return _response(204, None)
    if method != "GET":
        return _response(405, {"error": "Method Not Allowed", "allowed": ["GET", "OPTIONS"]})

    try:
        filters = parse_query_filters(event)
    except FilterValidationError as exc:
        return _response(400, {"error": "Bad Request", "message": str(exc)})

    repo = repository or TelemetryRepository()

    try:
        if path == "/api/v1/metrics":
            data = repo.get_dashboard_bundle(filters)
            payload = {"query_type": "metrics", "filters": filters.to_public_dict(), **data}
            return _response(200, payload)

        if path == "/api/v1/metrics/recordings":
            data = repo.list_recordings_page(filters)
            payload = {"query_type": "recordings", "filters": filters.to_public_dict(), **data}
            return _response(200, payload)

        if path == "/api/v1/metrics/cycles":
            data = repo.list_cycles_page(filters)
            payload = {"query_type": "cycles", "filters": filters.to_public_dict(), **data}
            return _response(200, payload)

        if path == "/api/v1/metrics/segments":
            data = repo.list_segments_page(filters)
            payload = {"query_type": "segments", "filters": filters.to_public_dict(), **data}
            return _response(200, payload)

        if path == "/api/v1/bboxes":
            data = repo.list_bboxes_template(filters)
            payload = {"filters": filters.to_public_dict(), **data}
            return _response(200, payload)

        if path == "/api/v1/videos":
            data = repo.list_videos_template(filters)
            payload = {"filters": filters.to_public_dict(), **data}
            return _response(200, payload)

        return _response(404, {"error": "Not Found", "path": path})

    except FilterValidationError as exc:
        return _response(400, {"error": "Bad Request", "message": str(exc)})
    except ClientError as exc:
        logger.exception("DynamoDB client error while handling %s", path)
        code = exc.response.get("Error", {}).get("Code", "DynamoDBError")
        return _response(502, {"error": "Upstream DynamoDB Error", "code": code})
    except Exception:  # noqa: BLE001
        logger.exception("Unexpected error while handling %s", path)
        return _response(500, {"error": "Internal Server Error"})
