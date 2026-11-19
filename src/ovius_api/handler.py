"""AWS Lambda proxy handler for the Ovius frontend data access and upload control APIs."""

from __future__ import annotations

import base64
import json
import logging
import re
from typing import Any
import urllib.parse

from botocore.exceptions import ClientError

from ovius_api.filters import FilterValidationError, parse_query_filters
from ovius_api.repository import TelemetryRepository
from ovius_api.uploads import (
    UploadConflictError,
    UploadControlService,
    UploadNotFoundError,
    UploadValidationError,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,PUT,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization,x-api-key",
    "Cache-Control": "no-store",
}

_SESSION_HEARTBEAT_RE = re.compile(r"^/api/v1/uploads/sessions/([^/]+)/heartbeat$")
_SESSION_STATUS_RE = re.compile(r"^/api/v1/uploads/sessions/([^/]+)$")
_RECORDING_ABORT_RE = re.compile(r"^/api/v1/uploads/recordings/([^/]+)/abort$")


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


def _parse_json_body(event: dict[str, Any], *, allow_empty: bool = False) -> dict[str, Any]:
    raw_body = event.get("body")
    if raw_body is None or raw_body == "":
        if allow_empty:
            return {}
        raise UploadValidationError("Request body is required")
    if isinstance(raw_body, dict):
        return raw_body
    if not isinstance(raw_body, str):
        raise UploadValidationError("Request body must be a JSON string")
    if event.get("isBase64Encoded"):
        try:
            raw_body = base64.b64decode(raw_body).decode("utf-8")
        except Exception as exc:  # noqa: BLE001
            raise UploadValidationError("Invalid base64 request body") from exc
    try:
        parsed = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise UploadValidationError(f"Malformed JSON request body: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise UploadValidationError("Request body must be a JSON object")
    return parsed


def _handle_upload_routes(
    method: str,
    path: str,
    event: dict[str, Any],
    service: UploadControlService,
) -> dict[str, Any]:
    try:
        if path == "/api/v1/uploads/sessions":
            if method != "POST":
                return _response(405, {"error": "Method Not Allowed", "allowed": ["POST", "OPTIONS"]})
            return _response(201, service.create_session(_parse_json_body(event)))

        if path == "/api/v1/uploads/chunks/presign":
            if method != "POST":
                return _response(405, {"error": "Method Not Allowed", "allowed": ["POST", "OPTIONS"]})
            return _response(200, service.presign_chunk_parts(_parse_json_body(event)))

        if path == "/api/v1/uploads/chunks/complete":
            if method != "POST":
                return _response(405, {"error": "Method Not Allowed", "allowed": ["POST", "OPTIONS"]})
            return _response(200, service.complete_chunk(_parse_json_body(event)))

        hb_match = _SESSION_HEARTBEAT_RE.match(path)
        if hb_match:
            if method != "PUT":
                return _response(405, {"error": "Method Not Allowed", "allowed": ["PUT", "OPTIONS"]})
            upload_id = urllib.parse.unquote(hb_match.group(1))
            return _response(200, service.update_heartbeat(upload_id, _parse_json_body(event)))

        status_match = _SESSION_STATUS_RE.match(path)
        if status_match:
            if method != "GET":
                return _response(405, {"error": "Method Not Allowed", "allowed": ["GET", "OPTIONS"]})
            upload_id = urllib.parse.unquote(status_match.group(1))
            return _response(200, service.get_session_status(upload_id))

        abort_match = _RECORDING_ABORT_RE.match(path)
        if abort_match:
            if method != "POST":
                return _response(405, {"error": "Method Not Allowed", "allowed": ["POST", "OPTIONS"]})
            rec_id = urllib.parse.unquote(abort_match.group(1))
            return _response(
                200,
                service.abort_recording(rec_id, _parse_json_body(event, allow_empty=True)),
            )

        return _response(404, {"error": "Not Found", "path": path})
    except UploadValidationError as exc:
        return _response(400, {"error": "Bad Request", "message": str(exc)})
    except UploadConflictError as exc:
        return _response(409, {"error": "Conflict", "message": str(exc)})
    except UploadNotFoundError as exc:
        return _response(404, {"error": "Not Found", "message": str(exc)})
    except ClientError as exc:
        logger.exception("AWS error while handling upload route %s", path)
        code = exc.response.get("Error", {}).get("Code", "UpstreamAWSError")
        return _response(502, {"error": "Upstream AWS Error", "code": code})
    except Exception:  # noqa: BLE001
        logger.exception("Unexpected error while handling upload route %s", path)
        return _response(500, {"error": "Internal Server Error"})


def lambda_handler(
    event: dict[str, Any],
    context: Any = None,
    *,
    repository: TelemetryRepository | None = None,
    upload_service: UploadControlService | None = None,
) -> dict[str, Any]:
    """Handle API Gateway REST/HTTP proxy requests for Ovius telemetry and upload endpoints."""
    method, path = _extract_method_and_path(event)
    if method == "OPTIONS":
        return _response(204, None)

    if path.startswith("/api/v1/uploads"):
        service = upload_service or UploadControlService()
        return _handle_upload_routes(method, path, event, service)

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
