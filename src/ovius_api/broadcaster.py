"""Filtered DynamoDB Stream broadcaster pushing upload/inference updates to WebSocket clients."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer
from botocore.config import Config
from botocore.exceptions import ClientError

from ovius_api.repository import get_dynamodb_resource, json_safe

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_DESERIALIZER = TypeDeserializer()
_CACHED_MGMT_CLIENTS: dict[str, Any] = {}


def _deserialize_image(ddb_map: dict[str, Any]) -> dict[str, Any]:
    return json_safe({k: _DESERIALIZER.deserialize(v) for k, v in ddb_map.items()})


def _get_apigw_management_client(endpoint_url: str) -> Any:
    if endpoint_url not in _CACHED_MGMT_CLIENTS:
        _CACHED_MGMT_CLIENTS[endpoint_url] = boto3.client(
            "apigatewaymanagementapi",
            endpoint_url=endpoint_url,
            config=Config(retries={"total_max_attempts": 5, "mode": "adaptive"}),
        )
    return _CACHED_MGMT_CLIENTS[endpoint_url]


def lambda_handler(
    event: dict[str, Any],
    context: Any = None,
    *,
    ddb_resource: Any | None = None,
    apigw_management_client: Any | None = None,
    ingest_table_name: str | None = None,
) -> dict[str, Any]:
    """Broadcast filtered SESSION#* (META / REC#*) DynamoDB Stream images to WebSocket clients."""
    _ = context
    ddb = ddb_resource or get_dynamodb_resource()
    table_name = ingest_table_name or os.environ.get("INGEST_STATE_TABLE", "ovius-ingest-state")
    table = ddb.Table(table_name)
    default_endpoint = os.environ.get("WS_MANAGEMENT_ENDPOINT", "").strip()

    pushed_count = 0
    stale_deleted = 0
    processed_records = 0

    for record in event.get("Records", []):
        ddb_payload = record.get("dynamodb") or {}
        keys_map = ddb_payload.get("Keys") or {}
        pk_attr = keys_map.get("pk", {}).get("S", "")
        sk_attr = keys_map.get("sk", {}).get("S", "")
        if not pk_attr.startswith("SESSION#") or not (sk_attr == "META" or sk_attr.startswith("REC#")):
            continue

        new_img_raw = ddb_payload.get("NewImage")
        if not new_img_raw:
            continue

        processed_records += 1
        upload_id = pk_attr[len("SESSION#") :]
        image = _deserialize_image(new_img_raw)
        frame_bytes = json.dumps(
            {
                "type": "ingest_progress",
                "upload_id": upload_id,
                "sk": sk_attr,
                "data": image,
            },
            separators=(",", ":"),
        ).encode("utf-8")

        conns = table.query(
            KeyConditionExpression=Key("pk").eq(f"WS#{upload_id}") & Key("sk").begins_with("CONN#")
        ).get("Items", [])

        for conn in conns:
            conn_id = str(conn.get("connection_id") or str(conn["sk"])[len("CONN#") :])
            endpoint = str(conn.get("callback_url") or default_endpoint).strip()
            mgmt = apigw_management_client or (
                _get_apigw_management_client(endpoint) if endpoint else None
            )
            if mgmt is None:
                continue
            try:
                mgmt.post_to_connection(ConnectionId=conn_id, Data=frame_bytes)
                pushed_count += 1
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if code == "GoneException" or status == 410:
                    table.delete_item(Key={"pk": conn["pk"], "sk": conn["sk"]})
                    stale_deleted += 1
                else:
                    raise

    return {
        "processed_records": processed_records,
        "pushed_count": pushed_count,
        "stale_deleted": stale_deleted,
    }
