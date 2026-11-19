"""Unit tests for Phase 2 resumable multipart upload, heartbeat, and ingest-state routes."""

from __future__ import annotations

from decimal import Decimal
import json
from typing import Any

from botocore.exceptions import ClientError

from ovius_api.handler import lambda_handler
from ovius_api.orchestrator import lambda_handler as orchestrator_handler
from ovius_api.uploads import UploadControlService


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, "MockOperation")


class _FakeBatchWriter:
    def __init__(self, table: "_FakeIngestTable") -> None:
        self._table = table

    def __enter__(self) -> "_FakeBatchWriter":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def put_item(self, *, Item: dict[str, Any]) -> None:
        self._table.put_item(Item=Item)


class _FakeIngestTable:
    name = "ovius-ingest-state"

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.queries: list[dict[str, Any]] = []
        self.batch_writer_calls: int = 0

    def put_item(
        self,
        *,
        Item: dict[str, Any],
        ConditionExpression: str | None = None,
    ) -> dict[str, Any]:
        key = (Item["pk"], Item["sk"])
        if ConditionExpression == "attribute_not_exists(pk)" and key in self.items:
            raise _client_error("ConditionalCheckFailedException", "Item already exists")
        self.items[key] = dict(Item)
        return {}

    def batch_writer(self) -> _FakeBatchWriter:
        self.batch_writer_calls += 1
        return _FakeBatchWriter(self)

    def get_item(self, *, Key: dict[str, str]) -> dict[str, Any]:
        item = self.items.get((Key["pk"], Key["sk"]))
        return {"Item": dict(item)} if item is not None else {}

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.queries.append(kwargs)
        index_name = kwargs.get("IndexName")
        if index_name == "GSI_ACTIVE_UPLOADS":
            # Verify Guard 1.1.1: never pass Limit=1 on GSI_ACTIVE_UPLOADS
            assert "Limit" not in kwargs
            active = [
                dict(it)
                for it in self.items.values()
                if it.get("GSI1PK") == "ACTIVE_UPLOAD"
            ]
            active.sort(key=lambda it: float(it.get("GSI1SK", 999999)))
            if not kwargs.get("ScanIndexForward", True):
                active.reverse()
            # Evaluate last_heartbeat_at >= or < cutoff filter if present
            f_expr = kwargs.get("FilterExpression")
            if f_expr is not None:
                expr = f_expr.get_expression()
                op = expr.get("operator", ">=")
                cutoff = int(expr["values"][1])
                if op == "<":
                    active = [it for it in active if int(it.get("last_heartbeat_at", 0)) < cutoff]
                else:
                    active = [it for it in active if int(it.get("last_heartbeat_at", 0)) >= cutoff]
            return {"Items": active}

        k_expr = kwargs["KeyConditionExpression"]
        pk_val = k_expr.get_expression()["values"][1]
        matched = [dict(it) for (pk, _), it in self.items.items() if pk == pk_val]
        matched.sort(key=lambda it: str(it["sk"]))
        return {"Items": matched}

    def update_item(
        self,
        *,
        Key: dict[str, str],
        UpdateExpression: str,
        ConditionExpression: str | None = None,
        ExpressionAttributeNames: dict[str, str] | None = None,
        ExpressionAttributeValues: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        k = (Key["pk"], Key["sk"])
        item = self.items.get(k)
        vals = ExpressionAttributeValues or {}
        _ = ExpressionAttributeNames

        if ConditionExpression:
            if ConditionExpression == "attribute_not_exists(s3_upload_id)":
                if item is None or "s3_upload_id" in item:
                    raise _client_error("ConditionalCheckFailedException")
            elif ConditionExpression == "attribute_exists(pk)":
                if item is None:
                    raise _client_error("ConditionalCheckFailedException")
            elif ConditionExpression == "upload_status = :uploading":
                if item is None or item.get("upload_status") != vals[":uploading"]:
                    raise _client_error("ConditionalCheckFailedException")
            elif ConditionExpression == "upload_status = :uploading AND last_heartbeat_at < :cutoff":
                if (
                    item is None
                    or item.get("upload_status") != vals[":uploading"]
                    or int(item.get("last_heartbeat_at", 0)) >= int(vals[":cutoff"])
                ):
                    raise _client_error("ConditionalCheckFailedException")
            elif ConditionExpression == "upload_status = :uploading AND uploaded_chunks >= expected_chunks":
                if (
                    item is None
                    or item.get("upload_status") != vals[":uploading"]
                    or int(item.get("uploaded_chunks", 0)) < int(item.get("expected_chunks", 0))
                ):
                    raise _client_error("ConditionalCheckFailedException")
            elif ConditionExpression == "attribute_not_exists(upload_status) OR upload_status <> :uploaded":
                if item is not None and item.get("upload_status") == vals[":uploaded"]:
                    raise _client_error("ConditionalCheckFailedException")
            elif (
                ConditionExpression
                == "attribute_not_exists(#st) OR #st IN (:idle, :exiting) OR lease_expires_at < :now_epoch"
            ):
                if item is not None and "status" in item:
                    st = item["status"]
                    lease = int(item.get("lease_expires_at", 0))
                    if st not in (vals[":idle"], vals[":exiting"]) and lease >= int(vals[":now_epoch"]):
                        raise _client_error("ConditionalCheckFailedException")

        if item is None:
            item = {"pk": Key["pk"], "sk": Key["sk"]}

        if "ADD uploaded_chunks :one" in UpdateExpression:
            item["uploaded_chunks"] = int(item.get("uploaded_chunks", 0)) + int(vals[":one"])
        if "ADD completed_recordings :one" in UpdateExpression:
            item["completed_recordings"] = int(item.get("completed_recordings", 0)) + int(vals[":one"])

        if "SET s3_upload_id = :uid" in UpdateExpression:
            item["s3_upload_id"] = vals[":uid"]
            item["updated_at"] = vals[":now"]
        elif "SET smoothed_upload_bps = :bps" in UpdateExpression:
            item["smoothed_upload_bps"] = vals[":bps"]
            item["last_heartbeat_at"] = vals[":hb"]
            item["updated_at"] = vals[":now"]
        elif "SET uploaded_bytes = :ub" in UpdateExpression:
            item["uploaded_bytes"] = vals[":ub"]
            item["upload_eta_s"] = vals[":eta"]
            item["GSI1PK"] = vals[":gpk"]
            item["GSI1SK"] = vals[":eta"]
            item["last_heartbeat_at"] = vals[":hb"]
            item["updated_at"] = vals[":now"]
        elif "SET upload_status = :uploaded" in UpdateExpression:
            item["upload_status"] = vals[":uploaded"]
            item["completed_at"] = vals[":now"]
            item["updated_at"] = vals[":now"]
        elif "SET upload_status = :ready" in UpdateExpression:
            item["upload_status"] = vals[":ready"]
            item["process_status"] = vals[":queued"]
            item["upload_eta_s"] = vals[":zero"]
            item["updated_at"] = vals[":now"]
        elif "SET upload_status = :aborted" in UpdateExpression:
            item["upload_status"] = vals[":aborted"]
            item["process_status"] = vals[":aborted"]
            item["updated_at"] = vals[":now"]
        elif "SET upload_status = :stalled" in UpdateExpression:
            item["upload_status"] = vals[":stalled"]
            item["updated_at"] = vals[":now"]
        elif "SET #st = :starting" in UpdateExpression:
            item["status"] = vals[":starting"]
            item["active_recording_id"] = vals[":rec"]
            item["lease_expires_at"] = vals[":lease"]
            item["updated_at"] = vals[":now_iso"]
        elif "SET #st = :idle" in UpdateExpression:
            item["status"] = vals[":idle"]
            item["lease_expires_at"] = vals[":zero"]
            item["updated_at"] = vals[":now_iso"]
        elif "SET batch_job_id = :jid" in UpdateExpression:
            item["batch_job_id"] = vals[":jid"]
            item["updated_at"] = vals[":now_iso"]

        if "REMOVE GSI1PK, GSI1SK" in UpdateExpression:
            item.pop("GSI1PK", None)
            item.pop("GSI1SK", None)

        self.items[k] = item
        return {"Attributes": dict(item)}


class _FakeIngestClient:
    def __init__(self, table: _FakeIngestTable) -> None:
        self._table = table

    def transact_write_items(self, *, TransactItems: list[dict[str, Any]]) -> dict[str, Any]:
        # boto3.resource("dynamodb").meta.client auto-serializes plain Python values
        updates: list[dict[str, Any]] = [entry["Update"] for entry in TransactItems]
        for upd in updates:
            key = upd["Key"]
            vals = upd.get("ExpressionAttributeValues", {})
            cond = upd.get("ConditionExpression")
            existing = self._table.items.get((key["pk"], key["sk"]))
            if cond == "attribute_not_exists(upload_status) OR upload_status <> :uploaded":
                if existing is not None and existing.get("upload_status") == vals[":uploaded"]:
                    raise _client_error("TransactionCanceledException", "ConditionalCheckFailed")

        for upd in updates:
            self._table.update_item(
                Key=upd["Key"],
                UpdateExpression=upd["UpdateExpression"],
                ConditionExpression=None,
                ExpressionAttributeValues=upd.get("ExpressionAttributeValues", {}),
            )
        return {}


class _FakeIngestResource:
    def __init__(self) -> None:
        self.table = _FakeIngestTable()
        self.meta = type("_Meta", (), {"client": _FakeIngestClient(self.table)})()

    def Table(self, _name: str) -> _FakeIngestTable:
        return self.table


class _FakeS3Client:
    def __init__(self) -> None:
        self._counter = 0
        self.uploads: dict[str, dict[str, Any]] = {}
        self.completed_objects: set[tuple[str, str]] = set()
        self.aborted_uploads: list[str] = []

    def create_multipart_upload(self, *, Bucket: str, Key: str, ContentType: str) -> dict[str, Any]:
        self._counter += 1
        uid = f"mpu_{self._counter:04d}"
        self.uploads[uid] = {"bucket": Bucket, "key": Key, "content_type": ContentType}
        return {"UploadId": uid}

    def generate_presigned_url(
        self,
        _op: str,
        *,
        Params: dict[str, Any],
        ExpiresIn: int,
        HttpMethod: str,
    ) -> str:
        assert HttpMethod == "PUT"
        return (
            f"https://{Params['Bucket']}.s3.ap-southeast-1.amazonaws.com/{Params['Key']}"
            f"?uploadId={Params['UploadId']}&partNumber={Params['PartNumber']}&X-Amz-Expires={ExpiresIn}"
        )

    def complete_multipart_upload(
        self,
        *,
        Bucket: str,
        Key: str,
        UploadId: str,
        MultipartUpload: dict[str, Any],
    ) -> dict[str, Any]:
        if UploadId not in self.uploads:
            raise _client_error("NoSuchUpload", "The specified upload does not exist.")
        assert len(MultipartUpload["Parts"]) >= 1
        del self.uploads[UploadId]
        self.completed_objects.add((Bucket, Key))
        return {"Bucket": Bucket, "Key": Key, "ETag": '"completed-etag"'}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if (Bucket, Key) not in self.completed_objects:
            raise _client_error("404", "Not Found")
        return {"ContentLength": 1024}

    def abort_multipart_upload(self, *, Bucket: str, Key: str, UploadId: str) -> dict[str, Any]:
        self.uploads.pop(UploadId, None)
        self.aborted_uploads.append(UploadId)
        return {}


class _FakeSQSClient:
    def __init__(self, *, queue_exists: bool = True) -> None:
        self.queue_exists = queue_exists
        self.messages: list[dict[str, Any]] = []

    def get_queue_url(self, *, QueueName: str) -> dict[str, str]:
        if not self.queue_exists:
            raise _client_error("AWS.SimpleQueueService.NonExistentQueue")
        return {"QueueUrl": f"https://sqs.ap-southeast-1.amazonaws.com/610331148578/{QueueName}"}

    def send_message(self, *, QueueUrl: str, MessageBody: str) -> dict[str, str]:
        self.messages.append({"queue_url": QueueUrl, "body": json.loads(MessageBody)})
        return {"MessageId": f"msg_{len(self.messages)}"}

    def get_queue_attributes(self, *, QueueUrl: str, AttributeNames: list[str]) -> dict[str, Any]:
        _ = QueueUrl, AttributeNames
        return {
            "Attributes": {
                "ApproximateNumberOfMessages": str(len(self.messages)),
                "ApproximateNumberOfMessagesNotVisible": "0",
            }
        }


def _session_payload(recording_id: str = "rec_alpha_01") -> dict[str, Any]:
    return {
        "site_id": "open_pit_mine_01",
        "recordings": [
            {
                "recording_id": recording_id,
                "camera_id": "CAM-ALPHA-01",
                "location_tag": "Section Alpha",
                "timestamp": "2026-11-18T06:00:00Z",
                "duration_s": 7200,
                "fps": 25.0,
                "resolution": "1920x1080",
                "chunks": [
                    {
                        "chunk_index": 0,
                        "start_offset_s": 0,
                        "duration_s": 3600,
                        "byte_size": 10_000_000,
                    },
                    {
                        "chunk_index": 1,
                        "start_offset_s": 3600,
                        "duration_s": 3600,
                        "byte_size": 10_000_000,
                    },
                ],
            }
        ],
    }


def test_full_resumable_upload_lifecycle_and_idempotency() -> None:
    clock = [1_700_000_000.0]
    ddb = _FakeIngestResource()
    s3 = _FakeS3Client()
    sqs = _FakeSQSClient(queue_exists=True)
    svc = UploadControlService(
        ddb_resource=ddb,
        s3_client=s3,
        sqs_client=sqs,
        bucket_name="ovius-pipeline-apse1-610331148578",
        now_fn=lambda: clock[0],
    )

    # 1. Create session via lambda_handler
    resp_create = lambda_handler(
        {
            "httpMethod": "POST",
            "path": "/api/v1/uploads/sessions",
            "body": json.dumps(_session_payload()),
        },
        upload_service=svc,
    )
    assert resp_create["statusCode"] == 201
    created = json.loads(resp_create["body"])
    upload_id = created["upload_id"]
    assert created["recordings"][0]["expected_chunks"] == 2
    assert created["recordings"][0]["chunks"][0]["s3_key"].endswith("chunk_00.mp4")

    rec_item = ddb.table.items[(f"SESSION#{upload_id}", "REC#rec_alpha_01")]
    assert rec_item["GSI1PK"] == "ACTIVE_UPLOAD"
    assert rec_item["GSI1SK"] == Decimal("999999")

    # 2. Lazy presign + re-entrant resume for remaining parts reuses s3_upload_id
    resp_presign_1 = lambda_handler(
        {
            "httpMethod": "POST",
            "path": "/api/v1/uploads/chunks/presign",
            "body": json.dumps(
                {
                    "upload_id": upload_id,
                    "recording_id": "rec_alpha_01",
                    "chunk_index": 0,
                    "part_numbers": [1],
                }
            ),
        },
        upload_service=svc,
    )
    assert resp_presign_1["statusCode"] == 200
    presign_1 = json.loads(resp_presign_1["body"])
    s3_uid_0 = presign_1["s3_upload_id"]
    assert len(presign_1["presigned_parts"]) == 1
    assert "partNumber=1" in presign_1["presigned_parts"][0]["url"]

    resp_presign_resume = lambda_handler(
        {
            "httpMethod": "POST",
            "path": "/api/v1/uploads/chunks/presign",
            "body": json.dumps(
                {
                    "upload_id": upload_id,
                    "recording_id": "rec_alpha_01",
                    "chunk_index": 0,
                    "s3_upload_id": s3_uid_0,
                    "part_numbers": [2],
                    "expires_in": 900,
                }
            ),
        },
        upload_service=svc,
    )
    assert resp_presign_resume["statusCode"] == 200
    presign_resume = json.loads(resp_presign_resume["body"])
    assert presign_resume["s3_upload_id"] == s3_uid_0
    assert "partNumber=2" in presign_resume["presigned_parts"][0]["url"]

    # 3. Heartbeat updates ETA and sparse GSI1SK
    clock[0] += 15.0
    resp_hb = lambda_handler(
        {
            "httpMethod": "PUT",
            "path": f"/api/v1/uploads/sessions/{upload_id}/heartbeat",
            "body": json.dumps(
                {
                    "smoothed_upload_bps": 100_000,
                    "recordings": [{"recording_id": "rec_alpha_01", "uploaded_bytes": 8_000_000}],
                }
            ),
        },
        upload_service=svc,
    )
    assert resp_hb["statusCode"] == 200
    hb_body = json.loads(resp_hb["body"])
    assert hb_body["recordings"][0]["upload_eta_s"] == 120
    assert ddb.table.items[(f"SESSION#{upload_id}", "REC#rec_alpha_01")]["GSI1SK"] == Decimal("120")

    # 4. Complete chunk 0 -> stays UPLOADING
    resp_c0 = lambda_handler(
        {
            "httpMethod": "POST",
            "path": "/api/v1/uploads/chunks/complete",
            "body": json.dumps(
                {
                    "upload_id": upload_id,
                    "recording_id": "rec_alpha_01",
                    "chunk_index": 0,
                    "s3_upload_id": s3_uid_0,
                    "parts": [{"PartNumber": 1, "ETag": '"e1"'}, {"PartNumber": 2, "ETag": '"e2"'}],
                }
            ),
        },
        upload_service=svc,
    )
    assert resp_c0["statusCode"] == 200
    c0_body = json.loads(resp_c0["body"])
    assert c0_body["uploaded_chunks"] == 1
    assert c0_body["upload_status"] == "UPLOADING"
    assert c0_body["transitioned_to_ready"] is False
    assert len(sqs.messages) == 0

    # Duplicate completion of chunk 0 is idempotent (does NOT double-count uploaded_chunks)
    resp_c0_dup = lambda_handler(
        {
            "httpMethod": "POST",
            "path": "/api/v1/uploads/chunks/complete",
            "body": json.dumps(
                {
                    "upload_id": upload_id,
                    "recording_id": "rec_alpha_01",
                    "chunk_index": 0,
                    "s3_upload_id": s3_uid_0,
                    "parts": [{"PartNumber": 1, "ETag": '"e1"'}, {"PartNumber": 2, "ETag": '"e2"'}],
                }
            ),
        },
        upload_service=svc,
    )
    assert resp_c0_dup["statusCode"] == 200
    c0_dup_body = json.loads(resp_c0_dup["body"])
    assert c0_dup_body["idempotent_replay"] is True
    assert c0_dup_body["uploaded_chunks"] == 1

    # 5. Presign & complete final chunk 1 -> atomic transition to READY + evict GSI + enqueue SQS once
    p1_body = json.loads(
        lambda_handler(
            {
                "httpMethod": "POST",
                "path": "/api/v1/uploads/chunks/presign",
                "body": json.dumps(
                    {
                        "upload_id": upload_id,
                        "recording_id": "rec_alpha_01",
                        "chunk_index": 1,
                        "part_numbers": [1],
                    }
                ),
            },
            upload_service=svc,
        )["body"]
    )
    c1_resp = lambda_handler(
        {
            "httpMethod": "POST",
            "path": "/api/v1/uploads/chunks/complete",
            "body": json.dumps(
                {
                    "upload_id": upload_id,
                    "recording_id": "rec_alpha_01",
                    "chunk_index": 1,
                    "s3_upload_id": p1_body["s3_upload_id"],
                    "parts": [{"PartNumber": 1, "ETag": '"e3"'}],
                }
            ),
        },
        upload_service=svc,
    )
    assert c1_resp["statusCode"] == 200
    c1_body = json.loads(c1_resp["body"])
    assert c1_body["upload_status"] == "READY"
    assert c1_body["transitioned_to_ready"] is True
    assert c1_body["enqueued_sqs"] is True
    assert len(sqs.messages) == 1
    assert "GSI1PK" not in ddb.table.items[(f"SESSION#{upload_id}", "REC#rec_alpha_01")]

    # Replay final chunk 1 -> must NOT re-enqueue SQS
    c1_dup = json.loads(
        lambda_handler(
            {
                "httpMethod": "POST",
                "path": "/api/v1/uploads/chunks/complete",
                "body": json.dumps(
                    {
                        "upload_id": upload_id,
                        "recording_id": "rec_alpha_01",
                        "chunk_index": 1,
                        "s3_upload_id": p1_body["s3_upload_id"],
                        "parts": [{"PartNumber": 1, "ETag": '"e3"'}],
                    }
                ),
            },
            upload_service=svc,
        )["body"]
    )
    assert c1_dup["idempotent_replay"] is True
    assert c1_dup["transitioned_to_ready"] is False
    assert len(sqs.messages) == 1

    # 6. Status polling returns READY recording & completed_recordings == 1
    status_resp = lambda_handler(
        {"httpMethod": "GET", "path": f"/api/v1/uploads/sessions/{upload_id}"},
        upload_service=svc,
    )
    assert status_resp["statusCode"] == 200
    status_body = json.loads(status_resp["body"])
    assert status_body["completed_recordings"] == 1
    assert status_body["worker_status"] == "IDLE"
    assert status_body["recordings"][0]["upload_status"] == "READY"
    assert status_body["min_remaining_upload_eta_s"] is None


def test_min_eta_gsi_ignores_stale_head_without_limit_one() -> None:
    clock = [1_700_000_500.0]
    ddb = _FakeIngestResource()
    s3 = _FakeS3Client()
    sqs = _FakeSQSClient(queue_exists=False)
    svc = UploadControlService(
        ddb_resource=ddb,
        s3_client=s3,
        sqs_client=sqs,
        now_fn=lambda: clock[0],
    )

    # Stale upload with lowest ETA (10s) at t = now - 200s
    ddb.table.put_item(
        Item={
            "pk": "SESSION#upl_stale",
            "sk": "REC#rec_stale",
            "upload_id": "upl_stale",
            "recording_id": "rec_stale",
            "upload_status": "UPLOADING",
            "upload_eta_s": Decimal("10"),
            "last_heartbeat_at": int(clock[0] - 200),
            "GSI1PK": "ACTIVE_UPLOAD",
            "GSI1SK": Decimal("10"),
        }
    )

    # Healthy fresh upload with ETA 140s
    created = svc.create_session(_session_payload("rec_fresh_01"))
    uid = created["upload_id"]
    svc.update_heartbeat(
        uid,
        {
            "smoothed_upload_bps": 100_000,
            "recordings": [{"recording_id": "rec_fresh_01", "uploaded_bytes": 6_000_000}],
        },
    )

    status = svc.get_session_status(uid)
    assert status["min_remaining_upload_eta_s"] == 140
    gsi_queries = [q for q in ddb.table.queries if q.get("IndexName") == "GSI_ACTIVE_UPLOADS"]
    assert gsi_queries and "Limit" not in gsi_queries[-1]


def test_abort_recording_and_route_errors() -> None:
    ddb = _FakeIngestResource()
    s3 = _FakeS3Client()
    sqs = _FakeSQSClient(queue_exists=False)
    svc = UploadControlService(ddb_resource=ddb, s3_client=s3, sqs_client=sqs)

    created = svc.create_session(_session_payload("rec_abort_01"))
    uid = created["upload_id"]
    presigned = svc.presign_chunk_parts(
        {
            "upload_id": uid,
            "recording_id": "rec_abort_01",
            "chunk_index": 0,
            "part_numbers": [1],
        }
    )

    abort_resp = lambda_handler(
        {
            "httpMethod": "POST",
            "path": "/api/v1/uploads/recordings/rec_abort_01/abort",
            "body": json.dumps({"upload_id": uid}),
        },
        upload_service=svc,
    )
    assert abort_resp["statusCode"] == 200
    abort_body = json.loads(abort_resp["body"])
    assert abort_body["upload_status"] == "ABORTED"
    assert abort_body["aborted_chunks"] == [0, 1]
    assert presigned["s3_upload_id"] in s3.aborted_uploads
    assert "GSI1PK" not in ddb.table.items[(f"SESSION#{uid}", "REC#rec_abort_01")]

    # Method / validation / not-found errors
    assert (
        lambda_handler(
            {"httpMethod": "GET", "path": "/api/v1/uploads/sessions"},
            upload_service=svc,
        )["statusCode"]
        == 405
    )
    assert (
        lambda_handler(
            {"httpMethod": "GET", "path": "/api/v1/uploads/sessions/upl_missing"},
            upload_service=svc,
        )["statusCode"]
        == 404
    )
    assert (
        lambda_handler(
            {
                "httpMethod": "POST",
                "path": "/api/v1/uploads/sessions",
                "body": "{not-json",
            },
            upload_service=svc,
        )["statusCode"]
        == 400
    )


class _FakeBatchClient:
    def __init__(self) -> None:
        self.submitted_jobs: list[dict[str, Any]] = []

    def submit_job(self, **kwargs: Any) -> dict[str, str]:
        self.submitted_jobs.append(kwargs)
        return {"jobId": f"batch_job_{len(self.submitted_jobs):03d}"}


def test_orchestrator_s3_object_created_fallback_and_batch_lock() -> None:
    clock = [1_700_001_000.0]
    ddb = _FakeIngestResource()
    s3 = _FakeS3Client()
    sqs = _FakeSQSClient(queue_exists=True)
    batch = _FakeBatchClient()
    svc = UploadControlService(
        ddb_resource=ddb,
        s3_client=s3,
        sqs_client=sqs,
        batch_client=batch,
        bucket_name="ovius-pipeline-apse1-610331148578",
        batch_job_queue="ovius-gpu-job-queue",
        batch_job_definition="ovius-vision-worker-job",
        auto_submit_batch=True,
        now_fn=lambda: clock[0],
    )

    created = svc.create_session(_session_payload("rec_fallback_01"))
    uid = created["upload_id"]
    key0 = created["recordings"][0]["chunks"][0]["s3_key"]
    key1 = created["recordings"][0]["chunks"][1]["s3_key"]

    # Chunk 0 via S3 EventBridge fallback
    res0 = orchestrator_handler(
        {
            "source": "aws.s3",
            "detail-type": "Object Created",
            "detail": {
                "bucket": {"name": "ovius-pipeline-apse1-610331148578"},
                "object": {"key": key0},
            },
        },
        service=svc,
    )
    assert res0["status"] == "processed"
    assert res0["upload_status"] == "UPLOADING"
    assert len(batch.submitted_jobs) == 0

    # Chunk 1 via S3 EventBridge fallback -> READY + SQS enqueue + WORKER#LOCK claim + Batch submit
    res1 = orchestrator_handler(
        {
            "source": "aws.s3",
            "detail-type": "Object Created",
            "detail": {
                "bucket": {"name": "ovius-pipeline-apse1-610331148578"},
                "object": {"key": key1},
            },
        },
        service=svc,
    )
    assert res1["upload_status"] == "READY"
    assert res1["transitioned_to_ready"] is True
    assert res1["enqueued_sqs"] is True
    assert res1["worker_lock"]["batch_submitted"] is True
    assert len(sqs.messages) == 1
    assert len(batch.submitted_jobs) == 1

    # Duplicate S3 EventBridge retry is an idempotent no-op
    res1_dup = orchestrator_handler(
        {
            "source": "aws.s3",
            "detail-type": "Object Created",
            "detail": {
                "bucket": {"name": "ovius-pipeline-apse1-610331148578"},
                "object": {"key": key1},
            },
        },
        service=svc,
    )
    assert res1_dup["idempotent_replay"] is True
    assert len(sqs.messages) == 1
    assert len(batch.submitted_jobs) == 1

    # Active WORKER#LOCK prevents duplicate Batch submission while lease is valid
    second_claim = svc.try_claim_worker_lock_and_submit_batch("rec_other_02")
    assert second_claim["claimed"] is False
    assert len(batch.submitted_jobs) == 1

    # Expired lease allows re-claiming WORKER#LOCK
    clock[0] += 400.0
    expired_claim = svc.try_claim_worker_lock_and_submit_batch("rec_other_02")
    assert expired_claim["claimed"] is True
    assert expired_claim["batch_submitted"] is True
    assert len(batch.submitted_jobs) == 2


def test_orchestrator_stale_upload_watchdog() -> None:
    clock = [1_700_002_000.0]
    ddb = _FakeIngestResource()
    s3 = _FakeS3Client()
    sqs = _FakeSQSClient(queue_exists=True)
    svc = UploadControlService(ddb_resource=ddb, s3_client=s3, sqs_client=sqs, now_fn=lambda: clock[0])

    # Stale recording (last heartbeat 1200s ago) and fresh recording (last heartbeat 30s ago)
    created = svc.create_session(_session_payload("rec_stale_wd"))
    uid_stale = created["upload_id"]
    ddb.table.items[(f"SESSION#{uid_stale}", "REC#rec_stale_wd")]["last_heartbeat_at"] = int(
        clock[0] - 1200
    )

    created_fresh = svc.create_session(_session_payload("rec_fresh_wd"))
    uid_fresh = created_fresh["upload_id"]
    ddb.table.items[(f"SESSION#{uid_fresh}", "REC#rec_fresh_wd")]["last_heartbeat_at"] = int(
        clock[0] - 30
    )

    sweep = orchestrator_handler(
        {
            "source": "aws.events",
            "detail-type": "Scheduled Event",
            "action": "stale_watchdog",
            "stale_after_s": 900,
        },
        service=svc,
    )
    assert sweep["mode"] == "stale_watchdog"
    assert sweep["stalled_recording_ids"] == ["rec_stale_wd"]

    stale_rec = ddb.table.items[(f"SESSION#{uid_stale}", "REC#rec_stale_wd")]
    assert stale_rec["upload_status"] == "STALLED"
    assert "GSI1PK" not in stale_rec

    fresh_rec = ddb.table.items[(f"SESSION#{uid_fresh}", "REC#rec_fresh_wd")]
    assert fresh_rec["upload_status"] == "UPLOADING"
    assert fresh_rec["GSI1PK"] == "ACTIVE_UPLOAD"


class _FakeEC2Client:
    def __init__(self, *, state: str = "stopped", fail_code: str | None = None) -> None:
        self.state = state
        self.fail_code = fail_code
        self.start_calls: list[list[str]] = []

    def describe_instances(self, *, InstanceIds: list[str]) -> dict[str, Any]:
        return {
            "Reservations": [
                {"Instances": [{"InstanceId": InstanceIds[0], "State": {"Name": self.state}}]}
            ]
        }

    def start_instances(self, *, InstanceIds: list[str]) -> dict[str, Any]:
        self.start_calls.append(list(InstanceIds))
        if self.fail_code:
            raise _client_error(self.fail_code, "Simulated EC2 capacity error")
        self.state = "pending"
        return {"StartingInstances": [{"InstanceId": InstanceIds[0]}]}


def test_ec2_worker_wake_capacity_retry_and_watchdog() -> None:
    clock = [1_700_003_000.0]
    ddb = _FakeIngestResource()
    s3 = _FakeS3Client()
    sqs = _FakeSQSClient(queue_exists=True)
    ec2 = _FakeEC2Client(state="stopped", fail_code="InsufficientInstanceCapacity")
    svc = UploadControlService(
        ddb_resource=ddb,
        s3_client=s3,
        sqs_client=sqs,
        ec2_client=ec2,
        worker_instance_id="i-0g5worker123",
        auto_start_ec2=True,
        bucket_name="ovius-pipeline-apse3-610331148578",
        now_fn=lambda: clock[0],
    )

    # 1. Capacity error on initial wake keeps SQS message queued and releases WORKER#LOCK to IDLE
    created = svc.create_session(_session_payload("rec_ec2_cap_01"))
    uid = created["upload_id"]
    for idx in (0, 1):
        p = svc.presign_chunk_parts(
            {"upload_id": uid, "recording_id": "rec_ec2_cap_01", "chunk_index": idx, "part_numbers": [1]}
        )
        res = svc.complete_chunk(
            {
                "upload_id": uid,
                "recording_id": "rec_ec2_cap_01",
                "chunk_index": idx,
                "s3_upload_id": p["s3_upload_id"],
                "parts": [{"PartNumber": 1, "ETag": f'"e{idx}"'}],
            }
        )
    assert res["upload_status"] == "READY"
    assert res["enqueued_sqs"] is True
    assert res["worker_lock"]["ec2_started"] is False
    assert res["worker_lock"]["retry_in_watchdog"] is True
    assert res["worker_lock"]["error_code"] == "InsufficientInstanceCapacity"
    assert ddb.table.items[("WORKER#LOCK", "PRIMARY")]["status"] == "IDLE"
    assert len(sqs.messages) == 1

    # 2. Capacity recovers -> 5-minute watchdog retries and starts the stopped g5 instance
    ec2.fail_code = None
    sweep = orchestrator_handler(
        {
            "source": "aws.events",
            "detail-type": "Scheduled Event",
            "action": "stale_watchdog",
            "stale_after_s": 900,
        },
        service=svc,
    )
    assert sweep["ec2_watchdog"]["queued_messages"] == 1
    assert sweep["ec2_watchdog"]["wake_attempted"] is True
    assert sweep["ec2_watchdog"]["wake_result"]["ec2_started"] is True
    assert ec2.state == "pending"
    assert ddb.table.items[("WORKER#LOCK", "PRIMARY")]["status"] == "STARTING"
    assert len(ec2.start_calls) == 2

    # 3. Already active instance skips start_instances
    ec2.state = "running"
    already = svc.try_start_ec2_worker("rec_ec2_cap_02")
    assert already["ec2_started"] is False
    assert already["reason"] == "already_active"
    assert len(ec2.start_calls) == 2


def test_session_duplicate_guard_timestamp_normalization_bounds_and_derived_status() -> None:
    clock = [1_700_004_000.0]
    ddb = _FakeIngestResource()
    s3 = _FakeS3Client()
    sqs = _FakeSQSClient(queue_exists=True)
    svc = UploadControlService(ddb_resource=ddb, s3_client=s3, sqs_client=sqs, now_fn=lambda: clock[0])

    payload = _session_payload("rec_norm_01")
    payload["upload_id"] = "upl_fixed_01"
    payload["recordings"][0]["timestamp"] = "2026-11-19T15:00:00+07:00"

    resp1 = lambda_handler(
        {"httpMethod": "POST", "path": "/api/v1/uploads/sessions", "body": json.dumps(payload)},
        upload_service=svc,
    )
    assert resp1["statusCode"] == 201
    assert ddb.table.batch_writer_calls == 1
    assert ddb.table.items[("SESSION#upl_fixed_01", "REC#rec_norm_01")]["timestamp"] == "2026-11-19T08:00:00+00:00"

    # Duplicate upload_id returns HTTP 409 Conflict instead of overwriting
    resp_dup = lambda_handler(
        {"httpMethod": "POST", "path": "/api/v1/uploads/sessions", "body": json.dumps(payload)},
        upload_service=svc,
    )
    assert resp_dup["statusCode"] == 409

    # Invalid ISO timestamp rejected with 400
    bad_ts = _session_payload("rec_bad_ts")
    bad_ts["recordings"][0]["timestamp"] = "not-a-timestamp"
    assert (
        lambda_handler(
            {"httpMethod": "POST", "path": "/api/v1/uploads/sessions", "body": json.dumps(bad_ts)},
            upload_service=svc,
        )["statusCode"]
        == 400
    )

    # Exceeding chunk bound (>100 chunks) rejected with 400
    too_many = _session_payload("rec_many_chunks")
    too_many["recordings"][0]["chunks"] = [
        {"chunk_index": i, "start_offset_s": i * 10, "duration_s": 10, "byte_size": 100}
        for i in range(101)
    ]
    assert (
        lambda_handler(
            {"httpMethod": "POST", "path": "/api/v1/uploads/sessions", "body": json.dumps(too_many)},
            upload_service=svc,
        )["statusCode"]
        == 400
    )

    # Derived session_status reflects live recording states instead of staying UPLOADING forever
    assert svc.get_session_status("upl_fixed_01")["session_status"] == "UPLOADING"
    rec = ddb.table.items[("SESSION#upl_fixed_01", "REC#rec_norm_01")]
    rec["upload_status"] = "READY"
    rec["process_status"] = "QUEUED"
    assert svc.get_session_status("upl_fixed_01")["session_status"] == "QUEUED"
    rec["process_status"] = "PROCESSING_CHUNK"
    assert svc.get_session_status("upl_fixed_01")["session_status"] == "PROCESSING"
    rec["process_status"] = "COMPLETED"
    assert svc.get_session_status("upl_fixed_01")["session_status"] == "COMPLETED"
    rec["process_status"] = "FAILED"
    assert svc.get_session_status("upl_fixed_01")["session_status"] == "FAILED"
    rec["process_status"] = "ABORTED"
    rec["upload_status"] = "ABORTED"
    assert svc.get_session_status("upl_fixed_01")["session_status"] == "ABORTED"


