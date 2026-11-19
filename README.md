# Ovius AWS Backend (`ovius_backend`)

Standalone repository containing all **AWS CloudFormation templates, DynamoDB operational telemetry schemas, REST API Lambda handlers, and EC2/Batch GPU vision worker infrastructure** for the Ovius / AIMLC4 Heavy Equipment Productivity Pipeline.

> [!NOTE]
> All AWS CloudFormation templates and AWS backend infrastructure code live in this repository ([`ovius_backend`](file:///Users/vio/PycharmProjects/ovius_backend)). Model training, vision research docs, frontend audit notes, and the pure REST-API Gradio E2E test bench live in [`AIMLC4_Playground`](file:///Users/vio/PycharmProjects/AIMLC4_Playground).

- **API Reference & FE–BE Flow:** [`API_documentation.md`](file:///Users/vio/PycharmProjects/ovius_backend/API_documentation.md) *(identical canonical copy in [`AIMLC4_Playground/docs/API_documentation.md`](file:///Users/vio/PycharmProjects/AIMLC4_Playground/docs/API_documentation.md))*
- **Frontend Audit & Handover Notes:** [`frontend_audit_notes.md`](file:///Users/vio/PycharmProjects/AIMLC4_Playground/docs/frontend_audit_notes.md)

---

## 1. Repository Structure

```text
ovius_backend/
├── infra/
│   ├── dynamodb_tables.yaml          # CloudFormation: 4-table DynamoDB schema + S3 pipeline bucket (GLACIER_IR)
│   ├── api_stack.yaml                # CloudFormation: CloudFront -> REST API Gateway -> Lambda (+ auto EC2 wake)
│   └── ingest_compute_stack.yaml     # CloudFormation: SQS + DLQ, S3 VPC Endpoint, EventBridge/Watchdog, Stream ESM & optional Batch
├── scripts/
│   ├── deploy_storage.py             # Storage stack validator, S3 bucket importer & CFN deployer (ovius-dynamodb-telemetry)
│   ├── deploy_api.py                 # Deterministic zip packager & CFN deployer (ovius-api-backend)
│   ├── deploy_ingest_compute.py      # Orchestrator/SQS/VPC Endpoint (+ optional Batch) deployer (ovius-ingest-compute)
│   ├── build_g5_ami_apse3.py         # 50 GB Ubuntu 22.04 g5.xlarge AMI builder (NVIDIA + PyTorch 2.6 + TensorRT 10.9 + NVMe + CW Logs)
│   ├── build_ptz_code_bundle.py      # Slim S-v5-XL PTZ pipeline bundler (3.0 FPS frame skip, GPU CLAHE, relink fast seek, no annotated_video)
│   ├── export_sv5xl_tensorrt.py      # Export S-v5-XL ONNX & compile SM86 (A10G) TensorRT FP16 engine (39.13 FPS)
│   ├── ingest_dynamodb.py            # Resumable multi-threaded BatchWriteItem loader & GSI verifier
│   ├── publish_worker_image.py       # Registry V2 pusher for parameterized Batch/ECR compute mode
│   ├── verify_api_live.py            # Live API Gateway + CloudFront read smoke test suite
│   └── test_upload_flow_live.py      # Live mock MP4 multipart upload, SQS, orchestrator, worker & read API verifier
├── src/
│   └── ovius_api/
│       ├── filters.py                # Datetime range (start_time/start_date aliases), Camera ID*, and Decimal cursor validation
│       ├── repository.py             # Pure DynamoDB raw-telemetry reader + multi-chunk /videos & /bboxes ("ready" vs "template")
│       ├── uploads.py                # Resumable multipart upload sessions (409 Conflict, <=50 rec / <=100 chunks, UTC +00:00), derived session_status, WORKER#LOCK & EC2 wake
│       ├── ec2_pipeline_worker.py    # Persistent g5.xlarge systemd worker (ETag hot-sync, NVMe scratch, chunk join, S-v5-XL TensorRT, cycle_id="NONE", poison-pill isolation, auto-stop)
│       ├── orchestrator.py           # EventBridge S3 fallback completion & 5m capacity-resilient EC2/stale watchdog
│       ├── worker.py                 # VisionWorkerRunner base logic, lease constants, checkpoints, EMF logs & min(ETA) keep-warm
│       ├── broadcaster.py            # Filtered DynamoDB Stream -> WebSocket progress push + 410 cleanup
│       └── handler.py                # API Gateway proxy router, CORS headers & error mapping (400/404/405/409/500/502)
├── tests/
│   ├── test_api_handler.py           # Filter aliases, Decimal cursors, byte guard, routes & "ready" vs "template"
│   ├── test_upload_routes.py         # Multipart uploads, 409 guard, derived status, GSI min(ETA), SQS, EC2 wake & watchdog
│   ├── test_ec2_pipeline_worker.py   # PTZ bundle patch (relink fast seek), cycle_id="NONE", UTC +00:00, poison-pill recovery & auto-shutdown
│   └── test_worker_and_broadcaster.py # Worker checkpoints, keep-warm, broadcaster & multi-chunk /videos /bboxes
├── API_documentation.md              # Full FE-BE REST API contract, sequence diagrams & payload schemas
├── AGENTS.md                         # Backend AWS engineering guidelines & resource tag guardrails
├── pyproject.toml                    # Python 3.11 project & uv dependencies (boto3, pytest)
└── README.md
```

> **Where is the E2E Web UI?** The pure REST-API Gradio 6 E2E streaming web UI lives strictly in [`AIMLC4_Playground/scripts/ovius_e2e_ui.py`](file:///Users/vio/PycharmProjects/AIMLC4_Playground/scripts/ovius_e2e_ui.py) (zero `boto3` imports) with tests in [`AIMLC4_Playground/tests/test_ovius_e2e_ui.py`](file:///Users/vio/PycharmProjects/AIMLC4_Playground/tests/test_ovius_e2e_ui.py).

---

## 2. Provisioned Storage & DynamoDB Architecture (`ap-southeast-3` Jakarta Primary)

Defined in [`infra/dynamodb_tables.yaml`](file:///Users/vio/PycharmProjects/ovius_backend/infra/dynamodb_tables.yaml) (`StackName: ovius-dynamodb-telemetry`, default `TablePrefix: ovius`, `PipelineBucketName: ovius-pipeline-apse3-610331148578`, Account `610331148578`, `IN_SYNC`, `0` drift):

| Resource / Table Name | Keys / Configuration | Global Secondary Indexes (`GSIs`) / Rules | Protection & Policies |
| :--- | :--- | :--- | :--- |
| **`ovius-recording`** | `PK: recording_id` (`S`) | `camera-id-timestamp-index` (`PK: camera_id`, `SK: timestamp` [`+00:00` UTC], `ALL`) | `PAY_PER_REQUEST`, SSE, PITR, `Retain` |
| **`ovius-haul-cycle`** | `PK: cycle_id` (`S`) | `recording-id-index` (`PK: recording_id`, `SK: t_load_start_s`, `ALL`)<br>`recording-excavator-index` (`PK: recording_id`, `SK: excavator_sort_key`, `ALL`) | `PAY_PER_REQUEST`, SSE, PITR, `Retain` |
| **`ovius-state-segment`** | `PK: segment_id` (`S`) | `recording-id-index` (`PK: recording_id`, `SK: start_s`, `ALL`)<br>`cycle-id-index` (`PK: cycle_id`, `SK: cycle_sort_key`, `ALL`) *(unlinked segments store `cycle_id: "NONE"` without `cycle_sort_key` to keep GSI sparse)* | `PAY_PER_REQUEST`, SSE, PITR, `Retain` |
| **`ovius-ingest-state`** | `PK: pk` (`S`), `SK: sk` (`S`) | `GSI_ACTIVE_UPLOADS` (`PK: GSI1PK`, `SK: GSI1SK` [`N`], `ALL`) *(sparse)* | `PAY_PER_REQUEST`, SSE, PITR, TTL `expires_at`, Stream `NEW_IMAGE`, `Retain` |
| **`ovius-pipeline-apse3-610331148578`** | S3 Bucket (`PipelineBucket` + `PipelineBucketPolicy`) | Lifecycle: `uploads/` 1-day incomplete multipart abort, `parquet/` 90-day **`GLACIER_IR`** instant-retrieval transition; CORS `ETag` exposed; EventBridge enabled | `BucketOwnerEnforced`, Public Access Blocked, AES256, TLS-only (`aws:SecureTransport`), `Retain` |

- **Schema, Client & Cloud Architecture References:**
  - [REST API Contract & FE–BE Flow (`API_documentation.md`)](file:///Users/vio/PycharmProjects/ovius_backend/API_documentation.md)
  - [Database Schema Plan (`AIMLC4_Playground`)](file:///Users/vio/PycharmProjects/AIMLC4_Playground/docs/database_schema_plan.md)
  - [AWS Production Architecture Plan (`AIMLC4_Playground`)](file:///Users/vio/PycharmProjects/AIMLC4_Playground/docs/aws_architecture_plan.md)
  - [Ovius Native macOS Client (`ovius`)](file:///Users/vio/XCodeProjects/ovius/README.md) & [Frontend Audit Notes](file:///Users/vio/PycharmProjects/AIMLC4_Playground/docs/frontend_audit_notes.md)

---

## 3. Environment Setup (`uv` + Python 3.11)

```bash
cd /Users/vio/PycharmProjects/ovius_backend
uv venv --python 3.11
uv sync
uv run pytest -v   # 30/30 unit tests passing
```

---

## 4. Common Operations

### 4.1 Validate & Deploy Storage CloudFormation Stack (`ap-southeast-3`)
```bash
# Validate template & change set without applying
uv run python scripts/deploy_storage.py \
  --region ap-southeast-3 \
  --pipeline-bucket-name ovius-pipeline-apse3-610331148578 \
  --validate-only

# Apply storage stack (GLACIER_IR, 4 DynamoDB tables) and verify IN_SYNC drift status
uv run python scripts/deploy_storage.py \
  --region ap-southeast-3 \
  --pipeline-bucket-name ovius-pipeline-apse3-610331148578 \
  --detect-drift
```

### 4.2 Ingest Synthetic Seed JSONL into DynamoDB
By default [`scripts/ingest_dynamodb.py`](file:///Users/vio/PycharmProjects/ovius_backend/scripts/ingest_dynamodb.py) resolves [`AIMLC4_Playground/mock_data/dynamodb_items.jsonl`](file:///Users/vio/PycharmProjects/AIMLC4_Playground/mock_data/dynamodb_items.jsonl):

```bash
# Live seed ingestion into ap-southeast-3 & post-load GetItem / GSI Query verification
uv run python scripts/ingest_dynamodb.py \
  --profile 610331148578_Project8Learner \
  --region ap-southeast-3 \
  --table-prefix ovius \
  --workers 28
```

### 4.3 Frontend Data Access & Upload API (`CloudFront -> API Gateway -> Lambda -> DynamoDB`)
Provisioned via [`infra/api_stack.yaml`](file:///Users/vio/PycharmProjects/ovius_backend/infra/api_stack.yaml) (`StackName: ovius-api-backend` in `ap-southeast-3`, `IN_SYNC`, `0` drift, executing under `Project8LambdaExecutionRole`, artifact bucket `ovius-pipeline-apse3-610331148578`, wired with `WorkerInstanceId=i-05dab2fadd5f3a113` and `EnableAutoEc2Start=true`).

All mathematical KPI calculations run strictly on the **frontend**; the Lambda backend filters, projects (`include_scoops=false`), and paginates raw DynamoDB items with a `4.2 MB` JSON payload guard (`limit <= 15000`).

- **Primary CloudFront Base URL:** `https://d1zj4agypqh81v.cloudfront.net/api/v1` (Distribution `E21IWGD0JCX7OL`, `Managed-CachingDisabled` + `Managed-AllViewerExceptHostHeader` + `Managed-SecurityHeadersPolicy`)
- **Direct API Gateway Base URL:** `https://4jg1b9qut2.execute-api.ap-southeast-3.amazonaws.com/v1/api/v1` (REST API `4jg1b9qut2`, Stage `v1`, `AuthType: NONE`, `RequireApiKey` parameter ready)
- **Lambda Function:** `ovius-api-backend-data-api` (Python 3.11 `arm64`, `512 MB`, `29s` timeout)
- **Routes (Full specs in [`API_documentation.md`](file:///Users/vio/PycharmProjects/ovius_backend/API_documentation.md)):**
  - `GET /api/v1/metrics` *(composite recordings + haul_cycles + optional `include_segments=true`)*
  - `GET /api/v1/metrics/recordings`
  - `GET /api/v1/metrics/cycles`
  - `GET /api/v1/metrics/segments`
  - `GET /api/v1/bboxes` *(multi-chunk Snappy Parquet URIs + `presigned_url`, `"status": "ready"` when live artifacts exist)*
  - `GET /api/v1/videos` *(multi-chunk `chunks` array with `GET`-only pre-signed S3 URLs, `"status": "ready"` when live chunks exist)*
  - `POST /api/v1/uploads/sessions` *(register up to `50` recordings / `100` chunks each; `409 Conflict` duplicate guard; UTC `+00:00` normalization)*
  - `POST /api/v1/uploads/chunks/presign` *(lazy & resumable SigV4 `upload_part` URL minting)*
  - `PUT /api/v1/uploads/sessions/{upload_id}/heartbeat` *(refresh sparse `GSI_ACTIVE_UPLOADS` ETA)*
  - `POST /api/v1/uploads/chunks/complete` *(complete multipart + `TransactWriteItems` + atomic `UPLOADING -> READY` + SQS enqueue + auto EC2 wake)*
  - `GET /api/v1/uploads/sessions/{upload_id}` *(5s status poll returning derived `session_status`, `worker_status`, `worker_instance_state`, and global `min_remaining_upload_eta_s`)*
  - `POST /api/v1/uploads/recordings/{recording_id}/abort` *(abort multipart upload + evict GSI)*
- **Filters (`GET` telemetry routes):** `start_time` / `end_time` (`start_date` / `end_date` accepted as aliases), `tod_start_s`, `tod_end_s`, `camera_id` (`*` or comma-separated), `location_tag`, `tag`, `filename`, `recording_id`, `cycle_id`, `excavator_id`, `state`, `include_scoops`, `include_segments`, `limit` (`1..15000`), `next_token`.

```bash
# Package & deploy API stack (ovius-api-backend) in ap-southeast-3
uv run python scripts/deploy_api.py \
  --region ap-southeast-3 \
  --s3-bucket ovius-pipeline-apse3-610331148578 \
  --analytics-bucket-name ovius-pipeline-apse3-610331148578 \
  --worker-instance-id i-05dab2fadd5f3a113 \
  --enable-auto-ec2-start true \
  --detect-drift

# Verify live API Gateway + CloudFront read endpoints
uv run python scripts/verify_api_live.py --region ap-southeast-3
```

### 4.4 Ingest Compute Orchestration & Auto-Waking `g5.xlarge` TensorRT Worker (`ap-southeast-3`)
Provisioned via [`infra/ingest_compute_stack.yaml`](file:///Users/vio/PycharmProjects/ovius_backend/infra/ingest_compute_stack.yaml) (`StackName: ovius-ingest-compute` in `ap-southeast-3`, `IN_SYNC`, `0` drift) with `EnableBatchCompute=false`, `WorkerInstanceId=i-05dab2fadd5f3a113`, and `EnableAutoEc2Start=true` *(AWS Batch `EnableBatchCompute=true` remains parameterized for future multi-node bursts)*:

- **SQS & VPC Networking:** `ovius-ready-recordings` (`VisibilityTimeout = 900s`, SSE-SQS, `maxReceiveCount = 5`) + `ovius-ready-recordings-dlq`, plus free S3 Gateway VPC Endpoint (`vpce-01a9d549d72513bbf` on `vpc-04cbff2c12511037a` / `rtb-0ff92726d27caa8cf`).
- **Orchestrator & Watchdog (`ovius-ingest-compute-orchestrator`):** S3 `Object Created` fallback on `uploads/` + 5-minute stale-upload and capacity-resilient EC2 retry watchdog.
- **Persistent Auto-Waking `g5.xlarge` Worker (`i-05dab2fadd5f3a113` / AMI `ami-0d16bf41d6e319a70`):**
  - **50 GB Encrypted Base AMI (`ap-southeast-3c`):** Built via [`scripts/build_g5_ami_apse3.py`](file:///Users/vio/PycharmProjects/ovius_backend/scripts/build_g5_ami_apse3.py) (Ubuntu 22.04 + NVIDIA 580 + PyTorch `2.6.0+cu124` + `tensorrt-cu12==10.9.0.34` + `ovius-pipeline-worker.service`).
  - **Zero-Rebuild S3 Hot-Sync:** On wake from `stopped` (~10s boot), `ovius-pipeline-worker.service` mounts the free **`229 GB` NVMe instance store at `/mnt/scratch`** (`WORKER_TMP_DIR=/mnt/scratch`) via `/opt/pipeline/mount_nvme_scratch.sh`, ETag hot-syncs `pipeline/ovius_api_pkg.tar.gz`, `pipeline/ptz_code_bundle.tar.gz`, and S3-decoupled `S-v5-XL` artifacts (`models/s-v5-xl/*`, including `s_v5_xl_1920x1088_fp16_sm86.engine` `128 MiB` @ `39.13 FPS` = `3.50×` speedup over PyTorch FP16).
  - **High-Throughput Execution:** Dynamic framerate detection (`target_fps = 3.0` $\to$ `skip=8` at 24/25 FPS, `skip=10` at 30 FPS, `skip=3` at 10 FPS), GPU-accelerated v3 Bandpass-Chroma CLAHE, SIMD BGR→RGB, pipelined async H.264 decode + concurrent `CameraMotion`/`Detect`, `relink()` single-segment skip + `CAP_PROP_POS_FRAMES` fast seek, and single `boxes.csv` → Snappy Parquet upload.
  - **Observed Live Performance:** `0.22×–0.38× RTF` (`mock_sample_4.mp4` `335.6s` in `107.9s` cold-to-DDB; `3 × 12h` footage $\approx 8.0\text{–}8.7\text{ hrs}$ on `1× g5.xlarge`).
  - **Observability & Self-Stopping:** Logs stream live from `/var/log/ovius-worker.log` to CloudWatch Logs (`/aws/ec2/ovius-vision-worker`, 30-day retention) alongside EMF metrics (`Ovius/VisionPipeline`) and host/GPU metrics (`Ovius/VisionWorker`). Poison-pill recordings are isolated for SQS DLQ retries without crashing healthy jobs, and the instance reliably sets `WORKER#LOCK = IDLE` and powers off back to **`stopped` (`$0/hr` idle)** when SQS drains and $\min(\text{ETA}) \ge 5\text{ min}$.

```bash
# Deploy ingest compute stack in ap-southeast-3
uv run python scripts/deploy_ingest_compute.py \
  --region ap-southeast-3 \
  --enable-batch-compute false \
  --worker-instance-id i-05dab2fadd5f3a113 \
  --enable-auto-ec2-start true \
  --detect-drift

# Rebuild & upload slim PTZ bundle (dynamically hot-synced by worker on next wake)
uv run python scripts/build_ptz_code_bundle.py --region ap-southeast-3
```

### 4.5 Pure REST-API Gradio E2E Streaming Test UI (`AIMLC4_Playground`)
Use the decoupled Gradio 6 tester in [`AIMLC4_Playground/scripts/ovius_e2e_ui.py`](file:///Users/vio/PycharmProjects/AIMLC4_Playground/scripts/ovius_e2e_ui.py) to split multi-recording `.mp4` files losslessly into `N` chunks, edit metadata inline, upload via pre-signed multipart PUTs, stream live `g5.xlarge` RTF/ETA progress, and inspect streamed DynamoDB rows strictly via CloudFront HTTPS:

```bash
cd /Users/vio/PycharmProjects/AIMLC4_Playground
.venv/bin/python scripts/ovius_e2e_ui.py --host 127.0.0.1 --port 7860
```
