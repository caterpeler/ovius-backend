# AGENTS.md — Ovius AWS Backend Repository Guidelines

Welcome to **`ovius_backend`**, the dedicated repository for AWS CloudFormation infrastructure, DynamoDB telemetry schemas, and backend cloud automation for the Ovius / AIMLC4 Heavy Equipment Productivity Pipeline.

---

## 1. Repository Boundary

- **Owns:** All AWS CloudFormation templates (`infra/`), DynamoDB / API Gateway / Lambda / EventBridge / CloudFront / S3 / AWS Batch backend IaC and ingestion scripts (`scripts/`).
- **Sibling Repositories:**
  - Computer vision training, CCTV dataset pipelines, local Excel/HTML dashboards, and telemetry/cloud specs live in [`AIMLC4_Playground`](file:///Users/vio/PycharmProjects/AIMLC4_Playground) (including planned Phase 2 upload/Batch compute specs in [`docs/aws_architecture_plan.md`](file:///Users/vio/PycharmProjects/AIMLC4_Playground/docs/aws_architecture_plan.md)).
  - Native macOS SwiftUI + SwiftData frontend client (`feat-setup-managers`) lives in [`ovius`](file:///Users/vio/XCodeProjects/ovius).

---

## 2. AWS Target & Tagging Guardrails

- **Active DynamoDB Account & Region:** Account `610331148578` (`--profile 610331148578_Project8Learner`), **Primary Region `ap-southeast-3` (Jakarta)** (all 3 stacks + persistent `g5.xlarge` GPU worker), with `ap-southeast-1` (Singapore) retained as secondary.
- **Mandatory Resource Tags:** Tag all provisioned AWS resources with:
  - `Owner: project8`
  - `Project: AIMLC4`
  - `Iteration: v5`
- **CloudFormation Safety Defaults:**
  - Always set `DeletionPolicy: Retain` and `UpdateReplacePolicy: Retain` on stateful tables, S3 buckets, and SQS queues.
  - Enable server-side encryption (`SSESpecification`) and continuous backups (`PointInTimeRecoverySpecification`) on DynamoDB tables.
  - Prefer parameterized resource prefixes (default `TablePrefix=ovius`) over hardcoded literal names.

---

## 3. Python & Engineering Standards

- **Runtime:** Python 3.11 managed via `uv` (`uv venv --python 3.11 && uv sync`).
- **AWS SDK Usage:** Reuse shared `boto3.Session` clients configured with `botocore.config.Config(retries={"total_max_attempts": 10, "mode": "adaptive"})` and handle exceptions via `botocore.exceptions.ClientError`.
- **Zero-Math Backend Invariant:** **ALL mathematical calculations, KPI aggregations, Z-scores, hourly averages, payload conversions (`51.1 m³`), and stationary idle discounts (`300s`) live strictly on the frontend (SwiftData settings + dashboard logic).** The AWS Lambda API (`src/ovius_api/`) only filters, projects, and paginates raw DynamoDB items (`ovius-recording`, `ovius-haul-cycle`, `ovius-state-segment`).
- **UI Boundary:** Client test web UIs (including the pure REST-API Gradio E2E test bench [`scripts/ovius_e2e_ui.py`](file:///Users/vio/PycharmProjects/AIMLC4_Playground/scripts/ovius_e2e_ui.py)) live strictly in `AIMLC4_Playground` and interact only via HTTPS REST APIs.

---

## 4. Provisioned Live Infrastructure (`ap-southeast-3` Primary)

1. **`ovius-dynamodb-telemetry` ([`infra/dynamodb_tables.yaml`](file:///Users/vio/PycharmProjects/ovius_backend/infra/dynamodb_tables.yaml)):**
   - Tables `ovius-recording` (`990` items), `ovius-haul-cycle` (`366,972` items), `ovius-state-segment` (`4,318,852` items), and Phase 2 control table `ovius-ingest-state` (`GSI_ACTIVE_UPLOADS`, `NEW_IMAGE` Stream, TTL `expires_at`), seeded via [`scripts/ingest_dynamodb.py`](file:///Users/vio/PycharmProjects/ovius_backend/scripts/ingest_dynamodb.py) (`4,686,814` telemetry items).
   - **S3 Pipeline & Staging Bucket (`PipelineBucket` + `PipelineBucketPolicy`):** `ovius-pipeline-apse3-610331148578` (`BucketOwnerEnforced`, Public Access Blocked, AES256, `uploads/` 1-day incomplete multipart abort, `parquet/` 90-day **`GLACIER_IR`** transition, CORS `ETag` exposed, EventBridge enabled, TLS-only bucket policy, `Retain` policies). Hosts S3-decoupled `S-v5-XL` artifacts (`models/s-v5-xl/*`, including `s_v5_xl_1920x1088_fp16_sm86.engine` `128 MiB` @ `39.13 FPS` / `3.50x` speedup over PyTorch FP16), `pipeline/ovius_api_pkg.tar.gz`, and `pipeline/ptz_code_bundle.tar.gz` (dynamic `3.0 FPS` frame skip, GPU v3 CLAHE, pipelined decode, `relink` fast seek).
2. **`ovius-api-backend` ([`infra/api_stack.yaml`](file:///Users/vio/PycharmProjects/ovius_backend/infra/api_stack.yaml)):**
   - **CloudFront (`E21IWGD0JCX7OL`):** `https://d1zj4agypqh81v.cloudfront.net/api/v1` (`Managed-CachingDisabled` + `Managed-AllViewerExceptHostHeader` + `Managed-SecurityHeadersPolicy`, `AllowedMethods` supporting `GET`/`POST`/`PUT`/`PATCH`/`DELETE`/`OPTIONS`).
   - **REST API Gateway (`4jg1b9qut2`, Stage `v1`):** `https://4jg1b9qut2.execute-api.ap-southeast-3.amazonaws.com/v1/api/v1` (`AuthType: NONE`, `GET`/`POST`/`PUT`/`OPTIONS`, parameterized `RequireApiKey` / `ApiKey` / `UsagePlan` ready for `x-api-key`).
   - **Lambda (`ovius-api-backend-data-api`):** Python 3.11 `arm64`, executing under pre-provisioned `arn:aws:iam::610331148578:role/Project8LambdaExecutionRole` (with managed policy `Project8LambdaExecutionRoleInstances` for tag-scoped `ec2:StartInstances`/`StopInstances`), wired to `WorkerInstanceId=i-05dab2fadd5f3a113` (`EnableAutoEc2Start=true`).
3. **`ovius-ingest-compute` ([`infra/ingest_compute_stack.yaml`](file:///Users/vio/PycharmProjects/ovius_backend/infra/ingest_compute_stack.yaml)):**
   - Deployed with `EnableBatchCompute=false`, `WorkerInstanceId=i-05dab2fadd5f3a113`, `EnableAutoEc2Start=true`: SQS `ovius-ready-recordings` (`VisibilityTimeout = 900s`, DLQ `ovius-ready-recordings-dlq`), free S3 Gateway VPC Endpoint (`vpce-01a9d549d72513bbf` on `vpc-04cbff2c12511037a` / `rtb-0ff92726d27caa8cf`), EventBridge S3 fallback + 5-minute capacity-resilient EC2 watchdog (`ovius-ingest-compute-orchestrator`), and filtered DynamoDB Stream broadcaster (`ovius-ingest-compute-progress-broadcaster`).
4. **Persistent GPU Worker & `50 GB` AMI (`ap-southeast-3c`):**
   - **AMI:** `ami-0d16bf41d6e319a70` (`ovius-sv5xl-ptz-v1-apse3`, `50 GB` encrypted `gp3`, Ubuntu 22.04 + NVIDIA 580 + PyTorch `2.6.0+cu124` + `tensorrt-cu12==10.9.0.34` + `ovius-pipeline-worker.service`), built via [`scripts/build_g5_ami_apse3.py`](file:///Users/vio/PycharmProjects/ovius_backend/scripts/build_g5_ami_apse3.py).
   - **Persistent Instance:** `i-05dab2fadd5f3a113` (`g5.xlarge` in `ap-southeast-3c`, `InstanceInitiatedShutdownBehavior=stop`, `Project8BatchInstanceRole`, IMDSv2). Auto-woken from `stopped` on `READY` recordings (~10s boot), auto-mounts its `229 GB` local NVMe instance store at `/mnt/scratch` (`WORKER_TMP_DIR=/mnt/scratch`) via `/opt/pipeline/mount_nvme_scratch.sh`, ETag hot-syncs `ovius_api_pkg.tar.gz`, `ptz_code_bundle.tar.gz`, and `S-v5-XL` TensorRT assets, streams `/var/log/ovius-worker.log` live to CloudWatch Logs (`/aws/ec2/ovius-vision-worker`, 30-day retention), isolates poison-pill failures without skipping healthy jobs or shutdown, publishes canonical `+00:00` UTC telemetry (`cycle_id: "NONE"` on unlinked segments) + single-pass Snappy Parquet, bridges `<5m` uploads via keep-warm, and auto-stops cleanly back to `stopped` (`$0/hr` idle).
   - **Handover Docs & Tests:** `30/30` pytest tests passing (`uv run pytest -v`). Full REST contract documented in [`API_documentation.md`](file:///Users/vio/PycharmProjects/ovius_backend/API_documentation.md) and peer frontend notes in [`frontend_audit_notes.md`](file:///Users/vio/PycharmProjects/AIMLC4_Playground/docs/frontend_audit_notes.md).
   - **Test & Deploy Commands:** `uv run pytest -v`, `uv run python scripts/deploy_storage.py --region ap-southeast-3 --pipeline-bucket-name ovius-pipeline-apse3-610331148578 --detect-drift`, `uv run python scripts/deploy_ingest_compute.py --region ap-southeast-3 --enable-batch-compute false --worker-instance-id i-05dab2fadd5f3a113 --enable-auto-ec2-start true --detect-drift`, `uv run python scripts/deploy_api.py --region ap-southeast-3 --s3-bucket ovius-pipeline-apse3-610331148578 --analytics-bucket-name ovius-pipeline-apse3-610331148578 --worker-instance-id i-05dab2fadd5f3a113 --enable-auto-ec2-start true --detect-drift`, `uv run python scripts/verify_api_live.py --region ap-southeast-3`.
