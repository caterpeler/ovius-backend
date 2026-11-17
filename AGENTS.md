# AGENTS.md — Ovius AWS Backend Repository Guidelines

Welcome to **`ovius_backend`**, the dedicated repository for AWS CloudFormation infrastructure, DynamoDB telemetry schemas, and backend cloud automation for the Ovius / AIMLC4 Heavy Equipment Productivity Pipeline.

---

## 1. Repository Boundary

- **Owns:** All AWS CloudFormation templates (`infra/`), DynamoDB / API Gateway / Lambda / EventBridge / CloudFront / S3 backend IaC and ingestion scripts (`scripts/`).
- **Upstream References (Separate Repo):** Computer vision training, CCTV dataset pipelines, local Excel/HTML dashboards, and SwiftData diagnostic specifications live in [`AIMLC4_Playground`](file:///Users/vio/PycharmProjects/AIMLC4_Playground) (mirrored to `CATERPELER`).

---

## 2. AWS Target & Tagging Guardrails

- **Active DynamoDB Account & Region:** Account `610331148578` (`--profile 610331148578_Project8Learner`), Region `ap-southeast-3` (Jakarta).
- **Mandatory Resource Tags:** Tag all provisioned AWS resources with:
  - `Owner: project8`
  - `Project: AIMLC4`
  - `Iteration: v5`
- **CloudFormation Safety Defaults:**
  - Always set `DeletionPolicy: Retain` and `UpdateReplacePolicy: Retain` on stateful tables and S3 buckets.
  - Enable server-side encryption (`SSESpecification`) and continuous backups (`PointInTimeRecoverySpecification`) on DynamoDB tables.
  - Prefer parameterized resource prefixes (default `TablePrefix=ovius`) over hardcoded literal names.

---

## 3. Python & Engineering Standards

- **Runtime:** Python 3.11 managed via `uv` (`uv venv --python 3.11 && uv sync`).
- **AWS SDK Usage:** Reuse shared `boto3.Session` clients configured with `botocore.config.Config(retries={"total_max_attempts": 10, "mode": "adaptive"})` and handle exceptions via `botocore.exceptions.ClientError`.
- **Zero-Math Backend Invariant:** **ALL mathematical calculations, KPI aggregations, Z-scores, hourly averages, payload conversions (`51.1 m³`), and stationary idle discounts (`300s`) live strictly on the frontend (SwiftData settings + dashboard logic).** The AWS Lambda API (`src/ovius_api/`) only filters, projects, and paginates raw DynamoDB items (`ovius-recording`, `ovius-haul-cycle`, `ovius-state-segment`).

---

## 4. Provisioned Live Infrastructure (`ap-southeast-3`)

1. **`ovius-dynamodb-telemetry` ([`infra/dynamodb_tables.yaml`](file:///Users/vio/PycharmProjects/ovius_backend/infra/dynamodb_tables.yaml)):**
   - Tables `ovius-recording` (`990` items), `ovius-haul-cycle` (`366,972` items), and `ovius-state-segment` (`4,318,852` items) seeded via [`scripts/ingest_dynamodb.py`](file:///Users/vio/PycharmProjects/ovius_backend/scripts/ingest_dynamodb.py) (`4,686,814` total items).
2. **`ovius-api-backend` ([`infra/api_stack.yaml`](file:///Users/vio/PycharmProjects/ovius_backend/infra/api_stack.yaml)):**
   - **CloudFront (`E21IWGD0JCX7OL`):** `https://d1zj4agypqh81v.cloudfront.net/api/v1` (`Managed-CachingDisabled` + `Managed-AllViewerExceptHostHeader` + `Managed-SecurityHeadersPolicy`).
   - **REST API Gateway (`4jg1b9qut2`, Stage `v1`):** `https://4jg1b9qut2.execute-api.ap-southeast-3.amazonaws.com/v1/api/v1` (`AuthType: NONE`, parameterized `RequireApiKey` / `ApiKey` / `UsagePlan` ready for `x-api-key`).
   - **Lambda (`ovius-api-backend-data-api`):** Python 3.11 `arm64`, executing under pre-provisioned `arn:aws:iam::610331148578:role/Project8LambdaExecutionRole` (`Project8Learner` does not permit `iam:CreateRole`), with wave-parallel querying, `4.2 MB` payload byte guard, `limit` up to `15,000`, and `Decimal`-preserving `next_token` pagination.
   - **Test & Deploy Commands:** `uv run pytest -v`, `uv run python scripts/deploy_api.py`, `uv run python scripts/verify_api_live.py`.
