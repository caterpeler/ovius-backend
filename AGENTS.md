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
- **Telemetry Decoupling Invariant:** Never store derived mathematical assumptions (`mean_payload_m3 = 51.1`, `idle_threshold_s = 300`, `shift_edge_window_min = 60`) in raw DynamoDB telemetry tables; those reside client-side in SwiftData `settings`.
