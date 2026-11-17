# Ovius AWS Backend (`ovius_backend`)

Standalone repository containing all **AWS CloudFormation templates, DynamoDB operational telemetry schemas, and backend AWS ingestion/provisioning tooling** for the Ovius / AIMLC4 Heavy Equipment Productivity Pipeline.

> [!NOTE]
> All AWS CloudFormation and AWS backend infrastructure code lives in this repository ([`ovius_backend`](file:///Users/vio/PycharmProjects/ovius_backend)). Model training scripts live in a separate repository.

---

## 1. Repository Structure

```text
ovius_backend/
├── infra/
│   ├── dynamodb_tables.yaml      # CloudFormation stack for 3-table DynamoDB schema (ovius-*)
│   └── api_stack.yaml            # CloudFormation stack: CloudFront -> REST API Gateway -> Lambda
├── scripts/
│   ├── ingest_dynamodb.py        # Resumable multi-threaded BatchWriteItem loader & GSI verifier
│   ├── deploy_api.py             # Deterministic zip packager & CFN deployer (ovius-api-backend)
│   └── verify_api_live.py        # Live API Gateway + CloudFront smoke test suite
├── src/
│   └── ovius_api/
│       ├── filters.py            # Datetime range, Camera ID*, and Decimal cursor validation
│       ├── repository.py         # Pure DynamoDB raw-telemetry reader (zero server-side math)
│       └── handler.py            # API Gateway proxy router + CORS + Decimal JSON serializer
├── tests/
│   └── test_api_handler.py       # Unit tests for filter parsing, cursors, byte guard & routes
├── AGENTS.md                     # Backend AWS engineering guidelines & resource tag guardrails
├── pyproject.toml                # Python 3.11 project & uv dependencies (boto3, boto3-stubs)
└── README.md
```

---

## 2. Provisioned DynamoDB Architecture (`ap-southeast-3` Jakarta)

Defined in [`infra/dynamodb_tables.yaml`](file:///Users/vio/PycharmProjects/ovius_backend/infra/dynamodb_tables.yaml) (`StackName: ovius-dynamodb-telemetry`, default `TablePrefix: ovius`):

| Table Name | Partition Key (`PK`) | Global Secondary Indexes (`GSIs`) | Billing & Protection |
| :--- | :--- | :--- | :--- |
| **`ovius-recording`** | `recording_id` (`S`) | `camera-id-timestamp-index` (`PK: camera_id`, `SK: timestamp`, `ALL`) | `PAY_PER_REQUEST`, SSE, PITR, `Retain` |
| **`ovius-haul-cycle`** | `cycle_id` (`S`) | `recording-id-index` (`PK: recording_id`, `SK: t_load_start_s`, `ALL`)<br>`recording-excavator-index` (`PK: recording_id`, `SK: excavator_sort_key`, `ALL`) | `PAY_PER_REQUEST`, SSE, PITR, `Retain` |
| **`ovius-state-segment`** | `segment_id` (`S`) | `recording-id-index` (`PK: recording_id`, `SK: start_s`, `ALL`)<br>`cycle-id-index` (`PK: cycle_id`, `SK: cycle_sort_key`, `ALL`) | `PAY_PER_REQUEST`, SSE, PITR, `Retain` |

- **Schema & Cloud Architecture References:**
  - [Database Schema Plan (`AIMLC4_Playground`)](file:///Users/vio/PycharmProjects/AIMLC4_Playground/docs/database_schema_plan.md)
  - [AWS Production Architecture Plan (`AIMLC4_Playground`)](file:///Users/vio/PycharmProjects/AIMLC4_Playground/docs/aws_architecture_plan.md)

---

## 3. Environment Setup (`uv` + Python 3.11)

```bash
cd /Users/vio/PycharmProjects/ovius_backend
uv venv --python 3.11
uv sync
```

---

## 4. Common Operations

### 4.1 Validate & Deploy CloudFormation Stack (`ap-southeast-3`)
```bash
aws cloudformation validate-template \
  --template-body file://infra/dynamodb_tables.yaml \
  --profile 610331148578_Project8Learner \
  --region ap-southeast-3

aws cloudformation deploy \
  --stack-name ovius-dynamodb-telemetry \
  --template-file infra/dynamodb_tables.yaml \
  --parameter-overrides TablePrefix=ovius EnvironmentName=dev \
  --tags Owner=project8 Project=AIMLC4 Iteration=v5 \
  --profile 610331148578_Project8Learner \
  --region ap-southeast-3
```

### 4.2 Ingest Synthetic Seed JSONL into DynamoDB
By default [`scripts/ingest_dynamodb.py`](file:///Users/vio/PycharmProjects/ovius_backend/scripts/ingest_dynamodb.py) resolves [`AIMLC4_Playground/mock_data/dynamodb_items.jsonl`](file:///Users/vio/PycharmProjects/AIMLC4_Playground/mock_data/dynamodb_items.jsonl) (or pass `--jsonl /path/to/dynamodb_items.jsonl`):

```bash
# Dry-run parse check
uv run python scripts/ingest_dynamodb.py --dry-run --max-lines 5000

# Live seed ingestion & post-load GetItem / GSI Query verification
uv run python scripts/ingest_dynamodb.py \
  --profile 610331148578_Project8Learner \
  --region ap-southeast-3 \
  --table-prefix ovius \
  --workers 28
```

### 4.3 Frontend Data Access Interface (`CloudFront -> API Gateway -> Lambda -> DynamoDB`)
Provisioned via [`infra/api_stack.yaml`](file:///Users/vio/PycharmProjects/ovius_backend/infra/api_stack.yaml) (`StackName: ovius-api-backend` in `ap-southeast-3` under `Project8LambdaExecutionRole`). All metric math happens on the frontend; the backend serves filtered, projected, and paginated raw DynamoDB telemetry (`ovius-recording` `990` items, `ovius-haul-cycle` `366,972` items, `ovius-state-segment` `4,318,852` items) with a `4.2 MB` JSON payload guard plus template endpoints for `bboxes` and `videos`.

- **CloudFront Base URL:** `https://d1zj4agypqh81v.cloudfront.net/api/v1` (Distribution `E21IWGD0JCX7OL`, `Managed-CachingDisabled` + `Managed-AllViewerExceptHostHeader` + `Managed-SecurityHeadersPolicy`)
- **API Gateway Base URL:** `https://4jg1b9qut2.execute-api.ap-southeast-3.amazonaws.com/v1/api/v1` (REST API `4jg1b9qut2`, Stage `v1`, `AuthType: NONE` with `RequireApiKey` parameter ready for future `x-api-key`)
- **Lambda Function:** `ovius-api-backend-data-api` (Python 3.11 `arm64`, `512 MB`, `29s` timeout, wave-parallel DynamoDB querying + lossless `Decimal` `next_token` pagination)
- **Routes:**
  - `GET /api/v1/metrics` *(composite recordings + haul_cycles + optional `include_segments=true`)*
  - `GET /api/v1/metrics/recordings`
  - `GET /api/v1/metrics/cycles`
  - `GET /api/v1/metrics/segments`
  - `GET /api/v1/bboxes` *(template)*
  - `GET /api/v1/videos` *(template)*
- **Filters:** `start_time`, `end_time` (`YYYY-MM-DD` or ISO-8601), `tod_start_s`, `tod_end_s`, `camera_id` (`*` or comma-separated), `location_tag`, `tag`, `filename`, `recording_id`, `cycle_id`, `excavator_id`, `state` (comma-separated, e.g. `idle`), `include_scoops` (`true|false`), `include_segments` (`true|false`), `limit` (`1..15000`), `next_token`.

```bash
# Run unit tests
uv run pytest -v

# Package & deploy CloudFormation API stack (ovius-api-backend)
uv run python scripts/deploy_api.py

# Verify live API Gateway + CloudFront endpoints
uv run python scripts/verify_api_live.py
```
