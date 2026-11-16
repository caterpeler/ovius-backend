# Ovius AWS Backend (`ovius_backend`)

Standalone repository containing all **AWS CloudFormation templates, DynamoDB operational telemetry schemas, and backend AWS ingestion/provisioning tooling** for the Ovius / AIMLC4 Heavy Equipment Productivity Pipeline.

> [!NOTE]
> All AWS CloudFormation and AWS backend infrastructure code lives in this repository ([`ovius_backend`](file:///Users/vio/PycharmProjects/ovius_backend)). Model training, SageMaker experiments, CCTV dataset balancing, local dashboards, and synthetic mock data generation remain in [`AIMLC4_Playground`](file:///Users/vio/PycharmProjects/AIMLC4_Playground).

---

## 1. Repository Structure

```text
ovius_backend/
├── infra/
│   └── dynamodb_tables.yaml      # CloudFormation stack for 3-table DynamoDB schema (ovius-*)
├── scripts/
│   └── ingest_dynamodb.py        # Resumable multi-threaded BatchWriteItem loader & GSI verifier
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
