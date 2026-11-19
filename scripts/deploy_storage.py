"""Validate, import existing S3 bucket if needed, and deploy infra/dynamodb_tables.yaml."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = ROOT / "infra" / "dynamodb_tables.yaml"

DEFAULT_STACK_NAME = "ovius-dynamodb-telemetry"
DEFAULT_PROFILE = "610331148578_Project8Learner"
DEFAULT_REGION = "ap-southeast-1"
DEFAULT_PIPELINE_BUCKET = "ovius-pipeline-apse1-610331148578"

_IMPORT_PARAM_BLOCK = """
  PipelineBucketName:
    Type: String
    Default: ovius-pipeline-apse1-610331148578
    AllowedPattern: "^[a-z0-9.-]+$"
    MinLength: 3
    MaxLength: 63
    ConstraintDescription: PipelineBucketName must be a valid lowercase S3 bucket name (3-63 chars).
    Description: S3 bucket name for video upload staging, checkpoints, Lambda artifacts, and Parquet lake.
"""

_IMPORT_RESOURCE_BLOCK = """
  PipelineBucket:
    Type: AWS::S3::Bucket
    DeletionPolicy: Retain
    UpdateReplacePolicy: Retain
    Properties:
      BucketName: !Ref PipelineBucketName
"""


def _wait_for_change_set(cfn: Any, change_set_arn: str) -> dict[str, Any]:
    """Poll a CloudFormation change set until CREATE_COMPLETE or FAILED."""
    while True:
        desc = cfn.describe_change_set(ChangeSetName=change_set_arn)
        status = desc["Status"]
        if status in {"CREATE_COMPLETE", "FAILED"}:
            return desc
        time.sleep(2)


def _list_stack_resource_ids(cfn: Any, stack_name: str) -> set[str]:
    """Return all logical resource IDs currently managed by the CloudFormation stack."""
    logical_ids: set[str] = set()
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"StackName": stack_name}
        if token is not None:
            kwargs["NextToken"] = token
        resp = cfn.list_stack_resources(**kwargs)
        for summary in resp.get("StackResourceSummaries", []):
            logical_ids.add(str(summary["LogicalResourceId"]))
        token = resp.get("NextToken")
        if not token:
            break
    return logical_ids


def _bucket_exists(s3: Any, bucket_name: str) -> bool:
    """Return True if the target S3 bucket already exists and is accessible."""
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchBucket"}:
            return False
        raise


def _build_import_template(live_template_body: str) -> str:
    """Inject only PipelineBucketName and minimal PipelineBucket into the live template for IMPORT."""
    if "\nResources:\n" not in live_template_body or "\nOutputs:\n" not in live_template_body:
        raise ValueError("Unexpected template structure: missing top-level Resources: or Outputs: sections")

    updated = live_template_body
    if "PipelineBucketName:" not in updated:
        updated = updated.replace("\nResources:\n", f"{_IMPORT_PARAM_BLOCK}\nResources:\n", 1)
    if "PipelineBucket:" not in updated:
        updated = updated.replace("\nOutputs:\n", f"{_IMPORT_RESOURCE_BLOCK}\nOutputs:\n", 1)
    return updated


def _validation_errors(cfn: Any, change_set_arn: str) -> list[dict[str, Any]]:
    """Collect any VALIDATION_ERROR events emitted during change set creation."""
    events = cfn.describe_events(ChangeSetName=change_set_arn).get("OperationEvents", [])
    return [ev for ev in events if ev.get("EventType") == "VALIDATION_ERROR"]


def _detect_drift(cfn: Any, stack_name: str) -> dict[str, Any]:
    """Run CloudFormation drift detection and wait until completion."""
    det_id = cfn.detect_stack_drift(StackName=stack_name)["StackDriftDetectionId"]
    while True:
        status = cfn.describe_stack_drift_detection_status(StackDriftDetectionId=det_id)
        if status["DetectionStatus"] in {"DETECTION_COMPLETE", "DETECTION_FAILED"}:
            return status
        time.sleep(2)


def _import_pipeline_bucket(
    cfn: Any,
    stack_name: str,
    bucket_name: str,
    parameters: list[dict[str, str]],
    *,
    validate_only: bool,
) -> dict[str, Any]:
    """Import an existing S3 bucket into the storage CloudFormation stack."""
    live_body = cfn.get_template(StackName=stack_name, TemplateStage="Original")["TemplateBody"]
    if not isinstance(live_body, str):
        raise RuntimeError("Expected YAML string from cloudformation.get_template")

    import_template = _build_import_template(live_body)
    cfn.validate_template(TemplateBody=import_template)

    cs_name = f"ovius-storage-import-{int(time.time())}"
    cs = cfn.create_change_set(
        StackName=stack_name,
        TemplateBody=import_template,
        Parameters=parameters,
        ChangeSetName=cs_name,
        ChangeSetType="IMPORT",
        ResourcesToImport=[
            {
                "ResourceType": "AWS::S3::Bucket",
                "LogicalResourceId": "PipelineBucket",
                "ResourceIdentifier": {"BucketName": bucket_name},
            }
        ],
    )
    cs_arn = cs["Id"]
    desc = _wait_for_change_set(cfn, cs_arn)
    val_errors = _validation_errors(cfn, cs_arn)

    if desc["Status"] == "FAILED":
        reason = desc.get("StatusReason", "")
        cfn.delete_change_set(ChangeSetName=cs_arn)
        raise RuntimeError(
            f"IMPORT change set failed: {reason}; validation_errors={json.dumps(val_errors, default=str)}"
        )

    if validate_only:
        changes_count = len(desc.get("Changes", []))
        cfn.delete_change_set(ChangeSetName=cs_arn)
        return {"import_status": "VALIDATED", "import_changes_count": changes_count}

    cfn.execute_change_set(ChangeSetName=cs_arn)
    cfn.get_waiter("stack_import_complete").wait(
        StackName=stack_name,
        WaiterConfig={"Delay": 5, "MaxAttempts": 120},
    )
    return {"import_status": "IMPORT_COMPLETE", "imported_bucket": bucket_name}


def main() -> int:
    """CLI entrypoint to import (when needed) and deploy the Ovius storage CloudFormation stack."""
    parser = argparse.ArgumentParser(
        description="Validate, import existing S3 bucket if needed, and deploy Ovius storage stack."
    )
    parser.add_argument("--stack-name", default=DEFAULT_STACK_NAME)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--table-prefix", default="ovius")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    parser.add_argument("--enable-pitr", default="true", choices=["true", "false"])
    parser.add_argument("--pipeline-bucket-name", default=DEFAULT_PIPELINE_BUCKET)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--detect-drift", action="store_true")
    args = parser.parse_args()

    cfg = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    cfn = session.client("cloudformation", config=cfg)
    s3 = session.client("s3", config=cfg)

    template_body = TEMPLATE_PATH.read_text(encoding="utf-8")
    template_bytes = len(template_body.encode("utf-8"))
    if template_bytes > 51_200:
        print(
            f"Template size {template_bytes} bytes exceeds 51,200-byte inline TemplateBody limit.",
            file=sys.stderr,
        )
        return 1

    sha12 = hashlib.sha256(template_body.encode("utf-8")).hexdigest()[:12]
    parameters = [
        {"ParameterKey": "TablePrefix", "ParameterValue": args.table_prefix},
        {"ParameterKey": "EnvironmentName", "ParameterValue": args.env},
        {"ParameterKey": "EnablePointInTimeRecovery", "ParameterValue": args.enable_pitr},
        {"ParameterKey": "PipelineBucketName", "ParameterValue": args.pipeline_bucket_name},
    ]
    tags = [
        {"Key": "Owner", "Value": "project8"},
        {"Key": "Project", "Value": "AIMLC4"},
        {"Key": "Iteration", "Value": "v5"},
        {"Key": "Environment", "Value": args.env},
    ]

    try:
        cfn.validate_template(TemplateBody=template_body)

        exists = False
        try:
            stacks = cfn.describe_stacks(StackName=args.stack_name).get("Stacks", [])
            if stacks and stacks[0]["StackStatus"] == "DELETE_IN_PROGRESS":
                cfn.get_waiter("stack_delete_complete").wait(StackName=args.stack_name)
                stacks = []
            exists = bool(stacks and stacks[0]["StackStatus"] != "REVIEW_IN_PROGRESS")
        except ClientError as exc:
            if "does not exist" not in str(exc):
                raise

        import_result: dict[str, Any] = {}
        if exists:
            logical_ids = _list_stack_resource_ids(cfn, args.stack_name)
            if "PipelineBucket" not in logical_ids and _bucket_exists(s3, args.pipeline_bucket_name):
                import_result = _import_pipeline_bucket(
                    cfn,
                    args.stack_name,
                    args.pipeline_bucket_name,
                    parameters,
                    validate_only=args.validate_only,
                )
                if args.validate_only:
                    print(
                        json.dumps(
                            {
                                "status": "VALIDATED",
                                "template_bytes": template_bytes,
                                **import_result,
                                "note": "IMPORT validated; full UPDATE change set runs after IMPORT executes.",
                            },
                            indent=2,
                        )
                    )
                    return 0

        cs_type = "UPDATE" if exists else "CREATE"
        cs_name = f"ovius-storage-{sha12}-{int(time.time())}"
        cs = cfn.create_change_set(
            StackName=args.stack_name,
            TemplateBody=template_body,
            Parameters=parameters,
            ChangeSetName=cs_name,
            ChangeSetType=cs_type,
            Tags=tags,
        )
        cs_arn = cs["Id"]
        desc = _wait_for_change_set(cfn, cs_arn)
        val_errors = _validation_errors(cfn, cs_arn)

        if desc["Status"] == "FAILED":
            reason = desc.get("StatusReason", "")
            cfn.delete_change_set(ChangeSetName=cs_arn)
            if not exists:
                try:
                    cur = cfn.describe_stacks(StackName=args.stack_name)["Stacks"][0]["StackStatus"]
                    if cur == "REVIEW_IN_PROGRESS":
                        cfn.delete_stack(StackName=args.stack_name)
                except ClientError:
                    pass
            if "The submitted information didn't contain changes" in reason:
                outputs = cfn.describe_stacks(StackName=args.stack_name)["Stacks"][0].get("Outputs", [])
                result: dict[str, Any] = {
                    "status": "NO_CHANGES",
                    "template_bytes": template_bytes,
                    **import_result,
                    "outputs": outputs,
                }
                if args.detect_drift:
                    drift = _detect_drift(cfn, args.stack_name)
                    result["drift"] = {
                        "StackDriftStatus": drift.get("StackDriftStatus"),
                        "DriftedStackResourceCount": drift.get("DriftedStackResourceCount"),
                    }
                print(json.dumps(result, indent=2, default=str))
                return 0
            print(
                json.dumps(
                    {"status": "FAILED", "reason": reason, "validation_errors": val_errors},
                    indent=2,
                    default=str,
                ),
                file=sys.stderr,
            )
            return 1

        if args.validate_only:
            result = {
                "status": "VALIDATED",
                "change_set_type": cs_type,
                "template_bytes": template_bytes,
                "changes_count": len(desc.get("Changes", [])),
                "validation_errors": val_errors,
            }
            cfn.delete_change_set(ChangeSetName=cs_arn)
            if not exists:
                cfn.delete_stack(StackName=args.stack_name)
            print(json.dumps(result, indent=2, default=str))
            return 0

        cfn.execute_change_set(ChangeSetName=cs_arn)
        waiter = cfn.get_waiter("stack_update_complete" if exists else "stack_create_complete")
        waiter.wait(StackName=args.stack_name, WaiterConfig={"Delay": 5, "MaxAttempts": 120})

        stack = cfn.describe_stacks(StackName=args.stack_name)["Stacks"][0]
        output_payload: dict[str, Any] = {
            "status": stack["StackStatus"],
            "template_bytes": template_bytes,
            **import_result,
            "outputs": stack.get("Outputs", []),
        }
        if args.detect_drift:
            drift = _detect_drift(cfn, args.stack_name)
            output_payload["drift"] = {
                "StackDriftStatus": drift.get("StackDriftStatus"),
                "DriftedStackResourceCount": drift.get("DriftedStackResourceCount"),
            }
        print(json.dumps(output_payload, indent=2, default=str))
        return 0
    except (ClientError, RuntimeError, ValueError) as exc:
        print(f"Storage deployment error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
