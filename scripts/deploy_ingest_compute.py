"""Ensure Batch/ECS SLRs, package src/ovius_api, and deploy infra/ingest_compute_stack.yaml."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from deploy_api import (  # noqa: E402
    DEFAULT_LAMBDA_ROLE_ARN,
    DEFAULT_PROFILE,
    DEFAULT_REGION,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_PREFIX,
    SRC_PKG,
    build_lambda_zip,
    wait_for_change_set,
)

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = ROOT / "infra" / "ingest_compute_stack.yaml"
DEFAULT_STACK_NAME = "ovius-ingest-compute"

REGION_NETWORK_DEFAULTS: dict[str, dict[str, str]] = {
    "ap-southeast-1": {
        "s3_bucket": "ovius-pipeline-apse1-610331148578",
        "vpc_id": "vpc-059c4cc3aaceafcfd",
        "subnet_ids": "subnet-09024caaeca78fc51,subnet-0c08c08e158d621ff,subnet-08c412192d036fdcf",
        "route_table_id": "rtb-020e968b674306ed6",
    },
    "ap-southeast-3": {
        "s3_bucket": "ovius-pipeline-apse3-610331148578",
        "vpc_id": "vpc-04cbff2c12511037a",
        "subnet_ids": "subnet-0610c0c53c7d9700a,subnet-0d9b24da3b46a67a1",
        "route_table_id": "rtb-0ff92726d27caa8cf",
    },
}


def _ensure_service_linked_roles(iam: Any) -> list[str]:
    """Idempotently create AWS Batch and ECS service-linked roles if missing."""
    results: list[str] = []
    for service_name in ("batch.amazonaws.com", "ecs.amazonaws.com"):
        try:
            iam.create_service_linked_role(AWSServiceName=service_name)
            results.append(f"{service_name}:created")
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            msg = str(exc)
            if code == "InvalidInput" and "has been taken" in msg:
                results.append(f"{service_name}:exists")
            else:
                raise
    return results


def _detect_drift(cfn: Any, stack_name: str) -> dict[str, Any]:
    """Run CloudFormation drift detection and wait until completion."""
    det_id = cfn.detect_stack_drift(StackName=stack_name)["StackDriftDetectionId"]
    while True:
        status = cfn.describe_stack_drift_detection_status(StackDriftDetectionId=det_id)
        if status["DetectionStatus"] in {"DETECTION_COMPLETE", "DETECTION_FAILED"}:
            return status
        time.sleep(2)


def _lookup_queue_url(sqs: Any, queue_name: str) -> str | None:
    try:
        return str(sqs.get_queue_url(QueueName=queue_name)["QueueUrl"])
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"}:
            return None
        raise


def _ecr_repo_exists(ecr: Any, repo_name: str) -> bool:
    try:
        ecr.describe_repositories(repositoryNames=[repo_name])
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "RepositoryNotFoundException":
            return False
        raise


def _import_retained_resources(
    *,
    cfn: Any,
    sqs: Any,
    ecr: Any,
    stack_name: str,
    table_prefix: str,
    env: str,
    validate_only: bool,
    include_ecr: bool = True,
) -> dict[str, Any] | None:
    """Import existing retained SQS queues and ECR repo into a new stack if present."""
    dlq_name = f"{table_prefix}-ready-recordings-dlq"
    queue_name = f"{table_prefix}-ready-recordings"
    repo_name = f"{table_prefix}-vision-worker"

    dlq_url = _lookup_queue_url(sqs, dlq_name)
    queue_url = _lookup_queue_url(sqs, queue_name)
    repo_exists = _ecr_repo_exists(ecr, repo_name) if include_ecr else False

    resources_to_import: list[dict[str, Any]] = []
    resource_blocks: list[str] = []

    if dlq_url:
        resources_to_import.append(
            {
                "ResourceType": "AWS::SQS::Queue",
                "LogicalResourceId": "OviusReadyRecordingsDlq",
                "ResourceIdentifier": {"QueueUrl": dlq_url},
            }
        )
        resource_blocks.append(
            """  OviusReadyRecordingsDlq:
    Type: AWS::SQS::Queue
    DeletionPolicy: Retain
    UpdateReplacePolicy: Retain
    Properties:
      QueueName: !Sub "${TablePrefix}-ready-recordings-dlq" """.rstrip()
        )
    if queue_url:
        resources_to_import.append(
            {
                "ResourceType": "AWS::SQS::Queue",
                "LogicalResourceId": "OviusReadyRecordingsQueue",
                "ResourceIdentifier": {"QueueUrl": queue_url},
            }
        )
        resource_blocks.append(
            """  OviusReadyRecordingsQueue:
    Type: AWS::SQS::Queue
    DeletionPolicy: Retain
    UpdateReplacePolicy: Retain
    Properties:
      QueueName: !Sub "${TablePrefix}-ready-recordings" """.rstrip()
        )
    if repo_exists:
        resources_to_import.append(
            {
                "ResourceType": "AWS::ECR::Repository",
                "LogicalResourceId": "OviusWorkerEcrRepository",
                "ResourceIdentifier": {"RepositoryName": repo_name},
            }
        )
        resource_blocks.append(
            """  OviusWorkerEcrRepository:
    Type: AWS::ECR::Repository
    DeletionPolicy: Retain
    UpdateReplacePolicy: Retain
    Properties:
      RepositoryName: !Sub "${TablePrefix}-vision-worker"
      EncryptionConfiguration:
        EncryptionType: AES256 """.rstrip()
        )

    if not resources_to_import:
        return None

    import_template = (
        'AWSTemplateFormatVersion: "2010-09-09"\n'
        "Description: Bootstrap import of retained Ovius ingest resources.\n"
        "Parameters:\n"
        "  TablePrefix:\n"
        "    Type: String\n"
        "    Default: ovius\n"
        "  EnvironmentName:\n"
        "    Type: String\n"
        "    Default: dev\n"
        "Resources:\n"
        + "\n".join(resource_blocks)
        + "\n"
    )
    cfn.validate_template(TemplateBody=import_template)

    cs_name = f"ovius-ingest-import-{int(time.time())}"
    cs = cfn.create_change_set(
        StackName=stack_name,
        TemplateBody=import_template,
        Parameters=[
            {"ParameterKey": "TablePrefix", "ParameterValue": table_prefix},
            {"ParameterKey": "EnvironmentName", "ParameterValue": env},
        ],
        ChangeSetName=cs_name,
        ChangeSetType="IMPORT",
        ResourcesToImport=resources_to_import,
    )
    cs_arn = cs["Id"]
    desc = wait_for_change_set(cfn, cs_arn)
    events = cfn.describe_events(ChangeSetName=cs_arn).get("OperationEvents", [])
    val_errors = [ev for ev in events if ev.get("EventType") == "VALIDATION_ERROR"]

    if desc["Status"] == "FAILED":
        reason = desc.get("StatusReason", "")
        cfn.delete_change_set(ChangeSetName=cs_arn)
        try:
            cur = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["StackStatus"]
            if cur == "REVIEW_IN_PROGRESS":
                cfn.delete_stack(StackName=stack_name)
        except ClientError:
            pass
        raise RuntimeError(
            f"IMPORT change set failed: {reason}; validation_errors={json.dumps(val_errors, default=str)}"
        )

    if validate_only:
        changes_count = len(desc.get("Changes", []))
        cfn.delete_change_set(ChangeSetName=cs_arn)
        try:
            cur = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["StackStatus"]
            if cur == "REVIEW_IN_PROGRESS":
                cfn.delete_stack(StackName=stack_name)
        except ClientError:
            pass
        return {"import_status": "VALIDATED", "import_changes_count": changes_count}

    cfn.execute_change_set(ChangeSetName=cs_arn)
    cfn.get_waiter("stack_import_complete").wait(
        StackName=stack_name,
        WaiterConfig={"Delay": 5, "MaxAttempts": 120},
    )
    return {
        "import_status": "IMPORT_COMPLETE",
        "imported_resources": [r["LogicalResourceId"] for r in resources_to_import],
    }


def main() -> int:
    """CLI entrypoint to deploy Ovius Phase 2 ingest orchestration & Batch compute stack."""
    parser = argparse.ArgumentParser(
        description="Package orchestrator Lambda and deploy Ovius Phase 2 ingest/compute stack."
    )
    parser.add_argument("--stack-name", default=DEFAULT_STACK_NAME)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--s3-bucket", default="")
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX)
    parser.add_argument("--table-prefix", default="ovius")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    parser.add_argument("--vpc-id", default="")
    parser.add_argument("--subnet-ids", default="")
    parser.add_argument("--route-table-id", default="")
    parser.add_argument("--enable-batch-compute", default="true", choices=["true", "false"])
    parser.add_argument("--enable-auto-batch-submit", default="false", choices=["false", "true"])
    parser.add_argument("--worker-instance-id", default="")
    parser.add_argument("--enable-auto-ec2-start", default="false", choices=["false", "true"])
    parser.add_argument("--worker-image-uri", default="")
    parser.add_argument("--storage-stack-name", default="ovius-dynamodb-telemetry")
    parser.add_argument("--ingest-state-stream-arn", default="")
    parser.add_argument("--lambda-role-arn", default=DEFAULT_LAMBDA_ROLE_ARN)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--detect-drift", action="store_true")
    args = parser.parse_args()

    region_defaults = REGION_NETWORK_DEFAULTS.get(args.region, REGION_NETWORK_DEFAULTS["ap-southeast-3"])
    s3_bucket = args.s3_bucket or region_defaults["s3_bucket"]
    vpc_id = args.vpc_id or region_defaults["vpc_id"]
    subnet_ids = args.subnet_ids or region_defaults["subnet_ids"]
    route_table_id = args.route_table_id or region_defaults["route_table_id"]
    batch_enabled = args.enable_batch_compute == "true"

    template_body = TEMPLATE_PATH.read_text(encoding="utf-8")
    template_bytes = len(template_body.encode("utf-8"))
    if template_bytes > 51_200:
        print(
            f"Template size {template_bytes} bytes exceeds 51,200-byte inline TemplateBody limit.",
            file=sys.stderr,
        )
        return 1

    cfg = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3", config=cfg)
    cfn = session.client("cloudformation", config=cfg)
    iam = session.client("iam", config=cfg)
    sqs = session.client("sqs", config=cfg)
    ecr = session.client("ecr", config=cfg)

    zip_bytes, sha12 = build_lambda_zip(SRC_PKG)
    s3_key = f"{args.s3_prefix.rstrip('/')}/ovius_api_{sha12}.zip"

    try:
        cfn.validate_template(TemplateBody=template_body)
        slr_status: list[str] = []
        if not args.validate_only:
            if batch_enabled:
                slr_status = _ensure_service_linked_roles(iam)
            s3.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=zip_bytes,
                ContentType="application/zip",
            )

        exists = False
        try:
            stacks = cfn.describe_stacks(StackName=args.stack_name).get("Stacks", [])
            if stacks and stacks[0]["StackStatus"] == "DELETE_IN_PROGRESS":
                cfn.get_waiter("stack_delete_complete").wait(StackName=args.stack_name)
                stacks = []
            elif stacks and stacks[0]["StackStatus"] == "ROLLBACK_COMPLETE" and not args.validate_only:
                cfn.delete_stack(StackName=args.stack_name)
                cfn.get_waiter("stack_delete_complete").wait(StackName=args.stack_name)
                stacks = []
            exists = bool(
                stacks and stacks[0]["StackStatus"] not in {"REVIEW_IN_PROGRESS", "ROLLBACK_COMPLETE"}
            )
        except ClientError as exc:
            if "does not exist" not in str(exc):
                raise

        import_result: dict[str, Any] = {}
        if not exists:
            imported = _import_retained_resources(
                cfn=cfn,
                sqs=sqs,
                ecr=ecr,
                stack_name=args.stack_name,
                table_prefix=args.table_prefix,
                env=args.env,
                validate_only=args.validate_only,
                include_ecr=batch_enabled,
            )
            if imported is not None:
                import_result = imported
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
                exists = True

        stream_arn = args.ingest_state_stream_arn
        if not stream_arn and args.storage_stack_name:
            try:
                st_stacks = cfn.describe_stacks(StackName=args.storage_stack_name).get("Stacks", [])
                if st_stacks:
                    st_outs = {
                        o["OutputKey"]: o["OutputValue"] for o in st_stacks[0].get("Outputs", [])
                    }
                    stream_arn = str(st_outs.get("IngestStateStreamArn", ""))
            except ClientError:
                stream_arn = ""

        cs_type = "UPDATE" if exists else "CREATE"
        cs_name = f"ovius-ingest-{sha12}-{int(time.time())}"
        parameters = [
            {"ParameterKey": "EnvironmentName", "ParameterValue": args.env},
            {"ParameterKey": "TablePrefix", "ParameterValue": args.table_prefix},
            {"ParameterKey": "PipelineBucketName", "ParameterValue": s3_bucket},
            {"ParameterKey": "VpcId", "ParameterValue": vpc_id},
            {"ParameterKey": "SubnetIds", "ParameterValue": subnet_ids},
            {"ParameterKey": "RouteTableId", "ParameterValue": route_table_id},
            {"ParameterKey": "EnableBatchCompute", "ParameterValue": args.enable_batch_compute},
            {"ParameterKey": "EnableAutoBatchSubmit", "ParameterValue": args.enable_auto_batch_submit},
            {"ParameterKey": "WorkerInstanceId", "ParameterValue": args.worker_instance_id},
            {"ParameterKey": "EnableAutoEc2Start", "ParameterValue": args.enable_auto_ec2_start},
            {"ParameterKey": "WorkerImageUri", "ParameterValue": args.worker_image_uri},
            {"ParameterKey": "IngestStateStreamArn", "ParameterValue": stream_arn},
            {
                "ParameterKey": "LambdaCodeS3Bucket",
                "ParameterValue": "" if args.validate_only else s3_bucket,
            },
            {
                "ParameterKey": "LambdaCodeS3Key",
                "ParameterValue": "" if args.validate_only else s3_key,
            },
            {"ParameterKey": "LambdaExecutionRoleArn", "ParameterValue": args.lambda_role_arn},
        ]
        tags = [
            {"Key": "Owner", "Value": "project8"},
            {"Key": "Project", "Value": "AIMLC4"},
            {"Key": "Iteration", "Value": "v5"},
            {"Key": "Environment", "Value": args.env},
        ]

        cs = cfn.create_change_set(
            StackName=args.stack_name,
            TemplateBody=template_body,
            Parameters=parameters,
            Capabilities=["CAPABILITY_IAM"],
            ChangeSetName=cs_name,
            ChangeSetType=cs_type,
            Tags=tags,
        )
        cs_arn = cs["Id"]
        desc = wait_for_change_set(cfn, cs_arn)
        events = cfn.describe_events(ChangeSetName=cs_arn).get("OperationEvents", [])
        val_errors = [ev for ev in events if ev.get("EventType") == "VALIDATION_ERROR"]

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
                out_payload: dict[str, Any] = {
                    "status": "NO_CHANGES",
                    "template_bytes": template_bytes,
                    "slr_status": slr_status,
                    "outputs": outputs,
                }
                if args.detect_drift:
                    drift = _detect_drift(cfn, args.stack_name)
                    out_payload["drift"] = {
                        "StackDriftStatus": drift.get("StackDriftStatus"),
                        "DriftedStackResourceCount": drift.get("DriftedStackResourceCount"),
                    }
                print(json.dumps(out_payload, indent=2, default=str))
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
        waiter.wait(StackName=args.stack_name, WaiterConfig={"Delay": 10, "MaxAttempts": 120})

        stack = cfn.describe_stacks(StackName=args.stack_name)["Stacks"][0]
        payload: dict[str, Any] = {
            "status": stack["StackStatus"],
            "template_bytes": template_bytes,
            "slr_status": slr_status,
            **import_result,
            "s3_artifact": f"s3://{s3_bucket}/{s3_key}",
            "outputs": stack.get("Outputs", []),
        }
        if args.detect_drift:
            drift = _detect_drift(cfn, args.stack_name)
            payload["drift"] = {
                "StackDriftStatus": drift.get("StackDriftStatus"),
                "DriftedStackResourceCount": drift.get("DriftedStackResourceCount"),
            }
        print(json.dumps(payload, indent=2, default=str))
        return 0
    except ClientError as exc:
        print(f"Ingest compute deployment error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
