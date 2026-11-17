"""Package src/ovius_api, upload to S3, and deploy infra/api_stack.yaml."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
import sys
import time
import zipfile

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
SRC_PKG = ROOT / "src" / "ovius_api"
TEMPLATE_PATH = ROOT / "infra" / "api_stack.yaml"

DEFAULT_STACK_NAME = "ovius-api-backend"
DEFAULT_PROFILE = "610331148578_Project8Learner"
DEFAULT_REGION = "ap-southeast-3"
DEFAULT_S3_BUCKET = "sagemaker-savio-p8-apse3"
DEFAULT_S3_PREFIX = "artifacts/ovius_backend"
DEFAULT_LAMBDA_ROLE_ARN = "arn:aws:iam::610331148578:role/Project8LambdaExecutionRole"


def build_lambda_zip(package_dir: Path) -> tuple[bytes, str]:
    """Create a deterministic zip archive of src/ovius_api and return (bytes, sha256_prefix)."""
    buf = io.BytesIO()
    py_files = sorted(
        p for p in package_dir.rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and not p.name.endswith(".pyc")
    )
    if not py_files:
        raise FileNotFoundError(f"No source files found in {package_dir}")

    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in py_files:
            arcname = str(Path("ovius_api") / file_path.relative_to(package_dir))
            info = zipfile.ZipInfo(arcname, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, file_path.read_bytes())

    raw = buf.getvalue()
    sha12 = hashlib.sha256(raw).hexdigest()[:12]
    return raw, sha12


def wait_for_change_set(cfn: object, change_set_arn: str) -> dict[str, object]:
    """Poll CloudFormation change set until CREATE_COMPLETE or FAILED."""
    while True:
        desc = cfn.describe_change_set(ChangeSetName=change_set_arn)
        status = desc["Status"]
        if status in {"CREATE_COMPLETE", "FAILED"}:
            return desc
        time.sleep(2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Package and deploy Ovius API CloudFormation stack.")
    parser.add_argument("--stack-name", default=DEFAULT_STACK_NAME)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET)
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX)
    parser.add_argument("--table-prefix", default="ovius")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    parser.add_argument("--require-api-key", default="false", choices=["false", "true"])
    parser.add_argument("--lambda-role-arn", default=DEFAULT_LAMBDA_ROLE_ARN)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    cfg = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3", config=cfg)
    cfn = session.client("cloudformation", config=cfg)

    template_body = TEMPLATE_PATH.read_text(encoding="utf-8")
    zip_bytes, sha12 = build_lambda_zip(SRC_PKG)
    s3_key = f"{args.s3_prefix.rstrip('/')}/ovius_api_{sha12}.zip"

    try:
        cfn.validate_template(TemplateBody=template_body)
        if not args.validate_only:
            s3.put_object(
                Bucket=args.s3_bucket,
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
            exists = bool(stacks and stacks[0]["StackStatus"] != "REVIEW_IN_PROGRESS")
        except ClientError as exc:
            if "does not exist" not in str(exc):
                raise

        cs_type = "UPDATE" if exists else "CREATE"
        cs_name = f"ovius-api-{sha12}-{int(time.time())}"
        parameters = [
            {"ParameterKey": "EnvironmentName", "ParameterValue": args.env},
            {"ParameterKey": "TablePrefix", "ParameterValue": args.table_prefix},
            {"ParameterKey": "RequireApiKey", "ParameterValue": args.require_api_key},
            {"ParameterKey": "LambdaCodeS3Bucket", "ParameterValue": "" if args.validate_only else args.s3_bucket},
            {"ParameterKey": "LambdaCodeS3Key", "ParameterValue": "" if args.validate_only else s3_key},
            {"ParameterKey": "LambdaExecutionRoleArn", "ParameterValue": args.lambda_role_arn},
            {"ParameterKey": "DeploymentNonce", "ParameterValue": f"{args.env}-{sha12}"},
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
                print(json.dumps({"status": "NO_CHANGES", "outputs": outputs}, indent=2))
                return 0
            print(json.dumps({"status": "FAILED", "reason": reason, "validation_errors": val_errors}, indent=2, default=str), file=sys.stderr)
            return 1

        if args.validate_only:
            result = {
                "status": "VALIDATED",
                "change_set_type": cs_type,
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
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        if exists and "RestApiId" in outputs:
            apigw = session.client("apigateway", config=cfg)
            apigw.create_deployment(
                restApiId=outputs["RestApiId"],
                stageName="v1",
                description=f"Stage rollout {sha12}",
            )
        print(
            json.dumps(
                {
                    "status": stack["StackStatus"],
                    "s3_artifact": f"s3://{args.s3_bucket}/{s3_key}",
                    "outputs": stack.get("Outputs", []),
                },
                indent=2,
            )
        )
        return 0
    except ClientError as exc:
        print(f"AWS deployment error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
