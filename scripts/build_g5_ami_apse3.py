"""Launch a 50 GB Ubuntu 22.04 g5.xlarge builder in ap-southeast-3, compile S-v5-XL TensorRT FP16, and cut AMI."""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

DEFAULT_PROFILE = "610331148578_Project8Learner"
DEFAULT_REGION = "ap-southeast-3"
DEFAULT_BUCKET = "ovius-pipeline-apse3-610331148578"
DEFAULT_BASE_AMI = "ami-0e00677743bba2f39"  # Canonical Ubuntu 22.04 LTS Server (8 GB snapshot)
DEFAULT_INSTANCE_TYPE = "g5.xlarge"
DEFAULT_SUBNETS = [
    "subnet-0d9b24da3b46a67a1",  # ap-southeast-3c (verified active g5.xlarge capacity)
    "subnet-0610c0c53c7d9700a",  # ap-southeast-3a
]
DEFAULT_SECURITY_GROUP = "sg-03fa3f8b218fe06c9"
DEFAULT_INSTANCE_PROFILE = "Project8BatchInstanceRole"
DEFAULT_VOLUME_SIZE_GB = 50
DEFAULT_AMI_NAME_PREFIX = "ovius-sv5xl-ptz-v1-apse3"

MANDATORY_TAGS = [
    {"Key": "Owner", "Value": "project8"},
    {"Key": "Project", "Value": "AIMLC4"},
    {"Key": "Iteration", "Value": "v5"},
    {"Key": "Environment", "Value": "dev"},
]


def build_userdata(*, bucket: str, region: str, instance_type: str, log_key: str) -> str:
    """Return 2-stage cloud-init bash script that installs NVIDIA/PyTorch/TRT, builds engine, and shuts down."""
    return f"""#!/bin/bash
set -euxo pipefail

LOG_FILE="/var/log/ovius_ami_build.log"
exec > >(tee -a "$LOG_FILE") 2>&1

BUCKET="{bucket}"
REGION="{region}"
INSTANCE_TYPE="{instance_type}"
LOG_KEY="{log_key}"

upload_log() {{
  if command -v aws >/dev/null 2>&1; then
    aws --region "$REGION" s3 cp "$LOG_FILE" "s3://$BUCKET/$LOG_KEY" || true
  elif [ -x /opt/pipeline/venv/bin/python ]; then
    /opt/pipeline/venv/bin/python -c "import boto3; boto3.client('s3', region_name='$REGION').upload_file('$LOG_FILE', '$BUCKET', '$LOG_KEY')" || true
  fi
}}
on_stage1_exit() {{
  rc=$?
  upload_log
  if [ "$rc" -ne 0 ]; then
    echo "=== [Stage 1] FAILED with exit code $rc at $(date -u +%FT%TZ); shutting down to prevent idle GPU cost ==="
    upload_log
    systemctl poweroff --no-block || shutdown -h now
  fi
}}
trap on_stage1_exit EXIT

echo "=== [Stage 1] Ovius g5.xlarge 50 GB AMI Bootstrap starting at $(date -u +%FT%TZ) ==="
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \\
  ca-certificates curl gnupg lsb-release unzip jq \\
  ffmpeg libgl1 libglib2.0-0 libsm6 libxext6 libxrender1 \\
  awscli

upload_log

echo "=== [Stage 1] Installing NVIDIA server open driver for kernel $(uname -r) ==="
KVER="$(uname -r)"
if apt-cache show "linux-modules-nvidia-570-server-open-${{KVER}}" >/dev/null 2>&1; then
  apt-get install -y --no-install-recommends \\
    "linux-modules-nvidia-570-server-open-${{KVER}}" \\
    nvidia-kernel-common-570-server \\
    nvidia-utils-570-server \\
    libnvidia-compute-570-server
elif apt-cache show "linux-modules-nvidia-550-server-open-${{KVER}}" >/dev/null 2>&1; then
  apt-get install -y --no-install-recommends \\
    "linux-modules-nvidia-550-server-open-${{KVER}}" \\
    nvidia-kernel-common-550-server \\
    nvidia-utils-550-server \\
    libnvidia-compute-550-server
elif apt-cache show "linux-modules-nvidia-550-open-${{KVER}}" >/dev/null 2>&1; then
  apt-get install -y --no-install-recommends \\
    "linux-modules-nvidia-550-open-${{KVER}}" \\
    nvidia-kernel-common-550 \\
    nvidia-utils-550-server \\
    libnvidia-compute-550-server
else
  apt-get install -y --no-install-recommends \\
    nvidia-headless-550-server-open \\
    nvidia-utils-550-server \\
    libnvidia-compute-550-server
fi

echo "=== [Stage 1] Installing Amazon CloudWatch Agent ==="
curl -fsSL "https://amazoncloudwatch-agent-${{REGION}}.s3.${{REGION}}.amazonaws.com/ubuntu/amd64/latest/amazon-cloudwatch-agent.deb" -o /tmp/cwagent.deb || \\
  curl -fsSL "https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/amd64/latest/amazon-cloudwatch-agent.deb" -o /tmp/cwagent.deb
dpkg -i /tmp/cwagent.deb || apt-get install -f -y
rm -f /tmp/cwagent.deb

mkdir -p /opt/aws/amazon-cloudwatch-agent/etc
cat <<'CWCFG' > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
{{
  "agent": {{
    "metrics_collection_interval": 60,
    "run_as_user": "root"
  }},
  "logs": {{
    "logs_collected": {{
      "files": {{
        "collect_list": [
          {{
            "file_path": "/var/log/ovius-worker.log",
            "log_group_name": "/aws/ec2/ovius-vision-worker",
            "log_stream_name": "{{instance_id}}",
            "retention_in_days": 30
          }}
        ]
      }}
    }}
  }},
  "metrics": {{
    "namespace": "Ovius/VisionWorker",
    "append_dimensions": {{
      "InstanceId": "${{aws:InstanceId}}",
      "InstanceType": "${{aws:InstanceType}}"
    }},
    "metrics_collected": {{
      "nvidia_gpu": {{
        "measurement": [
          "utilization_gpu",
          "utilization_memory",
          "memory_used",
          "memory_total",
          "temperature_gpu",
          "power_draw"
        ],
        "metrics_collection_interval": 60
      }},
      "disk": {{
        "measurement": ["used_percent"],
        "metrics_collection_interval": 60,
        "resources": ["/", "/tmp", "/mnt/scratch"]
      }},
      "mem": {{
        "measurement": ["mem_used_percent"],
        "metrics_collection_interval": 60
      }}
    }}
  }}
}}
CWCFG

echo "=== [Stage 1] Writing Stage 2 post-reboot build script ==="
mkdir -p /opt/pipeline
cat <<'STAGE2' > /opt/pipeline/stage2_build.sh
#!/bin/bash
set -euxo pipefail

LOG_FILE="/var/log/ovius_ami_build.log"
exec > >(tee -a "$LOG_FILE") 2>&1

BUCKET="{bucket}"
REGION="{region}"
INSTANCE_TYPE="{instance_type}"
LOG_KEY="{log_key}"

upload_log() {{
  aws --region "$REGION" s3 cp "$LOG_FILE" "s3://$BUCKET/$LOG_KEY" || true
}}
on_stage2_exit() {{
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "=== [Stage 2] FAILED with exit code $rc at $(date -u +%FT%TZ); shutting down to prevent idle GPU cost ==="
    upload_log
    systemctl poweroff --no-block || shutdown -h now
  else
    upload_log
  fi
}}
trap on_stage2_exit EXIT

echo "=== [Stage 2] Post-reboot GPU build starting at $(date -u +%FT%TZ) ==="
nvidia-smi
upload_log

# Enable CloudWatch GPU agent
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \\
  -a fetch-config -m ec2 -s \\
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json || true

echo "=== [Stage 2] Installing uv + self-contained Python 3.11 venv ==="
export UV_INSTALL_DIR="/usr/local/bin"
export UV_PYTHON_INSTALL_DIR="/opt/pipeline/python"
export UV_CACHE_DIR="/tmp/uv_cache"
curl -LsSf https://astral.sh/uv/install.sh | sh
/usr/local/bin/uv venv --python 3.11 /opt/pipeline/venv

echo "=== [Stage 2] Installing PyTorch cu124 + TensorRT + Vision pipeline deps ==="
/usr/local/bin/uv pip install --python /opt/pipeline/venv/bin/python \\
  --index-url https://download.pytorch.org/whl/cu124 \\
  torch==2.6.0 torchvision==0.21.0

/usr/local/bin/uv pip install --python /opt/pipeline/venv/bin/python \\
  "ultralytics>=8.4,<9" \\
  opencv-python-headless \\
  lap \\
  pandas \\
  pyarrow \\
  pyyaml \\
  pillow \\
  matplotlib \\
  boto3 \\
  safetensors \\
  onnx \\
  scipy \\
  faster-coco-eval \\
  tensorboard \\
  timm \\
  loguru \\
  defusedxml \\
  rich \\
  calflops \\
  transformers \\
  tensorrt-cu12==10.9.0.34

echo "=== [Stage 2] Verifying CUDA + TensorRT inside /opt/pipeline/venv ==="
unset LD_LIBRARY_PATH || true
/opt/pipeline/venv/bin/python - <<'PYCHECK'
import torch, tensorrt as trt, pyarrow, ultralytics, safetensors
print("torch:", torch.__version__, "cuda_available:", torch.cuda.is_available())
assert torch.cuda.is_available(), "CUDA not available!"
print("gpu:", torch.cuda.get_device_name(0), "capability:", torch.cuda.get_device_capability(0))
builder = trt.Builder(trt.Logger(trt.Logger.WARNING))
print("tensorrt:", trt.__version__, "fast_fp16:", builder.platform_has_fast_fp16, "pyarrow:", pyarrow.__version__, "ultralytics:", ultralytics.__version__)
PYCHECK
upload_log

echo "=== [Stage 2] Pulling PTZ code bundle, ovius_api worker, and S-v5-XL artifacts from S3 ==="
mkdir -p /opt/pipeline/src /opt/pipeline/bundle /opt/pipeline/scripts /tmp/stage2_dl
aws --region "$REGION" s3 cp "s3://$BUCKET/pipeline/ptz_code_bundle.tar.gz" /tmp/stage2_dl/ptz_code_bundle.tar.gz
tar -xzf /tmp/stage2_dl/ptz_code_bundle.tar.gz -C /opt/pipeline
aws --region "$REGION" s3 cp "s3://$BUCKET/pipeline/ovius_api_pkg.tar.gz" /tmp/stage2_dl/ovius_api_pkg.tar.gz
tar -xzf /tmp/stage2_dl/ovius_api_pkg.tar.gz -C /opt/pipeline/src
aws --region "$REGION" s3 cp "s3://$BUCKET/pipeline/export_sv5xl_tensorrt.py" /opt/pipeline/scripts/export_sv5xl_tensorrt.py
rm -rf /tmp/stage2_dl

echo "=== [Stage 2] Exporting S-v5-XL ONNX and compiling A10G (SM 8.6) TensorRT FP16 engine ==="
mkdir -p /opt/pipeline/bundle/models /tmp/trt_export
/opt/pipeline/venv/bin/python /opt/pipeline/scripts/export_sv5xl_tensorrt.py \\
  --bucket "$BUCKET" \\
  --data-region "$REGION" \\
  --bundle-dir /opt/pipeline/bundle \\
  --work-dir /tmp/trt_export

echo "=== [Stage 2] Syncing lean runtime assets (TRT engine + FP16 safetensors + config) via worker ==="
rm -rf /tmp/trt_export
PYTHONPATH=/opt/pipeline/src /opt/pipeline/venv/bin/python -m ovius_api.ec2_pipeline_worker \\
  --data-region "$REGION" \\
  --bucket "$BUCKET" \\
  --pipeline-dir /opt/pipeline \\
  --sync-only
ls -lh /opt/pipeline/bundle/models
upload_log

echo "=== [Stage 2] Installing NVMe scratch mounter & persistent ovius-pipeline-worker.service ==="
cat <<'NVMESCRATCH' > /opt/pipeline/mount_nvme_scratch.sh
#!/bin/bash
set -euo pipefail
mkdir -p /mnt/scratch
if mountpoint -q /mnt/scratch; then
  exit 0
fi
NVME_DEV=$(lsblk -dn -o NAME,TYPE,MODEL | awk '/Instance Storage/ && $2=="disk" {{print "/dev/"$1; exit}}')
if [ -n "$NVME_DEV" ] && [ -b "$NVME_DEV" ]; then
  if ! blkid "$NVME_DEV" >/dev/null 2>&1; then
    mkfs.ext4 -F -L ovius_scratch "$NVME_DEV"
  fi
  mount -o noatime "$NVME_DEV" /mnt/scratch
  chmod 1777 /mnt/scratch
fi
NVMESCRATCH
chmod +x /opt/pipeline/mount_nvme_scratch.sh

cat <<'UNIT' > /etc/systemd/system/ovius-pipeline-worker.service
[Unit]
Description=Ovius EC2 GPU S-v5-XL PTZ Vision Pipeline Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/pipeline
Environment=PYTHONPATH=/opt/pipeline/src
Environment=DATA_REGION={region}
Environment=PIPELINE_S3_BUCKET={bucket}
Environment=PIPELINE_DIR=/opt/pipeline
Environment=WORKER_TMP_DIR=/mnt/scratch
Environment=WORKER_INSTANCE_TYPE={instance_type}
Environment=AUTO_SHUTDOWN=true
Environment=PYTHONUNBUFFERED=1
ExecStartPre=/opt/pipeline/mount_nvme_scratch.sh
ExecStartPre=/bin/bash -c 'ETAG=$(aws --region {region} s3api head-object --bucket {bucket} --key pipeline/ovius_api_pkg.tar.gz --query ETag --output text 2>/dev/null || true); if [ -n "$ETAG" ] && [ "$ETAG" != "$(cat /opt/pipeline/.ovius_api_pkg.etag 2>/dev/null || true)" ]; then aws --region {region} s3 cp s3://{bucket}/pipeline/ovius_api_pkg.tar.gz /tmp/ovius_api_pkg.tar.gz && tar -xzf /tmp/ovius_api_pkg.tar.gz -C /opt/pipeline/src && rm -f /tmp/ovius_api_pkg.tar.gz && echo "$ETAG" > /opt/pipeline/.ovius_api_pkg.etag; fi'
ExecStart=/opt/pipeline/venv/bin/python -m ovius_api.ec2_pipeline_worker --auto-shutdown
Restart=no
StandardOutput=append:/var/log/ovius-worker.log
StandardError=append:/var/log/ovius-worker.log

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable ovius-pipeline-worker.service

echo "=== [Stage 2] Disabling one-shot build unit & cleaning caches to keep 50 GB AMI lean ==="
systemctl disable ovius-ami-stage2.service || true
rm -f /etc/systemd/system/ovius-ami-stage2.service /opt/pipeline/stage2_build.sh
rm -rf /tmp/uv_cache /root/.cache /var/cache/apt/archives/* /var/lib/apt/lists/*
apt-get autoremove -y || true
systemctl daemon-reload

echo "=== [Stage 2] Final disk usage ==="
df -h /
du -sh /opt/pipeline/* || true
echo "=== [Stage 2] BUILD_SUCCESS at $(date -u +%FT%TZ) ==="
upload_log

# Power off cleanly so InstanceInitiatedShutdownBehavior=stop brings instance to stopped state
systemctl poweroff --no-block || shutdown -h now
STAGE2
chmod +x /opt/pipeline/stage2_build.sh

cat <<'S2UNIT' > /etc/systemd/system/ovius-ami-stage2.service
[Unit]
Description=Ovius AMI Stage 2 Post-Reboot GPU Setup
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
KillMode=process
ExecStart=/opt/pipeline/stage2_build.sh
RemainAfterExit=no

[Install]
WantedBy=multi-user.target
S2UNIT

systemctl daemon-reload
systemctl enable ovius-ami-stage2.service
echo "=== [Stage 1] Complete at $(date -u +%FT%TZ); rebooting to load NVIDIA driver ==="
upload_log
reboot
"""


def launch_builder(
    *,
    ec2: Any,
    bucket: str,
    region: str,
    base_ami: str,
    instance_type: str,
    subnets: list[str],
    security_group: str,
    instance_profile: str,
    volume_size_gb: int,
    log_key: str,
) -> tuple[str, str]:
    userdata = build_userdata(
        bucket=bucket,
        region=region,
        instance_type=instance_type,
        log_key=log_key,
    )
    last_err: Exception | None = None
    for subnet_id in subnets:
        try:
            resp = ec2.run_instances(
                ImageId=base_ami,
                InstanceType=instance_type,
                MinCount=1,
                MaxCount=1,
                SubnetId=subnet_id,
                SecurityGroupIds=[security_group],
                IamInstanceProfile={"Name": instance_profile},
                InstanceInitiatedShutdownBehavior="stop",
                MetadataOptions={
                    "HttpEndpoint": "enabled",
                    "HttpTokens": "required",
                    "HttpPutResponseHopLimit": 2,
                },
                BlockDeviceMappings=[
                    {
                        "DeviceName": "/dev/sda1",
                        "Ebs": {
                            "VolumeSize": volume_size_gb,
                            "VolumeType": "gp3",
                            "Encrypted": True,
                            "DeleteOnTermination": True,
                        },
                    }
                ],
                TagSpecifications=[
                    {
                        "ResourceType": "instance",
                        "Tags": [{"Key": "Name", "Value": "ovius-g5-vision-worker"}, *MANDATORY_TAGS],
                    },
                    {
                        "ResourceType": "volume",
                        "Tags": [{"Key": "Name", "Value": "ovius-g5-vision-worker-root"}, *MANDATORY_TAGS],
                    },
                ],
                UserData=userdata,
            )
            inst = resp["Instances"][0]
            return str(inst["InstanceId"]), subnet_id
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"InsufficientInstanceCapacity", "Unsupported"}:
                print(
                    f"[launch] Subnet {subnet_id} returned {code}; trying next subnet...",
                    file=sys.stderr,
                    flush=True,
                )
                last_err = exc
                continue
            raise
    raise RuntimeError(f"Unable to launch {instance_type} in any subnet {subnets}: {last_err}")


def wait_for_stopped(
    *,
    ec2: Any,
    s3: Any,
    bucket: str,
    log_key: str,
    instance_id: str,
    timeout_s: int = 2400,
    poll_s: int = 20,
) -> dict[str, Any]:
    t0 = time.monotonic()
    last_log_size = -1
    while time.monotonic() - t0 < timeout_s:
        desc = ec2.describe_instances(InstanceIds=[instance_id])
        inst = desc["Reservations"][0]["Instances"][0]
        state = str(inst["State"]["Name"])
        elapsed = int(time.monotonic() - t0)

        log_tail = ""
        try:
            head = s3.head_object(Bucket=bucket, Key=log_key)
            size = int(head["ContentLength"])
            if size != last_log_size:
                last_log_size = size
                raw = s3.get_object(Bucket=bucket, Key=log_key)["Body"].read().decode("utf-8", errors="replace")
                lines = [ln for ln in raw.splitlines() if ln.strip()]
                log_tail = lines[-1] if lines else ""
        except ClientError:
            pass

        print(
            f"[wait t={elapsed:4d}s] instance={instance_id} state={state} log_bytes={max(0, last_log_size)} last={log_tail[:120]}",
            flush=True,
        )
        if state == "stopped":
            return {"instance_id": instance_id, "state": state, "elapsed_s": elapsed}
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError(f"Instance {instance_id} entered unexpected terminal state: {state}")
        time.sleep(poll_s)
    raise TimeoutError(f"Instance {instance_id} did not reach stopped within {timeout_s}s")


def create_ami(*, ec2: Any, instance_id: str, ami_name: str) -> str:
    resp = ec2.create_image(
        InstanceId=instance_id,
        Name=ami_name,
        Description="Ovius 50 GB gp3 Ubuntu 22.04 + NVIDIA 550 + PyTorch cu124 + TensorRT FP16 S-v5-XL PTZ worker (ap-southeast-3)",
        NoReboot=True,
        TagSpecifications=[
            {
                "ResourceType": "image",
                "Tags": [{"Key": "Name", "Value": ami_name}, *MANDATORY_TAGS],
            },
            {
                "ResourceType": "snapshot",
                "Tags": [{"Key": "Name", "Value": f"{ami_name}-root"}, *MANDATORY_TAGS],
            },
        ],
    )
    ami_id = str(resp["ImageId"])
    print(f"[ami] Created {ami_id} ({ami_name}); waiting for available...", flush=True)
    ec2.get_waiter("image_available").wait(
        ImageIds=[ami_id],
        WaiterConfig={"Delay": 15, "MaxAttempts": 80},
    )
    return ami_id


def main() -> int:
    parser = argparse.ArgumentParser(description="Build 50 GB g5.xlarge S-v5-XL TensorRT AMI in ap-southeast-3.")
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--base-ami", default=DEFAULT_BASE_AMI)
    parser.add_argument("--instance-type", default=DEFAULT_INSTANCE_TYPE)
    parser.add_argument("--volume-size-gb", type=int, default=DEFAULT_VOLUME_SIZE_GB)
    parser.add_argument("--ami-name", default="")
    parser.add_argument("--skip-create-ami", action="store_true")
    parser.add_argument("--wait-timeout-s", type=int, default=2400)
    args = parser.parse_args()

    cfg = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
    sess = boto3.Session(profile_name=args.profile, region_name=args.region)
    ec2 = sess.client("ec2", config=cfg)
    s3 = sess.client("s3", config=cfg)

    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    log_key = f"pipeline/build_logs/g5_build_{ts}.log"
    ami_name = args.ami_name or f"{DEFAULT_AMI_NAME_PREFIX}-{ts}"

    instance_id, subnet_id = launch_builder(
        ec2=ec2,
        bucket=args.bucket,
        region=args.region,
        base_ami=args.base_ami,
        instance_type=args.instance_type,
        subnets=DEFAULT_SUBNETS,
        security_group=DEFAULT_SECURITY_GROUP,
        instance_profile=DEFAULT_INSTANCE_PROFILE,
        volume_size_gb=args.volume_size_gb,
        log_key=log_key,
    )
    print(
        json.dumps(
            {
                "event": "launched",
                "instance_id": instance_id,
                "instance_type": args.instance_type,
                "subnet_id": subnet_id,
                "region": args.region,
                "log_s3": f"s3://{args.bucket}/{log_key}",
            }
        ),
        flush=True,
    )

    wait_res = wait_for_stopped(
        ec2=ec2,
        s3=s3,
        bucket=args.bucket,
        log_key=log_key,
        instance_id=instance_id,
        timeout_s=args.wait_timeout_s,
    )

    # Verify BUILD_SUCCESS marker in S3 log before cutting AMI
    log_text = s3.get_object(Bucket=args.bucket, Key=log_key)["Body"].read().decode("utf-8", errors="replace")
    if "BUILD_SUCCESS" not in log_text:
        print("ERROR: Instance stopped without BUILD_SUCCESS marker! Tail of log:", file=sys.stderr)
        print("\n".join(log_text.splitlines()[-40:]), file=sys.stderr)
        return 1

    ami_id = None
    if not args.skip_create_ami:
        ami_id = create_ami(ec2=ec2, instance_id=instance_id, ami_name=ami_name)

    print(
        json.dumps(
            {
                "status": "SUCCESS",
                "instance_id": instance_id,
                "instance_state": wait_res["state"],
                "build_elapsed_s": wait_res["elapsed_s"],
                "ami_id": ami_id,
                "ami_name": ami_name if ami_id else None,
                "log_s3": f"s3://{args.bucket}/{log_key}",
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
