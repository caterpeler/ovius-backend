# Production GPU Vision Inference Worker Image for AWS Batch (g4dn.4xlarge / T4 CUDA 12.1)
FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/var/task

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3-pip \
    ffmpeg \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    python3 -m pip install --no-cache-dir \
      boto3>=1.35.0 \
      numpy>=1.26.0 \
      opencv-python-headless>=4.9.0 \
      pyarrow>=17.0.0 \
      torch>=2.1.0 \
      torchvision>=0.16.0 \
      ultralytics>=8.3.0

WORKDIR /var/task
COPY src/ovius_api /var/task/ovius_api

ENTRYPOINT ["python3", "-m", "ovius_api.worker"]
