"""Export S-v5-XL (1920x1088) to ONNX and compile an A10G (SM 8.6) TensorRT FP16 engine on EC2 GPU."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tarfile
import tempfile
import time
from typing import Any

import boto3
from botocore.config import Config
import numpy as np
import torch
import torch.nn as nn
import yaml

DEFAULT_BUCKET = "ovius-pipeline-apse1-610331148578"
DEFAULT_DATA_REGION = "ap-southeast-1"
MODEL_PREFIX = "models/s-v5-xl"
BUNDLE_KEY = "pipeline/ptz_code_bundle.tar.gz"


class _DFineExportWrapper(nn.Module):
    """Wraps DFINE.deploy() to emit pure (pred_logits, pred_boxes) tensors for ONNX/TensorRT."""

    def __init__(self, model: nn.Module) -> None:
        super().__init__()
        self.model = model

    def forward(self, images: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        out = self.model(images)
        return out["pred_logits"], out["pred_boxes"]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _gpu_arch_tag() -> str:
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA GPU required to compile TensorRT engine")
    major, minor = torch.cuda.get_device_capability(0)
    return f"sm{major}{minor}"


def build_tensorrt_fp16_engine(
    *,
    onnx_path: Path,
    engine_path: Path,
    workspace_gb: int = 4,
) -> dict[str, Any]:
    import tensorrt as trt

    trt_logger = trt.Logger(trt.Logger.WARNING)
    builder = trt.Builder(trt_logger)
    network = builder.create_network(1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH))
    parser = trt.OnnxParser(network, trt_logger)

    parsed = parser.parse_from_file(str(onnx_path))
    if not parsed:
        errors = [str(parser.get_error(i)) for i in range(parser.num_errors)]
        raise RuntimeError(f"ONNX parse failed: {errors}")

    config = builder.create_builder_config()
    config.set_memory_pool_limit(trt.MemoryPoolType.WORKSPACE, workspace_gb * (1 << 30))
    if builder.platform_has_fast_fp16:
        config.set_flag(trt.BuilderFlag.FP16)

    t0 = time.monotonic()
    serialized = builder.build_serialized_network(network, config)
    if serialized is None:
        raise RuntimeError("TensorRT build_serialized_network returned None")
    engine_path.write_bytes(bytes(serialized))
    build_s = round(time.monotonic() - t0, 2)
    return {
        "engine_path": str(engine_path),
        "engine_bytes": engine_path.stat().st_size,
        "build_seconds": build_s,
        "trt_version": trt.__version__,
        "fp16_enabled": bool(builder.platform_has_fast_fp16),
    }


def benchmark_and_verify_parity(
    *,
    wrapper: nn.Module,
    engine_path: Path,
    eval_h: int,
    eval_w: int,
    warmup: int = 10,
    iters: int = 30,
) -> dict[str, Any]:
    import tensorrt as trt

    device = torch.device("cuda:0")
    rng = np.random.default_rng(42)
    sample_np = rng.uniform(0.0, 1.0, size=(1, 3, eval_h, eval_w)).astype(np.float32)
    sample_t = torch.from_numpy(sample_np).to(device)

    with torch.inference_mode():
        pt_logits, pt_boxes = wrapper(sample_t)

    trt_logger = trt.Logger(trt.Logger.WARNING)
    runtime = trt.Runtime(trt_logger)
    engine = runtime.deserialize_cuda_engine(engine_path.read_bytes())
    assert engine is not None, "Failed to deserialize newly built TensorRT engine"
    ctx = engine.create_execution_context()

    in_name = None
    out_names: list[str] = []
    for i in range(engine.num_io_tensors):
        tname = engine.get_tensor_name(i)
        if engine.get_tensor_mode(tname) == trt.TensorIOMode.INPUT:
            in_name = tname
        else:
            out_names.append(tname)
    assert in_name is not None and len(out_names) == 2

    def _run_trt(x: torch.Tensor) -> dict[str, torch.Tensor]:
        ctx.set_input_shape(in_name, tuple(x.shape))
        ctx.set_tensor_address(in_name, int(x.data_ptr()))
        outs: dict[str, torch.Tensor] = {}
        for name in out_names:
            shape = tuple(ctx.get_tensor_shape(name))
            dt = torch.float16 if engine.get_tensor_dtype(name) == trt.DataType.HALF else torch.float32
            buf = torch.empty(shape, dtype=dt, device=device)
            ctx.set_tensor_address(name, int(buf.data_ptr()))
            outs[name] = buf
        stream = torch.cuda.current_stream(device)
        ok = ctx.execute_async_v3(stream_handle=stream.cuda_stream)
        assert ok, "TensorRT execute_async_v3 failed"
        stream.synchronize()
        return outs

    trt_out = _run_trt(sample_t)
    trt_logits = trt_out.get("pred_logits", trt_out[out_names[0]]).float()
    trt_boxes = trt_out.get("pred_boxes", trt_out[out_names[1]]).float()

    max_logit_diff = float((pt_logits.float() - trt_logits).abs().max().item())
    mean_logit_diff = float((pt_logits.float() - trt_logits).abs().mean().item())
    max_box_diff = float((pt_boxes.float() - trt_boxes).abs().max().item())
    mean_box_diff = float((pt_boxes.float() - trt_boxes).abs().mean().item())

    # Benchmark TensorRT FP16 vs PyTorch FP16 AMP
    for _ in range(warmup):
        _run_trt(sample_t)
    torch.cuda.synchronize()
    t0 = time.perf_counter()
    for _ in range(iters):
        _run_trt(sample_t)
    torch.cuda.synchronize()
    trt_ms = (time.perf_counter() - t0) * 1000.0 / iters

    for _ in range(warmup):
        with torch.inference_mode(), torch.amp.autocast("cuda", dtype=torch.float16):
            wrapper(sample_t)
    torch.cuda.synchronize()
    t1 = time.perf_counter()
    for _ in range(iters):
        with torch.inference_mode(), torch.amp.autocast("cuda", dtype=torch.float16):
            wrapper(sample_t)
    torch.cuda.synchronize()
    pt_fp16_ms = (time.perf_counter() - t1) * 1000.0 / iters

    return {
        "gpu_name": torch.cuda.get_device_name(0),
        "gpu_arch": _gpu_arch_tag(),
        "max_logit_diff": round(max_logit_diff, 5),
        "mean_logit_diff": round(mean_logit_diff, 6),
        "max_box_diff": round(max_box_diff, 5),
        "mean_box_diff": round(mean_box_diff, 6),
        "trt_fp16_ms_per_frame": round(trt_ms, 2),
        "trt_fp16_fps": round(1000.0 / max(0.001, trt_ms), 2),
        "pytorch_fp16_ms_per_frame": round(pt_fp16_ms, 2),
        "pytorch_fp16_fps": round(1000.0 / max(0.001, pt_fp16_ms), 2),
        "speedup_vs_pytorch_fp16": round(pt_fp16_ms / max(0.001, trt_ms), 2),
    }


def export_and_upload(
    *,
    bucket: str = DEFAULT_BUCKET,
    data_region: str = DEFAULT_DATA_REGION,
    model_prefix: str = MODEL_PREFIX,
    bundle_dir: Path | None = None,
    work_dir: Path | None = None,
    upload: bool = True,
) -> dict[str, Any]:
    cfg = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
    s3 = boto3.Session(region_name=data_region).client("s3", config=cfg)

    base_work = work_dir or Path(tempfile.mkdtemp(prefix="sv5xl_trt_"))
    base_work.mkdir(parents=True, exist_ok=True)

    # Ensure D-FINE source tree is available
    dfine_root = (bundle_dir / "dfine") if bundle_dir and (bundle_dir / "dfine" / "src").is_dir() else None
    if dfine_root is None:
        tgz = base_work / "ptz_code_bundle.tar.gz"
        s3.download_file(bucket, BUNDLE_KEY, str(tgz))
        with tarfile.open(tgz, "r:gz") as tf:
            tf.extractall(base_work)
        dfine_root = base_work / "bundle" / "dfine"
    if str(dfine_root) not in sys.path:
        sys.path.insert(0, str(dfine_root))
    from src.core import YAMLConfig

    manifest_path = base_work / "manifest.json"
    s3.download_file(bucket, f"{model_prefix}/manifest.json", str(manifest_path))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    cfg_name = manifest["artifacts"]["dfine_config"]["file"]
    fp32_name = manifest["artifacts"]["ema_fp32_pth"]["file"]
    dfine_cfg_path = base_work / cfg_name
    fp32_path = base_work / fp32_name
    if not dfine_cfg_path.exists():
        s3.download_file(bucket, f"{model_prefix}/{cfg_name}", str(dfine_cfg_path))
    if not fp32_path.exists():
        s3.download_file(bucket, f"{model_prefix}/{fp32_name}", str(fp32_path))

    meta = json.loads(dfine_cfg_path.read_text(encoding="utf-8"))
    eval_h, eval_w = [int(x) for x in meta["config"]["eval_spatial_size"]]

    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as tf_yml:
        yaml.safe_dump(meta["config"], tf_yml)
        yml_path = tf_yml.name
    try:
        yaml_cfg = YAMLConfig(yml_path)
        model = yaml_cfg.model
        ck = torch.load(str(fp32_path), map_location="cpu", weights_only=False)
        model.load_state_dict(ck["ema"]["module"])
        model = model.deploy().cuda().eval()
    finally:
        os.unlink(yml_path)

    wrapper = _DFineExportWrapper(model).cuda().eval()
    dummy = torch.randn(1, 3, eval_h, eval_w, dtype=torch.float32, device="cuda:0")

    onnx_name = f"s_v5_xl_{eval_w}x{eval_h}.onnx"
    onnx_path = base_work / onnx_name
    t_onnx = time.monotonic()
    with torch.inference_mode():
        torch.onnx.export(
            wrapper,
            dummy,
            str(onnx_path),
            export_params=True,
            opset_version=17,
            do_constant_folding=True,
            input_names=["images"],
            output_names=["pred_logits", "pred_boxes"],
            dynamo=False,
        )
    onnx_seconds = round(time.monotonic() - t_onnx, 2)

    arch = _gpu_arch_tag()
    engine_name = f"s_v5_xl_{eval_w}x{eval_h}_fp16_{arch}.engine"
    engine_path = base_work / engine_name
    build_info = build_tensorrt_fp16_engine(onnx_path=onnx_path, engine_path=engine_path)
    parity_info = benchmark_and_verify_parity(
        wrapper=wrapper,
        engine_path=engine_path,
        eval_h=eval_h,
        eval_w=eval_w,
    )

    manifest["artifacts"]["onnx_fp32"] = {
        "file": onnx_name,
        "bytes": onnx_path.stat().st_size,
        "sha256": _sha256(onnx_path),
        "opset": 17,
    }
    manifest["artifacts"][f"tensorrt_fp16_{arch}"] = {
        "file": engine_name,
        "bytes": engine_path.stat().st_size,
        "sha256": _sha256(engine_path),
        "trt_version": build_info["trt_version"],
        "gpu_name": parity_info["gpu_name"],
        "gpu_arch": arch,
    }
    manifest["benchmark"] = parity_info
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    uploaded_keys: list[str] = []
    if upload:
        for p in (onnx_path, engine_path, manifest_path):
            key = f"{model_prefix}/{p.name}"
            s3.upload_file(str(p), bucket, key)
            uploaded_keys.append(f"s3://{bucket}/{key}")

    return {
        "onnx_seconds": onnx_seconds,
        "onnx_bytes": onnx_path.stat().st_size,
        "build_info": build_info,
        "parity_info": parity_info,
        "uploaded": uploaded_keys,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export S-v5-XL to ONNX & TensorRT FP16 on EC2 GPU.")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--data-region", default=DEFAULT_DATA_REGION)
    parser.add_argument("--model-prefix", default=MODEL_PREFIX)
    parser.add_argument("--bundle-dir", default="/opt/pipeline/bundle")
    parser.add_argument("--work-dir", default="/opt/pipeline/models_cache")
    parser.add_argument("--no-upload", action="store_true")
    args = parser.parse_args()

    res = export_and_upload(
        bucket=args.bucket,
        data_region=args.data_region,
        model_prefix=args.model_prefix,
        bundle_dir=Path(args.bundle_dir) if args.bundle_dir else None,
        work_dir=Path(args.work_dir) if args.work_dir else None,
        upload=not args.no_upload,
    )
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
