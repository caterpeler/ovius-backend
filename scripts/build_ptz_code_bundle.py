"""Build and upload the slim, S-v5-XL, no-annotated-video PTZ pipeline code bundle to S3."""

from __future__ import annotations

import argparse
import io
import json
from pathlib import Path
import py_compile
import re
import shutil
import sys
import tarfile
import tempfile

import boto3
from botocore.config import Config

DEFAULT_PROFILE = "610331148578_Project8Learner"
DEFAULT_REGION = "ap-southeast-3"
DEFAULT_BUCKET = "ovius-pipeline-apse3-610331148578"
RAW_BUNDLE_KEY = "pipeline/raw_ptz_code_bundle.tar.gz"
PROD_BUNDLE_KEY = "pipeline/ptz_code_bundle.tar.gz"

SV5XL_MODEL_ID = "b0456a9c-67f9-4df4-b460-b369d2a0fdef"
SV5XL_MODEL_NAME = "S-v5-XL"
SV5XL_CONFIDENCE = 0.56
TARGET_ANALYSED_FPS = 3.0  # every=8 on 24/25 FPS, every=10 on 30 FPS, every=20 on 60 FPS

GPU_V3_CLAHE_SOURCE = '''import cv2
import numpy as np

_CLAHE = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
_GPU_STATE = {}


def _get_gpu_kernels(device):
    import torch

    if device in _GPU_STATE:
        return _GPU_STATE[device]

    def _gauss_1d(sigma, radius):
        x = torch.arange(-radius, radius + 1, dtype=torch.float32, device=device)
        k = torch.exp(-(x * x) / (2.0 * sigma * sigma))
        k = k / k.sum()
        return k.view(1, 1, 1, -1), k.view(1, 1, -1, 1)

    state = {
        "k1": _gauss_1d(1.0, 3),
        "k2": _gauss_1d(3.0, 9),
    }
    _GPU_STATE[device] = state
    return state


def _gauss_blur_gpu(t, kh, kv):
    import torch.nn.functional as F

    rx = kh.shape[-1] // 2
    ry = kv.shape[-2] // 2
    out = F.pad(t, (rx, rx, 0, 0), mode="reflect")
    out = F.conv2d(out, kh)
    out = F.pad(out, (0, 0, ry, ry), mode="reflect")
    return F.conv2d(out, kv)


def preprocess(image):
    """
    GPU-accelerated v3 Bandpass-Chroma CLAHE:
    1. Downscales oversized frames (>1920x1088) to native 1920x1088 before LAB/CLAHE.
    2. Midtone/Shadow Alpha-CLAHE on L channel (cached cv2.CLAHE).
    3. Difference-of-Gaussians (DoG) high-luminance bandpass + CIE LAB chromaticity stretch
       executed in fused PyTorch CUDA kernels when CUDA is available (CPU fallback otherwise).
    """
    h, w = image.shape[:2]
    if w > 1920 or h > 1088:
        image = cv2.resize(image, (1920, 1088), interpolation=cv2.INTER_AREA)

    lab = cv2.cvtColor(image, cv2.COLOR_RGB2LAB)
    L_clahe = _CLAHE.apply(lab[:, :, 0])

    try:
        import torch

        if torch.cuda.is_available():
            device = torch.device("cuda:0")
            kernels = _get_gpu_kernels(device)
            with torch.inference_mode():
                lab_t = torch.from_numpy(lab).to(device, non_blocking=True)
                L_f = lab_t[:, :, 0].to(torch.float32)
                L_c_f = torch.from_numpy(L_clahe).to(device, non_blocking=True).to(torch.float32)

                L_4d = L_f.unsqueeze(0).unsqueeze(0)
                g1 = _gauss_blur_gpu(L_4d, *kernels["k1"]).squeeze(0).squeeze(0)
                g2 = _gauss_blur_gpu(L_4d, *kernels["k2"]).squeeze(0).squeeze(0)
                dog = g1 - g2

                gate_high = torch.clamp((L_f - 140.0) / 40.0, 0.0, 1.0)
                L_shadow = L_f + 0.50 * (L_c_f - L_f)
                L_highlight = L_f + 0.35 * dog
                L_out = torch.clamp((1.0 - gate_high) * L_shadow + gate_high * L_highlight, 0.0, 255.0).to(torch.uint8)

                eff_chroma = 1.0 + 0.25 * gate_high
                a_out = torch.clamp(128.0 + (lab_t[:, :, 1].to(torch.float32) - 128.0) * eff_chroma, 0.0, 255.0).to(torch.uint8)
                b_out = torch.clamp(128.0 + (lab_t[:, :, 2].to(torch.float32) - 128.0) * eff_chroma, 0.0, 255.0).to(torch.uint8)

                lab_out = torch.stack((L_out, a_out, b_out), dim=2).cpu().numpy()
            return cv2.cvtColor(lab_out, cv2.COLOR_LAB2RGB)
    except Exception:
        pass

    L_f = lab[:, :, 0].astype(np.float32)
    L_c_f = L_clahe.astype(np.float32)
    g1 = cv2.GaussianBlur(L_f, (0, 0), sigmaX=1.0, sigmaY=1.0)
    g2 = cv2.GaussianBlur(L_f, (0, 0), sigmaX=3.0, sigmaY=3.0)
    dog = g1 - g2
    gate_high = np.clip((L_f - 140.0) / 40.0, 0.0, 1.0)
    L_shadow = L_f + 0.50 * (L_c_f - L_f)
    L_highlight = L_f + 0.35 * dog
    L_out = np.clip((1.0 - gate_high) * L_shadow + gate_high * L_highlight, 0, 255).astype(np.uint8)
    eff_chroma = 1.0 + 0.25 * gate_high
    a_out = np.clip(128.0 + (lab[:, :, 1].astype(np.float32) - 128.0) * eff_chroma, 0, 255).astype(np.uint8)
    b_out = np.clip(128.0 + (lab[:, :, 2].astype(np.float32) - 128.0) * eff_chroma, 0, 255).astype(np.uint8)
    return cv2.cvtColor(cv2.merge([L_out, a_out, b_out]), cv2.COLOR_LAB2RGB)
'''

VIDEO_AND_FRAME_PASS_UPGRADE = '''@node("video")
def video(ctx, p):
    """Detect native video framerate (vfps) and compute dynamic frame-skip stride `every`."""
    cap = cv2.VideoCapture(str(ctx.video))
    if not cap.isOpened():
        raise SystemExit(f"{ctx.video} is not a video OpenCV can read")
    raw_vfps = cap.get(cv2.CAP_PROP_FPS)
    vfps = float(raw_vfps) if (raw_vfps and 1.0 < raw_vfps <= 240.0) else 24.0
    n = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    W, H = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)), int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()
    max_s = p["max_seconds"] if ctx.max_s is None else ctx.max_s
    explicit_skip = p.get("frame_skip")
    if explicit_skip is not None and int(explicit_skip) >= 1:
        every = int(explicit_skip)
    else:
        target_fps = float(p.get("fps") or 3.0)
        every = max(1, int(round(vfps / max(0.1, target_fps))))
    last = min(n, int(max_s * vfps)) if max_s else n
    examined = (last + every - 1) // every if last > 0 else 0
    print(
        f"[video] vfps={vfps:.3f} | frame_skip_every={every} | analysed_fps={vfps / every:.3f} | "
        f"total_frames={last} | examined_frames={examined}",
        flush=True,
    )
    return {"frames": SimpleNamespace(video=ctx.video, vfps=vfps, every=every, last=last, width=W, height=H, count=0)}


def frame_pass(frames, workers):
    """Pipelined async H.264 decode + concurrent CameraMotion/Detect overlap in exact frame order."""
    from concurrent.futures import ThreadPoolExecutor
    import queue
    import threading

    decoded_q = queue.Queue(maxsize=4)
    decode_err = []

    def _decoder():
        cap = cv2.VideoCapture(str(frames.video))
        try:
            for i in range(frames.last):
                if not cap.grab():
                    break
                if i % frames.every:
                    continue
                ok, frame = cap.retrieve()
                if not ok or frame is None:
                    break
                decoded_q.put((i, frame))
        except Exception as exc:
            decode_err.append(exc)
        finally:
            cap.release()
            decoded_q.put(None)

    detect_workers = [w for w in workers if isinstance(w, Detect)]
    dependent_workers = [w for w in workers if getattr(w, "boxes", None) is not None and not isinstance(w, Detect)]
    independent_workers = [w for w in workers if w not in detect_workers and w not in dependent_workers]

    t_dec = threading.Thread(target=_decoder, daemon=True)
    t_dec.start()
    count, t0 = 0, time.time()
    pool = ThreadPoolExecutor(max_workers=max(1, len(independent_workers))) if independent_workers else None
    try:
        while True:
            item = decoded_q.get()
            if item is None:
                break
            i, frame = item
            futs = [pool.submit(w.step, i, frame) for w in independent_workers] if pool else []
            for w in detect_workers:
                w.step(i, frame)
            for fut in futs:
                fut.result()
            for w in dependent_workers:
                w.step(i, frame)
            count += 1
            if count % 25 == 0:
                elapsed = max(0.001, time.time() - t0)
                print(
                    f"{i / frames.vfps:6.1f} s of video | {count} frames | {count / elapsed:.2f} frames/s",
                    flush=True,
                )
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        t_dec.join(timeout=5.0)
    if decode_err:
        raise decode_err[0]
    if count > 0:
        elapsed = max(0.001, time.time() - t0)
        print(
            f"{frames.last / frames.vfps:6.1f} s of video | {count} frames | {count / elapsed:.2f} frames/s",
            flush=True,
        )
    return count'''

DFINE_MODEL_UPGRADE = '''class DFineModel:
    """D-FINE detector supporting TensorRT FP16 (.engine), ONNXRuntime (.onnx), SafeTensors, and PyTorch FP16 (.pth).

    Uses exact native widescreen eval_spatial_size [H, W] (1088x1920) instead of squaring to 1920x1920.
    """

    BATCH = 1

    def __init__(self, path, device=None):
        import tempfile

        import torch
        import yaml

        path = Path(path)
        cfg_path = self._find_dfine_config(path)
        meta = json.loads(cfg_path.read_text())
        code = HERE / "dfine" if (HERE / "dfine" / "src").is_dir() else Path(os.environ.get("DFINE_DIR", ""))
        if not (code / "src" / "core").is_dir():
            raise SystemExit("a D-FINE model needs the D-FINE code: the bundle's dfine/ folder, or DFINE_DIR set to a checkout")
        if str(code) not in sys.path:
            sys.path.insert(0, str(code))
        from src.core import YAMLConfig

        eval_size = meta.get("config", {}).get("eval_spatial_size")
        if isinstance(eval_size, (list, tuple)) and len(eval_size) == 2:
            self.eval_h, self.eval_w = int(eval_size[0]), int(eval_size[1])
        else:
            sz = int(meta["imgsz"])
            self.eval_h, self.eval_w = sz, sz

        device = device or ("cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu")
        self.device = torch.device(f"cuda:{device}" if str(device).isdigit() else device)
        self.names = list(meta["names"])
        self.imgsz = int(meta["imgsz"])
        self.torch = torch
        self.backend = "pytorch"
        self.model = None
        self._trt_context = None
        self._trt_engine = None
        self._trt_runtime = None
        self._trt_input_name = None
        self._trt_output_names = []
        self._ort_session = None

        with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
            yaml.safe_dump(meta["config"], f)
        try:
            cfg = YAMLConfig(f.name)
            self.post = cfg.postprocessor.deploy().to(self.device).eval()

            resolved = self._resolve_best_artifact(path)
            if resolved.suffix == ".engine" and self.device.type == "cuda":
                if self._try_load_tensorrt(resolved):
                    self.backend = "tensorrt"
                    return
                # Engine load failed (e.g., GPU arch mismatch) -> fall back to ONNX or PyTorch
                resolved = self._resolve_fallback_artifact(path)

            if resolved.suffix == ".onnx":
                if self._try_load_onnx(resolved):
                    self.backend = "onnxruntime"
                    return
                resolved = self._resolve_fallback_artifact(path, skip_onnx=True)

            model = cfg.model
            state_dict = self._load_state_dict(resolved)
            model.load_state_dict(state_dict)
            self.model = model.deploy().to(self.device).eval()
            self.backend = "pytorch_fp16" if self.device.type == "cuda" else "pytorch_fp32"
        finally:
            os.unlink(f.name)

    @staticmethod
    def _find_dfine_config(path: Path) -> Path:
        direct = path.with_suffix(".dfine.json")
        if direct.exists():
            return direct
        siblings = sorted(path.parent.glob("*.dfine.json"))
        if siblings:
            return siblings[0]
        raise FileNotFoundError(f"No .dfine.json config found beside {path}")

    def _resolve_best_artifact(self, path: Path) -> Path:
        if path.exists() and path.suffix in {".engine", ".onnx", ".safetensors", ".pth"}:
            if path.suffix == ".engine" and self.device.type == "cuda":
                return path
        if self.device.type == "cuda":
            engines = sorted(path.parent.glob("*.engine"))
            if engines:
                return engines[0]
        for pattern in ("*.onnx", "*.safetensors", "*fp16*.pth", "*fp32*.pth", "*.pth"):
            matches = sorted(path.parent.glob(pattern))
            if matches:
                return matches[0]
        return path

    @staticmethod
    def _resolve_fallback_artifact(path: Path, skip_onnx: bool = False) -> Path:
        patterns = ("*.safetensors", "*fp16*.pth", "*fp32*.pth", "*.pth")
        if not skip_onnx:
            patterns = ("*.onnx",) + patterns
        for pattern in patterns:
            matches = sorted(path.parent.glob(pattern))
            if matches:
                return matches[0]
        raise FileNotFoundError(f"No fallback D-FINE weights found in {path.parent}")

    @staticmethod
    def _load_state_dict(path: Path):
        import torch

        if path.suffix == ".safetensors":
            from safetensors.torch import load_file
            sd = load_file(str(path), device="cpu")
            return {k: (v.float() if v.dtype == torch.float16 else v) for k, v in sd.items()}
        try:
            checkpoint = torch.load(str(path), map_location="cpu", weights_only=True)
        except Exception:
            checkpoint = torch.load(str(path), map_location="cpu", weights_only=False)
        ema = checkpoint.get("ema") if isinstance(checkpoint, dict) else None
        raw = ema["module"] if isinstance(ema, dict) and "module" in ema else checkpoint.get("model", checkpoint)
        return {k: (v.float() if torch.is_tensor(v) and v.dtype == torch.float16 else v) for k, v in raw.items()}

    def _try_load_tensorrt(self, engine_path: Path) -> bool:
        try:
            import tensorrt as trt

            trt_logger = trt.Logger(trt.Logger.WARNING)
            runtime = trt.Runtime(trt_logger)
            engine = runtime.deserialize_cuda_engine(engine_path.read_bytes())
            if engine is None:
                return False
            context = engine.create_execution_context()
            if context is None:
                return False
            in_name = None
            out_names = []
            for idx in range(engine.num_io_tensors):
                tname = engine.get_tensor_name(idx)
                mode = engine.get_tensor_mode(tname)
                if mode == trt.TensorIOMode.INPUT:
                    in_name = tname
                else:
                    out_names.append(tname)
            if not in_name or len(out_names) < 2:
                return False
            self._trt_runtime = runtime
            self._trt_engine = engine
            self._trt_context = context
            self._trt_input_name = in_name
            self._trt_output_names = out_names
            print(f"[DFineModel] Loaded TensorRT FP16 engine: {engine_path.name} ({self.eval_w}x{self.eval_h})", file=sys.stderr)
            return True
        except Exception as exc:
            print(f"[DFineModel] TensorRT load skipped ({exc}); falling back", file=sys.stderr)
            return False

    def _try_load_onnx(self, onnx_path: Path) -> bool:
        try:
            import onnxruntime as ort

            providers = ["CUDAExecutionProvider", "CPUExecutionProvider"] if self.device.type == "cuda" else ["CPUExecutionProvider"]
            self._ort_session = ort.InferenceSession(str(onnx_path), providers=providers)
            print(f"[DFineModel] Loaded ONNXRuntime ({self._ort_session.get_providers()[0]}): {onnx_path.name}", file=sys.stderr)
            return True
        except Exception as exc:
            print(f"[DFineModel] ONNXRuntime load skipped ({exc}); falling back", file=sys.stderr)
            return False

    def _forward_raw(self, x):
        torch = self.torch
        if self.backend == "tensorrt":
            import tensorrt as trt

            x = x.contiguous().float()
            self._trt_context.set_input_shape(self._trt_input_name, tuple(x.shape))
            self._trt_context.set_tensor_address(self._trt_input_name, int(x.data_ptr()))
            outputs = {}
            for name in self._trt_output_names:
                shape = tuple(self._trt_context.get_tensor_shape(name))
                trt_dtype = self._trt_engine.get_tensor_dtype(name)
                dtype = torch.float16 if trt_dtype == trt.DataType.HALF else torch.float32
                buf = torch.empty(shape, dtype=dtype, device=self.device)
                self._trt_context.set_tensor_address(name, int(buf.data_ptr()))
                outputs[name] = buf
            stream = torch.cuda.current_stream(self.device)
            ok = self._trt_context.execute_async_v3(stream_handle=stream.cuda_stream)
            if not ok:
                raise RuntimeError("TensorRT execute_async_v3 failed")
            stream.synchronize()
            logits = outputs.get("pred_logits", outputs[self._trt_output_names[0]]).float()
            boxes = outputs.get("pred_boxes", outputs[self._trt_output_names[1]]).float()
            return {"pred_logits": logits, "pred_boxes": boxes}
        if self.backend == "onnxruntime":
            inp_name = self._ort_session.get_inputs()[0].name
            outs = self._ort_session.run(None, {inp_name: x.detach().cpu().numpy().astype(np.float32)})
            return {
                "pred_logits": torch.from_numpy(outs[0]).to(self.device),
                "pred_boxes": torch.from_numpy(outs[1]).to(self.device),
            }
        if self.device.type == "cuda":
            with torch.amp.autocast("cuda", dtype=torch.float16):
                out = self.model(x)
            return {"pred_logits": out["pred_logits"].float(), "pred_boxes": out["pred_boxes"].float()}
        return self.model(x)

    def detect(self, pictures, conf):
        """Boxes on RGB pictures: (xyxy, confidences, classes) for each, best first."""
        import cv2

        torch, answers = self.torch, []
        batch_size = 1 if self.backend in {"tensorrt", "onnxruntime"} else 4
        for start in range(0, len(pictures), batch_size):
            group = pictures[start : start + batch_size]
            resized_imgs = [
                cv2.resize(p, (self.eval_w, self.eval_h), interpolation=cv2.INTER_LINEAR)
                if (p.shape[1], p.shape[0]) != (self.eval_w, self.eval_h)
                else p
                for p in group
            ]
            batch = torch.stack([torch.from_numpy(img).permute(2, 0, 1) for img in resized_imgs])
            sizes = torch.tensor([[p.shape[1], p.shape[0]] for p in group], device=self.device)
            with torch.inference_mode():
                raw_out = self._forward_raw(batch.to(self.device).float() / 255.0)
                labels, boxes, scores = self.post(raw_out, sizes)
            for label, box, score in zip(labels.cpu(), boxes.float().cpu(), scores.float().cpu()):
                keep = (score > conf).nonzero().squeeze(1)
                keep = keep[score[keep].argsort(descending=True, stable=True)][:MAX_DETECTIONS]
                answers.append((box[keep].numpy(), score[keep].numpy(), label[keep].numpy().astype(int)))
        return answers
'''


RELINK_OLD_SCAN = '''    wanted, small = {int(f) for f in np.r_[span["min"].to_numpy(), span["max"].to_numpy()]}, {}
    cap = cv2.VideoCapture(str(frames.video))
    for i in range(max(wanted, default=-1) + 1):
        if not cap.grab():
            break
        if i in wanted:
            try:
                ok, img = cap.retrieve()
            except cv2.error:
                ok = False
            if ok:
                small[i] = cv2.cvtColor(cv2.resize(img, mask.shape[::-1], interpolation=cv2.INTER_AREA), cv2.COLOR_BGR2GRAY)
    cap.release()'''

RELINK_FAST_SEEK = '''    wanted, small = {int(f) for f in np.r_[span["min"].to_numpy(), span["max"].to_numpy()]}, {}
    if len(span) > 1 and wanted:
        cap = cv2.VideoCapture(str(frames.video))
        missing = set()
        for f in sorted(wanted):
            ok, img = False, None
            try:
                if cap.set(cv2.CAP_PROP_POS_FRAMES, float(f)):
                    ok, img = cap.read()
            except cv2.error:
                ok = False
            if ok and img is not None:
                small[f] = cv2.cvtColor(cv2.resize(img, mask.shape[::-1], interpolation=cv2.INTER_AREA), cv2.COLOR_BGR2GRAY)
            else:
                missing.add(f)
        if missing:
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0.0)
            for i in range(max(missing) + 1):
                if not cap.grab():
                    break
                if i in missing:
                    try:
                        ok, img = cap.retrieve()
                    except cv2.error:
                        ok = False
                    if ok and img is not None:
                        small[i] = cv2.cvtColor(cv2.resize(img, mask.shape[::-1], interpolation=cv2.INTER_AREA), cv2.COLOR_BGR2GRAY)
        cap.release()'''


def patch_extracted_bundle(root_dir: Path) -> dict[str, object]:
    """Patch extracted raw PTZ bundle for S-v5-XL, TensorRT FP16, and no annotated_video."""
    bundle_dir = root_dir / "bundle"
    if not bundle_dir.is_dir():
        raise FileNotFoundError(f"Expected bundle/ inside {root_dir}")

    # 1. Patch pipeline.json: remove drawn/annotated_video node & configure S-v5-XL detect params
    pipeline_path = bundle_dir / "pipeline.json"
    pipeline_doc = json.loads(pipeline_path.read_text(encoding="utf-8"))
    orig_nodes = list(pipeline_doc.get("nodes", []))
    kept_nodes = []
    removed_nodes = []
    for node in orig_nodes:
        if node.get("id") == "drawn" or node.get("kind") == "annotated_video":
            removed_nodes.append(node.get("id"))
            continue
        if node.get("id") == "video" and node.get("kind") == "video":
            params = dict(node.get("params") or {})
            params["fps"] = TARGET_ANALYSED_FPS
            node = dict(node, params=params)
        if node.get("id") == "detect" and node.get("kind") == "detect":
            params = dict(node.get("params") or {})
            params["model"] = SV5XL_MODEL_ID
            params["model_name"] = SV5XL_MODEL_NAME
            params["min_confidence"] = SV5XL_CONFIDENCE
            params["class_confidence"] = {
                "excavator": SV5XL_CONFIDENCE,
                "dumptruck": SV5XL_CONFIDENCE,
            }
            params["class_map"] = {"negative_vehicle": None}
            node = dict(node, params=params)
        kept_nodes.append(node)
    pipeline_doc["nodes"] = kept_nodes
    pipeline_doc["name"] = "PTZ tracker + heuristic state (S-v5-XL, 3 FPS skip + GPU CLAHE)"
    pipeline_path.write_text(json.dumps(pipeline_doc, indent=2), encoding="utf-8")

    # 2. Patch detector-detect.json to use S-v5-XL & GPU-accelerated v3 Bandpass-Chroma CLAHE
    detector_path = bundle_dir / "detector-detect.json"
    det_doc = json.loads(detector_path.read_text(encoding="utf-8"))
    det_doc["name"] = SV5XL_MODEL_NAME
    det_doc["classes"] = ["excavator", "dumptruck", "negative_vehicle"]
    for step in det_doc.get("steps", []):
        step["model"] = "models/s_v5_xl_1920x1088_fp16_sm86.engine"
        step["model_name"] = SV5XL_MODEL_NAME
        preprocess = dict(step.get("preprocess") or {})
        code_blocks = list(preprocess.get("code") or [])
        if code_blocks:
            code_blocks[0] = {
                "name": "v3 Bandpass-Chroma CLAHE (GPU)",
                "source": GPU_V3_CLAHE_SOURCE,
            }
            preprocess["code"] = code_blocks
        step["preprocess"] = preprocess
        rules = dict(step.get("rules") or {})
        rules["min_confidence"] = SV5XL_CONFIDENCE
        rules["class_confidence"] = {
            "excavator": SV5XL_CONFIDENCE,
            "dumptruck": SV5XL_CONFIDENCE,
        }
        rules["classes"] = ["excavator", "dumptruck"]
        step["rules"] = rules
    detector_path.write_text(json.dumps(det_doc, indent=2), encoding="utf-8")

    # Remove old S-v3-dfinex-wide config inside models/
    models_dir = bundle_dir / "models"
    if models_dir.is_dir():
        for old_file in models_dir.glob("*S-v3*"):
            old_file.unlink()

    # 3. Patch run_pipeline.py: pass start=ctx.start into node_ctx + dynamic frame-skip + async frame_pass
    run_pipeline_path = bundle_dir / "run_pipeline.py"
    rp_text = run_pipeline_path.read_text(encoding="utf-8")
    old_ctx = (
        '    return SimpleNamespace(video=ctx.video, out=ctx.out, frames=frames, fps=frames.vfps / frames.every, video_fps=frames.vfps,\n'
        '                           every=frames.every, node=n["id"], path=path)'
    )
    new_ctx = (
        '    return SimpleNamespace(video=ctx.video, out=ctx.out, frames=frames, fps=frames.vfps / frames.every, video_fps=frames.vfps,\n'
        '                           every=frames.every, node=n["id"], path=path, start=ctx.start)'
    )
    if old_ctx in rp_text:
        rp_text = rp_text.replace(old_ctx, new_ctx, 1)

    v_marker = '@node("video")\ndef video(ctx, p):'
    fp_end_marker = "\n# ---- frame by frame ----"
    if v_marker in rp_text and fp_end_marker in rp_text:
        v_idx = rp_text.index(v_marker)
        e_idx = rp_text.index(fp_end_marker, v_idx)
        rp_text = rp_text[:v_idx] + VIDEO_AND_FRAME_PASS_UPGRADE + "\n\n" + rp_text[e_idx:]

    if RELINK_OLD_SCAN in rp_text:
        rp_text = rp_text.replace(RELINK_OLD_SCAN, RELINK_FAST_SEEK, 1)
    run_pipeline_path.write_text(rp_text, encoding="utf-8")

    # 4. Patch run_workflow.py: upgrade DFineModel for TensorRT FP16, ONNX, SafeTensors, FP16 AMP, 1920x1088 resize + SIMD BGR->RGB
    run_workflow_path = bundle_dir / "run_workflow.py"
    rw_text = run_workflow_path.read_text(encoding="utf-8")
    start_marker = "class DFineModel:"
    end_marker = "\nclass Workflow:"
    s_idx = rw_text.index(start_marker)
    e_idx = rw_text.index(end_marker, s_idx)
    rw_text = rw_text[:s_idx] + DFINE_MODEL_UPGRADE + rw_text[e_idx:]

    old_bgr = '        picture = np.ascontiguousarray(frame[:, :, ::-1])'
    new_bgr = '        import cv2\n        picture = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)'
    if old_bgr in rw_text:
        rw_text = rw_text.replace(old_bgr, new_bgr, 1)

    dispatch_re = re.compile(
        r'^(?P<indent>[ \t]*)models\[file\] = DFineModel\(file, device\) if file\.suffix == "\.pth" else Model\(file, device\)$',
        re.MULTILINE,
    )
    m_disp = dispatch_re.search(rw_text)
    if not m_disp:
        raise RuntimeError("Could not locate Workflow model dispatch line in run_workflow.py")
    ind = m_disp.group("indent")
    new_dispatch = (
        f'{ind}is_dfine = file.suffix in {{".pth", ".safetensors", ".engine", ".onnx"}} and (\n'
        f'{ind}    file.with_suffix(".dfine.json").exists() or any(file.parent.glob("*.dfine.json"))\n'
        f'{ind})\n'
        f'{ind}models[file] = DFineModel(file, device) if is_dfine else Model(file, device)'
    )
    rw_text = rw_text[: m_disp.start()] + new_dispatch + rw_text[m_disp.end() :]
    run_workflow_path.write_text(rw_text, encoding="utf-8")
    py_compile.compile(str(run_workflow_path), doraise=True)
    py_compile.compile(str(run_pipeline_path), doraise=True)

    # 5. Trim unused imageio-ffmpeg (only used by removed annotated_video node)
    req_path = bundle_dir / "requirements.txt"
    if req_path.exists():
        lines = [
            line for line in req_path.read_text(encoding="utf-8").splitlines()
            if not line.strip().startswith("imageio-ffmpeg")
        ]
        req_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "removed_nodes": removed_nodes,
        "remaining_node_count": len(kept_nodes),
        "detector_model_name": SV5XL_MODEL_NAME,
        "detector_model_path": det_doc["steps"][0]["model"],
    }


def build_and_upload_bundle(
    *,
    bucket: str = DEFAULT_BUCKET,
    raw_key: str = RAW_BUNDLE_KEY,
    out_key: str = PROD_BUNDLE_KEY,
    profile: str = DEFAULT_PROFILE,
    region: str = DEFAULT_REGION,
) -> dict[str, object]:
    cfg = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
    s3 = boto3.Session(profile_name=profile, region_name=region).client("s3", config=cfg)

    with tempfile.TemporaryDirectory(prefix="ovius_ptz_bundle_") as td:
        tmp = Path(td)
        raw_tgz = tmp / "raw.tar.gz"
        s3.download_file(bucket, raw_key, str(raw_tgz))
        extract_dir = tmp / "extracted"
        extract_dir.mkdir()
        with tarfile.open(raw_tgz, "r:gz") as tf:
            tf.extractall(extract_dir)

        patch_info = patch_extracted_bundle(extract_dir)

        out_tgz = tmp / "ptz_code_bundle.tar.gz"
        with tarfile.open(out_tgz, "w:gz") as tf:
            tf.add(extract_dir / "bundle", arcname="bundle")

        size_bytes = out_tgz.stat().st_size
        s3.upload_file(str(out_tgz), bucket, out_key)
        head = s3.head_object(Bucket=bucket, Key=out_key)

    return {
        "s3_uri": f"s3://{bucket}/{out_key}",
        "bytes": size_bytes,
        "etag": head["ETag"],
        **patch_info,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Patch and upload slim S-v5-XL PTZ code bundle to S3.")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--raw-key", default=RAW_BUNDLE_KEY)
    parser.add_argument("--out-key", default=PROD_BUNDLE_KEY)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--region", default=DEFAULT_REGION)
    args = parser.parse_args()

    res = build_and_upload_bundle(
        bucket=args.bucket,
        raw_key=args.raw_key,
        out_key=args.out_key,
        profile=args.profile,
        region=args.region,
    )
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
