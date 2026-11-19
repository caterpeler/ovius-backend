"""Publish the Ovius vision worker container image directly to AWS ECR without local Docker."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import ssl
import sys
import tarfile
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
SRC_PKG = ROOT / "src" / "ovius_api"

DEFAULT_PROFILE = "610331148578_Project8Learner"
DEFAULT_REGION = "ap-southeast-1"
DEFAULT_REPO = "ovius-vision-worker"
DEFAULT_TAG = "latest"
PUBLIC_BASE_REPO = "lambda/python"
PUBLIC_BASE_TAG = "3.11"


def _ssl_context() -> ssl.SSLContext:
    mac_ca = Path("/etc/ssl/cert.pem")
    if mac_ca.exists():
        return ssl.create_default_context(cafile=str(mac_ca))
    return ssl.create_default_context()


_SSL_CONTEXT = _ssl_context()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> Any:
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(
    urllib.request.HTTPSHandler(context=_SSL_CONTEXT),
    _NoRedirectHandler(),
)


def _http_call(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
    follow_redirects: bool = True,
) -> tuple[int, dict[str, str], bytes]:
    cur_url = url
    cur_headers = dict(headers or {})
    origin_host = urllib.parse.urlparse(url).netloc

    for _ in range(8):
        req = urllib.request.Request(cur_url, data=data, method=method)
        for k, v in cur_headers.items():
            req.add_header(k, v)
        try:
            with _NO_REDIRECT_OPENER.open(req, timeout=180) as resp:
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                body = b"" if method == "HEAD" else resp.read()
                return resp.status, resp_headers, body
        except urllib.error.HTTPError as exc:
            resp_headers = {k.lower(): v for k, v in exc.headers.items()}
            if follow_redirects and exc.code in {301, 302, 303, 307, 308} and "location" in resp_headers:
                next_url = urllib.parse.urljoin(cur_url, resp_headers["location"])
                if urllib.parse.urlparse(next_url).netloc != origin_host:
                    cur_headers.pop("Authorization", None)
                cur_url = next_url
                continue
            body = exc.read()
            return exc.code, resp_headers, body

    raise RuntimeError(f"Exceeded redirect limit for {url}")


def _build_worker_layer(src_pkg: Path) -> tuple[bytes, str, str]:
    """Build deterministic tar and tar.gz bytes containing /var/task/ovius_api."""
    tar_buf = io.BytesIO()
    py_files = sorted(
        p for p in src_pkg.rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and not p.name.endswith(".pyc")
    )
    if not py_files:
        raise FileNotFoundError(f"No source files found in {src_pkg}")

    with tarfile.open(fileobj=tar_buf, mode="w", format=tarfile.USTAR_FORMAT) as tf:
        for dir_path in ("var", "var/task", "var/task/ovius_api"):
            dinfo = tarfile.TarInfo(dir_path)
            dinfo.type = tarfile.DIRTYPE
            dinfo.mode = 0o755
            dinfo.mtime = 1700000000
            tf.addfile(dinfo)

        for file_path in py_files:
            rel = file_path.relative_to(src_pkg).as_posix()
            raw = file_path.read_bytes()
            tinfo = tarfile.TarInfo(f"var/task/ovius_api/{rel}")
            tinfo.size = len(raw)
            tinfo.mode = 0o644
            tinfo.mtime = 1700000000
            tf.addfile(tinfo, io.BytesIO(raw))

    tar_bytes = tar_buf.getvalue()
    diff_id = "sha256:" + hashlib.sha256(tar_bytes).hexdigest()

    gz_buf = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buf, mode="wb", mtime=0) as gf:
        gf.write(tar_bytes)
    gz_bytes = gz_buf.getvalue()
    compressed_digest = "sha256:" + hashlib.sha256(gz_bytes).hexdigest()
    return gz_bytes, compressed_digest, diff_id


def _get_public_ecr_token() -> str:
    status, _, body = _http_call("https://public.ecr.aws/token/")
    if status != 200:
        raise RuntimeError(f"Failed to fetch public.ecr.aws token: {status} {body!r}")
    return str(json.loads(body.decode("utf-8"))["token"])


def _fetch_public_amd64_manifest(pub_token: str) -> tuple[dict[str, Any], str]:
    accept = ", ".join(
        [
            "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.v2+json",
            "application/vnd.oci.image.manifest.v1+json",
        ]
    )
    headers = {"Authorization": f"Bearer {pub_token}", "Accept": accept}
    url = f"https://public.ecr.aws/v2/{PUBLIC_BASE_REPO}/manifests/{PUBLIC_BASE_TAG}"
    status, resp_headers, body = _http_call(url, headers=headers)
    if status != 200:
        raise RuntimeError(f"Failed to fetch public manifest {url}: {status} {body!r}")

    doc = json.loads(body.decode("utf-8"))
    if "manifests" in doc:
        selected = next(
            (
                m
                for m in doc["manifests"]
                if m.get("platform", {}).get("architecture") == "amd64"
                and m.get("platform", {}).get("os") == "linux"
            ),
            None,
        )
        if selected is None:
            raise RuntimeError("No linux/amd64 manifest found in public.ecr.aws/lambda/python:3.11")
        sub_url = f"https://public.ecr.aws/v2/{PUBLIC_BASE_REPO}/manifests/{selected['digest']}"
        status, resp_headers, body = _http_call(sub_url, headers=headers)
        if status != 200:
            raise RuntimeError(f"Failed to fetch amd64 manifest {sub_url}: {status} {body!r}")
        doc = json.loads(body.decode("utf-8"))

    media_type = doc.get("mediaType") or resp_headers.get(
        "content-type", "application/vnd.docker.distribution.manifest.v2+json"
    )
    return doc, str(media_type).split(";")[0].strip()


def _upload_blob_to_ecr(
    *,
    registry: str,
    repo: str,
    auth_header: str,
    digest: str,
    blob_bytes: bytes,
) -> bool:
    head_url = f"https://{registry}/v2/{repo}/blobs/{digest}"
    status, _, _ = _http_call(
        head_url,
        method="HEAD",
        headers={"Authorization": auth_header},
        follow_redirects=False,
    )
    if status == 200:
        return False

    init_url = f"https://{registry}/v2/{repo}/blobs/uploads/"
    status, resp_headers, body = _http_call(
        init_url,
        method="POST",
        headers={"Authorization": auth_header},
        data=b"",
        follow_redirects=False,
    )
    if status not in {201, 202}:
        raise RuntimeError(f"ECR upload init failed: {status} {body!r}")

    location = resp_headers["location"]
    upload_url = urllib.parse.urljoin(f"https://{registry}", location)
    sep = "&" if "?" in upload_url else "?"
    put_url = f"{upload_url}{sep}digest={urllib.parse.quote(digest)}"
    status, _, body = _http_call(
        put_url,
        method="PUT",
        headers={
            "Authorization": auth_header,
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(blob_bytes)),
        },
        data=blob_bytes,
        follow_redirects=False,
    )
    if status not in {201, 202}:
        raise RuntimeError(f"ECR blob PUT failed for {digest}: {status} {body!r}")
    return True


def publish_image(
    *,
    profile: str,
    region: str,
    repo: str,
    tag: str,
) -> dict[str, Any]:
    cfg = Config(retries={"total_max_attempts": 10, "mode": "adaptive"})
    session = boto3.Session(profile_name=profile, region_name=region)
    ecr = session.client("ecr", config=cfg)

    auth_data = ecr.get_authorization_token()["authorizationData"][0]
    ecr_token = str(auth_data["authorizationToken"])
    registry = str(auth_data["proxyEndpoint"]).replace("https://", "").rstrip("/")
    ecr_auth = f"Basic {ecr_token}"

    pub_token = _get_public_ecr_token()
    manifest, manifest_media_type = _fetch_public_amd64_manifest(pub_token)

    cfg_digest = str(manifest["config"]["digest"])
    cfg_url = f"https://public.ecr.aws/v2/{PUBLIC_BASE_REPO}/blobs/{cfg_digest}"
    st, _, cfg_bytes = _http_call(cfg_url, headers={"Authorization": f"Bearer {pub_token}"})
    if st != 200:
        raise RuntimeError(f"Failed to download base config blob {cfg_digest}: {st}")
    config_obj = json.loads(cfg_bytes.decode("utf-8"))

    # Copy base layers into private ECR if not already present
    copied_layers = 0
    for layer in manifest["layers"]:
        layer_digest = str(layer["digest"])
        head_status, _, _ = _http_call(
            f"https://{registry}/v2/{repo}/blobs/{layer_digest}",
            method="HEAD",
            headers={"Authorization": ecr_auth},
            follow_redirects=False,
        )
        if head_status == 200:
            continue
        blob_url = f"https://public.ecr.aws/v2/{PUBLIC_BASE_REPO}/blobs/{layer_digest}"
        b_st, _, b_bytes = _http_call(blob_url, headers={"Authorization": f"Bearer {pub_token}"})
        if b_st != 200:
            raise RuntimeError(f"Failed to fetch base layer {layer_digest}: {b_st}")
        _upload_blob_to_ecr(
            registry=registry,
            repo=repo,
            auth_header=ecr_auth,
            digest=layer_digest,
            blob_bytes=b_bytes,
        )
        copied_layers += 1

    # Build and upload ovius_api application layer
    gz_bytes, layer_digest, diff_id = _build_worker_layer(SRC_PKG)
    _upload_blob_to_ecr(
        registry=registry,
        repo=repo,
        auth_header=ecr_auth,
        digest=layer_digest,
        blob_bytes=gz_bytes,
    )

    layer_media_type = str(manifest["layers"][-1]["mediaType"])
    manifest["layers"].append(
        {"mediaType": layer_media_type, "size": len(gz_bytes), "digest": layer_digest}
    )

    config_obj["architecture"] = "amd64"
    config_obj["os"] = "linux"
    config_obj.setdefault("rootfs", {"type": "layers", "diff_ids": []})
    config_obj["rootfs"]["diff_ids"].append(diff_id)
    cfg_section = config_obj.setdefault("config", {})
    env_list = [e for e in cfg_section.get("Env", []) if not str(e).startswith("PYTHONPATH=")]
    env_list.append("PYTHONPATH=/var/task")
    cfg_section["Env"] = env_list
    cfg_section["WorkingDir"] = "/var/task"
    cfg_section["Entrypoint"] = ["/var/lang/bin/python3", "-m", "ovius_api.worker"]
    cfg_section["Cmd"] = []
    config_obj.setdefault("history", []).append(
        {"created_by": "ovius_backend/scripts/publish_worker_image.py"}
    )

    new_cfg_bytes = json.dumps(config_obj, separators=(",", ":")).encode("utf-8")
    new_cfg_digest = "sha256:" + hashlib.sha256(new_cfg_bytes).hexdigest()
    _upload_blob_to_ecr(
        registry=registry,
        repo=repo,
        auth_header=ecr_auth,
        digest=new_cfg_digest,
        blob_bytes=new_cfg_bytes,
    )
    manifest["config"]["digest"] = new_cfg_digest
    manifest["config"]["size"] = len(new_cfg_bytes)

    manifest_bytes = json.dumps(manifest, separators=(",", ":")).encode("utf-8")
    manifest_url = f"https://{registry}/v2/{repo}/manifests/{tag}"
    m_st, _, m_body = _http_call(
        manifest_url,
        method="PUT",
        headers={
            "Authorization": ecr_auth,
            "Content-Type": manifest_media_type,
            "Content-Length": str(len(manifest_bytes)),
        },
        data=manifest_bytes,
        follow_redirects=False,
    )
    if m_st not in {200, 201}:
        raise RuntimeError(f"ECR manifest PUT failed: {m_st} {m_body!r}")

    desc = ecr.describe_images(repositoryName=repo, imageIds=[{"imageTag": tag}])["imageDetails"][0]
    return {
        "image_uri": f"{registry}/{repo}:{tag}",
        "image_digest": desc["imageDigest"],
        "image_size_bytes": desc["imageSizeInBytes"],
        "copied_base_layers": copied_layers,
        "app_layer_bytes": len(gz_bytes),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish ovius-vision-worker image to ECR.")
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--tag", default=DEFAULT_TAG)
    args = parser.parse_args()

    result = publish_image(
        profile=args.profile,
        region=args.region,
        repo=args.repo,
        tag=args.tag,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
