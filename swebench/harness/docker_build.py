from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import subprocess
import traceback
from dataclasses import dataclass

import docker
import docker.errors
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

USE_HOST_NETWORK = False

from swebench.harness.constants import (
    BASE_IMAGE_BUILD_DIR,
    ENV_IMAGE_BUILD_DIR,
    INSTANCE_IMAGE_BUILD_DIR,
    MAP_REPO_VERSION_TO_SPECS,
    USE_X86,
)
from swebench.harness.dockerfiles import (
    get_dockerfile_base,
    get_dockerfile_env,
    get_dockerfile_instance,
)
from swebench.harness.test_spec import (
    get_test_specs_from_dataset,
    make_test_spec,
    TestSpec
)
from swebench.harness.docker_utils import (
    cleanup_container,
    remove_image,
    find_dependent_images
)

ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")


class BuildImageError(Exception):
    def __init__(self, image_name, message, logger):
        super().__init__(message)
        self.super_str = super().__str__()
        self.image_name = image_name
        self.log_path = logger.log_file
        self.logger = logger

    def __str__(self):
        return (
            f"Error building image {self.image_name}: {self.super_str}\n"
            f"Check ({self.log_path}) for more information."
        )


def setup_logger(instance_id: str, log_file: Path, mode="w"):
    """
    This logger is used for logging the build process of images and containers.
    It writes logs to the log file.
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"{instance_id}.{log_file.name}")
    handler = logging.FileHandler(log_file, mode=mode)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    setattr(logger, "log_file", log_file)
    return logger


def close_logger(logger):
    # To avoid too many open files
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


def build_image(
        image_name: str,
        setup_scripts: dict,
        dockerfile: str,
        platform: str,
        client: docker.DockerClient,
        build_dir: Path,
        nocache: bool = False
    ):
    """
    Builds a docker image with the given name, setup scripts, dockerfile, and platform.

    Args:
        image_name (str): Name of the image to build
        setup_scripts (dict): Dictionary of setup script names to setup script contents
        dockerfile (str): Contents of the Dockerfile
        platform (str): Platform to build the image for
        client (docker.DockerClient): Docker client to use for building the image
        build_dir (Path): Directory for the build context (will also contain logs, scripts, and artifacts)
        nocache (bool): Whether to use the cache when building
    """
    # Create a logger for the build process
    logger = setup_logger(image_name, build_dir / "build_image.log")
    logger.info(
        f"Building image {image_name}\n"
        f"Using dockerfile:\n{dockerfile}\n"
        f"Adding ({len(setup_scripts)}) setup scripts to image build repo"
    )

    for setup_script_name, setup_script in setup_scripts.items():
        logger.info(f"[SETUP SCRIPT] {setup_script_name}:\n{setup_script}")
    try:
        # Write the setup scripts to the build directory
        for setup_script_name, setup_script in setup_scripts.items():
            setup_script_path = build_dir / setup_script_name
            with open(setup_script_path, "w") as f:
                f.write(setup_script)
            if setup_script_name not in dockerfile:
                logger.warning(
                    f"Setup script {setup_script_name} may not be used in Dockerfile"
                )

        # Write the dockerfile to the build directory
        dockerfile_path = build_dir / "Dockerfile"
        with open(dockerfile_path, "w") as f:
            f.write(dockerfile)

        # Build the image
        logger.info(
            f"Building docker image {image_name} in {build_dir} with platform {platform}"
        )

        proxy_buildargs = {}
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
                    "http_proxy", "https_proxy", "no_proxy"):
            val = os.environ.get(var, "")
            if val:
                proxy_buildargs[var] = val

        response = client.api.build(
            path=str(build_dir),
            tag=image_name,
            rm=True,
            forcerm=True,
            decode=True,
            platform=platform,
            nocache=nocache,
            network_mode="host" if USE_HOST_NETWORK else None,
            container_limits={"memory": 8 * 1024 * 1024 * 1024},
            buildargs=proxy_buildargs if proxy_buildargs else None,
        )

        # Log the build process continuously
        buildlog = ""
        for chunk in response:
            if "stream" in chunk:
                # Remove ANSI escape sequences from the log
                chunk_stream = ansi_escape.sub("", chunk["stream"])
                logger.info(chunk_stream.strip())
                buildlog += chunk_stream
            elif "errorDetail" in chunk:
                # Decode error message, raise BuildError
                logger.error(
                    f"Error: {ansi_escape.sub('', chunk['errorDetail']['message'])}"
                )
                raise docker.errors.BuildError(
                    chunk["errorDetail"]["message"], buildlog
                )
        logger.info("Image built successfully!")
    except docker.errors.BuildError as e:
        logger.error(f"docker.errors.BuildError during {image_name}: {e}")
        raise BuildImageError(image_name, str(e), logger) from e
    except Exception as e:
        logger.error(f"Error building image {image_name}: {e}")
        raise BuildImageError(image_name, str(e), logger) from e
    finally:
        close_logger(logger)  # functions that create loggers should close them


_SKOPEO_MIN_VERSION = (1, 14, 2)


def _check_skopeo_available():
    if not shutil.which("skopeo"):
        raise RuntimeError(
            "skopeo not found. Install: brew install skopeo (macOS) / "
            "apt install skopeo (Debian)"
        )
    try:
        result = subprocess.run(
            ["skopeo", "--version"],
            capture_output=True, text=True, timeout=10,
        )
        version_str = result.stdout.strip().split()[-1]
        parts = version_str.split(".")
        import re as _re
        version = tuple(int(m.group()) for p in parts[:3] if (m := _re.match(r'\d+', p)))
        version = version + (0,) * (3 - len(version))
        if version < _SKOPEO_MIN_VERSION:
            min_str = ".".join(str(v) for v in _SKOPEO_MIN_VERSION)
            raise RuntimeError(
                f"skopeo {version_str} too old. Need >= {min_str} for "
                "Docker 25+ compatibility. See skopeo#2202."
            )
    except (ValueError, IndexError, AttributeError) as e:
        logging.getLogger("swebench").warning(
            f"Could not parse skopeo version (proceeding anyway): {e}"
        )


@dataclass
class RegistryAddrs:
    """Registry addresses for host-side and buildkit-side operations.

    On Linux native Docker, both are ``localhost:<port>``.
    On Docker Desktop (macOS/Windows), buildkitd runs in a VM where
    ``localhost`` points at the VM's own loopback — not the host.
    ``buildkit`` is set to the host-gateway IP so buildkitd's FROM
    image resolution can reach the host registry.
    """
    host: str
    buildkit: str


_BUILDKITD_TOML_DIR = Path.home() / ".swe-bench"


def _is_docker_desktop() -> bool:
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{.OperatingSystem}}"],
            capture_output=True, text=True, timeout=10,
        )
        return "Desktop" in (result.stdout or "")
    except Exception:
        return False


def _discover_host_gateway_ip() -> str | None:
    """Discover the host-gateway IP that buildkitd can use to reach the host.

    On Docker Desktop, ``host-gateway`` resolves to the VM-internal IP of the
    macOS/Windows host (e.g. 192.168.65.254).  This IP is reachable from
    containers using ``--network host`` (which gives the VM's namespace).
    """
    try:
        result = subprocess.run(
            [
                "docker", "run", "--rm",
                "--add-host=host.docker.internal:host-gateway",
                "alpine", "grep", "host.docker.internal", "/etc/hosts",
            ],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return None
        for line in result.stdout.strip().splitlines():
            parts = line.split()
            if len(parts) >= 2 and "host.docker.internal" in parts[1]:
                ip = parts[0]
                # Skip IPv6 entries
                if ":" not in ip:
                    return ip
        return None
    except Exception:
        return None


def _get_buildkit_registry_addr(port: int) -> str:
    if not _is_docker_desktop():
        return f"localhost:{port}"

    ip = _discover_host_gateway_ip()
    if ip:
        print(f"Docker Desktop detected — buildkitd will reach registry via {ip}:{port}")
        return f"{ip}:{port}"

    print(
        "WARNING: Docker Desktop detected but host-gateway IP discovery failed.\n"
        "Falling back to localhost:5000 — buildx FROM lines may time out.\n"
        "Workaround: pass --registry <reachable-host-ip>:5000"
    )
    return f"localhost:{port}"


def _ensure_buildkitd_toml(buildkit_addr: str) -> Path | None:
    """Generate a buildkitd.toml with HTTP access for the given registry address.

    BuildKit auto-allows HTTP for ``localhost`` / ``127.0.0.1``, but any other
    address (like a host-gateway IP) needs explicit ``http = true``.

    Returns the config file path, or None if no config is needed.
    """
    host_part = buildkit_addr.split(":")[0] if ":" in buildkit_addr else buildkit_addr
    if host_part in ("localhost", "127.0.0.1"):
        return None

    _BUILDKITD_TOML_DIR.mkdir(parents=True, exist_ok=True)
    toml_path = _BUILDKITD_TOML_DIR / "buildkitd.toml"

    content = (
        f'[registry."{buildkit_addr}"]\n'
        f'  http = true\n'
        f'  insecure = true\n'
    )

    if toml_path.exists() and toml_path.read_text() == content:
        return toml_path

    toml_path.write_text(content)
    return toml_path


def _make_registry_addrs(user_addr: str) -> RegistryAddrs:
    """Convert a user-provided registry address string to RegistryAddrs.

    If the address is localhost-based and we're on Docker Desktop,
    the buildkit address is swapped to the host-gateway IP so that
    buildkitd can reach the registry.
    """
    host_part = user_addr.split(":")[0] if ":" in user_addr else user_addr
    port_part = user_addr.split(":")[1] if ":" in user_addr else "5000"
    if host_part in ("localhost", "127.0.0.1"):
        buildkit_addr = _get_buildkit_registry_addr(int(port_part))
        return RegistryAddrs(host=user_addr, buildkit=buildkit_addr)
    return RegistryAddrs(host=user_addr, buildkit=user_addr)


def _check_buildx_builder(
    builder_name: str = "swegym-multiarch",
    buildkitd_toml: Path | None = None,
):
    result = subprocess.run(
        ["docker", "buildx", "inspect", "--bootstrap", builder_name],
        capture_output=True, text=True, timeout=60,
    )

    needs_create = result.returncode != 0
    needs_recreate = False

    if not needs_create:
        if "network=" in result.stdout and 'network="host"' not in result.stdout:
            print(
                f"WARNING: Builder '{builder_name}' exists but lacks --driver-opt network=host.\n"
                f"Recreating to fix registry access..."
            )
            needs_recreate = True

        if buildkitd_toml and "buildkitd.toml" not in result.stdout:
            print(
                f"Builder '{builder_name}' exists without buildkitd config.\n"
                f"Recreating with registry config for Docker Desktop compatibility..."
            )
            needs_recreate = True

    if needs_recreate:
        print(f"Removing existing builder '{builder_name}'...")
        subprocess.run(
            ["docker", "buildx", "rm", builder_name],
            capture_output=True, text=True, timeout=30,
        )
        needs_create = True

    if needs_create:
        print(f"Builder '{builder_name}' not found — creating with host networking...")
        cmd = [
            "docker", "buildx", "create",
            "--name", builder_name,
            "--driver", "docker-container",
            "--driver-opt", "network=host",
        ]
        if buildkitd_toml:
            cmd.extend(["--buildkitd-config", str(buildkitd_toml)])
        cmd.append("--bootstrap")

        create = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if create.returncode != 0:
            raise RuntimeError(
                f"Failed to create buildx builder '{builder_name}': {create.stderr}\n"
                f"Try manually: docker buildx create --name {builder_name} "
                f"--driver docker-container --driver-opt network=host --bootstrap"
            )
        result = subprocess.run(
            ["docker", "buildx", "inspect", builder_name],
            capture_output=True, text=True, timeout=30,
        )

    if "linux/amd64" not in result.stdout or "linux/arm64" not in result.stdout:
        raise RuntimeError(
            f"Builder '{builder_name}' missing platform support. "
            "Run: docker run --rm --privileged tonistiigi/binfmt --install all"
        )


def ensure_registry_running(
    port: int = 5000,
    timeout: int = 30,
) -> RegistryAddrs:
    """Start a local OCI registry if not already running.

    Returns a RegistryAddrs with host-side and buildkit-side addresses.
    """
    import urllib.request
    import time

    host_addr = f"localhost:{port}"
    health_url = f"http://{host_addr}/v2/"

    def _make_addrs() -> RegistryAddrs:
        buildkit_addr = _get_buildkit_registry_addr(port)
        return RegistryAddrs(host=host_addr, buildkit=buildkit_addr)

    try:
        urllib.request.urlopen(health_url, timeout=3)
        if _probe_registry_push(host_addr):
            return _make_addrs()
    except Exception:
        pass

    container_name = "swegym-registry"
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        capture_output=True, timeout=10,
    )
    result = subprocess.run(
        [
            "docker", "run", "-d",
            "-p", f"{port}:{port}",
            "--restart=always",
            "--name", container_name,
            "-e", "REGISTRY_STORAGE_DELETE_ENABLED=true",
            "registry:2",
        ],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to start registry on port {port}: {result.stderr}\n"
            f"Possible port conflict. Try: --registry localhost:5001"
        )

    delay = 0.5
    elapsed = 0.0
    while elapsed < timeout:
        try:
            urllib.request.urlopen(health_url, timeout=2)
            if _probe_registry_push(host_addr):
                return _make_addrs()
        except Exception:
            pass
        time.sleep(delay)
        elapsed += delay
        delay = min(delay * 1.5, 5.0)

    raise RuntimeError(f"Registry started but not healthy after {timeout}s")


def _probe_registry_push(registry_addr: str) -> bool:
    import json
    import tempfile
    import tarfile
    import gzip

    try:
        with tempfile.TemporaryDirectory(prefix="swegym-probe-") as tmpdir:
            probe_dir = Path(tmpdir)

            oci_layout = {"imageLayoutVersion": "1.0.0"}
            (probe_dir / "oci-layout").write_text(json.dumps(oci_layout))

            blobs_dir = probe_dir / "blobs" / "sha256"
            blobs_dir.mkdir(parents=True)

            empty_tar_content = b"\x00" * 1024
            config = json.dumps({
                "architecture": "amd64",
                "os": "linux",
                "rootfs": {"type": "layers", "diff_ids": ["sha256:" + hashlib.sha256(empty_tar_content).hexdigest()]},
            }).encode()
            config_digest = hashlib.sha256(config).hexdigest()
            (blobs_dir / config_digest).write_bytes(config)

            empty_layer = gzip.compress(empty_tar_content)
            layer_digest = hashlib.sha256(empty_layer).hexdigest()
            (blobs_dir / layer_digest).write_bytes(empty_layer)

            manifest = json.dumps({
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": f"sha256:{config_digest}",
                    "size": len(config),
                },
                "layers": [{
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": f"sha256:{layer_digest}",
                    "size": len(empty_layer),
                }],
            }).encode()
            manifest_digest = hashlib.sha256(manifest).hexdigest()
            (blobs_dir / manifest_digest).write_bytes(manifest)

            index = json.dumps({
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.index.v1+json",
                "manifests": [{
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": f"sha256:{manifest_digest}",
                    "size": len(manifest),
                    "platform": {"architecture": "amd64", "os": "linux"},
                }],
            }).encode()
            (probe_dir / "index.json").write_text(index.decode())

            probe_tar = probe_dir / "probe.tar"
            with tarfile.open(probe_tar, "w") as tf:
                for p in probe_dir.rglob("*"):
                    if p == probe_tar:
                        continue
                    tf.add(p, arcname=str(p.relative_to(probe_dir)))

            result = subprocess.run(
                [
                    "skopeo", "copy", "--dest-tls-verify=false",
                    f"oci-archive:{probe_tar}",
                    f"docker://{registry_addr}/swegym-probe:latest",
                ],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                probe_ref = f"docker://{registry_addr}/swegym-probe:latest"
                inspect_res = subprocess.run(
                    ["skopeo", "inspect", "--tls-verify=false", "--raw", probe_ref],
                    capture_output=True, timeout=10,
                )
                if inspect_res.returncode == 0:
                    probe_digest = "sha256:" + hashlib.sha256(inspect_res.stdout).hexdigest()
                    subprocess.run(
                        ["skopeo", "delete", "--tls-verify=false",
                         f"docker://{registry_addr}/swegym-probe@{probe_digest}"],
                        capture_output=True, timeout=10,
                    )
                else:
                    subprocess.run(
                        ["skopeo", "delete", "--tls-verify=false", probe_ref],
                        capture_output=True, timeout=10,
                    )
                return True
    except Exception:
        pass
    return False


def build_image_buildx(
    image_name: str,
    setup_scripts: dict[str, str],
    dockerfile: str,
    platforms: list[str],
    build_dir: Path,
    output_dir: Path | None = None,
    nocache: bool = False,
    builder_name: str = "swegym-multiarch",
    registry: str | None = None,
    cache_dir: Path | None = None,
    push_to_registry: bool = False,
    timeout: int = 7200,
) -> tuple[Path, str]:
    if registry is None:
        registry = "localhost:5000"
    if output_dir is None:
        output_dir = build_dir / "output"

    build_dir.mkdir(parents=True, exist_ok=True)
    (build_dir / "Dockerfile").write_text(dockerfile)
    for fname, content in setup_scripts.items():
        (build_dir / fname).write_text(content)

    tar_name = image_name.replace(":", "_").replace("/", "_") + ".tar"
    output_dir.mkdir(parents=True, exist_ok=True)
    tar_path = output_dir / tar_name

    log_dir = output_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / (
        image_name.replace(":", "_").replace("/", "_") + ".build.log"
    )

    _MAX_LOG_BYTES = 100 * 1024 * 1024
    if log_file.exists() and log_file.stat().st_size > _MAX_LOG_BYTES:
        with open(log_file, "rb") as f:
            f.seek(-(_MAX_LOG_BYTES // 2), 2)
            tail = f.read()
        with open(log_file, "wb") as f:
            f.write(b"[... truncated by log rotation ...]\n")
            f.write(tail)

    cmd = [
        "docker", "buildx", "build",
        "--builder", builder_name,
        "--platform", ",".join(platforms),
        "--output", f"type=oci,dest={tar_path}",
        "--progress", "plain",
        "--allow", "network.host",
        "--network", "host",
        "--provenance=false",
        "--sbom=false",
        "--file", str(build_dir / "Dockerfile"),
    ]

    if cache_dir:
        cache_key = image_name.split(":")[0].replace("/", "_").replace(".", "_")
        cache_path = cache_dir / cache_key
        cmd.extend(["--cache-to", f"type=local,dest={cache_path},mode=max"])
        cmd.extend(["--cache-from", f"type=local,src={cache_path}"])

    if nocache:
        cmd.append("--no-cache")

    for var in ("HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
                "http_proxy", "https_proxy", "no_proxy"):
        val = os.environ.get(var, "")
        if val:
            cmd.extend(["--build-arg", f"{var}={val}"])

    mitm_cert = os.environ.get("MITM_CERT_FILE", "")
    if mitm_cert and Path(mitm_cert).is_file():
        cmd.extend(["--secret", f"id=mitm_ca,src={mitm_cert}"])

    cmd.append(str(build_dir))

    logger = setup_logger(image_name, log_dir / "orchestration.log", mode="a")
    logger.info(f"Building multi-arch image: {image_name}")
    logger.info(f"  Platforms: {', '.join(platforms)}")
    logger.info(f"  Output: {tar_path}")
    logger.info(f"  Command: {' '.join(cmd)}")

    with open(log_file, "a") as log_fh:
        result = subprocess.run(
            cmd, stdout=log_fh, stderr=subprocess.STDOUT, timeout=timeout,
        )

    if result.returncode != 0:
        with open(log_file, "r") as f:
            lines = f.readlines()
            error_tail = "".join(lines[-50:])
        logger.error(f"Build failed for {image_name}. Log: {log_file}\n{error_tail}")
        close_logger(logger)
        raise BuildImageError(image_name, f"buildx failed. See {log_file}", logger)

    size_gb = tar_path.stat().st_size / 1e9
    logger.info(f"Built {image_name} -> {tar_path} ({size_gb:.2f} GB)")

    if not _validate_oci_tar(tar_path, platforms, logger):
        close_logger(logger)
        raise BuildImageError(image_name, f"OCI tar validation failed. See {log_file}", logger)

    digest = ""
    if push_to_registry:
        digest = _push_tar_to_registry(tar_path, image_name, registry, logger)

    close_logger(logger)
    return tar_path, digest


def _push_tar_to_registry(tar_path: Path, image_name: str, registry: str, logger) -> str:
    _check_skopeo_available()
    registry_ref = f"docker://{registry}/{image_name}"
    cmd = [
        "skopeo", "copy", "--all",
        "--dest-tls-verify=false",
        f"oci-archive:{tar_path}",
        registry_ref,
    ]
    logger.info(f"Pushing {tar_path} -> {registry_ref}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        logger.error(f"Push failed: {result.stderr}")
        raise RuntimeError(f"skopeo push failed for {image_name}: {result.stderr[-500:]}")

    inspect_result = subprocess.run(
        ["skopeo", "inspect", "--tls-verify=false", "--raw", registry_ref],
        capture_output=True, timeout=30,
    )
    pushed_digest = ""
    if inspect_result.returncode == 0:
        pushed_digest = "sha256:" + hashlib.sha256(inspect_result.stdout).hexdigest()

    logger.info(f"Pushed {image_name} to {registry} (digest: {pushed_digest or 'unknown'})")
    return pushed_digest


def _rollback_registry_image(image_name: str, registry: str, logger, digest: str = ""):
    if digest:
        registry_ref = f"docker://{registry}/{image_name.split(':')[0]}@{digest}"
    else:
        registry_ref = f"docker://{registry}/{image_name}"
        logger.warning(
            f"No digest available for rollback of {image_name}. "
            "Falling back to tag deletion — this may affect shared tags."
        )
    try:
        result = subprocess.run(
            ["skopeo", "delete", "--tls-verify=false", registry_ref],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            logger.warning(f"Rolled back stale image from registry: {image_name}")
        else:
            logger.warning(f"Rollback skipped (image may not exist): {result.stderr[:200]}")
    except Exception as e:
        logger.warning(f"Rollback failed for {image_name}: {e}")


def _validate_oci_tar(tar_path: Path, expected_platforms: list[str], logger) -> bool:
    import json

    if not tar_path.exists():
        logger.error(f"OCI tar not found: {tar_path}")
        return False

    size_bytes = tar_path.stat().st_size
    if size_bytes < 1024:
        logger.error(f"OCI tar suspiciously small ({size_bytes} bytes): {tar_path}")
        return False

    try:
        result = subprocess.run(
            ["skopeo", "inspect", "--raw", f"oci-archive:{tar_path}"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            logger.error(f"skopeo inspect failed on {tar_path}: {result.stderr[:200]}")
            return False

        manifest = json.loads(result.stdout)

        if manifest.get("mediaType", "").endswith("image.index") or "manifests" in manifest:
            found_platforms = set()
            for m in manifest.get("manifests", []):
                plat = m.get("platform", {})
                arch = plat.get("architecture", "")
                os_ = plat.get("os", "")
                if os_ == "unknown":
                    logger.debug(f"Skipping attestation manifest: {plat}")
                    continue
                if arch and os_:
                    found_platforms.add(f"{os_}/{arch}")

            for expected in expected_platforms:
                if expected not in found_platforms:
                    logger.error(
                        f"OCI tar missing platform {expected}. "
                        f"Found: {found_platforms}. Tar: {tar_path}"
                    )
                    return False

            logger.info(
                f"OCI tar validated: {tar_path} "
                f"({size_bytes / 1e9:.2f} GB, platforms: {found_platforms})"
            )
        else:
            config_digest = manifest.get("config", {}).get("digest", "")
            if config_digest and expected_platforms:
                config_result = subprocess.run(
                    ["skopeo", "inspect", f"oci-archive:{tar_path}"],
                    capture_output=True, text=True, timeout=30,
                )
                if config_result.returncode == 0:
                    config_info = json.loads(config_result.stdout)
                    tar_arch = config_info.get("Architecture", "")
                    tar_os = config_info.get("Os", "linux")
                    tar_platform = f"{tar_os}/{tar_arch}"
                    if expected_platforms and tar_platform not in expected_platforms:
                        logger.error(
                            f"OCI tar platform mismatch: {tar_path} has {tar_platform}, "
                            f"expected one of {expected_platforms}"
                        )
                        return False
            logger.info(
                f"OCI tar validated (single-arch): {tar_path} ({size_bytes / 1e9:.2f} GB)"
            )

        return True
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Failed to parse OCI manifest in {tar_path}: {e}")
        return False


def build_base_images(
        client: docker.DockerClient,
        dataset: list,
        force_rebuild: bool = False,
        use_buildx: bool = False,
        platforms: list[str] | None = None,
        output_dir: Path | None = None,
        registry: str | RegistryAddrs | None = None,
        cache_dir: Path | None = None,
        skip_registry: bool = False,
        multiarch: bool = False,
    ):
    """
    Builds the base images required for the dataset if they do not already exist.

    Args:
        client (docker.DockerClient): Docker client to use for building the images
        dataset (list): List of test specs or dataset to build images for
        force_rebuild (bool): Whether to force rebuild the images even if they already exist
    """
    if use_buildx:
        if isinstance(registry, RegistryAddrs):
            registry_addrs = registry
        elif not skip_registry:
            registry_addrs = ensure_registry_running()
        else:
            registry_addrs = _make_registry_addrs(registry or "localhost:5000")

        toml = _ensure_buildkitd_toml(registry_addrs.buildkit)
        _check_buildx_builder(buildkitd_toml=toml)
        _check_skopeo_available()
        platforms = platforms or ["linux/amd64", "linux/arm64"]

        test_specs = get_test_specs_from_dataset(dataset, multiarch=True)
        base_images = {x.base_image_key: (x.base_dockerfile, x.platform) for x in test_specs}
        for image_name, (dockerfile, platform) in base_images.items():
            tar_path, digest = build_image_buildx(
                image_name=image_name,
                setup_scripts={},
                dockerfile=get_dockerfile_base("", "", use_buildx=True),
                platforms=platforms,
                build_dir=BASE_IMAGE_BUILD_DIR / image_name.replace(":", "__"),
                output_dir=output_dir,
                push_to_registry=not skip_registry,
                registry=registry_addrs.host,
                cache_dir=cache_dir,
            )
        print("Base images built successfully (buildx).")
        return

    # Get the base images to build from the dataset
    test_specs = get_test_specs_from_dataset(dataset, multiarch=multiarch)
    base_images = {
        x.base_image_key: (x.base_dockerfile, x.platform) for x in test_specs
    }
    if force_rebuild:
        for key in base_images:
            remove_image(client, key, "quiet")

    # Build the base images
    for image_name, (dockerfile, platform) in base_images.items():
        try:
            # Check if the base image already exists
            client.images.get(image_name)
            if force_rebuild:
                # Remove the base image if it exists and force rebuild is enabled
                remove_image(client, image_name, "quiet")
            else:
                print(f"Base image {image_name} already exists, skipping build.")
                continue
        except docker.errors.ImageNotFound:
            pass
        # Build the base image (if it does not exist or force rebuild is enabled)
        print(f"Building base image ({image_name})")
        build_image(
            image_name=image_name,
            setup_scripts={},
            dockerfile=dockerfile,
            platform=platform,
            client=client,
            build_dir=BASE_IMAGE_BUILD_DIR / image_name.replace(":", "__"),
        )
    print("Base images built successfully.")


def get_env_configs_to_build(
        client: docker.DockerClient,
        dataset: list,
        multiarch: bool = False,
    ):
    """
    Returns a dictionary of image names to build scripts and dockerfiles for environment images.
    Returns only the environment images that need to be built.

    Args:
        client (docker.DockerClient): Docker client to use for building the images
        dataset (list): List of test specs or dataset to build images for
    """
    image_scripts = dict()
    base_images = dict()
    test_specs = get_test_specs_from_dataset(dataset, multiarch=multiarch)

    for test_spec in test_specs:
        # Check if the base image exists
        try:
            if test_spec.base_image_key not in base_images:
                base_images[test_spec.base_image_key] = client.images.get(
                    test_spec.base_image_key
                )
            base_image = base_images[test_spec.base_image_key]
        except docker.errors.ImageNotFound:
            raise Exception(
                f"Base image {test_spec.base_image_key} not found for {test_spec.env_image_key}\n."
                "Please build the base images first."
            )

        # Check if the environment image exists
        image_exists = False
        try:
            env_image = client.images.get(test_spec.env_image_key)
            image_exists = True

            if env_image.attrs["Created"] < base_image.attrs["Created"]:
                # Remove the environment image if it was built after the base_image
                for dep in find_dependent_images(client, test_spec.env_image_key):
                    # Remove instance images that depend on this environment image
                    remove_image(client, dep, "quiet")
                remove_image(client, test_spec.env_image_key, "quiet")
                image_exists = False
        except docker.errors.ImageNotFound:
            pass
        if not image_exists:
            # Add the environment image to the list of images to build
            image_scripts[test_spec.env_image_key] = {
                "setup_script": test_spec.setup_env_script,
                "dockerfile": test_spec.env_dockerfile,
                "platform": test_spec.platform,
            }
    return image_scripts


def build_env_images(
        client: docker.DockerClient,
        dataset: list,
        force_rebuild: bool = False,
        max_workers: int = 4,
        use_buildx: bool = False,
        platforms: list[str] | None = None,
        output_dir: Path | None = None,
        registry: str | RegistryAddrs | None = None,
        cache_dir: Path | None = None,
        skip_registry: bool = False,
        multiarch: bool = False,
    ):
    """
    Builds the environment images required for the dataset if they do not already exist.

    Args:
        client (docker.DockerClient): Docker client to use for building the images
        dataset (list): List of test specs or dataset to build images for
        force_rebuild (bool): Whether to force rebuild the images even if they already exist
        max_workers (int): Maximum number of workers to use for building images
    """
    if use_buildx:
        if isinstance(registry, RegistryAddrs):
            registry_addrs = registry
        elif not skip_registry:
            registry_addrs = ensure_registry_running()
        else:
            registry_addrs = _make_registry_addrs(registry or "localhost:5000")

        _check_skopeo_available()
        platforms = platforms or ["linux/amd64", "linux/arm64"]

        build_base_images(
            client, dataset, force_rebuild,
            use_buildx=True, platforms=platforms,
            output_dir=output_dir, registry=registry_addrs,
            cache_dir=cache_dir, skip_registry=skip_registry,
        )

        test_specs = get_test_specs_from_dataset(dataset, multiarch=True)
        env_configs = {}
        for x in test_specs:
            base_ref = f"{registry_addrs.buildkit}/{x.base_image_key}"
            env_configs[x.env_image_key] = {
                "setup_script": x.setup_env_script,
                "dockerfile": get_dockerfile_env(
                    use_buildx=True, base_image_ref=base_ref,
                ),
                "platform": x.platform,
            }

        pushed_digests: dict[str, str] = {}
        for image_name, config in env_configs.items():
            try:
                tar_path, digest = build_image_buildx(
                    image_name=image_name,
                    setup_scripts={"setup_env.sh": config["setup_script"]},
                    dockerfile=config["dockerfile"],
                    platforms=platforms,
                    build_dir=ENV_IMAGE_BUILD_DIR / image_name.replace(":", "__"),
                    output_dir=output_dir,
                    push_to_registry=not skip_registry,
                    registry=registry_addrs.host,
                    cache_dir=cache_dir,
                )
                pushed_digests[image_name] = digest
            except Exception:
                for prev_name, prev_digest in pushed_digests.items():
                    log_path = (output_dir or ENV_IMAGE_BUILD_DIR) / "logs" / "rollback.log"
                    logger = setup_logger(prev_name, log_path, mode="a")
                    _rollback_registry_image(prev_name, registry_addrs.host, logger, prev_digest)
                    close_logger(logger)
                raise

        print("Environment images built successfully (buildx).")
        return list(pushed_digests.keys()), []

    # Get the environment images to build from the dataset
    if force_rebuild:
        env_image_keys = {x.env_image_key for x in get_test_specs_from_dataset(dataset, multiarch=multiarch)}
        for key in env_image_keys:
            remove_image(client, key, "quiet")
    build_base_images(client, dataset, force_rebuild, multiarch=multiarch)
    configs_to_build = get_env_configs_to_build(client, dataset, multiarch=multiarch)
    if len(configs_to_build) == 0:
        print("No environment images need to be built.")
        return [], []
    print(f"Total environment images to build: {len(configs_to_build)}")

    # Build the environment images
    successful, failed = list(), list()
    with tqdm(
        total=len(configs_to_build), smoothing=0, desc="Building environment images"
    ) as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create a future for each image to build
            futures = {
                executor.submit(
                    build_image,
                    image_name,
                    {"setup_env.sh": config["setup_script"]},
                    config["dockerfile"],
                    config["platform"],
                    client,
                    ENV_IMAGE_BUILD_DIR / image_name.replace(":", "__"),
                ): image_name
                for image_name, config in configs_to_build.items()
            }

            # Wait for each future to complete
            for future in as_completed(futures):
                pbar.update(1)
                try:
                    # Update progress bar, check if image built successfully
                    future.result()
                    successful.append(futures[future])
                except BuildImageError as e:
                    print(f"BuildImageError {e.image_name}")
                    traceback.print_exc()
                    failed.append(futures[future])
                    continue
                except Exception:
                    print("Error building image")
                    traceback.print_exc()
                    failed.append(futures[future])
                    continue

    # Show how many images failed to build
    if len(failed) == 0:
        print("All environment images built successfully.")
    else:
        print(f"{len(failed)} environment images failed to build.")

    # Return the list of (un)successfuly built images
    return successful, failed


def build_instance_images(
        client: docker.DockerClient,
        dataset: list,
        force_rebuild: bool = False,
        max_workers: int = 4,
        use_buildx: bool = False,
        platforms: list[str] | None = None,
        output_dir: Path | None = None,
        registry: str | RegistryAddrs | None = None,
        cache_dir: Path | None = None,
        skip_registry: bool = False,
        multiarch: bool = False,
    ):
    """
    Builds the instance images required for the dataset if they do not already exist.

    Args:
        dataset (list): List of test specs or dataset to build images for
        client (docker.DockerClient): Docker client to use for building the images
        force_rebuild (bool): Whether to force rebuild the images even if they already exist
        max_workers (int): Maximum number of workers to use for building images
    """
    if use_buildx:
        if isinstance(registry, RegistryAddrs):
            registry_addrs = registry
        elif not skip_registry:
            registry_addrs = ensure_registry_running()
        else:
            registry_addrs = _make_registry_addrs(registry or "localhost:5000")

        _check_skopeo_available()
        platforms = platforms or ["linux/amd64", "linux/arm64"]

        build_env_images(
            client, dataset, force_rebuild, max_workers,
            use_buildx=True, platforms=platforms,
            output_dir=output_dir, registry=registry_addrs,
            cache_dir=cache_dir, skip_registry=skip_registry,
        )

        test_specs = [make_test_spec(inst, multiarch=True) for inst in dataset]
        successful, failed = [], []
        for spec in test_specs:
            instance_platforms = ["linux/amd64"] if spec.instance_id in USE_X86 else platforms
            env_ref = f"{registry_addrs.buildkit}/{spec.env_image_key}"
            instance_dockerfile = get_dockerfile_instance(
                use_buildx=True, env_image_ref=env_ref,
            )
            try:
                tar_path, digest = build_image_buildx(
                    image_name=spec.instance_image_key,
                    setup_scripts={"setup_repo.sh": spec.install_repo_script},
                    dockerfile=instance_dockerfile,
                    platforms=instance_platforms,
                    build_dir=INSTANCE_IMAGE_BUILD_DIR / spec.instance_image_key.replace(":", "__"),
                    output_dir=output_dir,
                    push_to_registry=False,
                    registry=registry_addrs.host,
                    cache_dir=cache_dir,
                )
                successful.append(spec)
            except Exception:
                traceback.print_exc()
                failed.append(spec)

        if len(failed) == 0:
            print("All instance images built successfully (buildx).")
        else:
            print(f"{len(failed)} instance images failed to build (buildx).")
        return successful, failed

    # Build environment images (and base images as needed) first
    test_specs = [make_test_spec(inst, multiarch=multiarch) for inst in dataset]
    if force_rebuild:
        for spec in test_specs:
            remove_image(client, spec.instance_image_key, "quiet")
    _, env_failed = build_env_images(client, test_specs, force_rebuild, max_workers, multiarch=multiarch)

    if len(env_failed) > 0:
        # Don't build images for instances that depend on failed-to-build env images
        dont_run_specs = [spec for spec in test_specs if spec.env_image_key in env_failed]
        test_specs = [spec for spec in test_specs if spec.env_image_key not in env_failed]
        print(f"Skipping {len(dont_run_specs)} instances - due to failed env image builds")
    print(f"Building instance images for {len(test_specs)} instances")
    successful, failed = list(), list()

    # Build the instance images
    with tqdm(
        total=len(test_specs), smoothing=0, desc="Building instance images"
    ) as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create a future for each image to build
            futures = {
                executor.submit(
                    build_instance_image,
                    test_spec,
                    client,
                    None,  # logger is created in build_instance_image, don't make loggers before you need them
                    False,
                ): test_spec
                for test_spec in test_specs
            }

            # Wait for each future to complete
            for future in as_completed(futures):
                pbar.update(1)
                try:
                    # Update progress bar, check if image built successfully
                    future.result()
                    successful.append(futures[future])
                except BuildImageError as e:
                    print(f"BuildImageError {e.image_name}")
                    traceback.print_exc()
                    failed.append(futures[future])
                    continue
                except Exception:
                    print("Error building image")
                    traceback.print_exc()
                    failed.append(futures[future])
                    continue

    # Show how many images failed to build
    if len(failed) == 0:
        print("All instance images built successfully.")
    else:
        print(f"{len(failed)} instance images failed to build.")

    # Return the list of (un)successfuly built images
    return successful, failed


def build_instance_image(
        test_spec: TestSpec,
        client: docker.DockerClient,
        logger: logging.Logger|None,
        nocache: bool,
    ):
    """
    Builds the instance image for the given test spec if it does not already exist.

    Args:
        test_spec (TestSpec): Test spec to build the instance image for
        client (docker.DockerClient): Docker client to use for building the image
        logger (logging.Logger): Logger to use for logging the build process
        nocache (bool): Whether to use the cache when building
    """
    # Set up logging for the build process
    build_dir = INSTANCE_IMAGE_BUILD_DIR / test_spec.instance_image_key.replace(":", "__")
    new_logger = False
    if logger is None:
        new_logger = True
        logger = setup_logger(test_spec.instance_id, build_dir / "prepare_image.log")

    # Get the image names and dockerfile for the instance image
    image_name = test_spec.instance_image_key
    env_image_name = test_spec.env_image_key
    dockerfile = test_spec.instance_dockerfile

    # Check that the env. image the instance image is based on exists
    try:
        env_image = client.images.get(env_image_name)
    except docker.errors.ImageNotFound as e:
        raise BuildImageError(
            test_spec.instance_id,
            f"Environment image {env_image_name} not found for {test_spec.instance_id}",
            logger,
        ) from e
    logger.info(
        f"Environment image {env_image_name} found for {test_spec.instance_id}\n"
        f"Building instance image {image_name} for {test_spec.instance_id}"
    )

    # Check if the instance image already exists
    image_exists = False
    try:
        instance_image = client.images.get(image_name)
        if instance_image.attrs["Created"] < env_image.attrs["Created"]:
            # the environment image is newer than the instance image, meaning the instance image may be outdated
            remove_image(client, image_name, "quiet")
            image_exists = False
        else:
            image_exists = True
    except docker.errors.ImageNotFound:
        pass

    # Build the instance image
    if not image_exists:
        build_image(
            image_name=image_name,
            setup_scripts={
                "setup_repo.sh": test_spec.install_repo_script,
            },
            dockerfile=dockerfile,
            platform=test_spec.platform,
            client=client,
            build_dir=build_dir,
            nocache=nocache,
        )
    else:
        logger.info(f"Image {image_name} already exists, skipping build.")

    if new_logger:
        close_logger(logger)


def build_container(
        test_spec: TestSpec,
        client: docker.DockerClient,
        run_id: str,
        logger: logging.Logger,
        nocache: bool,
        force_rebuild: bool = False
    ):
    """
    Builds the instance image for the given test spec and creates a container from the image.

    Args:
        test_spec (TestSpec): Test spec to build the instance image and container for
        client (docker.DockerClient): Docker client for building image + creating the container
        run_id (str): Run ID identifying process, used for the container name
        logger (logging.Logger): Logger to use for logging the build process
        nocache (bool): Whether to use the cache when building
        force_rebuild (bool): Whether to force rebuild the image even if it already exists
    """
    # Build corresponding instance image
    if force_rebuild:
        remove_image(client, test_spec.instance_image_key, "quiet")
    build_instance_image(test_spec, client, logger, nocache)

    container = None
    try:
        # Get configurations for how container should be created
        config = MAP_REPO_VERSION_TO_SPECS[test_spec.repo][test_spec.version]
        user = "root" if not config.get("execute_test_as_nonroot", False) else "nonroot"
        nano_cpus = config.get("nano_cpus")

        # Create the container
        logger.info(f"Creating container for {test_spec.instance_id}...")
        container = client.containers.create(
            image=test_spec.instance_image_key,
            name=test_spec.get_instance_container_name(run_id),
            user=user,
            detach=True,
            command="tail -f /dev/null",
            nano_cpus=nano_cpus,
            platform=test_spec.platform,
            network_mode="host" if USE_HOST_NETWORK else None,
            mem_limit="16g",
            oom_kill_disable=False,
            oom_score_adj=1000,
        )
        logger.info(f"Container for {test_spec.instance_id} created: {container.id}")
        return container
    except Exception as e:
        # If an error occurs, clean up the container and raise an exception
        logger.error(f"Error creating container for {test_spec.instance_id}: {e}")
        logger.info(traceback.format_exc())
        cleanup_container(client, container, logger)
        raise BuildImageError(test_spec.instance_id, str(e), logger) from e
