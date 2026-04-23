"""Utility functions for image naming and cache key computation.

Isolated from docker_build.py and test_spec.py to avoid circular imports.
Imported by: test_spec.py, docker_build.py, dockerfiles.py.
"""
from __future__ import annotations

import hashlib
import os


def _proxy_hash_suffix() -> str:
    """Return a short hash suffix if proxy is active, empty string otherwise.

    Hash includes BOTH the proxy URL AND the CA cert fingerprint.
    If the cert rotates (same proxy URL, new cert), the hash changes — no stale cache.

    Uses MITM_CERT_FILE env var (NOT SSL_CERT_FILE) to hash the specific MITM cert.
    Falls back to hashing the proxy URL only if MITM_CERT_FILE is unset.
    """
    proxy = os.environ.get("HTTP_PROXY", "") or os.environ.get("HTTPS_PROXY", "")
    if not proxy:
        return ""

    cert_path = os.environ.get("MITM_CERT_FILE", "")
    cert_fingerprint = ""
    if cert_path:
        try:
            with open(cert_path, "rb") as f:
                cert_fingerprint = hashlib.sha256(f.read()).hexdigest()[:12]
        except (OSError, IOError):
            import logging
            logging.getLogger("swebench").warning(
                f"MITM_CERT_FILE unreadable: {cert_path}"
            )

    combined = f"{proxy}|{cert_fingerprint}"
    # SHA256 for FIPS compliance (MD5 fails in FIPS-enabled environments)
    return ".p" + hashlib.sha256(combined.encode()).hexdigest()[:6]
