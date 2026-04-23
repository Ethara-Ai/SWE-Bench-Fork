# IF you change the base image, you need to rebuild all images (run with --force_rebuild)
from __future__ import annotations

from string import Template

from swebench.harness._image_utils import _proxy_hash_suffix

_BUILDKIT_HEADER = "# syntax=docker/dockerfile:1.6\n"

# Explicit proxy ARGs + ENV propagation — required for conda/pip/wget inside RUN.
# Docker auto-injects --build-arg values, but only ARG-declared vars propagate to ENV.
# One ARG per line for readability; CA_CERT_PATH configurable (default: Debian bundle).
_PROXY_ARG_BLOCK = r"""
ARG http_proxy=""
ARG https_proxy=""
ARG HTTP_PROXY=""
ARG HTTPS_PROXY=""
ARG no_proxy="localhost,127.0.0.1,::1"
ARG NO_PROXY="localhost,127.0.0.1,::1"
ARG CA_CERT_PATH="/etc/ssl/certs/ca-certificates.crt"
"""

# Consolidated ENV block: proxy propagation + SSL cert paths.
_PROXY_ENV_BLOCK = r"""
ENV http_proxy=$${http_proxy} \
    https_proxy=$${https_proxy} \
    HTTP_PROXY=$${HTTP_PROXY} \
    HTTPS_PROXY=$${HTTPS_PROXY} \
    no_proxy=$${no_proxy} \
    NO_PROXY=$${NO_PROXY} \
    SSL_CERT_FILE=$${CA_CERT_PATH} \
    REQUESTS_CA_BUNDLE=$${CA_CERT_PATH} \
    CURL_CA_BUNDLE=$${CA_CERT_PATH}
"""

_CA_CERT_BLOCK = r"""
RUN mkdir -p /etc/pki/tls/certs /etc/pki/ca-trust/extracted/pem /etc/ssl/certs /usr/local/share/ca-certificates && \
    ln -sf /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt && \
    ln -sf /etc/ssl/certs/ca-certificates.crt /etc/ssl/cert.pem && \
    ln -sf /etc/ssl/certs/ca-certificates.crt /etc/ssl/ca-bundle.pem
"""

# BuildKit secret mount: cert at /run/secrets/mitm_ca injected into trust store
# without baking into a layer. No-op when --secret not passed (required=false).
_MITM_CA_BLOCK = r"""
RUN --mount=type=secret,id=mitm_ca,required=false \
    if [ -f /run/secrets/mitm_ca ]; then \
      cp /run/secrets/mitm_ca /usr/local/share/ca-certificates/mitm-ca.crt && \
      update-ca-certificates; \
    fi
"""

# Strips proxy config so final image doesn't leak corporate settings.
_PROXY_CLEANUP_BLOCK = r"""
RUN conda config --remove-key proxy_servers 2>/dev/null || true && \
    pip config unset global.cert 2>/dev/null || true
ENV http_proxy="" \
    https_proxy="" \
    HTTP_PROXY="" \
    HTTPS_PROXY="" \
    no_proxy="" \
    NO_PROXY=""
"""

_OCI_LABELS = Template(r"""
LABEL org.opencontainers.image.title="swebench/${image_type}" \
      org.opencontainers.image.description="SWE-bench ${image_type} image" \
      org.opencontainers.image.source="https://github.com/Ethara-AI/SWE-Bench-Fork" \
      org.opencontainers.image.vendor="swebench" \
      org.opencontainers.image.authors="https://www.ethara.ai/" \
      org.opencontainers.image.type="${image_type}"
""")

_DOCKERFILE_BASE = Template(
    r"""FROM ${from_line}
""" + _PROXY_ARG_BLOCK + r"""
ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC \
    LANG=C.UTF-8
""" + _PROXY_ENV_BLOCK + r"""
""" + _OCI_LABELS.safe_substitute(image_type="base") + r"""
RUN apt update && apt install -y \
    wget git build-essential libffi-dev libtiff-dev \
    python3 python3-pip python-is-python3 jq curl \
    locales locales-all tzdata ca-certificates \
    && rm -rf /var/lib/apt/lists/*
""" + _CA_CERT_BLOCK + r"""
RUN wget "https://repo.anaconda.com/miniconda/Miniconda3-py311_24.7.1-0-Linux-$$(uname -m).sh" \
    -O miniconda.sh && bash miniconda.sh -b -p /opt/miniconda3 && rm miniconda.sh
ENV PATH=/opt/miniconda3/bin:$$PATH
RUN conda init --all && conda config --append channels conda-forge

RUN if [ -n "$$HTTP_PROXY" ]; then \
      conda config --set proxy_servers.http "$$HTTP_PROXY" && \
      conda config --set proxy_servers.https "$${HTTPS_PROXY:-$$HTTP_PROXY}" && \
      pip config set global.cert /etc/ssl/certs/ca-certificates.crt; \
    fi

RUN adduser --disabled-password --gecos 'dog' nonroot
""")

_DOCKERFILE_ENV = Template(
    r"""FROM ${from_line}
""" + _PROXY_ARG_BLOCK + _PROXY_ENV_BLOCK + r"""
""" + _OCI_LABELS.safe_substitute(image_type="env") + r"""
COPY ./setup_env.sh /root/
RUN chmod +x /root/setup_env.sh && /bin/bash -c "source ~/.bashrc && /root/setup_env.sh"
WORKDIR /testbed/
RUN echo "source /opt/miniconda3/etc/profile.d/conda.sh && conda activate testbed" > /root/.bashrc
""")

_DOCKERFILE_INSTANCE = Template(
    r"""FROM ${from_line}
""" + _PROXY_ARG_BLOCK + _PROXY_ENV_BLOCK + r"""
""" + _OCI_LABELS.safe_substitute(image_type="instance") + r"""
COPY ./setup_repo.sh /root/
RUN /bin/bash /root/setup_repo.sh
WORKDIR /testbed/
""" + _PROXY_CLEANUP_BLOCK)


def get_dockerfile_base(platform: str, arch: str, use_buildx: bool = False) -> str:
    if use_buildx:
        from_line = "ubuntu:22.04"
        body = _DOCKERFILE_BASE.safe_substitute(from_line=from_line)
        marker = "ln -sf /etc/ssl/certs/ca-certificates.crt /etc/ssl/ca-bundle.pem\n"
        body = body.replace(marker, marker + _MITM_CA_BLOCK, 1)
        return _BUILDKIT_HEADER + body
    from_line = f"--platform={platform} ubuntu:22.04"
    return _DOCKERFILE_BASE.safe_substitute(from_line=from_line)


def get_dockerfile_env(
    platform: str = "",
    arch: str = "",
    use_buildx: bool = False,
    base_image_ref: str = "",
    base_image_key: str = "",
) -> str:
    if use_buildx:
        body = _DOCKERFILE_ENV.safe_substitute(from_line=base_image_ref)
        return _BUILDKIT_HEADER + body
    if not base_image_key:
        base_image_key = f"sweb.base.{arch}{_proxy_hash_suffix()}:latest"
    from_line = f"--platform={platform} {base_image_key}"
    return _DOCKERFILE_ENV.safe_substitute(from_line=from_line)


def get_dockerfile_instance(
    platform: str = "",
    env_image_name: str = "",
    use_buildx: bool = False,
    env_image_ref: str = "",
) -> str:
    if use_buildx:
        body = _DOCKERFILE_INSTANCE.safe_substitute(from_line=env_image_ref)
        return _BUILDKIT_HEADER + body
    from_line = f"--platform={platform} {env_image_name}"
    return _DOCKERFILE_INSTANCE.safe_substitute(from_line=from_line)
