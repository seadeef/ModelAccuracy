"""Shared boto3 session factory that honors ``AWS_PROFILE`` from the environment.

On developer machines ``AWS_PROFILE`` is populated from the repo-root ``.env``
(loaded by :mod:`backend.api` at startup). On Fargate/Lambda the variable is
unset and boto3 falls through to the container/IAM-role credentials chain.
"""

from __future__ import annotations

import os
from typing import Any

try:
    import boto3
except ImportError:  # pragma: no cover - optional for local dev
    boto3 = None  # type: ignore[assignment]


def _profile_name() -> str | None:
    """Return the configured profile name, or ``None`` to use the default chain.

    Skipped on Lambda so a stray ``AWS_PROFILE`` in the environment never
    shadows the execution-role credentials.
    """
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        return None
    name = os.getenv("AWS_PROFILE", "").strip()
    return name or None


def get_session() -> Any:
    """Return a ``boto3.Session`` configured from ``AWS_PROFILE`` (if any)."""
    if boto3 is None:
        raise RuntimeError("boto3 is required but is not installed")
    profile = _profile_name()
    if profile:
        return boto3.Session(profile_name=profile)
    return boto3.Session()


def client(service_name: str, **kwargs: Any) -> Any:
    """Shortcut for ``get_session().client(service_name, **kwargs)``."""
    return get_session().client(service_name, **kwargs)


def resource(service_name: str, **kwargs: Any) -> Any:
    """Shortcut for ``get_session().resource(service_name, **kwargs)``."""
    return get_session().resource(service_name, **kwargs)
