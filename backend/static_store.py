from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def default_static_site_root() -> Path:
    """``static_export``: site root; ``data/*`` is read for static stats (URL ``/data``)."""
    return _PROJECT_ROOT / "static_export"

try:
    import boto3
    from backend.aws_session import client as _aws_client
except ImportError:  # pragma: no cover - optional for local dev
    boto3 = None
    _aws_client = None  # type: ignore[assignment]


class StaticStore(Protocol):
    cache_key: str

    def read_text(self, relative_path: str) -> str:
        ...

    def read_bytes(self, relative_path: str) -> bytes:
        ...

    def exists(self, relative_path: str) -> bool:
        ...


class LocalStaticStore:
    def __init__(self, root: Path):
        self.root = root
        self.cache_key = str(root.resolve())

    def _resolve(self, relative_path: str) -> Path:
        return self.root / relative_path

    def read_text(self, relative_path: str) -> str:
        return self._resolve(relative_path).read_text()

    def read_bytes(self, relative_path: str) -> bytes:
        return self._resolve(relative_path).read_bytes()

    def exists(self, relative_path: str) -> bool:
        rel = relative_path.lstrip("/")
        if not rel:
            return self.root.is_dir() and any(self.root.iterdir())
        return self._resolve(relative_path).exists()


class S3StaticStore:
    def __init__(self, bucket: str, prefix: str = "", client=None):
        if not bucket:
            raise ValueError("S3 bucket is required")
        if client is None:
            if boto3 is None or _aws_client is None:
                raise RuntimeError("boto3 is required for S3StaticStore")
            client = _aws_client("s3")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client
        self.cache_key = f"s3://{bucket}/{self.prefix}"

    def _key(self, relative_path: str) -> str:
        rel = relative_path.lstrip("/")
        if not self.prefix:
            return rel
        return f"{self.prefix}/{rel}"

    def read_text(self, relative_path: str) -> str:
        return self.read_bytes(relative_path).decode("utf-8")

    def read_bytes(self, relative_path: str) -> bytes:
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=self._key(relative_path))
        except self.client.exceptions.NoSuchKey:
            raise FileNotFoundError(relative_path)
        return obj["Body"].read()

    def exists(self, relative_path: str) -> bool:
        rel = relative_path.lstrip("/").rstrip("/")
        if not rel:
            list_prefix = f"{self.prefix}/" if self.prefix else ""
            try:
                resp = self.client.list_objects_v2(
                    Bucket=self.bucket, Prefix=list_prefix, MaxKeys=1
                )
                return bool(resp.get("KeyCount", 0))
            except Exception:
                return False
        key = self._key(rel)
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            pass
        prefix = key.rstrip("/") + "/"
        try:
            resp = self.client.list_objects_v2(
                Bucket=self.bucket, Prefix=prefix, MaxKeys=1
            )
            return bool(resp.get("KeyCount", 0))
        except Exception:
            return False


class LogicalPathStripDataStore:
    """Maps app paths ``data/...`` to object keys ``...`` under the S3 prefix (``<model>/grid.json``, etc.)."""

    def __init__(self, inner: StaticStore):
        self._inner = inner
        # Same keying as the underlying S3 store; stripping is fixed for all S3 stats reads.
        self.cache_key = inner.cache_key

    @staticmethod
    def _mapped(relative_path: str) -> str:
        p = relative_path.lstrip("/")
        if p == "data":
            return ""
        if p.startswith("data/"):
            return p[5:].lstrip("/")
        return p

    def read_text(self, relative_path: str) -> str:
        return self._inner.read_text(self._mapped(relative_path))

    def read_bytes(self, relative_path: str) -> bytes:
        return self._inner.read_bytes(self._mapped(relative_path))

    def exists(self, relative_path: str) -> bool:
        return self._inner.exists(self._mapped(relative_path))


def _s3_stats_store(bucket: str, prefix: str = "") -> StaticStore:
    """S3 layout matches ``static_export/data/`` *contents* (``<model>/…``), not a ``data/`` prefix in the bucket."""
    return LogicalPathStripDataStore(S3StaticStore(bucket=bucket, prefix=prefix))


def _running_on_aws_lambda() -> bool:
    return bool(os.getenv("AWS_LAMBDA_FUNCTION_NAME"))


def _stats_s3_bucket_and_prefix() -> tuple[str, str] | None:
    """Parse ``MODELACCURACY_DATA_S3_URI=s3://bucket[/prefix]`` — prefix is the stats root (``<model>/…``)."""
    uri = os.getenv("MODELACCURACY_DATA_S3_URI", "").strip()
    if not uri:
        return None
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(
            "MODELACCURACY_DATA_S3_URI must look like s3://bucket or s3://bucket/optional/prefix"
        )
    return parsed.netloc, (parsed.path or "").strip("/")


def _s3_store_from_data_uri() -> StaticStore | None:
    loc = _stats_s3_bucket_and_prefix()
    if loc is None:
        return None
    bucket, prefix = loc
    return _s3_stats_store(bucket=bucket, prefix=prefix)


def _lambda_s3_store_or_raise() -> StaticStore:
    """Production Lambda: stats data always comes from S3 (never the deployment package)."""
    store = _s3_store_from_data_uri()
    if store is not None:
        return store
    raise RuntimeError(
        "Lambda is configured to read stats only from S3. Set MODELACCURACY_DATA_S3_URI to "
        "s3://bucket/prefix where that prefix holds the exported stats tree (same layout as "
        "static_export/data/: <model>/grid.json and .bin paths)."
    )


def store_from_env(
    *,
    default_local_root: Path | None = None,
) -> StaticStore:
    if _running_on_aws_lambda():
        return _lambda_s3_store_or_raise()

    s3_from_uri = _s3_store_from_data_uri()
    if s3_from_uri is not None:
        return s3_from_uri
    root = default_local_root or default_static_site_root()
    return LocalStaticStore(root)


def forecast_store_from_env(
    *,
    default_local_root: Path | None = None,
) -> StaticStore | None:
    """Return a store for forecast data, or ``None`` if unavailable.

    Forecasts live in the same S3 bucket as the stats tree, in a ``forecast/``
    folder that is a **sibling** of the stats prefix (not nested inside it).
    For ``MODELACCURACY_DATA_S3_URI=s3://bucket/static`` the forecast objects
    are at ``s3://bucket/forecast/{model}/lead_*.bin``; for ``s3://bucket``
    they are at ``s3://bucket/forecast/…``.

    Local dev falls back to *default_local_root* (typically
    ``static_export/forecast/``).
    """
    stats_loc = _stats_s3_bucket_and_prefix()
    if stats_loc is not None:
        bucket, stats_prefix = stats_loc
        parent = stats_prefix.rsplit("/", 1)[0] if "/" in stats_prefix else ""
        forecast_prefix = f"{parent}/forecast" if parent else "forecast"
        return S3StaticStore(bucket=bucket, prefix=forecast_prefix)

    root = default_local_root or (default_static_site_root() / "forecast")
    if root.is_dir():
        return LocalStaticStore(root)
    return None
