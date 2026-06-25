#!/usr/bin/env bash
# Refresh the S3-served static site: download the latest forecast, rebuild the
# static export, and sync it to the stats bucket. No command-line options.
#
# Steps:
#   1. python3 download.py --forecast        # latest forecast → stats_output/{model}/forecast/
#   2. python3 export_static.py --static      # builds static_export/ root assets AND static_export/forecast/
#   3. aws s3 sync static_export/{config.json,zip,admin,wbd,tiles,ranges} → s3://$BUCKET/… (root)
#   4. aws s3 sync static_export/forecast/ → s3://$BUCKET/$FORECAST_PREFIX/
#   5. CloudFront invalidation of the data paths (/config.json, /zip/*, /admin/*, …)
#
# The two synced subtrees:
#
#   static_export/ root      Browser-fetched via CloudFront from the site root
#                            (config.json, tiles/, ranges/, zip/, admin/, wbd/ map assets),
#                            uploaded to s3://$BUCKET/… (keys mirror the local layout).
#                            They share the bucket root with the SPA (./deploy_frontend.sh);
#                            each sync uses --delete only within its own subdir.
#                            Cache-Control matches backend/api.py:LongCacheStaticMiddleware:
#                              zip/, admin/, wbd/ (map assets only)  → 1yr immutable
#                              tiles/, ranges/, config.json          → 300s must-revalidate
#
#   static_export/forecast/  Read by the Fargate backend via forecast_store_from_env
#                            (refreshes every ~5 min), so daily forecast updates ship
#                            without rebuilding the API image. FORECAST_PREFIX is the
#                            sibling of the DATA_S3_URI stats prefix:
#                              s3://bucket            → s3://bucket/forecast/
#                              s3://bucket/prefix     → s3://bucket/forecast/
#                              s3://bucket/a/b        → s3://bucket/a/forecast/
#                            No CloudFront invalidation (backend reads S3 directly).
#
# Not uploaded here:
#   * SPA shell (frontend/index.html, assets/, favicons) — use ./deploy_frontend.sh.
#   * static_export/model_data/ (verification .bin + grid.json) — baked into the API
#     image by Dockerfile COPY; refreshed by rebuilding via ./deploy_fargate.sh.
#
# Env (loaded from .env if present):
#   DATA_S3_URI                  s3://bucket[/prefix]  (required)
#   CLOUDFRONT_DISTRIBUTION_ID   required unless SKIP_INVALIDATE=1
#   AWS_REGION                   default us-west-1
#   AWS_PROFILE                  optional
#   SKIP_BUILD=1                 skip download+export; upload existing static_export/
#   SKIP_INVALIDATE=1            skip the data-paths CloudFront invalidation

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ $# -gt 0 ]]; then
  echo "ERROR: $0 takes no arguments (got: $*)" >&2
  exit 2
fi

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

if [[ -z "${DATA_S3_URI:-}" ]]; then
  echo "ERROR: DATA_S3_URI not set (expected s3://bucket[/prefix])." >&2
  exit 1
fi

# Parse DATA_S3_URI=s3://bucket[/stats_prefix] → (BUCKET, FORECAST_PREFIX).
# Mirror backend/static_store.py:forecast_store_from_env:
#   parent = stats_prefix.rsplit("/", 1)[0] if "/" in stats_prefix else ""
#   forecast_prefix = f"{parent}/forecast" if parent else "forecast"
WITHOUT_SCHEME="${DATA_S3_URI#s3://}"
BUCKET="${WITHOUT_SCHEME%%/*}"
if [[ "$WITHOUT_SCHEME" == "$BUCKET" ]]; then
  STATS_PREFIX=""
else
  STATS_PREFIX="${WITHOUT_SCHEME#*/}"
  STATS_PREFIX="${STATS_PREFIX%/}"
fi
if [[ "$STATS_PREFIX" == *"/"* ]]; then
  FORECAST_PREFIX="${STATS_PREFIX%/*}/forecast"
else
  FORECAST_PREFIX="forecast"
fi

AWS_REGION="${AWS_REGION:-us-west-1}"
if [[ -n "${AWS_PROFILE:-}" ]]; then
  export AWS_PROFILE
  echo "==> Using AWS_PROFILE=${AWS_PROFILE}"
fi

SITE="$ROOT/static_export"
# Flat layout: browser data assets live at the static_export root, uploaded to the
# S3 bucket root (keys mirror the local layout → served from the site root, e.g.
# /config.json, /wbd/…). They share the root with the SPA (./deploy_frontend.sh).
STATIC_SRC="$SITE"
FORECAST_SRC="$SITE/forecast"

CC_MUTABLE="public, max-age=300, must-revalidate"
CC_IMMUTABLE="public, max-age=31536000, immutable"

build() {
  if [[ -n "${SKIP_BUILD:-}" ]]; then
    echo "==> SKIP_BUILD set — using existing $SITE"
    return
  fi
  # Download the freshest forecast for every model, then export the static tree.
  # export_static.py --static writes both the root data assets and forecast/ via the
  # _write_*_if_changed helpers, so unchanged files keep their mtime and the
  # `aws s3 sync` calls below skip them.
  echo "==> Downloading latest forecast (download.py --forecast)"
  python3 download.py --forecast
  echo "==> Rebuilding static export (export_static.py --static)"
  python3 export_static.py --static
}

sync_static() {
  if [[ ! -d "$STATIC_SRC" ]]; then
    echo "ERROR: $STATIC_SRC does not exist — run without SKIP_BUILD." >&2
    exit 1
  fi
  # Data files are served from the site root now (no /stats prefix). Each sync
  # targets a specific subdir with --delete; config.json + the forecast calendar
  # use cp (no --delete) so they don't disturb the SPA files that share the root.
  local dest="s3://$BUCKET"

  # Immutable (zip + admin boundaries — change yearly at most).
  for sub in zip admin; do
    if [[ -d "$STATIC_SRC/$sub" ]]; then
      echo "==> Sync $dest/$sub/ ($CC_IMMUTABLE)"
      aws s3 sync "$STATIC_SRC/$sub" "$dest/$sub" \
        --region "$AWS_REGION" --delete \
        --cache-control "$CC_IMMUTABLE"
    fi
  done

  # WBD frontend map assets only (backend chunks/ are baked into the API image).
  WBD="$STATIC_SRC/wbd"
  if [[ -d "$WBD" ]]; then
    echo "==> Sync $dest/wbd/ map assets ($CC_IMMUTABLE; excludes chunks/)"
    aws s3 sync "$WBD" "$dest/wbd" \
      --region "$AWS_REGION" --delete \
      --cache-control "$CC_IMMUTABLE" \
      --exclude "chunks/*"
  fi

  # Mutable (refreshed each stats rerun).
  for sub in tiles ranges; do
    if [[ -d "$STATIC_SRC/$sub" ]]; then
      echo "==> Sync $dest/$sub/ ($CC_MUTABLE)"
      aws s3 sync "$STATIC_SRC/$sub" "$dest/$sub" \
        --region "$AWS_REGION" --delete \
        --cache-control "$CC_MUTABLE"
    fi
  done

  if [[ -f "$STATIC_SRC/config.json" ]]; then
    echo "==> Upload $dest/config.json ($CC_MUTABLE)"
    aws s3 cp "$STATIC_SRC/config.json" "$dest/config.json" \
      --region "$AWS_REGION" \
      --cache-control "$CC_MUTABLE"
  fi

  # Browser-facing copy of the forecast calendar only (init dates). The SPA fetches
  # /forecast/forecast_calendar.json directly so it stays current with new forecasts;
  # the forecast *bins* stay at $FORECAST_PREFIX (backend-read, below).
  if [[ -f "$FORECAST_SRC/forecast_calendar.json" ]]; then
    echo "==> Upload $dest/forecast/forecast_calendar.json ($CC_MUTABLE)"
    aws s3 cp "$FORECAST_SRC/forecast_calendar.json" "$dest/forecast/forecast_calendar.json" \
      --region "$AWS_REGION" \
      --cache-control "$CC_MUTABLE"
  fi
}

sync_forecast() {
  if [[ ! -d "$FORECAST_SRC" ]]; then
    echo "ERROR: $FORECAST_SRC does not exist — run without SKIP_BUILD." >&2
    exit 1
  fi
  local dest="s3://$BUCKET/$FORECAST_PREFIX"
  echo "==> Sync $dest/ (--delete)"
  aws s3 sync "$FORECAST_SRC" "$dest" \
    --region "$AWS_REGION" --delete
}

build
sync_static
sync_forecast

if [[ -z "${SKIP_INVALIDATE:-}" ]]; then
  if [[ -z "${CLOUDFRONT_DISTRIBUTION_ID:-}" ]]; then
    echo "ERROR: CLOUDFRONT_DISTRIBUTION_ID not set (use SKIP_INVALIDATE=1 to skip)." >&2
    exit 1
  fi
  echo "==> CloudFront invalidation ($CLOUDFRONT_DISTRIBUTION_ID): data paths at root"
  aws cloudfront create-invalidation \
    --distribution-id "$CLOUDFRONT_DISTRIBUTION_ID" \
    --paths "/config.json" "/zip/*" "/admin/*" "/wbd/*" "/tiles/*" "/ranges/*" "/forecast/*" \
    --query 'Invalidation.{Id:Id,Status:Status}' --output table
else
  echo "==> SKIP_INVALIDATE set — not invalidating CloudFront"
fi

echo "==> Forecast: backend re-reads S3 within ~5 min."
echo "==> Done"
