#!/usr/bin/env bash
# Refresh ALL data in S3: download the latest forecast, rebuild the static
# export (browser assets + backend-read data), and sync it to the bucket.
# No command-line options.
#
# Steps:
#   1. python3 download.py --forecast        # latest forecast → stats_output/{model}/forecast/
#   2. python3 export_static.py --static     # site root assets + boundary polygons + forecast/
#   3. python3 export_static.py --data       # static_export/model_data/ (.bin + grid.json)
#   4. aws s3 sync each subtree → s3://$BUCKET/…
#   5. CloudFront invalidation of the mutable browser paths
#
# The synced subtrees and who reads them:
#
#   Browser (via CloudFront, cache headers set here):
#     config.json, tiles/, ranges/, ar/     mutable  (300s must-revalidate)
#     zip/, admin/, wbd/ map assets,
#       military/sites.geojson              immutable (1yr)
#     forecast/forecast_calendar.json       mutable
#
#   Backend Lambda (direct S3 GetObject — no CloudFront, no cache headers):
#     model_data/<model>/…                  stats bins + grid.json (DATA_S3_URI)
#     cube/<model>/…                        per-cell daily cube (POST /api/series)
#     wbd/chunks/…                          HUC mask polygons, fetched per-parent
#     admin/boundaries.json,
#       military/boundaries.json            region mask polygons
#     forecast/{model}/{var}/lead_*.bin     re-read every ~5 min, so daily
#                                           forecast updates ship without an
#                                           API deploy
#
# Not uploaded here:
#   * SPA shell (index.html, assets/, favicons) — ./deploy_frontend.sh
#   * API code — ./deploy_api.sh
#
# Env (loaded from .env if present):
#   DATA_S3_URI                  s3://bucket/model_data  (required; the stats
#                                prefix — bucket root holds the site, so a
#                                bare s3://bucket is refused to keep --delete
#                                syncs away from the SPA files)
#   CLOUDFRONT_DISTRIBUTION_ID   required unless SKIP_INVALIDATE=1
#   AWS_REGION                   default us-west-1
#   AWS_PROFILE                  optional
#   LAMBDA_FUNCTION_NAME         default <STACK_NAME>-api — recycled after sync so
#                                warm environments drop pre-deploy caches
#   SKIP_BUILD=1                 skip download+export; upload existing static_export/
#   SKIP_INVALIDATE=1            skip the data-paths CloudFront invalidation
#   SKIP_FLUSH=1                 skip the Lambda cache-flush recycle

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
source "$ROOT/deploy_common.sh"
deploy_init "$ROOT"

if [[ $# -gt 0 ]]; then
  echo "ERROR: $0 takes no arguments (got: $*)" >&2
  exit 2
fi

if [[ -z "${DATA_S3_URI:-}" ]]; then
  echo "ERROR: DATA_S3_URI not set (expected s3://bucket/model_data)." >&2
  exit 1
fi

# Parse DATA_S3_URI=s3://bucket/stats_prefix → (BUCKET, STATS_PREFIX, FORECAST_PREFIX).
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
if [[ -z "$STATS_PREFIX" ]]; then
  echo "ERROR: DATA_S3_URI needs a prefix (s3://bucket/model_data): the bucket root" >&2
  echo "       holds the SPA + site assets, and the stats sync uses --delete." >&2
  exit 1
fi
if [[ "$STATS_PREFIX" == *"/"* ]]; then
  FORECAST_PREFIX="${STATS_PREFIX%/*}/forecast"
else
  FORECAST_PREFIX="forecast"
fi

SITE="$ROOT/static_export"
# Flat layout: browser data assets live at the static_export root, uploaded to the
# S3 bucket root (keys mirror the local layout → served from the site root, e.g.
# /config.json, /wbd/…). They share the root with the SPA (./deploy_frontend.sh).
STATIC_SRC="$SITE"
FORECAST_SRC="$SITE/forecast"
DATA_SRC="$SITE/model_data"

CC_MUTABLE="public, max-age=300, must-revalidate"
CC_IMMUTABLE="public, max-age=31536000, immutable"

build() {
  if [[ -n "${SKIP_BUILD:-}" ]]; then
    echo "==> SKIP_BUILD set — using existing $SITE"
    return
  fi
  # Download the freshest forecast for every model, then export the static tree.
  # Both export modes write via the _write_*_if_changed helpers, so unchanged
  # files keep their mtime and the `aws s3 sync` calls below skip them.
  echo "==> Downloading latest forecast (download.py --forecast)"
  python3 download.py --forecast
  echo "==> Checking canonical tARget dataset (download.py --ar)"
  python3 download.py --ar
  echo "==> Refreshing AR episode statistics and basin dependencies"
  python3 compute_stats.py --only episodes
  echo "==> Rebuilding static export (export_static.py --static)"
  python3 export_static.py --static
  echo "==> Rebuilding model data (export_static.py --data)"
  python3 export_static.py --data
}

sync_static() {
  if [[ ! -d "$STATIC_SRC" ]]; then
    echo "ERROR: $STATIC_SRC does not exist — run without SKIP_BUILD." >&2
    exit 1
  fi
  # Data files are served from the site root (no /stats prefix). Each sync
  # targets a specific subdir with --delete; config.json + the forecast calendar
  # use cp (no --delete) so they don't disturb the SPA files that share the root.
  local dest="s3://$BUCKET"

  # Immutable browser assets (change yearly at most). wbd/ and military/ also
  # carry the backend polygon files (chunks/, boundaries.json) — those are read
  # straight from S3 by the Lambda, so the browser cache header is irrelevant
  # to them and syncing whole directories keeps --delete pruning correct.
  for sub in zip admin wbd military; do
    if [[ -d "$STATIC_SRC/$sub" ]]; then
      echo "==> Sync $dest/$sub/ ($CC_IMMUTABLE)"
      aws s3 sync "$STATIC_SRC/$sub" "$dest/$sub" \
        --region "$AWS_REGION" --delete \
        --cache-control "$CC_IMMUTABLE"
    fi
  done

  # tARget v4 frames are date-addressed but may be extended as new source files
  # are ingested, so publish them with the mutable data cache policy.
  if [[ -d "$STATIC_SRC/ar" ]]; then
    echo "==> Sync $dest/ar/ ($CC_MUTABLE)"
    aws s3 sync "$STATIC_SRC/ar" "$dest/ar" \
      --region "$AWS_REGION" --delete \
      --cache-control "$CC_MUTABLE"
  fi

  # Per-cell cube (backend-read: POST /api/series pools it over any region).
  # 100–200 MB per model; the Lambda loads a model's cube whole on first use,
  # so this sync pairs with LAMBDA_MEMORY_MB ≥ 3008 in deploy_infra.sh.
  if [[ -d "$STATIC_SRC/cube" ]]; then
    echo "==> Sync $dest/cube/ (backend-read)"
    aws s3 sync "$STATIC_SRC/cube" "$dest/cube" \
      --region "$AWS_REGION" --delete
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

sync_model_data() {
  if [[ ! -d "$DATA_SRC" ]]; then
    echo "ERROR: $DATA_SRC does not exist — run without SKIP_BUILD (or export_static.py --data)." >&2
    exit 1
  fi
  # Backend-read stats tree (DATA_S3_URI). The Lambda maps model_data/<x> →
  # <STATS_PREFIX>/<x>, so the uploaded keys must live under the same prefix
  # the URI names.
  local dest="s3://$BUCKET/$STATS_PREFIX"
  echo "==> Sync $dest/ (--delete)"
  aws s3 sync "$DATA_SRC" "$dest" \
    --region "$AWS_REGION" --delete
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

# The Lambda caches bins/masks/chunks per execution environment (bounded LRUs
# keyed by S3 path — same key, so a warm environment would serve pre-deploy
# values). Bumping an env var recycles every environment: the next invocation
# cold-starts against the fresh data. No-op when the function doesn't exist
# (dev-only checkout) or SKIP_FLUSH=1.
flush_lambda_caches() {
  if [[ -n "${SKIP_FLUSH:-}" ]]; then
    echo "==> SKIP_FLUSH set — warm Lambda environments keep pre-deploy caches"
    return
  fi
  local fn="${LAMBDA_FUNCTION_NAME:-${STACK_NAME:-raincheck}-api}"
  if ! aws lambda get-function-configuration --region "$AWS_REGION" \
        --function-name "$fn" >/dev/null 2>&1; then
    echo "==> Lambda ${fn} not found — skipping cache flush"
    return
  fi
  echo "==> Recycling ${fn} environments (DATA_DEPLOYED_AT bump → fresh caches)"
  local env_file
  env_file="$(mktemp)"
  aws lambda get-function-configuration --region "$AWS_REGION" \
      --function-name "$fn" --query 'Environment.Variables' --output json \
    | python3 -c 'import json, sys, time
v = json.load(sys.stdin) or {}
v["DATA_DEPLOYED_AT"] = str(int(time.time()))
json.dump({"Variables": v}, sys.stdout)' > "$env_file"
  aws lambda update-function-configuration --region "$AWS_REGION" \
    --function-name "$fn" --environment "file://${env_file}" \
    --no-cli-pager --query 'LastUpdateStatus' --output text
  rm -f "$env_file"
  aws lambda wait function-updated --region "$AWS_REGION" --function-name "$fn"
}

build
sync_static
sync_model_data
sync_forecast
flush_lambda_caches

if [[ -z "${SKIP_INVALIDATE:-}" ]]; then
  if [[ -z "${CLOUDFRONT_DISTRIBUTION_ID:-}" ]]; then
    echo "ERROR: CLOUDFRONT_DISTRIBUTION_ID not set (use SKIP_INVALIDATE=1 to skip)." >&2
    exit 1
  fi
  # Only invalidate the mutable browser trees. zip/, admin/, wbd/ map assets are
  # immutable (1yr Cache-Control) so a bumped URL — not an invalidation — is how
  # they'd change. model_data/, wbd/chunks/, boundaries.json and forecast bins
  # are backend-read straight from S3, so CloudFront never caches them.
  echo "==> CloudFront invalidation ($CLOUDFRONT_DISTRIBUTION_ID): mutable data paths"
  aws cloudfront create-invalidation \
    --distribution-id "$CLOUDFRONT_DISTRIBUTION_ID" \
    --paths "/config.json" "/tiles/*" "/ranges/*" "/ar/*" "/forecast/forecast_calendar.json" \
    --query 'Invalidation.{Id:Id,Status:Status}' --output table
else
  echo "==> SKIP_INVALIDATE set — not invalidating CloudFront"
fi

echo "==> Forecast + boundaries: the Lambda re-reads S3 (forecast within ~5 min;"
echo "    boundary/chunk caches refresh on the next cold start)."
echo "==> Done"
