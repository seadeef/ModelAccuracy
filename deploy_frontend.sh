#!/usr/bin/env bash
# Rebuild the Svelte SPA and upload its artifacts to S3, then invalidate CloudFront.
#
# Scope: only the bundled Vite SPA (static_export/frontend/: index.html, assets/,
# favicon.svg|ico). Its CONTENTS upload to the bucket ROOT so index.html serves at /.
# Does NOT touch the rest of static_export/:
#   * root data assets (config, tiles, ranges, zip, admin, wbd, military),
#     forecast/, and model_data/ — all ./deploy_static.sh
#   * API code — ./deploy_api.sh
#
# Invalidation is scoped to non-hashed files. Files under /assets/ are content-hashed
# by Vite, so their URLs change on every build and don't need invalidation.
#
# Env (loaded from .env if present):
#   DATA_S3_URI                  s3://bucket[/prefix] — bucket is extracted for upload target
#   CLOUDFRONT_DISTRIBUTION_ID   CF distribution ID (required)
#   AWS_REGION                   default us-west-1
#   AWS_PROFILE                  optional
#   SKIP_BUILD=1                 use existing static_export/ (skip `export_static.py --frontend`)
#   SKIP_INVALIDATE=1            upload only; don't create CF invalidation
#
# Usage:
#   ./deploy_frontend.sh
#   SKIP_BUILD=1 ./deploy_frontend.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

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
# Strip scheme and any path → bucket name.
BUCKET="${DATA_S3_URI#s3://}"
BUCKET="${BUCKET%%/*}"
if [[ -z "${CLOUDFRONT_DISTRIBUTION_ID:-}" ]]; then
  echo "ERROR: CLOUDFRONT_DISTRIBUTION_ID not set." >&2
  exit 1
fi
DISTRIBUTION_ID="$CLOUDFRONT_DISTRIBUTION_ID"
AWS_REGION="${AWS_REGION:-us-west-1}"
if [[ -n "${AWS_PROFILE:-}" ]]; then
  export AWS_PROFILE
  echo "==> Using AWS_PROFILE=${AWS_PROFILE}"
fi

SPA="$ROOT/static_export/frontend"

if [[ -z "${SKIP_BUILD:-}" ]]; then
  echo "==> Building frontend (export_static.py --frontend)"
  python3 export_static.py --frontend
else
  echo "==> SKIP_BUILD set — using existing static_export/frontend/"
fi

if [[ ! -f "$SPA/index.html" || ! -d "$SPA/assets" ]]; then
  echo "ERROR: $SPA is missing index.html or assets/ — run without SKIP_BUILD." >&2
  exit 1
fi

# The SPA lives in static_export/frontend/ locally, but its contents are uploaded to
# the bucket ROOT so index.html is served at /. assets/ is content-hashed and SPA-only,
# so --delete is safe there; index.html + favicons use cp (no --delete) to leave the
# sibling data files (./deploy_static.sh) untouched.
echo "==> Sync s3://$BUCKET/assets/ (immutable, 1yr cache)"
aws s3 sync "$SPA/assets" "s3://$BUCKET/assets" \
  --region "$AWS_REGION" \
  --delete \
  --cache-control "public, max-age=31536000, immutable"

echo "==> Upload index.html (no-cache)"
aws s3 cp "$SPA/index.html" "s3://$BUCKET/index.html" \
  --region "$AWS_REGION" \
  --cache-control "no-cache, must-revalidate" \
  --content-type "text/html; charset=utf-8"

for f in favicon.svg favicon.ico; do
  if [[ -f "$SPA/$f" ]]; then
    echo "==> Upload $f"
    aws s3 cp "$SPA/$f" "s3://$BUCKET/$f" \
      --region "$AWS_REGION" \
      --cache-control "public, max-age=86400"
  fi
done

if [[ -z "${SKIP_INVALIDATE:-}" ]]; then
  echo "==> CloudFront invalidation ($DISTRIBUTION_ID)"
  aws cloudfront create-invalidation \
    --distribution-id "$DISTRIBUTION_ID" \
    --paths "/" "/index.html" "/favicon.svg" "/favicon.ico" \
    --query 'Invalidation.{Id:Id,Status:Status}' --output table
else
  echo "==> SKIP_INVALIDATE set — not invalidating CloudFront"
fi

echo "==> Done"
