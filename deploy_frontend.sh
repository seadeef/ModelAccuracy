#!/usr/bin/env bash
# Rebuild the Svelte SPA and upload its artifacts to S3, then invalidate CloudFront.
#
# Scope: only the Vite output (index.html, assets/, favicon.svg|ico, export_manifest.json).
# Does NOT touch data/, forecast/, or static/ — those have their own upload flows.
#
# Invalidation is scoped to non-hashed files. Files under /assets/ are content-hashed
# by Vite, so their URLs change on every build and don't need invalidation.
#
# Env (loaded from .env if present):
#   FRONTEND_BUCKET              S3 bucket (default: raincheck-static)
#   CLOUDFRONT_DISTRIBUTION_ID   CF distribution (default: E1QEB4BM43ELTB)
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

BUCKET="${FRONTEND_BUCKET:-raincheck-static}"
DISTRIBUTION_ID="${CLOUDFRONT_DISTRIBUTION_ID:-E1QEB4BM43ELTB}"
AWS_REGION="${AWS_REGION:-us-west-1}"
if [[ -n "${AWS_PROFILE:-}" ]]; then
  export AWS_PROFILE
  echo "==> Using AWS_PROFILE=${AWS_PROFILE}"
fi

SITE="$ROOT/static_export"

if [[ -z "${SKIP_BUILD:-}" ]]; then
  echo "==> Building frontend (export_static.py --frontend)"
  python3 export_static.py --frontend
else
  echo "==> SKIP_BUILD set — using existing static_export/"
fi

if [[ ! -f "$SITE/index.html" || ! -d "$SITE/assets" ]]; then
  echo "ERROR: $SITE is missing index.html or assets/ — run without SKIP_BUILD." >&2
  exit 1
fi

echo "==> Sync s3://$BUCKET/assets/ (immutable, 1yr cache)"
aws s3 sync "$SITE/assets" "s3://$BUCKET/assets" \
  --region "$AWS_REGION" \
  --delete \
  --cache-control "public, max-age=31536000, immutable"

echo "==> Upload index.html (no-cache)"
aws s3 cp "$SITE/index.html" "s3://$BUCKET/index.html" \
  --region "$AWS_REGION" \
  --cache-control "no-cache, must-revalidate" \
  --content-type "text/html; charset=utf-8"

for f in favicon.svg favicon.ico export_manifest.json; do
  if [[ -f "$SITE/$f" ]]; then
    echo "==> Upload $f"
    aws s3 cp "$SITE/$f" "s3://$BUCKET/$f" \
      --region "$AWS_REGION" \
      --cache-control "public, max-age=86400"
  fi
done

if [[ -z "${SKIP_INVALIDATE:-}" ]]; then
  echo "==> CloudFront invalidation ($DISTRIBUTION_ID)"
  aws cloudfront create-invalidation \
    --distribution-id "$DISTRIBUTION_ID" \
    --paths "/index.html" "/favicon.svg" "/favicon.ico" "/export_manifest.json" "/" \
    --query 'Invalidation.{Id:Id,Status:Status}' --output table
else
  echo "==> SKIP_INVALIDATE set — not invalidating CloudFront"
fi

echo "==> Done"
