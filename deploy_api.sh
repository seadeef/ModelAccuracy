#!/usr/bin/env bash
# Build the API Lambda deployment zip and push it with update-function-code.
# Replaces the old Fargate/ECR image pipeline (deploy_fargate.sh) — no Docker,
# no ECR: dependencies are fetched as prebuilt manylinux/aarch64 wheels, so
# this runs on any machine with python3 + zip + the AWS CLI.
#
# The package holds CODE ONLY (~25 MB zipped). All data — model_data bins,
# WBD chunks, admin/military boundaries, forecasts — is read from S3 at
# runtime (see backend/static_store.py) and shipped by ./deploy_static.sh,
# so stats refreshes never require an API deploy, and vice versa.
#
# Env (loaded from .env if present):
#   LAMBDA_FUNCTION_NAME   default <STACK_NAME>-api (STACK_NAME default raincheck);
#                          printed by ./deploy_infra.sh
#   AWS_REGION             default us-west-1
#   AWS_PROFILE            optional
#   PYTHON_RUNTIME_VERSION default 3.12 — MUST match the stack's PythonRuntime
#   SKIP_PUSH=1            build and validate the zip locally; do not call AWS
#   SKIP_SMOKE=1           skip the post-deploy /health check
#
# Usage:
#   ./deploy_api.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
source "$ROOT/deploy_common.sh"
deploy_init "$ROOT"
FN_NAME="${LAMBDA_FUNCTION_NAME:-${STACK_NAME}-api}"
PY_VER="${PYTHON_RUNTIME_VERSION:-3.12}"

BUILD_DIR="$ROOT/build/lambda"
PKG_DIR="$BUILD_DIR/package"
ZIP_PATH="$BUILD_DIR/api.zip"
rm -rf "$BUILD_DIR"
mkdir -p "$PKG_DIR"

# ── dependencies: prebuilt wheels for the Lambda arch/runtime ────────────────
# --platform/--python-version make pip resolve for Lambda (linux/aarch64,
# python$PY_VER) regardless of the local machine; --only-binary refuses any
# sdist that would need local compilation. boto3/botocore are intentionally
# absent (the runtime provides them) — see requirements.lambda.txt.
# Any python with pip works (wheels are fetched for the Lambda platform, not
# run locally); prefer the project venv when present.
PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then
  if [[ -x "$ROOT/.venv/bin/python" ]]; then PYTHON="$ROOT/.venv/bin/python"; else PYTHON="python3"; fi
fi

echo "==> pip install (manylinux aarch64, python${PY_VER}) → package/"
"$PYTHON" -m pip install --quiet \
  --target "$PKG_DIR" \
  --platform manylinux2014_aarch64 \
  --implementation cp \
  --python-version "$PY_VER" \
  --only-binary=:all: \
  -r requirements.lambda.txt

# ── application code (same file set the old Docker image copied) ────────────
echo "==> copying application code"
cp model_registry.py stats_grid_metadata.py forecast_variables.py "$PKG_DIR/"
rsync -a --exclude '__pycache__' backend "$PKG_DIR/"
rsync -a --exclude '__pycache__' statistics_plugins "$PKG_DIR/"

# Never ship local credentials/config; Lambda env comes from the stack.
rm -f "$PKG_DIR/.env"
find "$PKG_DIR" -name '__pycache__' -type d -prune -exec rm -rf {} +

echo "==> zipping"
(cd "$PKG_DIR" && zip -qr "$ZIP_PATH" .)
ZIP_MB=$(du -m "$ZIP_PATH" | cut -f1)
echo "==> ${ZIP_PATH} (${ZIP_MB} MB)"
zip -T "$ZIP_PATH" >/dev/null
echo "==> zip integrity OK"
if (( ZIP_MB > 45 )); then
  echo "ERROR: zip exceeds the ~50 MB direct-upload limit. Trim dependencies" >&2
  echo "       (requirements.lambda.txt) or switch update-function-code to --s3-bucket." >&2
  exit 1
fi

if [[ -n "${SKIP_PUSH:-}" ]]; then
  echo "==> SKIP_PUSH set — package built locally; not updating Lambda"
  exit 0
fi

echo "==> update-function-code ${FN_NAME}"
aws lambda update-function-code --region "$AWS_REGION" \
  --function-name "$FN_NAME" \
  --zip-file "fileb://$ZIP_PATH" \
  --no-cli-pager --query '{CodeSha256:CodeSha256,CodeSize:CodeSize}' --output table
aws lambda wait function-updated --region "$AWS_REGION" --function-name "$FN_NAME"

if [[ -z "${SKIP_SMOKE:-}" ]]; then
  FN_URL="$(aws lambda get-function-url-config --region "$AWS_REGION" \
    --function-name "$FN_NAME" --query 'FunctionUrl' --output text)"
  echo "==> smoke test ${FN_URL}health"
  if curl -sf --max-time 30 "${FN_URL}health" >/dev/null; then
    echo "==> /health OK"
  else
    echo "ERROR: ${FN_URL}health failed — check CloudWatch logs:" >&2
    echo "       aws logs tail /aws/lambda/${FN_NAME} --since 10m --region ${AWS_REGION}" >&2
    exit 1
  fi
fi

echo "==> Done: ${FN_NAME} is running the new code"
