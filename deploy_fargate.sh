#!/usr/bin/env bash
# Build the API Docker image (export → verify → build) and push to Amazon ECR.
#
# Prerequisites: Docker, AWS CLI v2, Python 3 (for export_static.py), credentials with ECR push.
#
# Environment (optional):
#   AWS_REGION      default us-west-1
#   ECR_REPOSITORY  default modelaccuracy-api
#   IMAGE_TAG       default latest (pushed tag)
#   LOCAL_IMAGE     default modelaccuracy-api:build  (local name before tag to ECR)
#   SKIP_EXPORT=1   skip python3 export_static.py (use existing static_export/)
#   SKIP_PUSH=1     build only; do not login/push to ECR
#
# Usage:
#   ./deploy_fargate.sh
#   IMAGE_TAG=v1.2.3 ./deploy_fargate.sh
#   SKIP_EXPORT=1 SKIP_PUSH=1 ./deploy_fargate.sh
#
# Container entrypoint (also set in Dockerfile ENTRYPOINT):
#   exec uvicorn backend.api:app --host 0.0.0.0 --port ${PORT:-8080} --proxy-headers --forwarded-allow-ips '*'
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

AWS_REGION="${AWS_REGION:-us-west-1}"
ECR_REPOSITORY="${ECR_REPOSITORY:-modelaccuracy-api}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
LOCAL_IMAGE="${LOCAL_IMAGE:-modelaccuracy-api:build}"

usage() {
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
fi

verify_static_export() {
  local DATA="$ROOT/static_export/data"
  if [[ ! -d "$DATA" ]]; then
    echo "ERROR: $DATA is not a directory. Run export_static.py first (or unset SKIP_EXPORT)." >&2
    exit 1
  fi
  local ok=0
  local d
  for d in "$DATA"/*; do
    [[ -d "$d" ]] || continue
    if [[ -f "$d/grid.json" ]]; then
      ok=1
      break
    fi
  done
  if [[ "$ok" -ne 1 ]]; then
    echo "ERROR: no */grid.json under $DATA (need at least one model export)." >&2
    exit 1
  fi
  echo "OK: static_export/data contains grid.json"
}

if [[ -z "${SKIP_EXPORT:-}" ]]; then
  echo "==> export_static.py"
  python3 export_static.py
else
  echo "==> SKIP_EXPORT set — using existing static_export/"
fi

echo "==> verify static_export/data"
verify_static_export

echo "==> docker build -t ${LOCAL_IMAGE}"
docker build -t "$LOCAL_IMAGE" .

if [[ -n "${SKIP_PUSH:-}" ]]; then
  echo "==> SKIP_PUSH set — not pushing to ECR"
  echo "Done: local image ${LOCAL_IMAGE}"
  exit 0
fi

echo "==> ECR login and push"
AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}"

aws ecr describe-repositories --repository-names "$ECR_REPOSITORY" --region "$AWS_REGION" &>/dev/null \
  || { echo "==> Creating ECR repository ${ECR_REPOSITORY}"; aws ecr create-repository --repository-name "$ECR_REPOSITORY" --region "$AWS_REGION" >/dev/null; }

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

docker tag "$LOCAL_IMAGE" "${ECR_URI}:${IMAGE_TAG}"
docker push "${ECR_URI}:${IMAGE_TAG}"

echo "==> Done: pushed ${ECR_URI}:${IMAGE_TAG}"
