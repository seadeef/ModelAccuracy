#!/usr/bin/env bash
# Build the API Docker image (export_static.py --data → verify → build) and push to Amazon ECR.
#
# Prerequisites: Docker, AWS CLI v2, Python 3 (for export_static.py), credentials with ECR push.
#
# Environment (optional):
#   AWS_REGION       default us-west-1
#   ECR_REPOSITORY   ECR repo name (if unset + interactive push, you are prompted; else default modelaccuracy-api)
#   IMAGE_TAG        default latest (pushed tag)
#   LOCAL_IMAGE      default modelaccuracy-api:build  (local name before tag to ECR)
#   DOCKER_PLATFORM  default linux/amd64 (Fargate x86). Use linux/arm64 for Graviton Fargate
#                    or faster local builds on Apple Silicon when SKIP_PUSH=1.
#   SKIP_EXPORT=1    skip python3 export_static.py --data (use existing static_export/data/)
#   SKIP_PUSH=1      build only; do not login/push to ECR
#   ECR_PUSH_LATEST=1   also tag and push :latest (in addition to IMAGE_TAG), same as --tag-latest
#   ECS_FORCE_REDEPLOY=1  after push (or alone with SKIP_PUSH), run ECS force-new-deployment; same as --force-redeploy
#   ECS_CLUSTER / ECS_CLUSTER_ARN   only used with --force-redeploy; prompted before data export if unset
#   ECS_SERVICE / ECS_SERVICE_ARN   only used with --force-redeploy; prompted before data export if unset
#
# Flags:
#   --tag-latest       push IMAGE_TAG and also push the same image as :latest
#   --force-redeploy   aws ecs update-service --force-new-deployment (needs cluster + service)
#   -h, --help
#
# Usage:
#   ./deploy_fargate.sh
#   IMAGE_TAG=v1.2.3 ./deploy_fargate.sh
#   SKIP_EXPORT=1 SKIP_PUSH=1 ./deploy_fargate.sh
#   ./deploy_fargate.sh --tag-latest --force-redeploy
#   ECS_CLUSTER=my-cluster ECS_SERVICE=my-svc ./deploy_fargate.sh --force-redeploy
#
# API image pip deps live in Dockerfile RUN pip install (same pins as README local install).
#
# Container entrypoint (also set in Dockerfile ENTRYPOINT):
#   exec uvicorn backend.api:app --host 0.0.0.0 --port ${PORT:-8080} --proxy-headers --forwarded-allow-ips '*'
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

AWS_REGION="${AWS_REGION:-us-west-1}"
# Unset ECR_REPOSITORY → prompt on TTY when pushing; otherwise default (never prompted before).
if [[ -n "${ECR_REPOSITORY+x}" ]]; then
  :
elif [[ -n "${SKIP_PUSH:-}" ]]; then
  ECR_REPOSITORY="modelaccuracy-api"
elif [[ -t 0 ]]; then
  read -r -p "ECR repository name [modelaccuracy-api]: " _ecr_reply
  ECR_REPOSITORY="${_ecr_reply:-modelaccuracy-api}"
else
  ECR_REPOSITORY="modelaccuracy-api"
fi
IMAGE_TAG="${IMAGE_TAG:-latest}"
LOCAL_IMAGE="${LOCAL_IMAGE:-modelaccuracy-api:build}"
# Fargate default CPU is x86_64; plain `docker build` on Apple Silicon is arm64 → ECS CannotPullContainerError.
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"

TAG_LATEST=0
FORCE_ECS_REDEPLOY=0
if [[ -n "${ECR_PUSH_LATEST:-}" ]]; then TAG_LATEST=1; fi
if [[ -n "${ECS_FORCE_REDEPLOY:-}" ]]; then FORCE_ECS_REDEPLOY=1; fi

usage() {
  sed -n '2,37p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    --tag-latest) TAG_LATEST=1; shift ;;
    --force-redeploy|--redeploy) FORCE_ECS_REDEPLOY=1; shift ;;
    *)
      echo "Unknown option: $1 (try --help)" >&2
      exit 1
      ;;
  esac
done

ecs_cluster_resolved() {
  if [[ -n "${ECS_CLUSTER:-}" ]]; then printf '%s' "$ECS_CLUSTER"
  elif [[ -n "${ECS_CLUSTER_ARN:-}" ]]; then printf '%s' "$ECS_CLUSTER_ARN"
  else printf ''; fi
}

ecs_service_resolved() {
  if [[ -n "${ECS_SERVICE:-}" ]]; then printf '%s' "$ECS_SERVICE"
  elif [[ -n "${ECS_SERVICE_ARN:-}" ]]; then printf '%s' "$ECS_SERVICE_ARN"
  else printf ''; fi
}

# Set when FORCE_ECS_REDEPLOY; collected immediately before export_static.py --data (see below).
ECS_CLUSTER_VAL=""
ECS_SERVICE_VAL=""

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

collect_ecs_targets_for_redeploy() {
  if [[ "$FORCE_ECS_REDEPLOY" -ne 1 ]]; then
    return 0
  fi
  echo "==> ECS target (before data export — set ECS_CLUSTER / ECS_SERVICE to skip prompts)"
  ECS_CLUSTER_VAL="$(ecs_cluster_resolved)"
  ECS_SERVICE_VAL="$(ecs_service_resolved)"
  if [[ -z "$ECS_CLUSTER_VAL" ]]; then
    if [[ -t 0 ]]; then
      read -r -p "ECS cluster ARN or name: " ECS_CLUSTER_VAL
    else
      echo "ERROR: Non-interactive deploy with --force-redeploy requires ECS_CLUSTER or ECS_CLUSTER_ARN." >&2
      exit 1
    fi
  fi
  if [[ -z "$ECS_SERVICE_VAL" ]]; then
    if [[ -t 0 ]]; then
      read -r -p "ECS service ARN or name: " ECS_SERVICE_VAL
    else
      echo "ERROR: Non-interactive deploy with --force-redeploy requires ECS_SERVICE or ECS_SERVICE_ARN." >&2
      exit 1
    fi
  fi
  if [[ -z "$ECS_CLUSTER_VAL" || -z "$ECS_SERVICE_VAL" ]]; then
    echo "ERROR: --force-redeploy requires ECS cluster and service (env or prompts)." >&2
    exit 1
  fi
}

collect_ecs_targets_for_redeploy

if [[ -z "${SKIP_EXPORT:-}" ]]; then
  echo "==> export_static.py --data"
  python3 export_static.py --data
else
  echo "==> SKIP_EXPORT set — using existing static_export/data/"
fi

echo "==> verify static_export/data"
verify_static_export

echo "==> docker build --platform ${DOCKER_PLATFORM} -t ${LOCAL_IMAGE}"
docker build --platform "$DOCKER_PLATFORM" -t "$LOCAL_IMAGE" .

if [[ -n "${SKIP_PUSH:-}" ]]; then
  echo "==> SKIP_PUSH set — not pushing to ECR"
  if [[ "$TAG_LATEST" -eq 1 ]]; then
    echo "NOTE: --tag-latest / ECR_PUSH_LATEST ignored when SKIP_PUSH is set." >&2
  fi
else
  echo "==> ECR login and push"
  AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
  ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}"

  aws ecr describe-repositories --repository-names "$ECR_REPOSITORY" --region "$AWS_REGION" &>/dev/null \
    || { echo "==> Creating ECR repository ${ECR_REPOSITORY}"; aws ecr create-repository --repository-name "$ECR_REPOSITORY" --region "$AWS_REGION" >/dev/null; }

  aws ecr get-login-password --region "$AWS_REGION" \
    | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

  docker tag "$LOCAL_IMAGE" "${ECR_URI}:${IMAGE_TAG}"
  docker push "${ECR_URI}:${IMAGE_TAG}"
  echo "==> Pushed ${ECR_URI}:${IMAGE_TAG}"

  if [[ "$TAG_LATEST" -eq 1 && "${IMAGE_TAG}" != "latest" ]]; then
    docker tag "$LOCAL_IMAGE" "${ECR_URI}:latest"
    docker push "${ECR_URI}:latest"
    echo "==> Pushed ${ECR_URI}:latest"
  elif [[ "$TAG_LATEST" -eq 1 && "${IMAGE_TAG}" == "latest" ]]; then
    echo "==> --tag-latest: IMAGE_TAG is already latest (single push)"
  fi
fi

if [[ "$FORCE_ECS_REDEPLOY" -eq 1 ]]; then
  echo "==> ECS force new deployment: cluster=${ECS_CLUSTER_VAL} service=${ECS_SERVICE_VAL}"
  aws ecs update-service \
    --region "$AWS_REGION" \
    --cluster "$ECS_CLUSTER_VAL" \
    --service "$ECS_SERVICE_VAL" \
    --force-new-deployment \
    --no-cli-pager >/dev/null
  echo "==> ECS update-service requested (tasks will roll)"
fi

if [[ -n "${SKIP_PUSH:-}" ]]; then
  echo "==> Done: local image ${LOCAL_IMAGE}"
else
  echo "==> Done: pushed ${ECR_URI}:${IMAGE_TAG}"
fi
