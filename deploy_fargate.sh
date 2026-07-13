#!/usr/bin/env bash
# Build the API Docker image (export_static.py --image → verify → build) and push to Amazon ECR.
#
# Prerequisites: Docker, AWS CLI v2, Python 3 (for export_static.py), credentials with ECR push.
#
# Environment (optional):
#   AWS_REGION       default us-west-1
#   ECR_REPOSITORY   ECR repo name (if unset + interactive push, you are prompted; else default modelaccuracy-api)
#   IMAGE_TAG        default: an immutable per-build tag derived from git
#                    (git-<shortsha>, plus -dirty-<timestamp> when the tree has
#                    uncommitted changes; build-<timestamp> outside a git repo).
#                    Immutable tags are what make --force-redeploy deterministic:
#                    the task def is pinned to this exact tag, so the running
#                    version can't drift the way a mutable :latest does. Override
#                    to pin a specific tag (e.g. IMAGE_TAG=v1.2.3).
#   LOCAL_IMAGE      default modelaccuracy-api:build  (local name before tag to ECR)
#   DOCKER_PLATFORM  default linux/amd64 (Fargate x86). Use linux/arm64 for Graviton Fargate
#                    or faster local builds on Apple Silicon when SKIP_PUSH=1.
#   SKIP_EXPORT=1    skip python3 export_static.py --image (use existing static_export/)
#   SKIP_PUSH=1      build only; do not login/push to ECR
#   ECR_PUSH_LATEST=1   also tag and push :latest (in addition to IMAGE_TAG), same as --tag-latest
#   ECS_FORCE_REDEPLOY=1  after push (or alone with SKIP_PUSH, if the tag already
#                    exists in ECR), register a new task-definition revision pinned
#                    to ECR_REPOSITORY:IMAGE_TAG and roll the service onto it;
#                    same as --force-redeploy.
#   ECS_CLUSTER / ECS_CLUSTER_ARN   only used with --force-redeploy; prompted before data export if unset
#   ECS_SERVICE / ECS_SERVICE_ARN   only used with --force-redeploy; prompted before data export if unset
#
# ECS Task Definition environment variables (set in AWS, not this script):
#   DATA_S3_URI                 s3://bucket[/prefix] for stats data. Forecasts are read
#                               from the same bucket at a `forecast/` folder that is a
#                               sibling of <prefix> (keys: forecast/{model}/{variable}/lead_{n}.bin,
#                               forecast/forecast_calendar.json). NOTE: the backend
#                               image must match this per-variable layout — a stale
#                               image reading the old flat forecast/{model}/lead_{n}.bin
#                               path returns 200 with all no_data.
#   COGNITO_USER_POOL_ID        Cognito User Pool ID (required for auth)
#   COGNITO_APP_CLIENT_ID       Cognito App Client ID (required for auth)
#   COGNITO_REGION              Cognito region (defaults to AWS_REGION / us-west-1)
#   COGNITO_DOMAIN_PREFIX       Hosted UI domain label (*.auth.<region>.amazoncognito.com)
#   COGNITO_OAUTH_BASE_URL      Optional full https origin for Hosted UI (overrides prefix+region; useful for demos)
#   DYNAMODB_USER_ITEMS_TABLE   DynamoDB table for saved shapes (required, no default)
#
# Flags:
#   --tag-latest       push IMAGE_TAG and also push the same image as :latest
#   --force-redeploy   register a new task-def revision pinned to ECR_REPOSITORY:IMAGE_TAG
#                      and roll the service onto it (needs cluster + service). Unlike a
#                      bare force-new-deployment, this pins an immutable image so the
#                      running version is deterministic and can't silently drift.
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

# Load .env (AWS_PROFILE, AWS_REGION, …) so `aws` CLI calls below pick up the
# same credentials the Python code uses. Lines starting with `#` are skipped.
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

AWS_REGION="${AWS_REGION:-us-west-1}"
COGNITO_REGION="${COGNITO_REGION:-$AWS_REGION}"
if [[ -n "${AWS_PROFILE:-}" ]]; then
  export AWS_PROFILE
  echo "==> Using AWS_PROFILE=${AWS_PROFILE}"
fi
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
# 8-char digest of the model_data bins baked into the image (paths + sizes +
# mtimes), so refreshed stats produce a distinct immutable tag even at the same
# git SHA (a stats-only redeploy). Portable across BSD (macOS) and GNU stat.
_model_data_digest() {
  local dir="$ROOT/static_export/model_data"
  [[ -d "$dir" ]] || { printf 'nodata'; return; }
  local statcmd
  if stat -f '%z' "$dir" >/dev/null 2>&1; then
    statcmd=(stat -f '%N %z %m')   # BSD / macOS
  else
    statcmd=(stat -c '%n %s %Y')   # GNU
  fi
  { find "$dir" -type f 2>/dev/null | LC_ALL=C sort | tr '\n' '\0' \
      | xargs -0 "${statcmd[@]}" 2>/dev/null; } \
    | { shasum -a 256 2>/dev/null || sha256sum; } | cut -c1-8
}

# Default to an immutable per-build tag so a redeploy pins an exact image rather
# than a mutable :latest (whose "pushed" and "running" states can silently
# diverge). Clean tree → git-<sha>-data<digest>; dirty tree (incl. untracked) →
# git-<sha>-dirty-<ts>; non-git checkout → build-<ts>.
if [[ -z "${IMAGE_TAG:-}" ]]; then
  if _sha="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null)"; then
    _dirty=0
    git -C "$ROOT" diff --quiet 2>/dev/null || _dirty=1
    git -C "$ROOT" diff --cached --quiet 2>/dev/null || _dirty=1
    [[ -n "$(git -C "$ROOT" ls-files --others --exclude-standard 2>/dev/null)" ]] && _dirty=1
    if [[ "$_dirty" -eq 1 ]]; then
      IMAGE_TAG="git-${_sha}-dirty-$(date +%Y%m%d%H%M%S)"
    else
      IMAGE_TAG="git-${_sha}-data$(_model_data_digest)"
    fi
  else
    IMAGE_TAG="build-$(date +%Y%m%d%H%M%S)"
  fi
fi
echo "==> IMAGE_TAG=${IMAGE_TAG}"
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

# Resolve the full ECR image URI (account-qualified). Reused by push and redeploy.
ensure_ecr_uri() {
  if [[ -n "${ECR_URI:-}" ]]; then return 0; fi
  AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
  ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}"
}

# Idempotently keep ECR from filling up with immutable per-build tags: retain the
# last 10 git-* images, expire untagged after 1 day. put-lifecycle-policy
# overwrites, so this is safe to run on every push. (Documented in README.)
ensure_ecr_lifecycle_policy() {
  aws ecr put-lifecycle-policy --region "$AWS_REGION" \
    --repository-name "$ECR_REPOSITORY" \
    --lifecycle-policy-text '{"rules":[{"rulePriority":1,"description":"keep last 10 git-* tags","selection":{"tagStatus":"tagged","tagPrefixList":["git-"],"countType":"imageCountMoreThan","countNumber":10},"action":{"type":"expire"}},{"rulePriority":2,"description":"expire untagged after 1 day","selection":{"tagStatus":"untagged","countType":"sinceImagePushed","countUnit":"days","countNumber":1},"action":{"type":"expire"}}]}' \
    >/dev/null 2>&1 || echo "WARN: could not set ECR lifecycle policy (non-fatal)" >&2
}

# Register a new task-definition revision pinned to ECR_REPOSITORY:IMAGE_TAG and
# roll the service onto it. Pinning an immutable tag (not :latest) is what makes
# the deployed version deterministic: `aws ecs update-service --force-new-deployment`
# against a :latest-pinned task def re-pulls whatever :latest currently resolves
# to, so "pushed a new image" and "running the new image" can silently diverge.
deploy_pinned_taskdef() {
  ensure_ecr_uri
  local pinned_image="${ECR_URI}:${IMAGE_TAG}"

  # The pinned image must actually exist in ECR (it may not under SKIP_PUSH).
  if ! aws ecr describe-images --region "$AWS_REGION" \
        --repository-name "$ECR_REPOSITORY" --image-ids imageTag="$IMAGE_TAG" \
        >/dev/null 2>&1; then
    echo "ERROR: ${ECR_REPOSITORY}:${IMAGE_TAG} not found in ECR. Push it first" >&2
    echo "       (drop SKIP_PUSH) or set IMAGE_TAG to a tag that already exists." >&2
    exit 1
  fi

  local current_arn
  current_arn="$(aws ecs describe-services --region "$AWS_REGION" \
    --cluster "$ECS_CLUSTER_VAL" --services "$ECS_SERVICE_VAL" \
    --query 'services[0].taskDefinition' --output text)"
  if [[ -z "$current_arn" || "$current_arn" == "None" ]]; then
    echo "ERROR: could not resolve current task definition for service ${ECS_SERVICE_VAL}." >&2
    exit 1
  fi
  echo "==> Current task definition: ${current_arn}"

  # Guard against a CPU-arch mismatch: an arm64 image pinned into an X86_64 task
  # def (or vice-versa) fails at runtime with CannotPullContainerError. Derive the
  # image arch from DOCKER_PLATFORM and compare to the task def's runtimePlatform
  # (which defaults to X86_64 when unset). This must pass before any Graviton
  # cutover — flip DOCKER_PLATFORM=linux/arm64 only after the task def is ARM64.
  local image_arch=""
  case "$DOCKER_PLATFORM" in
    *arm64*) image_arch="ARM64" ;;
    *amd64*|*x86_64*) image_arch="X86_64" ;;
  esac
  local td_arch
  td_arch="$(aws ecs describe-task-definition --region "$AWS_REGION" \
    --task-definition "$current_arn" \
    --query 'taskDefinition.runtimePlatform.cpuArchitecture' --output text 2>/dev/null)"
  [[ -z "$td_arch" || "$td_arch" == "None" ]] && td_arch="X86_64"
  if [[ -n "$image_arch" && "$image_arch" != "$td_arch" ]]; then
    echo "ERROR: image arch ${image_arch} (DOCKER_PLATFORM=${DOCKER_PLATFORM}) != task-def runtimePlatform ${td_arch}." >&2
    echo "       Set the task def's runtimePlatform.cpuArchitecture to ${image_arch}, or rebuild with a matching DOCKER_PLATFORM." >&2
    exit 1
  fi

  # Take the running task def, strip the read-only fields register-task-definition
  # rejects, and repoint the app container's image at the pinned tag.
  local new_td_file
  new_td_file="$(mktemp)"
  aws ecs describe-task-definition --region "$AWS_REGION" \
      --task-definition "$current_arn" --query 'taskDefinition' --output json \
    | IMAGE_URI="$pinned_image" ECR_REPOSITORY="$ECR_REPOSITORY" python3 -c '
import json, os, sys
td = json.load(sys.stdin)
for k in ("taskDefinitionArn", "revision", "status", "requiresAttributes",
          "compatibilities", "registeredAt", "registeredBy", "deregisteredAt"):
    td.pop(k, None)
repo = os.environ["ECR_REPOSITORY"]
new_image = os.environ["IMAGE_URI"]
changed = 0
for c in td.get("containerDefinitions", []):
    img = c.get("image", "")
    if ("/" + repo + ":") in img or img.rsplit(":", 1)[0].endswith("/" + repo):
        c["image"] = new_image
        changed += 1
if changed != 1:
    sys.stderr.write(
        "ERROR: expected exactly one container image referencing repo %r, found %d\n"
        % (repo, changed))
    sys.exit(1)
json.dump(td, sys.stdout)
' > "$new_td_file"

  local new_arn
  new_arn="$(aws ecs register-task-definition --region "$AWS_REGION" \
    --cli-input-json "file://${new_td_file}" \
    --query 'taskDefinition.taskDefinitionArn' --output text)"
  rm -f "$new_td_file"
  echo "==> Registered ${new_arn} (image ${pinned_image})"

  aws ecs update-service --region "$AWS_REGION" \
    --cluster "$ECS_CLUSTER_VAL" --service "$ECS_SERVICE_VAL" \
    --task-definition "$new_arn" --no-cli-pager >/dev/null
  echo "==> ECS update-service → ${new_arn} (tasks will roll to ${IMAGE_TAG})"
}

# Set when FORCE_ECS_REDEPLOY; collected immediately before export_static.py --image (see below).
ECS_CLUSTER_VAL=""
ECS_SERVICE_VAL=""

verify_static_export() {
  local DATA="$ROOT/static_export/model_data"
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
  local BOUNDARIES="$ROOT/static_export/admin/boundaries.json"
  if [[ ! -f "$BOUNDARIES" ]]; then
    echo "ERROR: $BOUNDARIES missing — run export_static.py --image." >&2
    exit 1
  fi
  local WBD_CHUNKS="$ROOT/static_export/wbd/chunks/2/_root.json"
  if [[ ! -f "$WBD_CHUNKS" ]]; then
    echo "ERROR: $WBD_CHUNKS missing — run export_static.py --image (after load_boundaries.py --wbd or --wbd-sample)." >&2
    exit 1
  fi
  local MILITARY="$ROOT/static_export/military/boundaries.json"
  if [[ ! -f "$MILITARY" ]]; then
    echo "ERROR: $MILITARY missing — run export_static.py --image (after load_boundaries.py --military or --military-sample)." >&2
    exit 1
  fi
  echo "OK: static_export/model_data contains grid.json; admin/military boundaries.json and wbd/chunks present"
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
  echo "==> export_static.py --image"
  python3 export_static.py --image
else
  echo "==> SKIP_EXPORT set — using existing static_export/"
fi

echo "==> verify static_export (Docker image inputs)"
verify_static_export

echo "==> docker build --platform ${DOCKER_PLATFORM} -t ${LOCAL_IMAGE}"
docker build --platform "$DOCKER_PLATFORM" -t "$LOCAL_IMAGE" \
  --build-arg DATA_S3_URI="${DATA_S3_URI:-}" \
  --build-arg COGNITO_USER_POOL_ID="${COGNITO_USER_POOL_ID:-}" \
  --build-arg COGNITO_APP_CLIENT_ID="${COGNITO_APP_CLIENT_ID:-}" \
  --build-arg COGNITO_REGION="${COGNITO_REGION:-}" \
  --build-arg COGNITO_DOMAIN_PREFIX="${COGNITO_DOMAIN_PREFIX:-}" \
  --build-arg COGNITO_OAUTH_BASE_URL="${COGNITO_OAUTH_BASE_URL:-}" \
  --build-arg DYNAMODB_USER_ITEMS_TABLE="${DYNAMODB_USER_ITEMS_TABLE:-}" \
  .

if [[ -n "${SKIP_PUSH:-}" ]]; then
  echo "==> SKIP_PUSH set — not pushing to ECR"
  if [[ "$TAG_LATEST" -eq 1 ]]; then
    echo "NOTE: --tag-latest / ECR_PUSH_LATEST ignored when SKIP_PUSH is set." >&2
  fi
else
  echo "==> ECR login and push"
  ensure_ecr_uri

  aws ecr describe-repositories --repository-names "$ECR_REPOSITORY" --region "$AWS_REGION" &>/dev/null \
    || { echo "==> Creating ECR repository ${ECR_REPOSITORY}"; aws ecr create-repository --repository-name "$ECR_REPOSITORY" --region "$AWS_REGION" >/dev/null; }
  ensure_ecr_lifecycle_policy

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
  echo "==> ECS redeploy (pinned): cluster=${ECS_CLUSTER_VAL} service=${ECS_SERVICE_VAL}"
  deploy_pinned_taskdef
fi

if [[ -n "${SKIP_PUSH:-}" ]]; then
  echo "==> Done: local image ${LOCAL_IMAGE}"
else
  echo "==> Done: pushed ${ECR_URI}:${IMAGE_TAG}"
fi
