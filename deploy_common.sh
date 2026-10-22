#!/usr/bin/env bash
# Shared environment and CloudFormation-output helpers for independently runnable deploy layers.

deploy_init() {
  local root="$1"
  if [[ -z "${DEPLOY_SKIP_DOTENV:-}" && -f "$root/.env" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$root/.env"
    set +a
  fi
  AWS_REGION="${AWS_REGION:-us-west-1}"
  STACK_NAME="${STACK_NAME:-raincheck}"
  export AWS_REGION STACK_NAME
  if [[ -n "${AWS_PROFILE:-}" ]]; then
    export AWS_PROFILE
    echo "==> Using AWS_PROFILE=${AWS_PROFILE}"
  fi
}

stack_output() {
  aws cloudformation describe-stacks --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue | [0]" \
    --output text
}
