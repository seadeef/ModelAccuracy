#!/usr/bin/env bash
# Deploy the complete application in one command:
#   CloudFormation -> Lambda code -> data -> frontend.
#
# Stack outputs are exported directly to the child deploy scripts, so a first
# deployment does not require copying the block printed by deploy_infra.sh into
# .env before the remaining stages can run. .env is still loaded for operator
# inputs such as AWS_PROFILE, AWS_REGION, existing Cognito/DynamoDB resources,
# and frontend build keys.
#
# Optional stage skips (useful when only one layer changed):
#   SKIP_INFRA=1 SKIP_API=1 SKIP_DATA=1 SKIP_FRONTEND=1

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
source "$ROOT/deploy_common.sh"
deploy_init "$ROOT"

if [[ $# -gt 0 ]]; then
  echo "ERROR: $0 takes no arguments (configure it with environment variables)." >&2
  exit 2
fi

# Child scripts normally load .env themselves. We already loaded it and will
# overlay authoritative stack outputs below, so prevent a stale .env from
# replacing those outputs when each child starts.
export DEPLOY_SKIP_DOTENV=1

if [[ -z "${SKIP_INFRA:-}" ]]; then
  "$ROOT/deploy_infra.sh"
else
  echo "==> SKIP_INFRA set — using outputs from existing stack ${STACK_NAME}"
fi

# These are the complete interface between infrastructure and application
# deployment. Keep the output names in sync with infra/stack.yaml.
DATA_S3_URI="$(stack_output DataS3Uri)"
CLOUDFRONT_DISTRIBUTION_ID="$(stack_output DistributionId)"
LAMBDA_FUNCTION_NAME="$(stack_output ApiFunctionName)"
DYNAMODB_USER_ITEMS_TABLE="$(stack_output UserItemsTableName)"
COGNITO_USER_POOL_ID="$(stack_output UserPoolId)"
COGNITO_APP_CLIENT_ID="$(stack_output UserPoolClientId)"
COGNITO_OAUTH_BASE_URL="$(stack_output CognitoOauthBaseUrl)"
export DATA_S3_URI CLOUDFRONT_DISTRIBUTION_ID LAMBDA_FUNCTION_NAME
export DYNAMODB_USER_ITEMS_TABLE COGNITO_USER_POOL_ID
export COGNITO_APP_CLIENT_ID COGNITO_OAUTH_BASE_URL

for required in DATA_S3_URI CLOUDFRONT_DISTRIBUTION_ID LAMBDA_FUNCTION_NAME; do
  if [[ -z "${!required}" || "${!required}" == "None" ]]; then
    echo "ERROR: stack ${STACK_NAME} has no usable ${required} output." >&2
    exit 1
  fi
done

if [[ -z "${SKIP_API:-}" ]]; then
  "$ROOT/deploy_api.sh"
else
  echo "==> SKIP_API set"
fi

if [[ -z "${SKIP_DATA:-}" ]]; then
  "$ROOT/deploy_static.sh"
else
  echo "==> SKIP_DATA set"
fi

if [[ -z "${SKIP_FRONTEND:-}" ]]; then
  "$ROOT/deploy_frontend.sh"
else
  echo "==> SKIP_FRONTEND set"
fi

DOMAIN="$(stack_output DistributionDomain)"
echo "==> Complete: https://${DOMAIN}/"
