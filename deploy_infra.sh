#!/usr/bin/env bash
# One-command infrastructure setup/update for the raincheck serverless stack.
#
# Creates (or idempotently updates) everything in infra/stack.yaml via a single
# CloudFormation stack: S3 bucket, CloudFront distribution (S3 default origin +
# /api/* → Lambda Function URL), the API Lambda (placeholder code until
# ./deploy_api.sh), DynamoDB user-items table, Cognito user pool/client/domain,
# and the EventBridge warm ping. Safe to re-run any time; unchanged stacks are
# a no-op.
#
# For a complete first-time or repeat application deploy, use ./deploy.sh.
# Running this infrastructure layer by itself is useful when changing only the
# stack. Moving to a new region or account manually is:
#   1. point AWS_PROFILE / AWS_REGION (env or .env) at the target
#   2. ./deploy_infra.sh
#   3. paste the printed block into .env (./deploy.sh does this propagation
#      in-process instead)
#   4. ./deploy_api.sh && ./deploy_static.sh && ./deploy_frontend.sh
#
# The origin-verify secret (CloudFront → Lambda shared header) is generated
# once and kept in SSM Parameter Store (free tier) at
#   /<stack>/origin-verify-secret
# so re-runs never rotate it out from under the running distribution.
#
# When the stack CREATES the Cognito user pool, the OAuth callback URL needs
# the CloudFront domain, which doesn't exist until after the first deploy —
# the script detects this and automatically runs a fast second pass that adds
# it. (Chicken-and-egg is CloudFront's, not yours.)
#
# Env (loaded from .env if present):
#   AWS_REGION                      default us-west-1
#   AWS_PROFILE                     optional
#   STACK_NAME                      default raincheck (lowercase letters/digits/
#                                   hyphens only — reused in bucket + Cognito
#                                   domain names)
#   BUCKET_NAME                     optional explicit bucket name (must NOT
#                                   already exist; default auto-names
#                                   <stack>-static-<account-id>)
#   WARM_PING=false                 disable the 5-min warm ping
#   LAMBDA_MEMORY_MB                default 1024
#
# Reusing existing resources (e.g. keep current users when rebuilding infra
# in the same account):
#   EXISTING_USER_POOL_ID           existing Cognito pool (skips pool creation)
#   EXISTING_USER_POOL_CLIENT_ID    required with EXISTING_USER_POOL_ID
#   EXISTING_COGNITO_OAUTH_BASE_URL required with EXISTING_USER_POOL_ID
#   EXISTING_USER_ITEMS_TABLE       existing DynamoDB table name (skips table)
#
# Usage:
#   ./deploy_infra.sh              # create or update
#   STACK_NAME=raincheck-eu AWS_REGION=eu-west-1 ./deploy_infra.sh
#
# Teardown: aws cloudformation delete-stack --stack-name <stack>
#   (bucket, DynamoDB table, and user pool are Retain — delete manually after
#   copying anything you need).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
source "$ROOT/deploy_common.sh"
deploy_init "$ROOT"
TEMPLATE="$ROOT/infra/stack.yaml"
echo "==> Stack ${STACK_NAME} in ${AWS_REGION}"

if [[ ! "$STACK_NAME" =~ ^[a-z][a-z0-9-]*$ ]]; then
  echo "ERROR: STACK_NAME must be lowercase letters/digits/hyphens (it names the" >&2
  echo "       S3 bucket and the Cognito hosted-UI domain)." >&2
  exit 1
fi

if [[ -n "${EXISTING_USER_POOL_ID:-}" ]]; then
  if [[ -z "${EXISTING_USER_POOL_CLIENT_ID:-}" || -z "${EXISTING_COGNITO_OAUTH_BASE_URL:-}" ]]; then
    echo "ERROR: EXISTING_USER_POOL_ID needs EXISTING_USER_POOL_CLIENT_ID and" >&2
    echo "       EXISTING_COGNITO_OAUTH_BASE_URL as well." >&2
    exit 1
  fi
fi

# ── origin-verify secret: generate once, keep in SSM (free) ─────────────────
SECRET_PARAM="/${STACK_NAME}/origin-verify-secret"
if ! aws ssm get-parameter --region "$AWS_REGION" --name "$SECRET_PARAM" \
      --with-decryption >/dev/null 2>&1; then
  echo "==> Generating origin-verify secret → SSM ${SECRET_PARAM}"
  aws ssm put-parameter --region "$AWS_REGION" --name "$SECRET_PARAM" \
    --type SecureString --value "$(openssl rand -hex 24)" >/dev/null
fi
ORIGIN_VERIFY_SECRET="$(aws ssm get-parameter --region "$AWS_REGION" \
  --name "$SECRET_PARAM" --with-decryption \
  --query 'Parameter.Value' --output text)"

# ── current callback domain (empty on first create; set on re-runs) ─────────
stack_output() {
  aws cloudformation describe-stacks --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
    --output text 2>/dev/null || true
}

CALLBACK_DOMAIN=""
EXISTING_DOMAIN="$(stack_output DistributionDomain)"
if [[ -n "$EXISTING_DOMAIN" && "$EXISTING_DOMAIN" != "None" ]]; then
  CALLBACK_DOMAIN="$EXISTING_DOMAIN"
fi

deploy() {
  aws cloudformation deploy --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --template-file "$TEMPLATE" \
    --capabilities CAPABILITY_IAM \
    --no-fail-on-empty-changeset \
    --parameter-overrides \
      "OriginVerifySecret=${ORIGIN_VERIFY_SECRET}" \
      "BucketName=${BUCKET_NAME:-}" \
      "WarmPing=${WARM_PING:-true}" \
      "LambdaMemoryMb=${LAMBDA_MEMORY_MB:-1024}" \
      "AppCallbackDomain=${CALLBACK_DOMAIN}" \
      "ExistingUserPoolId=${EXISTING_USER_POOL_ID:-}" \
      "ExistingUserPoolClientId=${EXISTING_USER_POOL_CLIENT_ID:-}" \
      "ExistingCognitoOauthBaseUrl=${EXISTING_COGNITO_OAUTH_BASE_URL:-}" \
      "ExistingUserItemsTable=${EXISTING_USER_ITEMS_TABLE:-}"
}

echo "==> cloudformation deploy (CloudFront creation takes ~5-10 min on first run)"
deploy

DOMAIN="$(stack_output DistributionDomain)"

# Second pass only when the pool is stack-created and the callback URL doesn't
# know the (just-created) distribution domain yet.
if [[ -z "${EXISTING_USER_POOL_ID:-}" && "$CALLBACK_DOMAIN" != "$DOMAIN" ]]; then
  echo "==> Second pass: adding https://${DOMAIN}/ to the Cognito callback URLs"
  CALLBACK_DOMAIN="$DOMAIN"
  deploy
fi

# ── print outputs + ready-to-paste .env block ────────────────────────────────
DIST_ID="$(stack_output DistributionId)"
BUCKET="$(stack_output BucketName)"
DATA_URI="$(stack_output DataS3Uri)"
FN_NAME="$(stack_output ApiFunctionName)"
FN_URL="$(stack_output ApiFunctionUrl)"
POOL_ID="$(stack_output UserPoolId)"
CLIENT_ID="$(stack_output UserPoolClientId)"
OAUTH_BASE="$(stack_output CognitoOauthBaseUrl)"
TABLE="$(stack_output UserItemsTableName)"

cat <<EOF

==> Stack ${STACK_NAME} is up.

    Site:          https://${DOMAIN}/
    Function URL:  ${FN_URL}   (direct calls 403 by design; /health is open)
    Bucket:        s3://${BUCKET}

--- paste into .env (deploy scripts + backend read these) -------------------
AWS_REGION=${AWS_REGION}
DATA_S3_URI=${DATA_URI}
CLOUDFRONT_DISTRIBUTION_ID=${DIST_ID}
LAMBDA_FUNCTION_NAME=${FN_NAME}
DYNAMODB_USER_ITEMS_TABLE=${TABLE}
COGNITO_USER_POOL_ID=${POOL_ID}
COGNITO_APP_CLIENT_ID=${CLIENT_ID}
COGNITO_OAUTH_BASE_URL=${OAUTH_BASE}
-----------------------------------------------------------------------------

Next (in order, all idempotent):
    ./deploy_api.sh        # build + push the Lambda code
    ./deploy_static.sh     # data: model_data, boundaries, tiles, forecast → S3
    ./deploy_frontend.sh   # SPA → S3
EOF
