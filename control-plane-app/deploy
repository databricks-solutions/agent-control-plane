#!/usr/bin/env bash
set -euo pipefail

# ── AI Control Plane Deployment Script ───────────────────────
#
# Usage:
#   ./deploy.sh                        # Deploy using default CLI profile
#   ./deploy.sh --profile my-profile   # Deploy using a specific CLI profile
#
# Prerequisites:
#   1. Databricks CLI installed and authenticated
#   2. A Databricks App created in your workspace
#   3. A Lakebase instance created
#   4. Copy .env.example → .env and fill in your values
#
# See docs/installation.md for detailed setup instructions.

# ── Parse arguments ──────────────────────────────────────────
PROFILE_FLAG=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --profile) PROFILE_FLAG="--profile $2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

DB="databricks"

# ── Load configuration from .env ─────────────────────────────
if [ ! -f .env ]; then
  echo "Error: .env file not found. Copy .env.example to .env and fill in your values."
  exit 1
fi

# Parse .env safely (skip comments, handle values with spaces)
set -a
while IFS='=' read -r key value; do
  [[ -z "$key" || "$key" =~ ^# ]] && continue
  export "$key=$value"
done < .env
set +a

# Validate required variables
: "${LAKEBASE_DNS:?Set LAKEBASE_DNS in .env}"
: "${LAKEBASE_DATABASE:?Set LAKEBASE_DATABASE in .env}"
: "${LAKEBASE_INSTANCE:?Set LAKEBASE_INSTANCE in .env}"

# App name — update this to match your Databricks App name
APP_NAME="${APP_NAME:-ai-control-plane}"

# Workspace path — derived from your Databricks identity
WORKSPACE_USER=$($DB auth describe $PROFILE_FLAG 2>/dev/null | grep -i "user" | head -1 | awk '{print $NF}' || echo "")
if [ -z "$WORKSPACE_USER" ]; then
  echo "Error: Cannot determine workspace user. Make sure you are authenticated:"
  echo "  databricks auth login --host <your-workspace-url>"
  exit 1
fi
WORKSPACE_PATH="/Workspace/Users/${WORKSPACE_USER}/ai-control-plane/control-plane-app"

echo "Deploying $APP_NAME"
echo "  Workspace user: $WORKSPACE_USER"
echo "  Workspace path: $WORKSPACE_PATH"

# ── Generate app.yaml ────────────────────────────────────────
cat > app.yaml <<EOF
command:
  - "uvicorn"
  - "backend.main:app"
  - "--host"
  - "0.0.0.0"
  - "--port"
  - "8000"

env:
  - name: LAKEBASE_DNS
    value: "${LAKEBASE_DNS}"
  - name: LAKEBASE_DATABASE
    value: "${LAKEBASE_DATABASE}"
  - name: LAKEBASE_INSTANCE
    value: "${LAKEBASE_INSTANCE}"
  - name: DATABRICKS_ACCOUNT_ID
    value: "${DATABRICKS_ACCOUNT_ID:-}"

resources:
  - name: obo-auth
    description: "User authorization for OBO identity"
    permission: "all-apis"
EOF
echo "  Generated app.yaml"

# ── Build frontend ────────────────────────────────────────────
echo "Building frontend..."
(cd frontend && npm run build)
echo "  Built dist/"

# ── Clean build artifacts before upload ────────────────────────
find backend/ -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
rm -rf backend/.databricks 2>/dev/null || true
echo "  Cleaned __pycache__ and .databricks from backend/"

# ── Upload only runtime files ─────────────────────────────────
echo "Uploading dist/ ..."
$DB workspace import-dir dist "$WORKSPACE_PATH/dist" --overwrite $PROFILE_FLAG 2>/dev/null \
  || $DB sync dist "$WORKSPACE_PATH/dist" --watch=false --full $PROFILE_FLAG
echo "  dist/ uploaded"

echo "Uploading backend/ ..."
$DB workspace import-dir backend "$WORKSPACE_PATH/backend" --overwrite $PROFILE_FLAG 2>/dev/null \
  || $DB sync backend "$WORKSPACE_PATH/backend" --watch=false --full $PROFILE_FLAG
echo "  backend/ uploaded"

$DB workspace import "$WORKSPACE_PATH/app.yaml" --file app.yaml --format AUTO --overwrite $PROFILE_FLAG 2>/dev/null && echo "  app.yaml uploaded" || true

[ -f requirements.txt ] && {
  $DB workspace import "$WORKSPACE_PATH/requirements.txt" --file requirements.txt --format AUTO --overwrite $PROFILE_FLAG 2>/dev/null && echo "  requirements.txt uploaded" || true
}

# ── Deploy ────────────────────────────────────────────────────
echo "Deploying $APP_NAME ..."
$DB apps deploy "$APP_NAME" \
  --source-code-path "$WORKSPACE_PATH" \
  $PROFILE_FLAG \
  --no-wait

echo ""
echo "Deployment triggered. Monitor with:"
echo "  databricks apps get $APP_NAME $PROFILE_FLAG"
