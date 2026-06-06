#!/usr/bin/env bash
# Run grant_sp_lakebase.py as a one-shot Databricks job inside the workspace.
#
# Use this when the local machine cannot reach Lakebase Postgres directly
# (e.g. when the workspace blocks public PG access). The job runs on
# serverless compute as the deploying user, who is the Lakebase admin.
#
# Reads .env from the current directory for APP_NAME / LAKEBASE_* settings.
# Pass --profile <name> to use a non-default Databricks CLI profile.

set -euo pipefail

PROFILE_FLAG=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --profile) PROFILE_FLAG="--profile $2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

if [ ! -f .env ]; then
  echo "Error: .env not found in $(pwd). Run this from control-plane-app/."
  exit 1
fi

# Load .env
set -a
while IFS='=' read -r key value; do
  [[ -z "$key" || "$key" =~ ^# ]] && continue
  export "$key=$value"
done < .env
set +a

: "${APP_NAME:?Set APP_NAME in .env}"
: "${LAKEBASE_DNS:?Set LAKEBASE_DNS in .env}"
: "${LAKEBASE_DATABASE:?Set LAKEBASE_DATABASE in .env}"
LAKEBASE_INSTANCE="${LAKEBASE_INSTANCE:-}"
LAKEBASE_ENDPOINT_PATH="${LAKEBASE_ENDPOINT_PATH:-}"

DB="databricks"
WORKSPACE_USER=$($DB auth describe $PROFILE_FLAG 2>/dev/null | grep -i "user" | head -1 | awk '{print $NF}')
WORKSPACE_DIR="/Workspace/Users/${WORKSPACE_USER}/ai-control-plane/control-plane-app"
SCRIPT_PATH="${WORKSPACE_DIR}/grant_sp_lakebase.py"
NB_PATH="${WORKSPACE_DIR}/grant_sp_lakebase_notebook"

echo "Uploading grant_sp_lakebase.py ..."
$DB workspace import "$SCRIPT_PATH" \
  --file grant_sp_lakebase.py \
  --format AUTO --overwrite $PROFILE_FLAG

echo "Uploading grant_sp_lakebase_notebook.py ..."
$DB workspace import "$NB_PATH" \
  --file grant_sp_lakebase_notebook.py \
  --format SOURCE --language PYTHON --overwrite $PROFILE_FLAG

JOB_JSON=$(cat <<EOF
{
  "run_name": "ai-control-plane: grant_sp_lakebase",
  "tasks": [
    {
      "task_key": "grant_sp_lakebase",
      "notebook_task": {
        "notebook_path": "${NB_PATH}",
        "base_parameters": {
          "app_name": "${APP_NAME}",
          "lakebase_dns": "${LAKEBASE_DNS}",
          "lakebase_database": "${LAKEBASE_DATABASE}",
          "lakebase_instance": "${LAKEBASE_INSTANCE}",
          "lakebase_endpoint_path": "${LAKEBASE_ENDPOINT_PATH}",
          "script_path": "${SCRIPT_PATH}"
        }
      },
      "environment_key": "default"
    }
  ],
  "environments": [
    {
      "environment_key": "default",
      "spec": {
        "client": "1",
        "dependencies": ["databricks-sdk>=0.40.0", "psycopg2-binary", "requests"]
      }
    }
  ]
}
EOF
)

echo "Submitting one-shot job ..."
RESULT=$($DB jobs submit --json "$JOB_JSON" $PROFILE_FLAG -o json)
echo "$RESULT" | python3 -c "import sys,json
d=json.load(sys.stdin)
print('  run_id:', d.get('run_id'))
print('  result:', d.get('state',{}).get('result_state'))
print('  life_cycle:', d.get('state',{}).get('life_cycle_state'))
print('  message:', d.get('state',{}).get('state_message',''))"

RUN_ID=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('run_id',''))")

echo ""
echo "Task output:"
TASK_RUN_ID=$($DB jobs get-run "$RUN_ID" $PROFILE_FLAG -o json 2>&1 | python3 -c "import sys,json
d=json.load(sys.stdin); ts=d.get('tasks',[]); print(ts[0].get('run_id','') if ts else '')")
$DB jobs get-run-output "$TASK_RUN_ID" $PROFILE_FLAG -o json 2>&1 | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin)
  if d.get('error'): print('--- error ---'); print(d['error'])
  if d.get('error_trace'):
    import re
    cleaned = re.sub(r'\\x1b\\[[0-9;]*m','',d['error_trace'])
    print('--- error_trace ---'); print(cleaned[-1500:])
  if d.get('logs'): print('--- logs ---'); print(d['logs'][-1500:])
  if d.get('notebook_output',{}).get('result'): print('--- notebook result ---'); print(d['notebook_output']['result'])
except Exception as e: print('parse err:', e)
" || true

echo ""
echo "Run finished. Inspect with:"
echo "  databricks jobs get-run $RUN_ID $PROFILE_FLAG"
