#!/usr/bin/env bash
set -euo pipefail

# Propagate the app's service principal to all workspaces in the account
# via an account-level admin group. This ensures the SP has workspace admin
# privileges for cross-workspace permission management.
#
# Strategy:
#   1. Ensure an account-level group "control-plane-admins" exists
#   2. Add the app's SP to the group
#   3. Assign the group to every workspace as USER first, then upgrade to ADMIN
#      (direct ADMIN assignment without prior USER doesn't persist)
#
# Prerequisites:
#   - databricks CLI authenticated with an account admin profile
#   - The app must be deployed
#
# Usage:
#   export APP_NAME=ai-control-plane
#   export WORKSPACE_PROFILE=default          # CLI profile for the app's workspace
#   export ACCOUNT_PROFILE=ACCOUNT_OAUTH      # CLI profile for account-level ops
#   export DATABRICKS_ACCOUNT_ID=<your-account-id>
#   bash propagate-sp.sh [--parallel N]

APP_NAME="${APP_NAME:?Set APP_NAME (e.g. ai-control-plane)}"
WORKSPACE_PROFILE="${WORKSPACE_PROFILE:-default}"
ACCOUNT_PROFILE="${ACCOUNT_PROFILE:?Set ACCOUNT_PROFILE (account-level CLI profile)}"
ACCOUNT_ID="${DATABRICKS_ACCOUNT_ID:?Set DATABRICKS_ACCOUNT_ID}"
GROUP_NAME="${GROUP_NAME:-control-plane-admins}"
PARALLEL="${2:-10}"

echo "=== SP Propagation for $APP_NAME ==="

# Step 1: Get the app SP
echo "Looking up app service principal..."
SP_INFO=$(databricks apps get "$APP_NAME" --profile "$WORKSPACE_PROFILE" 2>/dev/null)
SP_PRINCIPAL_ID=$(echo "$SP_INFO" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(data.get('service_principal_id', ''))
" 2>/dev/null || true)

SP_DISPLAY=$(echo "$SP_INFO" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(data.get('service_principal_name', '') or data.get('service_principal_client_id', ''))
" 2>/dev/null || true)

if [ -z "$SP_PRINCIPAL_ID" ]; then
  echo "ERROR: Could not resolve SP principal_id from app '$APP_NAME'"
  exit 1
fi
echo "  SP: $SP_PRINCIPAL_ID ($SP_DISPLAY)"

# Step 2: Ensure account-level group exists
echo "Ensuring account-level group '$GROUP_NAME'..."
GROUP_ID=$(databricks api get "/api/2.0/accounts/${ACCOUNT_ID}/scim/v2/Groups?filter=displayName+eq+${GROUP_NAME}" \
  --profile "$ACCOUNT_PROFILE" 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
resources = data.get('Resources', [])
print(resources[0]['id'] if resources else '')
" 2>/dev/null || true)

if [ -z "$GROUP_ID" ]; then
  echo "  Creating group..."
  GROUP_ID=$(databricks api post "/api/2.0/accounts/${ACCOUNT_ID}/scim/v2/Groups" \
    --json "{\"displayName\": \"${GROUP_NAME}\", \"schemas\": [\"urn:ietf:params:scim:schemas:core:2.0:Group\"]}" \
    --profile "$ACCOUNT_PROFILE" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  echo "  Created group: $GROUP_ID"
else
  echo "  Group exists: $GROUP_ID"
fi

# Step 3: Add SP to group
echo "Adding SP to group..."
databricks api patch "/api/2.0/accounts/${ACCOUNT_ID}/scim/v2/Groups/${GROUP_ID}" \
  --json "{\"schemas\": [\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"], \"Operations\": [{\"op\": \"add\", \"value\": {\"members\": [{\"value\": \"${SP_PRINCIPAL_ID}\"}]}}]}" \
  --profile "$ACCOUNT_PROFILE" > /dev/null 2>&1
echo "  SP added to $GROUP_NAME"

# Step 4: Get all workspace IDs
echo "Fetching workspace list..."
WS_JSON=$(databricks api get "/api/2.0/accounts/${ACCOUNT_ID}/workspaces" --profile "$ACCOUNT_PROFILE" 2>/dev/null)
WS_COUNT=$(echo "$WS_JSON" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null)
echo "  Found $WS_COUNT workspaces"

# Step 5: Assign group to each workspace (USER first, then ADMIN)
echo "Assigning group to workspaces as ADMIN (parallel=$PARALLEL)..."
echo "$WS_JSON" | python3 -c "
import sys, json, subprocess, concurrent.futures, time

data = json.load(sys.stdin)
group_id = '$GROUP_ID'
account_id = '$ACCOUNT_ID'
profile = '$ACCOUNT_PROFILE'
parallel = int('$PARALLEL')

def assign_group(ws):
    wid = str(ws.get('workspace_id', ''))
    wname = ws.get('workspace_name', wid)
    if not wid:
        return ('skip', wname, '')

    base = f'/api/2.0/accounts/{account_id}/workspaces/{wid}/permissionassignments/principals/{group_id}'
    try:
        # Step A: Assign as USER first (required for ADMIN to stick)
        r1 = subprocess.run(
            ['databricks', 'api', 'put', base,
             '--json', '{\"permissions\": [\"USER\"]}',
             '--profile', profile],
            capture_output=True, text=True, timeout=30
        )
        if r1.returncode != 0 and 'not available' in r1.stderr.lower():
            return ('fail', wname, 'Permission APIs not available')

        # Step B: Upgrade to ADMIN
        r2 = subprocess.run(
            ['databricks', 'api', 'put', base,
             '--json', '{\"permissions\": [\"USER\", \"ADMIN\"]}',
             '--profile', profile],
            capture_output=True, text=True, timeout=30
        )
        if r2.returncode == 0:
            return ('ok', wname, '')
        return ('fail', wname, r2.stderr[:100])
    except subprocess.TimeoutExpired:
        return ('timeout', wname, '')
    except Exception as e:
        return ('fail', wname, str(e))

ok = 0
fail = 0
start = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as executor:
    futures = {executor.submit(assign_group, ws): ws for ws in data}
    for future in concurrent.futures.as_completed(futures):
        status, info, detail = future.result()
        if status == 'ok':
            ok += 1
        elif status == 'skip':
            pass
        else:
            fail += 1
            if fail <= 5:
                print(f'  FAIL: {info}: {detail}')

elapsed = time.time() - start
print(f'Done in {elapsed:.0f}s: {ok} assigned as ADMIN, {fail} failed')
"

echo "=== SP Propagation complete ==="
