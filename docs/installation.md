# Installation Guide

This guide walks you through deploying the AI Control Plane to your Databricks workspace.

## Prerequisites

- **Databricks workspace** with Unity Catalog enabled
- **Databricks CLI** installed and authenticated (`pip install databricks-cli`)
- **Node.js** 18+ (for building the frontend)
- **Python** 3.10+

## Step 1: Create a Lakebase Instance

The Control Plane uses Lakebase (PostgreSQL) for fast dashboard reads.

Lakebase comes in two modes: **Autoscaling** (the default for all new instances, scales to zero) and **Provisioned** (always-on). This app supports both. Autoscaling is recommended — the workload is bursty (workflow writes every 30 min + intermittent dashboard reads), so scale-to-zero is a natural fit.

### Autoscaling (recommended)

1. In your workspace, go to **SQL** > **Lakebase** > **Create Instance**
2. Name it (e.g., `ai-control-plane-db`)
3. Note these values from the instance detail page:
   - **DNS hostname** — e.g., `ep-xxxxxxxx.database.us-east-1.cloud.databricks.com`
   - **Endpoint path** — shape: `projects/<name>/branches/<branch>/endpoints/<endpoint>`. The default branch is `production` and default endpoint is `primary`, so for an instance named `ai-control-plane-db` the path is `projects/ai-control-plane-db/branches/production/endpoints/primary`.
   - **Database name** — create `control_plane` in the instance

### Provisioned (legacy)

Still supported for existing deployments. Note the **Instance name** (e.g., `ai-control-plane-db`), **DNS hostname**, and **database name**.

> **Tip**: Create a **dedicated** Lakebase instance for this app. The control plane owns its own schema (`discovered_agents`, `gateway_usage_daily`, `tool_registry`, etc.) — sharing an instance with unrelated workloads is messy.

## Step 2: Create a SQL Warehouse

The scheduled workflows need a SQL warehouse to query system tables.

1. Go to **SQL** > **SQL Warehouses**
2. Use an existing serverless warehouse or create one
3. Note the **Warehouse ID** (visible in the URL or warehouse detail page)

## Step 3: Create a Databricks App

> **Enable OBO on the account first.** Before creating the app, make sure *User authorization with OAuth* (also known as user-token passthrough) is enabled at the **account level**. If you create the app *before* the feature is enabled, the app's OAuth integration is provisioned in a pre-feature state and scope writes silently fail — the UI will show "scopes updated" but they won't persist on refresh. The only fix is to delete and recreate the app after the feature is on. Save yourself the round-trip and enable it first.

1. Go to **Apps** > **Create App**
2. Name it (e.g., `ai-control-plane`)
3. After creation, go to the app settings and **enable User Authorization** (OBO)

> **Important**: User Authorization (OBO) is required. Without it, the app runs as a read-only service principal with limited functionality — admin features (permission management, cache refresh) will be disabled. With OBO enabled, the app authenticates as the logged-in user, providing full access to workspace APIs and admin capabilities. The user must have workspace admin privileges for admin operations.

## Step 4: Configure the App

```bash
cd control-plane-app

# Copy the example env file
cp .env.example .env
```

Edit `.env` with your values. Set **either** `LAKEBASE_ENDPOINT_PATH` (Autoscaling) **or** `LAKEBASE_INSTANCE` (Provisioned), not both:

```env
# From Step 1
LAKEBASE_DNS=ep-xxxxxxxx.database.us-east-1.cloud.databricks.com
LAKEBASE_DATABASE=control_plane

# Autoscaling (recommended):
LAKEBASE_ENDPOINT_PATH=projects/ai-control-plane-db/branches/production/endpoints/primary

# OR Provisioned (legacy):
# LAKEBASE_INSTANCE=ai-control-plane-db

# Your workspace URL (auto-detected inside Databricks Apps — only needed for local dev)
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com

# Your Databricks account ID (find in workspace URL or account console)
# Required for cross-workspace features
DATABRICKS_ACCOUNT_ID=your-account-id
```

## Step 5: Initialize Lakebase Tables

```bash
# Make sure you're authenticated
databricks auth login --host https://your-workspace.cloud.databricks.com

# Set env vars and run setup (Autoscaling example)
export LAKEBASE_DNS=ep-xxxxxxxx.database.us-east-1.cloud.databricks.com
export LAKEBASE_DATABASE=control_plane
export LAKEBASE_ENDPOINT_PATH=projects/ai-control-plane-db/branches/production/endpoints/primary
# For Provisioned instead: export LAKEBASE_INSTANCE=ai-control-plane-db

cd ..  # back to repo root
pip install psycopg2-binary databricks-sdk requests
python setup_lakebase_tables.py
```

## Step 6: Deploy the App

```bash
cd control-plane-app

# Install frontend dependencies (first time only)
cd frontend && npm install && cd ..

# Deploy (uses your default Databricks CLI profile)
bash deploy.sh

# Or specify a profile:
bash deploy.sh --profile my-workspace
```

The deploy script will:
1. Build the React frontend
2. Generate `app.yaml` from your `.env` values
3. Upload `dist/` and `backend/` to the workspace
4. Deploy the Databricks App
5. Register the app's service principal as a Lakebase Postgres role and grant it the required privileges on the `control_plane` database (`grant_sp_lakebase.py`)

> **Lakebase SP access**: Step 5 is the one piece the app can't do for itself. The deploy script runs `grant_sp_lakebase.py` which uses your (admin) identity to register the app's SP in Lakebase and grant it Postgres privileges (`CONNECT`, `USAGE`/`CREATE` on `public`, `ALL` on tables and sequences). It's idempotent — safe to re-run. If it fails, run it manually:
>
> ```bash
> cd control-plane-app
> DATABRICKS_CONFIG_PROFILE=<profile> \
>   APP_NAME=<app-name> \
>   LAKEBASE_DNS=... \
>   LAKEBASE_DATABASE=control_plane \
>   LAKEBASE_ENDPOINT_PATH=projects/<name>/branches/<branch>/endpoints/<endpoint> \
>   python grant_sp_lakebase.py
> ```

## Step 7: Deploy the Discovery Workflows

The workflows periodically discover agents and sync observability data.

```bash
cd ../workflows

# Edit databricks.yml — fill in the target variables:
#   catalog, schema, lakebase_dns, lakebase_instance, warehouse_id, account_id
```

Example target configuration in `databricks.yml` (Autoscaling):

```yaml
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: my_catalog
      schema: control_plane
      lakebase_dns: "ep-xxxxxxxx.database.us-east-1.cloud.databricks.com"
      lakebase_endpoint_path: "projects/ai-control-plane-db/branches/production/endpoints/primary"
      lakebase_instance: ""   # Provisioned only — leave empty for Autoscaling
      warehouse_id: "xxxxxxxxxxxx"
      account_id: "your-account-id"
```

For Provisioned Lakebase, leave `lakebase_endpoint_path` empty and set `lakebase_instance` to the instance name.

Then deploy and run:

```bash
# Deploy the workflow bundle
databricks bundle deploy --target dev

# Trigger the first run
databricks bundle run agent_discovery --target dev

# The workflow will then run on schedule (every 30 min by default)
```

## Step 8: Grant System Table Access (Optional)

For cross-workspace observability, the app's service principal needs access to `system.mlflow` tables:

```sql
-- Run in a SQL editor connected to your warehouse
-- Replace <sp-application-id> with your app's service principal application ID
-- (found in the app detail page)

GRANT USE SCHEMA ON SCHEMA system.mlflow TO `<sp-application-id>`;
GRANT SELECT ON TABLE system.mlflow.experiments_latest TO `<sp-application-id>`;
GRANT SELECT ON TABLE system.mlflow.runs_latest TO `<sp-application-id>`;
GRANT SELECT ON TABLE system.mlflow.run_metrics_history TO `<sp-application-id>`;
```

## Step 9: Verify

1. Open your app URL (shown in the deploy output)
2. **Agents page**: Should show discovered agents across the workspace
3. **Governance page**: Should show billing/cost data (after the first workflow run)
4. **Observability page**: Should show MLflow experiments and runs from all workspaces

## Troubleshooting

### "SP only" shown instead of your username
User Authorization (OBO) is not enabled on the app. Go to the app settings in your workspace and enable it.

### OBO scopes show "updated" in the UI but disappear on refresh
This usually means the app was created *before* the account-level *User authorization with OAuth* feature was enabled. The app's OAuth integration is stuck in a pre-feature state; `user_api_scopes` writes succeed at the API layer but are silently discarded by the persistence layer — both via the UI and `databricks apps update --json '{"user_api_scopes":[...]}'`.

**Fix:** confirm the feature is enabled at the account level, then delete and recreate the app so its OAuth integration is provisioned against the current backend:

```bash
databricks apps delete <app-name> --profile <profile>
# wait for it to fully delete, then recreate and redeploy
databricks apps create <app-name> --profile <profile>
bash deploy.sh --profile <profile>
```

After redeploy, add scopes in the UI and refresh — they should persist.

### Empty Observability page
The workflow hasn't run yet, or `system.mlflow` access hasn't been granted. Check the workflow run output and Step 8 above.

### Lakebase connection errors
Verify your `LAKEBASE_DNS` is correct and that you set exactly one of `LAKEBASE_ENDPOINT_PATH` (Autoscaling) or `LAKEBASE_INSTANCE` (Provisioned). The instance must be in the `AVAILABLE` state.

### "No SQL warehouse found"
The workflow needs a running SQL warehouse to query system tables. Verify `warehouse_id` in `databricks.yml` points to a running warehouse.
