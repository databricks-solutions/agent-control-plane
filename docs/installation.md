# Installation Guide

This guide walks you through deploying the AI Control Plane to your Databricks workspace.

## Prerequisites

- **Databricks workspace** with Unity Catalog enabled
- **Databricks CLI** installed and authenticated (`pip install databricks-cli`)
- **Node.js** 18+ (for building the frontend)
- **Python** 3.10+

## Step 1: Create a Lakebase Instance

The Control Plane uses Lakebase (PostgreSQL) for fast dashboard reads.

1. In your workspace, go to **SQL** > **Lakebase** > **Create Instance**
2. Name it (e.g., `ai-control-plane-db`)
3. Note these values:
   - **Instance name**: `ai-control-plane-db`
   - **DNS hostname**: shown on the instance detail page (e.g., `instance-xxxx.database.cloud.databricks.com`)
   - **Database name**: `control_plane` (default)

## Step 2: Create a SQL Warehouse

The scheduled workflows need a SQL warehouse to query system tables.

1. Go to **SQL** > **SQL Warehouses**
2. Use an existing serverless warehouse or create one
3. Note the **Warehouse ID** (visible in the URL or warehouse detail page)

## Step 3: Create a Databricks App

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

Edit `.env` with your values:

```env
# From Step 1
LAKEBASE_DNS=instance-xxxx.database.cloud.databricks.com
LAKEBASE_DATABASE=control_plane
LAKEBASE_INSTANCE=ai-control-plane-db

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

# Set env vars and run setup
export LAKEBASE_DNS=instance-xxxx.database.cloud.databricks.com
export LAKEBASE_DATABASE=control_plane
export LAKEBASE_INSTANCE=ai-control-plane-db

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

## Step 7: Deploy the Discovery Workflows

The workflows periodically discover agents and sync observability data.

```bash
cd ../workflows

# Edit databricks.yml — fill in the target variables:
#   catalog, schema, lakebase_dns, lakebase_instance, warehouse_id, account_id
```

Example target configuration in `databricks.yml`:

```yaml
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: my_catalog
      schema: control_plane
      lakebase_dns: "instance-xxxx.database.cloud.databricks.com"
      lakebase_instance: "ai-control-plane-db"
      warehouse_id: "xxxxxxxxxxxx"
      account_id: "your-account-id"
```

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

### Empty Observability page
The workflow hasn't run yet, or `system.mlflow` access hasn't been granted. Check the workflow run output and Step 8 above.

### Lakebase connection errors
Verify your `LAKEBASE_DNS` and `LAKEBASE_INSTANCE` are correct. The instance must be in the `AVAILABLE` state.

### "No SQL warehouse found"
The workflow needs a running SQL warehouse to query system tables. Verify `warehouse_id` in `databricks.yml` points to a running warehouse.
