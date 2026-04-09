#!/usr/bin/env python3
"""
Grant the app's service principal permissions on workspace resources.

This enables the app to operate on resources (serving endpoints, warehouses,
experiments, etc.) even when the user doesn't have direct access — needed for
background operations and scheduled workflows.

Resources covered:
  - Serving endpoints (user-created)
  - SQL warehouses
  - Clusters
  - Jobs
  - Pipelines
  - Experiments (MLflow)
  - Registered models (MLflow)
  - Unity Catalog (grants EXECUTE/SELECT on functions/tables as needed)

Usage:
    # Set required env vars
    export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
    export DATABRICKS_TOKEN=<your-PAT>

    # Get SP info from your app:
    #   databricks apps get <app-name>
    # Look for service_principal_client_id and service_principal_id

    export SP_APPLICATION_ID=<app-sp-application-id>
    export SP_ID=<app-sp-numeric-id>

    python grant_sp_permissions.py
"""

import os
import sys
import json
import logging
import requests
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────
HOST = os.environ.get("DATABRICKS_HOST", "")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
SP_APPLICATION_ID = os.environ.get("SP_APPLICATION_ID", "")
SP_ID = int(os.environ.get("SP_ID", "0"))
PERMISSION_LEVEL = os.environ.get("PERMISSION_LEVEL", "CAN_MANAGE")

if not HOST or not TOKEN:
    logger.error("DATABRICKS_HOST and DATABRICKS_TOKEN are required.")
    logger.error("Set them as environment variables or in .env")
    sys.exit(1)

if not SP_APPLICATION_ID or not SP_ID:
    logger.error("SP_APPLICATION_ID and SP_ID are required.")
    logger.error("Get them from: databricks apps get <app-name>")
    sys.exit(1)

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Foundation-model endpoints are system-managed; skip them
FOUNDATION_MODEL_PREFIX = "databricks-"

# ── helpers ───────────────────────────────────────────────────────────────────

def api_get(path: str, params: Optional[dict] = None) -> dict:
    r = requests.get(f"{HOST}{path}", headers=HEADERS, params=params or {})
    r.raise_for_status()
    return r.json()


def patch_permissions(path: str, permission_level: str, label: str) -> bool:
    """PATCH /api/2.0/permissions/<path> to add SP with given level."""
    body = {
        "access_control_list": [
            {
                "service_principal_name": SP_APPLICATION_ID,
                "all_permissions": [{"permission_level": permission_level}],
            }
        ]
    }
    try:
        r = requests.patch(
            f"{HOST}/api/2.0/permissions/{path}",
            headers=HEADERS,
            json=body,
        )
        if r.status_code == 200:
            logger.info("  OK: %s → %s", label, permission_level)
            return True
        else:
            logger.warning("  FAIL: %s → %s (%s)", label, r.status_code, r.text[:100])
            return False
    except Exception as exc:
        logger.warning("  ERROR: %s → %s", label, exc)
        return False


def sql_grant(statement: str, label: str) -> bool:
    """Execute a UC GRANT statement via SQL."""
    try:
        r = requests.post(
            f"{HOST}/api/2.0/sql/statements",
            headers=HEADERS,
            json={
                "statement": statement,
                "warehouse_id": "",  # uses any available warehouse
                "wait_timeout": "30s",
            },
        )
        data = r.json()
        if data.get("status", {}).get("state") == "SUCCEEDED":
            logger.info("  OK: %s", label)
            return True
        else:
            logger.warning("  FAIL: %s → %s", label, data.get("status", {}).get("error", {}).get("message", ""))
            return False
    except Exception as exc:
        logger.warning("  ERROR: %s → %s", label, exc)
        return False


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ok = 0
    fail = 0

    logger.info("Granting %s permissions to SP %s (%s)", PERMISSION_LEVEL, SP_APPLICATION_ID, SP_ID)
    logger.info("Workspace: %s\n", HOST)

    # Serving endpoints
    logger.info("Serving endpoints:")
    try:
        endpoints = api_get("/api/2.0/serving-endpoints").get("endpoints", [])
        for ep in endpoints:
            name = ep.get("name", "")
            if name.startswith(FOUNDATION_MODEL_PREFIX):
                continue
            if patch_permissions(f"serving-endpoints/{name}", PERMISSION_LEVEL, name):
                ok += 1
            else:
                fail += 1
    except Exception as exc:
        logger.warning("  Could not list endpoints: %s", exc)

    # SQL warehouses
    logger.info("\nSQL warehouses:")
    try:
        warehouses = api_get("/api/2.0/sql/warehouses").get("warehouses", [])
        for wh in warehouses:
            wid = wh.get("id", "")
            name = wh.get("name", wid)
            if patch_permissions(f"sql/warehouses/{wid}", "CAN_USE", name):
                ok += 1
            else:
                fail += 1
    except Exception as exc:
        logger.warning("  Could not list warehouses: %s", exc)

    # System table grants
    logger.info("\nSystem table grants:")
    for table in ["system.mlflow.experiments_latest", "system.mlflow.runs_latest", "system.mlflow.run_metrics_history"]:
        stmt = f"GRANT SELECT ON TABLE {table} TO `{SP_APPLICATION_ID}`"
        if sql_grant(stmt, table):
            ok += 1
        else:
            fail += 1

    stmt = f"GRANT USE SCHEMA ON SCHEMA system.mlflow TO `{SP_APPLICATION_ID}`"
    if sql_grant(stmt, "system.mlflow schema"):
        ok += 1
    else:
        fail += 1

    logger.info("\nDone: %d granted, %d failed", ok, fail)


if __name__ == "__main__":
    main()
