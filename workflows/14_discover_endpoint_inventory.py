# Databricks notebook source
# MAGIC %md
# MAGIC # Endpoint Inventory Discovery (account-wide, read-only)
# MAGIC
# MAGIC Queries `system.serving.served_entities` (account-scoped) and writes a
# MAGIC flat, **account-wide** inventory of every current served entity across all
# MAGIC workspaces in the metastore — the fleet view the per-workspace serving API
# MAGIC (`serving_endpoints.list()`) can't give.
# MAGIC
# MAGIC **Read-only:** live management (ACL/config edits) stays per-workspace via the
# MAGIC serving API — this table is inventory only.
# MAGIC
# MAGIC **Table written:** `serving_endpoints_inventory`
# MAGIC
# MAGIC **Data flow:** system.serving.served_entities → Delta → Lakebase (sync task) → app reads

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
TABLE = f"{CATALOG}.{SCHEMA}.serving_endpoints_inventory"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the inventory
# MAGIC One row per current served entity (latest endpoint config version, not deleted).

# COMMAND ----------

rows = 0
try:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE} AS
        WITH latest AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY endpoint_id, served_entity_id
                       ORDER BY endpoint_config_version DESC, change_time DESC
                   ) AS rn
            FROM system.serving.served_entities
            WHERE endpoint_delete_time IS NULL
        )
        SELECT
            served_entity_id,
            endpoint_id,
            endpoint_name,
            CAST(workspace_id AS STRING)          AS workspace_id,
            served_entity_name,
            entity_type,
            entity_name,
            entity_version,
            external_model_config.provider        AS provider,
            task,
            created_by,
            endpoint_config_version,
            change_time,
            current_timestamp()                   AS discovered_at
        FROM latest
        WHERE rn = 1
    """)
    rows = spark.table(TABLE).count()
    print(f"✅ Wrote {rows} served entities to {TABLE}")
except Exception as exc:
    # Fail open to an empty table so the sync/app degrade gracefully (e.g. if
    # system.serving.served_entities isn't readable at this principal's scope).
    print(f"WARNING: served_entities query failed ({type(exc).__name__}: {exc}) — writing empty table")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE} (
            served_entity_id STRING, endpoint_id STRING, endpoint_name STRING,
            workspace_id STRING, served_entity_name STRING, entity_type STRING,
            entity_name STRING, entity_version STRING, provider STRING, task STRING,
            created_by STRING, endpoint_config_version INT, change_time TIMESTAMP,
            discovered_at TIMESTAMP
        )
    """)

# COMMAND ----------

result = {
    "status": "success",
    "endpoint_rows": rows,
    "discovered_at": datetime.now(timezone.utc).isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
