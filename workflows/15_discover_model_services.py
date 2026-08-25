# Databricks notebook source
# MAGIC %md
# MAGIC # Model Services Discovery (v3 Unity Gateway — account-wide, read-only)
# MAGIC
# MAGIC Lists v3 Unity Gateway **UC model services** via the UC REST API
# MAGIC (`GET /api/2.1/unity-catalog/model-services`) and writes a metastore-wide
# MAGIC inventory to Delta for the sync task to mirror into Lakebase.
# MAGIC
# MAGIC **Why a workflow:** the list endpoint requires metastore-admin-level access,
# MAGIC which the app's runtime identities (OBO token / app SP) don't reliably have.
# MAGIC The workflow runs as its **run-as identity** (a metastore admin), so it can
# MAGIC enumerate account-wide. The app then reads the cached inventory from Lakebase;
# MAGIC per-service **grants** stay live in the app (UC-enforced).
# MAGIC
# MAGIC **Table written:** `model_services_inventory`
# MAGIC
# MAGIC **Data flow:** UC model-services REST → Delta → Lakebase (sync task) → app reads

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from datetime import datetime, timezone

import requests
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
TABLE = f"{CATALOG}.{SCHEMA}.model_services_inventory"

SCHEMA_T = StructType([
    StructField("full_name", StringType(), False),
    StructField("owner", StringType(), True),
    StructField("supported_api_types", StringType(), True),  # comma-joined
    StructField("create_time", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# COMMAND ----------

def _load_model_services():
    """List UC model services via REST (run-as identity). Paginated. Fail-open to []."""
    try:
        w = WorkspaceClient()
        host = w.config.host.rstrip("/")
        token = w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
        headers = {"Authorization": f"Bearer {token}"}
        out, page_token = [], None
        while True:
            params = {"page_token": page_token} if page_token else {}
            r = requests.get(f"{host}/api/2.1/unity-catalog/model-services", headers=headers, params=params, timeout=60)
            r.raise_for_status()
            body = r.json()
            out.extend(body.get("model_services", []))
            page_token = body.get("next_page_token")
            if not page_token:
                break
        print(f"  Loaded {len(out)} model services")
        return out
    except Exception as exc:
        print(f"  WARNING: model-services list failed ({type(exc).__name__}: {exc}) — writing empty table")
        return []

# COMMAND ----------

now = datetime.now(timezone.utc)
raw = _load_model_services()
rows = [
    (
        (s.get("name", "") or "").replace("model-services/", ""),
        s.get("effective_owner", ""),
        ",".join(s.get("supported_api_types", []) or []),
        s.get("create_time"),
        now,
    )
    for s in raw if s.get("name")
]

if rows:
    spark.createDataFrame(rows, SCHEMA_T).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
else:
    spark.createDataFrame([], SCHEMA_T).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
print(f"✅ Wrote {len(rows)} model services to {TABLE}")

# COMMAND ----------

result = {"status": "success", "model_service_rows": len(rows), "discovered_at": now.isoformat()}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
