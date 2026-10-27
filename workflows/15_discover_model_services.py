# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Gateway Services Discovery (v3 — account-wide, read-only)
# MAGIC
# MAGIC Lists every Unity Gateway **UC service** via the UC REST API and writes a
# MAGIC metastore-wide inventory to Delta for the sync task to mirror into Lakebase:
# MAGIC
# MAGIC | Kind | REST list path | Config captured |
# MAGIC |---|---|---|
# MAGIC | Model service | `/api/2.1/unity-catalog/model-services` | rate limits, inference table, routing destinations |
# MAGIC | MCP service | `/api/2.1/unity-catalog/mcp-services` | rate limits, source connection |
# MAGIC | Model-provider service | `/api/2.1/unity-catalog/model-provider-services` | provider type, rate limits, inference table |
# MAGIC | Agent service (Beta) | `/api/2.1/unity-catalog/agent-services` | — (fail-open; often empty) |
# MAGIC
# MAGIC **Why a workflow:** the list endpoints are grant-filtered and the app's runtime
# MAGIC identities (OBO token / app SP) can't reliably enumerate them. The workflow runs
# MAGIC as its **run-as identity**, so it sees what that identity can see. Services it
# MAGIC can't list still show up in the app from `system.ai_gateway.usage` (traffic only).
# MAGIC Per-service **grants** stay live in the app (UC-enforced).
# MAGIC
# MAGIC **Tables written:**
# MAGIC - `gateway_services_inventory` — all service kinds, with config
# MAGIC - `model_services_inventory` — model services only (kept for compatibility)
# MAGIC
# MAGIC **Data flow:** UC services REST → Delta → Lakebase (sync task) → app reads

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from datetime import datetime, timezone

import requests
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, IntegerType, StructType, StructField, StringType, TimestampType

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
SERVICES_TABLE = f"{CATALOG}.{SCHEMA}.gateway_services_inventory"

SCHEMA_T = StructType([
    StructField("full_name", StringType(), False),
    StructField("owner", StringType(), True),
    StructField("supported_api_types", StringType(), True),  # comma-joined
    StructField("create_time", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

SERVICES_SCHEMA = StructType([
    StructField("full_name", StringType(), False),          # catalog.schema.name (= usage.service_name)
    StructField("service_type", StringType(), False),       # MODEL_SERVICE | MCP_SERVICE | MODEL_PROVIDER_SERVICE | AGENT_SERVICE
    StructField("owner", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("supported_api_types", StringType(), True), # comma-joined (model services)
    StructField("provider_type", StringType(), True),       # model-provider services
    StructField("source_connection", StringType(), True),   # MCP services
    StructField("destinations", StringType(), True),        # model services: comma-joined routing targets
    StructField("rate_limit_count", IntegerType(), True),
    StructField("rate_limits", StringType(), True),         # JSON list of {key, principal, renewal_period, requests, tokens}
    StructField("inference_table", StringType(), True),     # table / prefix when logging is configured
    StructField("inference_table_enabled", BooleanType(), True),
    StructField("create_time", StringType(), True),
    StructField("update_time", StringType(), True),
    StructField("updated_by", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# (kind, REST path, response key, service_type, name prefix in the `name` field)
_KINDS = [
    ("model-services", "model_services", "MODEL_SERVICE"),
    ("mcp-services", "mcp_services", "MCP_SERVICE"),
    ("model-provider-services", "model_provider_services", "MODEL_PROVIDER_SERVICE"),
    ("agent-services", "agent_services", "AGENT_SERVICE"),
]

# COMMAND ----------

def _list_kind(host: str, headers: dict, path: str, key: str):
    """List one service kind via REST (run-as identity). Paginated.
    Returns (items, ok) — ok=False when the call failed, so the caller can
    tell a failed list from a genuinely empty one."""
    out, page_token = [], None
    try:
        for _ in range(200):  # page cap — defends against a non-advancing cursor
            params = {"page_token": page_token} if page_token else {}
            r = requests.get(f"{host}/api/2.1/unity-catalog/{path}", headers=headers, params=params, timeout=60)
            r.raise_for_status()
            body = r.json()
            out.extend(body.get(key, []) or [])
            nxt = body.get("next_page_token")
            if not nxt or nxt == page_token:
                break
            page_token = nxt
        print(f"  {path}: {len(out)}")
        return out, True
    except Exception as exc:
        print(f"  WARNING: {path} list failed ({type(exc).__name__}: {exc})")
        return out, False


def _rate_limits(cfg: dict) -> list:
    return [
        {k: rl.get(k) for k in ("key", "principal", "renewal_period", "requests", "tokens") if rl.get(k) is not None}
        for rl in (cfg.get("rate_limits") or [])
    ]


def _inference_table(cfg: dict):
    it = cfg.get("inference_table") or {}
    if not it:
        return None, False
    name = it.get("table") or it.get("table_name_prefix") or ""
    enabled = not (it.get("disabled") or it.get("is_deleted"))
    return name, enabled


def _destinations(cfg: dict) -> str:
    names = []
    for d in ((cfg.get("routing") or {}).get("destinations") or []):
        model = ((d.get("pay_per_token_config") or {}).get("model") or "").replace("models/", "")
        label = model or d.get("name") or d.get("destination_type") or ""
        pct = d.get("traffic_percentage")
        names.append(f"{label} ({pct}%)" if pct not in (None, "", 100) else label)
    return ",".join(n for n in names if n)


def _row(s: dict, service_type: str, now):
    cfg = s.get("config") or {}
    rls = _rate_limits(cfg)
    it_name, it_enabled = _inference_table(cfg)
    name = (s.get("name") or "").split("/", 1)[-1]  # strip "model-services/" etc.
    src = cfg.get("source_connection")
    if isinstance(src, dict):
        src = src.get("name") or src.get("connection_name") or json.dumps(src)[:200]
    return (
        name, service_type, s.get("effective_owner") or "", s.get("comment") or "",
        ",".join(s.get("supported_api_types", []) or []),
        cfg.get("provider_type") or "", src or "",
        _destinations(cfg), len(rls), json.dumps(rls) if rls else "",
        it_name or "", bool(it_enabled) if it_name else None,
        s.get("create_time"), s.get("update_time"), s.get("updated_by") or "",
        now,
    )

# COMMAND ----------

now = datetime.now(timezone.utc)
w = WorkspaceClient()
host = w.config.host.rstrip("/")
token = w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
headers = {"Authorization": f"Bearer {token}"}

_MAX_DETAIL_GETS = 300  # bound per-run detail calls on large metastores


def _with_routing(items: list) -> list:
    """The LIST response omits config.routing (destinations / traffic split); only
    GET returns it. Fetch it for custom model services — system.ai.* built-ins
    route to themselves. Best-effort: on any error keep the list item as-is."""
    fetched = 0
    for s in items:
        full = (s.get("name") or "").split("/", 1)[-1]
        if not full or full.startswith("system.ai.") or fetched >= _MAX_DETAIL_GETS:
            continue
        fetched += 1
        try:
            r = requests.get(f"{host}/api/2.1/unity-catalog/model-services/{full}", headers=headers, timeout=30)
            r.raise_for_status()
            routing = (r.json().get("config") or {}).get("routing")
            if routing:
                s.setdefault("config", {})["routing"] = routing
        except Exception as exc:
            print(f"  routing detail failed for {full}: {type(exc).__name__}")
    print(f"  fetched routing for {fetched} custom model services")
    return items


service_rows, model_rows, counts, failed = [], [], {}, []
for path, key, stype in _KINDS:
    items, ok = _list_kind(host, headers, path, key)
    if not ok:
        failed.append(path)
    if stype == "MODEL_SERVICE":
        items = _with_routing(items)
    counts[stype] = len(items)
    for s in items:
        if not s.get("name"):
            continue
        service_rows.append(_row(s, stype, now))
        if stype == "MODEL_SERVICE":
            model_rows.append((
                (s.get("name") or "").split("/", 1)[-1], s.get("effective_owner", ""),
                ",".join(s.get("supported_api_types", []) or []), s.get("create_time"), now,
            ))

# A failed list must not wipe a good previous inventory (keep the snapshot); an
# empty-but-successful list is truthful and overwrites. First run → write empty.
def _write(rows, schema, table, keep_on_failure: bool):
    if not rows and keep_on_failure and spark.catalog.tableExists(table):
        print(f"  ↺ list failed — keeping previous snapshot of {table}")
        return
    spark.createDataFrame(rows, schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"✅ Wrote {len(rows)} rows to {table}")

_write(service_rows, SERVICES_SCHEMA, SERVICES_TABLE, keep_on_failure=bool(failed) and not service_rows)
_write(model_rows, SCHEMA_T, TABLE, keep_on_failure="model-services" in failed)

# COMMAND ----------

result = {"status": "success", "model_service_rows": len(model_rows), "service_rows": len(service_rows),
          "by_type": counts, "failed_lists": failed, "discovered_at": now.isoformat()}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
