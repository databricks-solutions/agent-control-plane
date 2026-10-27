# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space Auto-Provisioner (F — Ask Genie)
# MAGIC
# MAGIC Creates the **Agent Control Plane Analytics** Genie space over the discovered Delta
# MAGIC tables and records its id so the app's **Ask Genie** tab auto-appears — no manual
# MAGIC `setup_genie_space.py` run, no redeploy.
# MAGIC
# MAGIC   this task  →  `billing_cache_meta.genie_space_id` (Lakebase)  ←  app resolves it
# MAGIC
# MAGIC **Create-once + self-healing.** Reads the last space id from `billing_cache_meta`;
# MAGIC if the space still exists it is left **as-is** (so hand-curated instructions / tables
# MAGIC in the Genie UI are never overwritten). It is recreated **only** when the space is
# MAGIC genuinely gone (404) — any other GET error keeps the cached id and skips, so transient
# MAGIC failures never spawn duplicate spaces. Runs every discovery cycle. Best-effort: any
# MAGIC failure degrades (returns a status, writes nothing) and never fails the discovery run.
# MAGIC Skips entirely when a `GENIE_SPACE_ID` is explicitly pinned (that deployment owns its
# MAGIC space). The space definition mirrors `setup_genie_space.py`.
# MAGIC
# MAGIC **Schema:** this task does NO DDL on `billing_cache_meta` — the app owns that table
# MAGIC (`billing_service`), so we only SELECT/UPSERT and defer if it isn't there yet.
# MAGIC
# MAGIC **Auth:** created by the job run-as identity; the app queries it per-user via OBO, so
# MAGIC the task grants `CAN_RUN` to the share group (default `users`) so app users can query.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

import psycopg2
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse id the Genie space runs on")
dbutils.widgets.text("lakebase_dns", "", "Lakebase host (DNS)")
dbutils.widgets.text("lakebase_database", "control_plane", "Lakebase database")
dbutils.widgets.text("lakebase_instance", "", "Lakebase instance (Provisioned)")
dbutils.widgets.text("lakebase_endpoint_path", "", "Lakebase endpoint path (Autoscaling)")
dbutils.widgets.text("genie_parent_path", "", "Workspace folder for the space (default /Workspace/Users/<me>/genie)")
dbutils.widgets.text("genie_space_id", "", "Explicit space pin — if set, skip auto-provision (this deployment manages its own space)")
dbutils.widgets.text("genie_share_group", "users", "Group granted CAN_RUN on the space so app users can query it via OBO (empty to skip sharing)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
LAKEBASE_DNS = dbutils.widgets.get("lakebase_dns")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database") or "control_plane"
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
LAKEBASE_ENDPOINT_PATH = dbutils.widgets.get("lakebase_endpoint_path")
GENIE_PARENT_PATH = dbutils.widgets.get("genie_parent_path")
GENIE_SPACE_ID_PIN = (dbutils.widgets.get("genie_space_id") or "").strip()
GENIE_SHARE_GROUP = (dbutils.widgets.get("genie_share_group") or "").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Space definition (mirrors setup_genie_space.py — keep in sync)

# COMMAND ----------

SPACE_TITLE = "Agent Control Plane Analytics"
SPACE_DESCRIPTION = (
    "Natural-language interface to the Agent Control Plane's aggregated data: "
    "agents, users, usage, observability, billing, and knowledge bases. "
    "Tables are refreshed every 30 min by the ACP discovery workflow."
)
INSTRUCTIONS = """\
This space queries the Agent Control Plane's analytical Delta tables in {catalog}.{schema}.
Data is refreshed every ~30 minutes by a discovery workflow.
system.billing.usage data lags ~24h on the platform side; downstream tables inherit that lag.

KEY CONVENTIONS:
- Unity Gateway (UC services) tables — prefer these for model / MCP / AI-service questions:
    gateway_services_inventory  every UC service (model, MCP, provider, agent): owner, rate limits, inference table, routing
    uag_service_usage           7-day traffic per service: requests, errors, 429s, users, tokens, p95 latency
    uag_usage_summary           per gateway endpoint usage incl. cached tokens and latency percentiles
    uag_usage_timeseries_daily  daily gateway requests / tokens
    billing_user_cost_daily     ACTUAL $ per user (Unity Gateway-attributed, run_by) per endpoint per day
    billing_external_model_spend  actual $ for external models (OpenAI, Bedrock, Foundry, ...) via the gateway
- gateway_usage_daily is LEGACY serving-endpoint usage (system.serving.endpoint_usage):
  use it for tokens/requests on serving endpoints that are not routed through Unity Gateway.
- billing_serving_daily is the source for $ MODEL-SERVING cost per serving endpoint.
- billing_product_daily is the source for $ ALL-PRODUCT cost (Model Serving, Jobs, SQL, Vector Search, ...).
- usage_date is a DATE; trends should bucket by usage_date or DATE_TRUNC.
When asked about cost per user, model or AI service, prefer billing_user_cost_daily / uag_service_usage;
per serving endpoint use billing_serving_daily; cross-product use billing_product_daily.
For tokens/requests prefer uag_service_usage / uag_usage_summary; gateway_usage_daily is legacy serving usage.
"""
_CURATED_TABLES = [
    "discovered_agents", "gateway_usage_daily", "user_analytics_daily", "user_analytics_heatmap",
    "observability_traces", "vector_search_endpoints", "vector_search_indexes", "kb_billing_daily",
    "lakebase_instances", "billing_serving_daily", "billing_product_daily",
    # Unity Gateway (UC services) — v3
    "gateway_services_inventory", "uag_service_usage", "uag_usage_summary", "uag_usage_timeseries_daily", "billing_user_cost_daily", "billing_external_model_spend",
]
SAMPLE_QUESTIONS = [
    "How many agents are deployed per workspace, broken down by type?",
    "Top 10 users by token consumption this week",
    "Top 5 endpoints by Model Serving cost last week",
    "Total Databricks spend last month vs this month, broken down by product",
    "List serving endpoints with zero requests in the past 14 days",
    "Endpoints with the highest trace error rate in the last 24h",
    "What's the cost per 1000 tokens for each Model Serving endpoint?",
    "Daily active users over the past 30 days",
]


def _existing_tables() -> List[str]:
    """Curated tables that actually exist in UC (skip missing → no dead refs)."""
    out = []
    for t in _CURATED_TABLES:
        fq = f"{CATALOG}.{SCHEMA}.{t}"
        try:
            if spark.catalog.tableExists(fq):
                out.append(fq)
        except Exception:
            pass
    return out


def build_serialized_space(table_ids: List[str]) -> str:
    # Stable ids (uuid5 over content) so the built space is deterministic across runs.
    def _sid(kind: str, content: str) -> str:
        return uuid.uuid5(uuid.NAMESPACE_URL, f"acp-genie:{kind}:{content}").hex
    space = {
        "version": 2,
        "data_sources": {"tables": [{"identifier": t} for t in sorted(table_ids)]},
        "instructions": {"text_instructions": [
            {"id": _sid("instr", "main"), "content": [INSTRUCTIONS.format(catalog=CATALOG, schema=SCHEMA)]}]},
        "config": {"sample_questions": [
            {"id": _sid("q", q), "question": [q]} for q in SAMPLE_QUESTIONS]},
    }
    return json.dumps(space)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase helpers — SELECT/UPSERT only (the app owns the schema)

# COMMAND ----------

w = WorkspaceClient()


def _lakebase_conn():
    import requests as http_requests
    me = w.current_user.me()
    host = w.config.host.rstrip("/")
    try:
        token = (w.config.authenticate() or {}).get("Authorization", "").replace("Bearer ", "")
    except Exception:
        token = getattr(w.config, "token", "") or ""
    pw = None
    if LAKEBASE_ENDPOINT_PATH:
        r = http_requests.post(f"{host}/api/2.0/postgres/credentials",
                               headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                               json={"endpoint": LAKEBASE_ENDPOINT_PATH})
        r.raise_for_status(); pw = r.json().get("token", "")
    elif LAKEBASE_INSTANCE:
        pw = w.database.generate_database_credential(instance_names=[LAKEBASE_INSTANCE]).token
    if not pw:
        raise RuntimeError("could not generate Lakebase credential")
    return psycopg2.connect(host=LAKEBASE_DNS, port=5432, database=LAKEBASE_DATABASE,
                            user=me.user_name, password=pw, sslmode="require", connect_timeout=15)


def _read_cached_space_id(conn):
    """SELECT only (no DDL — the app owns billing_cache_meta's schema). Returns
    ``(read_ok, space_id)``:
      - (True, "<id>")  — a space is already recorded
      - (True, None)    — table readable, no row yet → safe to create the first space
      - (False, None)   — read FAILED (schema not ready yet, transient, permission).
    A failed read must NOT be treated as 'no cache': the caller skips instead of creating
    a space it cannot persist, which would orphan a new space on every run / transient blip."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value_text FROM billing_cache_meta WHERE cache_key = 'genie_space_id'")
            row = cur.fetchone()
        return True, (row[0].strip() if row and row[0] else None)
    except Exception as e:
        conn.rollback()
        print(f"  cache read failed — not creating a space to avoid orphans ({type(e).__name__})")
        return False, None


def _persist_space_id(conn, space_id: str) -> bool:
    """UPSERT only, best-effort. If billing_cache_meta / value_text isn't there yet
    (app-owned schema), defer — the next run persists once the app has created it."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO billing_cache_meta (cache_key, last_refreshed, rows_loaded, value_text)
                   VALUES ('genie_space_id', NOW(), 0, %s)
                   ON CONFLICT (cache_key) DO UPDATE
                   SET value_text = EXCLUDED.value_text, last_refreshed = NOW()""",
                (space_id,))
            conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"  persist deferred (meta table not ready: {type(e).__name__}: {e})")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create-once + grant + persist

# COMMAND ----------


def _api(method: str, path: str, body: Optional[dict] = None) -> dict:
    return w.api_client.do(method, path, body=body) if body is not None else w.api_client.do(method, path)


def _find_space_by_title() -> Optional[str]:
    """Idempotency backstop: find an existing space whose title is SPACE_TITLE. Used
    before creating, so a persist failure (space created but id not recorded) never
    spawns duplicates on the next run — we reuse the existing space instead. Best-effort."""
    try:
        page = None
        while True:
            path = "/api/2.0/genie/spaces?page_size=100" + (f"&page_token={page}" if page else "")
            resp = _api("GET", path)
            for s in (resp.get("spaces") or []):
                if (s.get("title") or s.get("display_name")) == SPACE_TITLE:
                    return s.get("space_id") or s.get("id")
            page = resp.get("next_page_token")
            if not page:
                break
    except Exception as e:
        print(f"  find-by-title failed (ignored): {type(e).__name__}")
    return None


def _grant_access(space_id: str) -> None:
    """Grant CAN_RUN to the share group so app users can query via OBO. Non-destructive
    PATCH (keeps creator/admin CAN_MANAGE). Idempotent, best-effort."""
    if not GENIE_SHARE_GROUP:
        return
    try:
        _api("PATCH", f"/api/2.0/permissions/genie/{space_id}",
             {"access_control_list": [{"group_name": GENIE_SHARE_GROUP, "permission_level": "CAN_RUN"}]})
        print(f"  granted {GENIE_SHARE_GROUP!r} CAN_RUN on space {space_id}")
    except Exception as e:
        print(f"  WARNING: could not grant {GENIE_SHARE_GROUP!r} on space: {type(e).__name__}: {e}")


def main() -> dict:
    # A deployment that pins its own GENIE_SPACE_ID manages its own space — don't
    # auto-provision a redundant one (mirrors the app's env-pin > meta precedence).
    if GENIE_SPACE_ID_PIN:
        return {"status": "skipped", "reason": "explicit GENIE_SPACE_ID pinned"}
    if not CATALOG or not SCHEMA or not WAREHOUSE_ID:
        return {"status": "skipped", "reason": "catalog/schema/warehouse_id required"}
    if not LAKEBASE_DNS or (not LAKEBASE_ENDPOINT_PATH and not LAKEBASE_INSTANCE):
        return {"status": "skipped", "reason": "lakebase connection not configured"}

    tables = _existing_tables()
    if not tables:
        return {"status": "skipped", "reason": f"no curated tables in {CATALOG}.{SCHEMA}"}
    print(f"  space over {len(tables)} tables: {[t.split('.')[-1] for t in tables]}")

    parent_path = GENIE_PARENT_PATH or f"/Workspace/Users/{w.current_user.me().user_name}/genie"
    try:
        w.workspace.mkdirs(parent_path)
    except Exception:
        pass

    conn = _lakebase_conn()
    try:
        read_ok, cached = _read_cached_space_id(conn)
        if not read_ok:
            # Can't read the cache → don't create a space we couldn't record (orphan guard).
            return {"status": "skipped",
                    "reason": "billing_cache_meta not readable yet — deferring space creation"}

        space_id = None
        if cached:
            try:
                _api("GET", f"/api/2.0/genie/spaces/{cached}")
                space_id = cached
                print(f"  space {cached} exists — leaving content as-is")
            except NotFound:
                print(f"  cached space {cached} is gone (404) — recreating")
            except Exception as e:
                # Transient / permission / other — never create a duplicate; keep the cached id.
                return {"status": "skipped", "space_id": cached,
                        "reason": f"space check failed, kept cached id ({type(e).__name__})"}

        # Idempotency backstop: if we have no usable cached id (first run, or a prior
        # persist failed and left the id unrecorded), reuse an existing space by title
        # before creating — so a write failure can't pile up duplicates run after run.
        if not space_id:
            found = _find_space_by_title()
            if found:
                space_id = found
                print(f"  reusing existing space by title: {space_id}")

        if not space_id:
            resp = _api("POST", "/api/2.0/genie/spaces", {
                "title": SPACE_TITLE, "description": SPACE_DESCRIPTION, "parent_path": parent_path,
                "warehouse_id": WAREHOUSE_ID, "serialized_space": build_serialized_space(tables)})
            space_id = resp.get("space_id") or resp.get("id") or ""
            if not space_id:
                return {"status": "error", "reason": f"create returned no space_id: {str(resp)[:200]}"}
            print(f"  created space {space_id}")

        _grant_access(space_id)
        persisted = _persist_space_id(conn, space_id)
    finally:
        conn.close()

    host = w.config.host.rstrip("/")
    return {"status": "success", "space_id": space_id, "persisted": persisted,
            "space_url": f"{host}/genie/rooms/{space_id}", "tables": len(tables),
            "at": datetime.now(timezone.utc).isoformat()}


# Single exit point — run outside any try so dbutils.notebook.exit()'s own control-flow
# signal is never swallowed and re-reported as an error.
try:
    _result = main()
except Exception as exc:
    _result = {"status": "error", "reason": f"{type(exc).__name__}: {str(exc)[:200]}"}
print(f"  genie-setup: {_result}")
dbutils.notebook.exit(json.dumps(_result))
