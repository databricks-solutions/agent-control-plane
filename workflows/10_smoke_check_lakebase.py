# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase smoke check
# MAGIC
# MAGIC Final task in the discovery workflow. Connects to Lakebase and asserts
# MAGIC every table the deployed app reads from exists. Surfaces row counts so a
# MAGIC silent-empty-table failure (like the gateway_usage_daily transaction
# MAGIC bug) shows up here as a workflow FAILED instead of as empty UI screens.
# MAGIC
# MAGIC Three categories:
# MAGIC
# MAGIC | Category    | Behaviour                                              |
# MAGIC |-------------|--------------------------------------------------------|
# MAGIC | `required`  | Must exist AND have ≥1 row. Failure raises.           |
# MAGIC | `expected`  | Must exist. 0 rows is allowed (quiet workspace) but   |
# MAGIC |             | logged at WARN. Doc gap if perpetually empty.         |
# MAGIC | `optional`  | App-managed; created on first user action. Existence  |
# MAGIC |             | NOT required.                                          |

# COMMAND ----------

# MAGIC %pip install psycopg2-binary "databricks-sdk>=0.40.0" --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import psycopg2
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("lakebase_dns", "", "Lakebase host (DNS)")
dbutils.widgets.text("lakebase_database", "control_plane", "Lakebase database")
dbutils.widgets.text("lakebase_instance", "", "Lakebase instance name (Provisioned only)")
dbutils.widgets.text("lakebase_endpoint_path", "", "Lakebase endpoint path (Autoscaling only)")

DNS = dbutils.widgets.get("lakebase_dns")
DB = dbutils.widgets.get("lakebase_database")
INSTANCE = dbutils.widgets.get("lakebase_instance")
ENDPOINT_PATH = dbutils.widgets.get("lakebase_endpoint_path")

if not DNS:
    raise ValueError("lakebase_dns must be set via job parameters")
if not INSTANCE and not ENDPOINT_PATH:
    raise ValueError("Either lakebase_instance or lakebase_endpoint_path must be set")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table contract
# MAGIC
# MAGIC Source: every `FROM <table>` reference in `control-plane-app/backend/services/`.
# MAGIC Required tables are those the dashboard relies on for non-empty rendering of a
# MAGIC primary KPI. Expected tables are populated by discovery but may be 0 in a quiet
# MAGIC workspace. Optional tables are app-managed (created lazily on user action).

# COMMAND ----------

REQUIRED = [
    # Sync workflow → Delta → Lakebase. If any of these are missing or empty
    # post-sync, a primary dashboard tab WILL render empty and we want CI red.
    "discovered_agents",
    "billing_user_endpoint_daily",
    "billing_user_cost_daily",
    "billing_token_daily",
    "billing_product_daily",
    "gateway_usage_daily",
    "gateway_usage_hourly",
    "user_analytics_daily",
    "user_analytics_heatmap",
    "lakebase_instances",
    "uag_usage_summary",
    "uag_usage_breakdown",
]

EXPECTED = [
    # Populated by discovery but legitimately empty in some workspaces.
    # We assert existence; row count of 0 produces a WARN, not a failure.
    "billing_serving_daily",         # may be empty if no model serving in last 90d
    "billing_cost_by_tag",           # MODEL_SERVING $ by custom_tag; empty if no tagged serving usage
    "billing_external_model_spend",  # external LLM $ via AI Gateway; empty if no external-model routing
    "observability_traces",
    "observability_trace_details",
    "observability_experiments",
    "observability_runs",
    "gateway_inference_logs",        # only populated when Mosaic AI Gateway inference logging is enabled
    "vector_search_endpoints",       # workspace may have no Vector Search endpoints
    "vector_search_indexes",         # endpoints can exist with 0 indexes
    "vector_search_health_history",
    "kb_billing_daily",              # only populated when Vector Search indexes exist
    "uag_mcp_tool_daily",            # only populated when MCP services route through UAG v2
    "uag_guardrail_daily",           # only populated when endpoints have UAG v2 guardrails active
    "uag_usage_timeseries_daily",    # daily UAG v2 usage series; empty when no v2 traffic
    "agent_tool_usage",              # TOOL/RETRIEVER span rollup; empty when no traced agents
    "agent_eval_scores",             # MLflow-3 assessment rollup; empty when no eval'd traces
    "uag_coding_agent_usage",        # coding-agent activity; empty when no coding-agent traffic
    "uag_throttling_daily",          # 429/5xx per endpoint; empty when no throttling/errors
    "billing_cache_meta",
    # App-managed tables created in Phase 7 of 02_sync_to_lakebase. These were
    # historically created lazily by the app's startup hook, but the app SP
    # does not always have Lakebase DDL privileges — when it doesn't, the
    # Tools page rendered empty with a 500 from /api/tools/overview while the
    # job still reported SUCCESS. Asserted as EXPECTED (existence required,
    # 0 rows allowed before first user activity).
    "tool_registry",
    "request_logs",
]

OPTIONAL = [
    # Created lazily by the app on first user action. Don't assert existence.
    "playground_sessions",
    "playground_messages",
    "workspace_registry",
    "agent_permissions_cache",
    "agent_registry",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect

# COMMAND ----------

w = WorkspaceClient()
me = w.current_user.me().user_name

if INSTANCE:
    creds = w.database.generate_database_credential(instance_names=[INSTANCE])
    token = creds.token
else:
    # Autoscaling endpoint path
    import requests
    auth_h = {}
    w.config.authenticate(auth_h)
    resp = requests.post(
        f"{w.config.host.rstrip('/')}/api/2.0/postgres/credentials",
        headers={**auth_h, "Content-Type": "application/json"},
        json={"endpoint": ENDPOINT_PATH},
        timeout=20,
    )
    resp.raise_for_status()
    token = resp.json().get("token", "")

print(f"Connecting to Lakebase {DNS}/{DB} as {me}")

conn = psycopg2.connect(
    host=DNS, port=5432, dbname=DB, user=me, password=token,
    sslmode="require", connect_timeout=15,
)
conn.autocommit = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run checks

# COMMAND ----------

def list_user_tables(cur) -> set:
    cur.execute(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname = 'public'"
    )
    return {r[0] for r in cur.fetchall()}

def safe_count(cur, table: str):
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        return cur.fetchone()[0], None
    except psycopg2.Error as exc:
        return None, str(exc).strip()

results = {"required": [], "expected": [], "optional": []}
failures = []
warnings = []

with conn.cursor() as cur:
    present = list_user_tables(cur)

    for t in REQUIRED:
        if t not in present:
            failures.append(f"MISSING required table: {t}")
            results["required"].append({"table": t, "status": "MISSING", "rows": None})
            continue
        rows, err = safe_count(cur, t)
        if err:
            failures.append(f"COUNT failed on required table {t}: {err}")
            results["required"].append({"table": t, "status": "ERROR", "rows": None, "error": err})
            continue
        if rows == 0:
            failures.append(f"EMPTY required table: {t}")
            results["required"].append({"table": t, "status": "EMPTY", "rows": 0})
        else:
            results["required"].append({"table": t, "status": "OK", "rows": rows})

    for t in EXPECTED:
        if t not in present:
            failures.append(f"MISSING expected table: {t}")
            results["expected"].append({"table": t, "status": "MISSING", "rows": None})
            continue
        rows, err = safe_count(cur, t)
        if err:
            warnings.append(f"COUNT failed on expected table {t}: {err}")
            results["expected"].append({"table": t, "status": "ERROR", "rows": None, "error": err})
            continue
        if rows == 0:
            warnings.append(f"empty expected table: {t}")
            results["expected"].append({"table": t, "status": "EMPTY", "rows": 0})
        else:
            results["expected"].append({"table": t, "status": "OK", "rows": rows})

    for t in OPTIONAL:
        if t not in present:
            results["optional"].append({"table": t, "status": "ABSENT", "rows": None})
            continue
        rows, err = safe_count(cur, t)
        results["optional"].append({
            "table": t,
            "status": "OK" if not err else "ERROR",
            "rows": rows,
            **({"error": err} if err else {}),
        })

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report

# COMMAND ----------

print("=" * 70)
print("Lakebase smoke check report")
print("=" * 70)

for category in ["required", "expected", "optional"]:
    print(f"\n### {category.upper()} ({len(results[category])} tables)")
    for r in sorted(results[category], key=lambda x: (x["status"] != "OK", x["table"])):
        rows = r.get("rows")
        rows_s = f"{rows:>10,}" if isinstance(rows, int) else f"{'-':>10}"
        marker = {
            "OK": "✅",
            "EMPTY": "⚠️ ",
            "MISSING": "❌",
            "ERROR": "❌",
            "ABSENT": "·  ",
        }.get(r["status"], "?  ")
        line = f"  {marker} {r['table']:<40s} {rows_s} rows  ({r['status']})"
        if r.get("error"):
            line += f"  [{r['error'][:80]}]"
        print(line)

print("\n" + "=" * 70)
if warnings:
    print(f"WARNINGS ({len(warnings)}):")
    for w_ in warnings:
        print(f"  ⚠️  {w_}")

summary = {
    "status": "FAIL" if failures else "PASS",
    "failures": failures,
    "warnings": warnings,
    "results": results,
}
# Always print the JSON so it shows up in task logs even when we raise.
print("\nSMOKE_RESULT_JSON:", json.dumps(summary))

if failures:
    print(f"\nFAILURES ({len(failures)}):")
    for f_ in failures:
        print(f"  ❌ {f_}")
    print("=" * 70)
    # Raise — this propagates as a task failure, which is what we want CI to gate on.
    # We deliberately do NOT call dbutils.notebook.exit() here: that would mark the
    # task SUCCEEDED with the JSON as its return value, hiding the FAIL from the
    # workflow-level result_state.
    #
    # Inline the failure list and a tight per-table breakdown into the exception
    # message because notebook stdout is not surfaced through `jobs get-run-output`
    # on failure — the message body is the only post-mortem channel that actually
    # round-trips back to a CI runner via the API.
    breakdown_lines = []
    for cat in ["required", "expected"]:
        for r in results[cat]:
            if r["status"] != "OK":
                rows = r.get("rows")
                rows_s = str(rows) if isinstance(rows, int) else "-"
                breakdown_lines.append(f"  [{cat}] {r['table']:<35s} {r['status']:<8s} rows={rows_s}")
    raise RuntimeError(
        "Lakebase smoke check FAILED: "
        + "; ".join(failures)
        + "\nBreakdown:\n"
        + "\n".join(breakdown_lines)
    )

print("\n✅ All required tables present and populated.")
if warnings:
    print(f"   ({len(warnings)} non-fatal warning(s) — see above)")
print("=" * 70)
dbutils.notebook.exit(json.dumps(summary))
