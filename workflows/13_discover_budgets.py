# Databricks notebook source
# MAGIC %md
# MAGIC # Budget Discovery Job (F5 — UAG budget status, read-only)
# MAGIC
# MAGIC Reads the **account Budgets API** (`GET /api/2.1/accounts/{account_id}/budgets`)
# MAGIC and writes one Delta table for the sync task (`02_sync_to_lakebase`) to
# MAGIC mirror into Lakebase. Surfaces each native budget's cap thresholds, whether
# MAGIC it **enforces** (BLOCK_USAGE) vs only **alerts**, its filter, and AI-relevance.
# MAGIC
# MAGIC **Read-only / visualize-only:** the app never creates or enforces budgets —
# MAGIC enforcement stays entirely platform-side. This is the fleet-wide status view
# MAGIC the native per-budget UI doesn't give.
# MAGIC
# MAGIC **Table written:** `uag_budget_status`
# MAGIC
# MAGIC **Account auth.** The Budgets API is account-scoped, so this task needs
# MAGIC account-level credentials — a service principal whose OAuth creds
# MAGIC (`client_id` / `client_secret`) live in the secret scope named by
# MAGIC `discovery_sp_secret_scope`, and which has account budget read. Without
# MAGIC them the task **degrades to an empty table** (app shows "account access
# MAGIC needed"), never failing the run.
# MAGIC
# MAGIC **Data flow:** account Budgets API → Delta → Lakebase (sync task) → app reads

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

from pyspark.sql import SparkSession
import re

from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DecimalType, DoubleType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("account_id", "", "Databricks account ID")
dbutils.widgets.text("account_host", "https://accounts.cloud.databricks.com", "Account console host")
dbutils.widgets.text("discovery_sp_secret_scope", "", "Secret scope with account-SP creds (keys: client_id, client_secret)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ACCOUNT_ID = dbutils.widgets.get("account_id")
ACCOUNT_HOST = (dbutils.widgets.get("account_host") or "https://accounts.cloud.databricks.com").rstrip("/")
SP_SECRET_SCOPE = dbutils.widgets.get("discovery_sp_secret_scope") or ""

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

BUDGET_TABLE = f"{CATALOG}.{SCHEMA}.uag_budget_status"

BUDGET_SCHEMA = StructType([
    StructField("budget_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("enforce", BooleanType(), True),
    StructField("alerting", BooleanType(), True),
    StructField("min_threshold_usd", DecimalType(18, 2), True),
    StructField("max_threshold_usd", DecimalType(18, 2), True),
    StructField("time_period", StringType(), True),
    StructField("filter_summary", StringType(), True),
    StructField("is_ai", BooleanType(), True),
    StructField("spent_usd", DecimalType(18, 2), True),   # current-period spend; NULL = not computable (n/a)
    StructField("pct_used", DoubleType(), True),          # spent / max cap * 100; NULL when spend or cap missing
    StructField("discovered_at", TimestampType(), False),
])

# Product values (on the `databricks-product` tag) that mark a budget as AI-scoped.
_AI_PRODUCTS = {
    "genie", "model_serving", "mosaic_ai_model_serving", "foundation_model",
    "vector_search", "ai_gateway", "agent", "mosaic_ai_agent",
}
# Tag keys that always mark a budget as AI-scoped regardless of value.
_AI_TAG_KEYS = {"ai_model"}
# Name markers (lowercased substring) that mark a budget as AI-scoped.
_AI_NAME_MARKERS = ("genie", "gateway", "ai_model", "ai-gateway", "llm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load account credentials + call the Budgets API

# COMMAND ----------

def _load_budgets() -> List[Dict[str, Any]]:
    """Return the raw budget objects from the account Budgets API, or [] if
    account credentials aren't configured / the call fails (degrade to empty)."""
    if not ACCOUNT_ID:
        print("  account_id not set — writing empty budget table")
        return []
    if not SP_SECRET_SCOPE:
        print("  discovery_sp_secret_scope not set — no account creds; writing empty budget table")
        return []
    try:
        client_id = dbutils.secrets.get(scope=SP_SECRET_SCOPE, key="client_id")
        client_secret = dbutils.secrets.get(scope=SP_SECRET_SCOPE, key="client_secret")
    except Exception as exc:
        print(f"  WARNING: could not load SP creds from scope '{SP_SECRET_SCOPE}': {exc}")
        return []

    try:
        from databricks.sdk import AccountClient
        ac = AccountClient(
            host=ACCOUNT_HOST,
            account_id=ACCOUNT_ID,
            client_id=client_id,
            client_secret=client_secret,
        )
        budgets: List[Dict[str, Any]] = []
        page_token = None
        while True:
            query = {"page_token": page_token} if page_token else None
            resp = ac.api_client.do(
                "GET", f"/api/2.1/accounts/{ACCOUNT_ID}/budgets", query=query
            )
            batch = (resp or {}).get("budgets", []) if isinstance(resp, dict) else []
            budgets.extend(batch)
            page_token = (resp or {}).get("next_page_token") if isinstance(resp, dict) else None
            if not page_token:
                break
        print(f"  Loaded {len(budgets)} budgets from the account Budgets API")
        return budgets
    except Exception as exc:
        print(f"  WARNING: Budgets API call failed ({type(exc).__name__}: {exc}) — writing empty table")
        return []


def _to_row(b: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten one raw budget object into our Delta schema."""
    alert_cfgs = b.get("alert_configurations") or []
    thresholds: List[float] = []
    actions: set = set()
    time_period = ""
    for a in alert_cfgs:
        try:
            thresholds.append(float(a.get("quantity_threshold") or 0))
        except (TypeError, ValueError):
            pass
        if not time_period:
            time_period = a.get("time_period") or ""
        for ac_ in (a.get("action_configurations") or []):
            if ac_.get("action_type"):
                actions.add(ac_["action_type"])

    filt = b.get("filter") or {}
    tags = filt.get("tags") or []
    tag_bits = []
    for t in tags:
        key = t.get("key", "")
        val = t.get("value") or {}
        op = val.get("operator", "IN")
        vals = val.get("values") or []
        tag_bits.append(f"{key} {op} [{', '.join(str(v) for v in vals[:3])}]")
    ws = filt.get("workspace_id") or {}
    ws_vals = ws.get("values") or []
    filter_bits = list(tag_bits)
    if ws_vals:
        filter_bits.append(f"workspaces: {len(ws_vals)}")
    filter_summary = "; ".join(filter_bits) if filter_bits else "account-wide"

    # AI-relevance: AI tag key, an AI product value, or a name marker.
    is_ai = False
    for t in tags:
        key = t.get("key", "")
        if key in _AI_TAG_KEYS:
            is_ai = True
        if key == "databricks-product":
            vals = {str(v).lower() for v in (t.get("value") or {}).get("values", [])}
            if vals & _AI_PRODUCTS:
                is_ai = True
    name_l = (b.get("display_name") or "").lower()
    if any(m in name_l for m in _AI_NAME_MARKERS):
        is_ai = True

    def _dec(v):
        return Decimal(str(round(v, 2))) if v else None

    return {
        "budget_id": b.get("budget_configuration_id", ""),
        "account_id": b.get("account_id", ACCOUNT_ID),
        "display_name": b.get("display_name", ""),
        "enforce": "BLOCK_USAGE" in actions,
        "alerting": "EMAIL_NOTIFICATION" in actions,
        "min_threshold_usd": _dec(min(thresholds)) if thresholds else None,
        "max_threshold_usd": _dec(max(thresholds)) if thresholds else None,
        "time_period": time_period,
        "filter_summary": filter_summary,
        "is_ai": is_ai,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current-period consumption (spend vs cap)
# MAGIC
# MAGIC Bounded current-month billing aggregates (per-workspace + per-tag-key, list-price
# MAGIC USD — the same join `09_discover_billing` uses), matched per budget. Correct for
# MAGIC **account-wide**, **workspace-only**, and **single-tag (IN/NOT_IN)** filters;
# MAGIC complex shapes (multi-tag, tag+workspace) are left NULL (n/a) rather than guessed.
# MAGIC Fails open to n/a if billing isn't readable.

# COMMAND ----------

_COST = "ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0)), 2)"
_FROM = """FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices lp
    ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud AND u.usage_unit = lp.usage_unit AND lp.price_end_time IS NULL
  WHERE u.usage_date >= date_trunc('MONTH', current_date())"""
_KEY_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")


def _referenced_tag_keys(budgets):
    """Distinct, safe tag keys referenced by budget filters, top 20 by frequency
    (bounds the number of aggregate queries)."""
    keys = {}
    for b in budgets:
        for t in ((b.get("filter") or {}).get("tags") or []):
            k = t.get("key", "")
            if _KEY_RE.match(k):
                keys[k] = keys.get(k, 0) + 1
    return [k for k, _ in sorted(keys.items(), key=lambda kv: -kv[1])[:20]]


def _billing_current_month(budgets):
    """Bounded month-to-date cost aggregates. Returns (ws_cost, tag_cost, total, keys),
    or (None, None, None, set()) if billing isn't readable (→ consumption = n/a)."""
    try:
        ws_rows = spark.sql(
            f"SELECT CAST(u.workspace_id AS STRING) AS ws, {_COST} AS cost {_FROM} "
            "AND u.workspace_id IS NOT NULL GROUP BY u.workspace_id"
        ).collect()
        ws_cost = {r["ws"]: float(r["cost"] or 0) for r in ws_rows}
        # Account-wide / NOT_IN spend uses `total`, which must include usage with a
        # NULL workspace_id (account-level SKUs) — so compute it separately rather than
        # summing the workspace-filtered ws_cost (which would under-count).
        total_rows = spark.sql(f"SELECT {_COST} AS cost {_FROM}").collect()
        total = round(float(total_rows[0]["cost"] or 0) if total_rows else 0.0, 2)
        keys = _referenced_tag_keys(budgets)
        tag_cost = {}
        for k in keys:
            tr = spark.sql(
                f"SELECT u.custom_tags['{k}'] AS v, {_COST} AS cost {_FROM} "
                f"AND u.custom_tags['{k}'] IS NOT NULL GROUP BY u.custom_tags['{k}']"
            ).collect()
            for r in tr:
                tag_cost[(k, str(r["v"]))] = float(r["cost"] or 0)
        print(f"  billing: {len(ws_cost)} workspaces, month-to-date ${total:,.0f}, tag keys={keys}")
        return ws_cost, tag_cost, total, set(keys)
    except Exception as exc:
        print(f"  WARNING: billing aggregate failed ({type(exc).__name__}: {exc}) — consumption = n/a")
        return None, None, None, set()


def _compute_spent(b, ws_cost, tag_cost, total, keys):
    """Month-to-date spend for one budget, or None (n/a) if its filter shape isn't
    supported by the bounded aggregates (account-wide / workspace-only / single-tag)."""
    if ws_cost is None:
        return None
    filt = b.get("filter") or {}
    tags = filt.get("tags") or []
    ws = (filt.get("workspace_id") or {}).get("values") or []
    if not tags and not ws:                      # account-wide
        return total
    if not tags and ws:                          # workspace-only
        return round(sum(ws_cost.get(str(w), 0.0) for w in ws), 2)
    if len(tags) == 1 and not ws:                # single-tag
        t = tags[0]
        k = t.get("key", "")
        if k not in keys:
            return None
        val = t.get("value") or {}
        vals = {str(v) for v in (val.get("values") or [])}
        matched = round(sum(c for (kk, vv), c in tag_cost.items() if kk == k and vv in vals), 2)
        return round(max(total - matched, 0.0), 2) if val.get("operator") == "NOT_IN" else matched
    return None                                  # multi-tag / tag+workspace → don't guess

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta table

# COMMAND ----------

now = datetime.now(timezone.utc)
raw = _load_budgets()
budgets = [b for b in raw if b.get("budget_configuration_id")]
rows = [_to_row(b) for b in budgets]

ws_cost, tag_cost, total_cost, cost_keys = _billing_current_month(budgets) if budgets else (None, None, None, set())
for b, r in zip(budgets, rows):
    spent = _compute_spent(b, ws_cost, tag_cost, total_cost, cost_keys)
    r["spent_usd"] = Decimal(str(round(spent, 2))) if spent is not None else None
    cap = float(r["max_threshold_usd"]) if r["max_threshold_usd"] is not None else 0.0
    r["pct_used"] = round(100.0 * spent / cap, 1) if (spent is not None and cap > 0) else None

if rows:
    tuples = [
        (
            r["budget_id"], r["account_id"], r["display_name"],
            r["enforce"], r["alerting"], r["min_threshold_usd"], r["max_threshold_usd"],
            r["time_period"], r["filter_summary"], r["is_ai"],
            r["spent_usd"], r["pct_used"], now,
        )
        for r in rows
    ]
    spark.createDataFrame(tuples, BUDGET_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(BUDGET_TABLE)
else:
    spark.createDataFrame([], BUDGET_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(BUDGET_TABLE)

computed = sum(1 for r in rows if r["spent_usd"] is not None)
print(f"✅ Wrote {len(rows)} budgets to {BUDGET_TABLE} ({computed} with computed spend)")

# COMMAND ----------

result = {
    "status": "success",
    "budget_rows": len(rows),
    "enforcing": sum(1 for r in rows if r["enforce"]),
    "ai_budgets": sum(1 for r in rows if r["is_ai"]),
    "spend_computed": computed,
    "account_creds": bool(SP_SECRET_SCOPE),
    "discovered_at": now.isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
