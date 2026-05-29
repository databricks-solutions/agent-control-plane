#!/usr/bin/env python3
"""
Bootstrap the Agent Control Plane Genie space.

Creates (or updates) a Databricks Genie space curated against ACP's
analytical Delta tables in Unity Catalog. The space is consumed by the
"Ask" tab in the ACP web app via an iframe.

The script is idempotent: pass --space-id to update an existing space,
or run without it to create a new one (the new space's id is printed at
the end — save it into your app's `.env` as `GENIE_SPACE_ID`).

Usage:
    # 1. Authenticate
    databricks auth login --host https://<your-workspace>.cloud.databricks.com

    # 2. Set env vars (or pass --catalog / --warehouse-id / --parent-path)
    export CATALOG=serverless_b4nc10_catalog
    export SCHEMA=control_plane
    export GENIE_WAREHOUSE_ID=<warehouse-id>
    export GENIE_PARENT_PATH=/Workspace/Users/$(whoami)@databricks.com/genie

    # 3. Create the space
    python3 setup_genie_space.py

    # 3. (alt) Update an existing space
    python3 setup_genie_space.py --space-id 01f15...

    # 3. (alt) Dump the serialized_space JSON without calling the API
    python3 setup_genie_space.py --print-payload

Requires `databricks-sdk` (already in the project's runtime deps).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from copy import deepcopy
from typing import Any, Dict, List, Optional
from uuid import uuid4


# =====================================================================
# Space definition — code-as-config
# =====================================================================

SPACE_TITLE = "Agent Control Plane Analytics"
SPACE_DESCRIPTION = (
    "Natural-language interface to the Agent Control Plane's aggregated data: "
    "agents, users, usage, observability, billing, and knowledge bases. "
    "Tables are refreshed every 30 min by the ACP discovery workflow."
)

# Top-of-space instructions. Genie injects these as a system prompt.
INSTRUCTIONS = """\
This space queries the Agent Control Plane's analytical Delta tables in {catalog}.{schema}.
Data is refreshed every ~30 minutes by a discovery workflow.
system.billing.usage data lags ~24h on the platform side; downstream
tables here inherit that lag.

KEY CONVENTIONS:
- gateway_usage_daily is the source for TOKEN questions (per-user/per-endpoint).
- billing_serving_daily is the source for $ MODEL-SERVING cost questions.
- billing_product_daily is the source for $ ALL-PRODUCT cost questions
  (Model Serving, Jobs, DLT, SQL, Vector Search, etc.).
- discovered_agents.type is one of:
    serving_endpoint, databricks_app, genie_space, agent_bricks_*
  Use this to break down "agents" by category.
- The user-identity column is named differently across tables:
    gateway_usage_daily.requester
    billing_user_endpoint_daily.user_identity
    observability_runs.user_id
  They all hold the user's email or a service-principal UUID.
- usage_date is a DATE; trends should bucket by usage_date or DATE_TRUNC.

When asked about "cost" or "$", prefer billing_serving_daily (or
billing_product_daily for cross-product). When asked about "tokens" or
"requests", use gateway_usage_daily.
"""


def _tables(catalog: str, schema: str) -> List[str]:
    """Curated 11 — analytical Delta tables Genie can query."""
    return [
        f"{catalog}.{schema}.discovered_agents",
        f"{catalog}.{schema}.gateway_usage_daily",
        f"{catalog}.{schema}.user_analytics_daily",
        f"{catalog}.{schema}.user_analytics_heatmap",
        f"{catalog}.{schema}.observability_traces",
        f"{catalog}.{schema}.vector_search_endpoints",
        f"{catalog}.{schema}.vector_search_indexes",
        f"{catalog}.{schema}.kb_billing_daily",
        f"{catalog}.{schema}.lakebase_instances",
        f"{catalog}.{schema}.billing_serving_daily",
        f"{catalog}.{schema}.billing_product_daily",
    ]


SAMPLE_QUESTIONS = [
    # === INVENTORY ===
    "How many agents are deployed per workspace, broken down by type?",
    "List vector search endpoints with their index count",
    "Show me Genie Spaces grouped by workspace",

    # === USAGE (tokens / requests) ===
    "Top 10 users by token consumption this week",
    "Compare total tokens this month vs last month, broken down by endpoint",
    "Daily active users over the past 30 days",

    # === COST ($) ===
    "Top 5 endpoints by Model Serving cost last week",
    "Show me cost trend by SKU over the past 30 days",
    "Which workspaces spent the most on Model Serving this month?",
    "Total Databricks spend last month vs this month, broken down by product",

    # === TRENDS / ANOMALIES ===
    "Show me endpoints with declining token usage in the past 14 days",
    "Which users had a usage spike yesterday (> 3x their 7d average)?",

    # === OBSERVABILITY ===
    "Endpoints with the highest trace error rate in the last 24h",
    "Slowest traces yesterday by P95 latency",

    # === KNOWLEDGE BASES ===
    "Vector search endpoints by total cost last week",

    # === ADOPTION / DORMANCY ===
    "List serving endpoints with zero requests in the past 14 days",
    "Which workspaces have agents deployed but no token usage this month?",
    "Show vector search indexes that haven't been updated recently",

    # === BEHAVIORAL PATTERNS ===
    "What's the typical peak hour of day for token consumption?",
    "Compare weekday vs weekend usage patterns over the past 30 days",
    "Which users have the most consistent daily activity?",

    # === PERFORMANCE / QUALITY ===
    "Which endpoints have the highest tail latency (P99) in the past week?",
    "Show me endpoints with more than 5% error rate in the past 7 days",
    "Average response latency by endpoint over the last 24 hours",

    # === COST EFFICIENCY ===
    "What's the cost per 1000 tokens for each Model Serving endpoint?",
    "Top 10 endpoints by cost per request last month",
    "Which workspaces have the lowest cost-to-usage ratio?",

    # === GROWTH / ONBOARDING ===
    "Show me new users (first request in the past 7 days) this month",
    "Daily new-user count over the past 60 days",

    # === GOVERNANCE / CONCENTRATION ===
    "Which 5 users account for the largest share of total tokens this quarter?",
]


# =====================================================================
# Benchmarks — evaluate the space against expected SQL behavior
# =====================================================================
# Disabled by default. Flip INCLUDE_BENCHMARKS to True (or pass
# --include-benchmarks) once we're ready to iterate on the eval set
# — they need a tuning pass against Genie's actual generated SQL.

INCLUDE_BENCHMARKS = False

# Genie benchmarks accept only SQL-format expected answers (the API
# enum BenchmarkAnswerFormat has just SQL and UNSPECIFIED — there is
# no TEXT/PROSE format). The benchmark runner compares the model's
# generated SQL against this expected SQL using an LLM judge that
# tolerates equivalent variations (different aliases, equivalent
# JOIN orderings, etc.).
#
# Designed to catch regressions in the things that matter most:
#   - cost ($) vs tokens disambiguation — the #1 way Genie can be wrong here
#   - cross-table JOIN reasoning
#   - workspace / date filtering
#   - the user-identity column naming quirk (requester vs user_identity vs user_id)
#   - dormancy / no-data handling

_C = "serverless_b4nc10_catalog.control_plane"  # short alias used in expected SQL — Genie
                                                 # accepts the fully-qualified form too.


def _benchmark_sqls(catalog: str, schema: str) -> List[Dict[str, str]]:
    """Build the benchmarks list with the right catalog/schema baked in."""
    q = f"{catalog}.{schema}"
    return [
        # --- Cost vs tokens disambiguation (critical) ---
        {
            "question": "How much did we spend on Model Serving last month?",
            "expected_sql": f"""SELECT SUM(total_cost_usd) AS total_cost_usd
FROM {q}.billing_serving_daily
WHERE usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE - INTERVAL 1 MONTH)
  AND usage_date <  DATE_TRUNC('MONTH', CURRENT_DATE)""",
        },
        {
            "question": "How many tokens were consumed last month?",
            "expected_sql": f"""SELECT SUM(input_tokens + output_tokens) AS total_tokens
FROM {q}.gateway_usage_daily
WHERE usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE - INTERVAL 1 MONTH)
  AND usage_date <  DATE_TRUNC('MONTH', CURRENT_DATE)""",
        },

        # --- Cross-table JOIN reasoning ---
        {
            "question": "Show me each endpoint's cost and request count for the past 7 days",
            "expected_sql": f"""SELECT
  b.endpoint_name,
  SUM(b.total_cost_usd) AS total_cost_usd,
  SUM(g.request_count)  AS total_requests
FROM {q}.billing_serving_daily b
JOIN {q}.gateway_usage_daily g
  ON b.usage_date    = g.usage_date
 AND b.workspace_id  = g.workspace_id
 AND b.endpoint_name = g.endpoint_name
WHERE b.usage_date >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY b.endpoint_name
ORDER BY total_cost_usd DESC""",
        },

        # --- User identity naming quirk ---
        {
            "question": "Show me Alice's token usage this week",
            "expected_sql": f"""SELECT SUM(input_tokens + output_tokens) AS total_tokens
FROM {q}.gateway_usage_daily
WHERE LOWER(requester) LIKE '%alice%'
  AND usage_date >= CURRENT_DATE - INTERVAL 7 DAY""",
        },

        # --- Workspace filtering (string type) ---
        {
            "question": "What were the top 3 endpoints by cost in workspace 7474647387698456 last week?",
            "expected_sql": f"""SELECT endpoint_name, SUM(total_cost_usd) AS total_cost_usd
FROM {q}.billing_serving_daily
WHERE workspace_id = '7474647387698456'
  AND usage_date >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY endpoint_name
ORDER BY total_cost_usd DESC
LIMIT 3""",
        },

        # --- Date arithmetic (two non-overlapping windows) ---
        {
            "question": "Compare total token usage this week vs last week",
            "expected_sql": f"""SELECT
  SUM(CASE WHEN usage_date >= CURRENT_DATE - INTERVAL 7 DAY
            THEN input_tokens + output_tokens ELSE 0 END) AS tokens_this_week,
  SUM(CASE WHEN usage_date >= CURRENT_DATE - INTERVAL 14 DAY
            AND usage_date <  CURRENT_DATE - INTERVAL 7 DAY
            THEN input_tokens + output_tokens ELSE 0 END) AS tokens_last_week
FROM {q}.gateway_usage_daily
WHERE usage_date >= CURRENT_DATE - INTERVAL 14 DAY""",
        },

        # --- Agent-type breakdown ---
        {
            "question": "How many serving endpoints, apps, and Genie spaces are deployed in total?",
            "expected_sql": f"""SELECT type, COUNT(*) AS count
FROM {q}.discovered_agents
WHERE type IN ('serving_endpoint', 'databricks_app', 'genie_space')
GROUP BY type
ORDER BY type""",
        },

        # --- Cost-per-token efficiency (cross-table division) ---
        {
            "question": "What's the cost per 1000 tokens for each Model Serving endpoint last week?",
            "expected_sql": f"""SELECT
  b.endpoint_name,
  SUM(b.total_cost_usd) * 1000.0
    / NULLIF(SUM(g.input_tokens + g.output_tokens), 0) AS cost_per_1k_tokens
FROM {q}.billing_serving_daily b
JOIN {q}.gateway_usage_daily g
  ON b.usage_date    = g.usage_date
 AND b.workspace_id  = g.workspace_id
 AND b.endpoint_name = g.endpoint_name
WHERE b.usage_date >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY b.endpoint_name
ORDER BY cost_per_1k_tokens DESC""",
        },

        # --- Dormancy (NOT IN / anti-join) ---
        {
            "question": "Which serving endpoints had zero requests in the past 14 days?",
            "expected_sql": f"""SELECT DISTINCT endpoint_name
FROM {q}.discovered_agents
WHERE type = 'serving_endpoint'
  AND endpoint_name NOT IN (
    SELECT DISTINCT endpoint_name
    FROM {q}.gateway_usage_daily
    WHERE usage_date >= CURRENT_DATE - INTERVAL 14 DAY
  )""",
        },

        # --- Top-N concentration (window + ratio) ---
        {
            "question": "What percentage of total tokens did the top 5 users consume this month?",
            "expected_sql": f"""WITH user_totals AS (
  SELECT requester,
         SUM(input_tokens + output_tokens) AS user_tokens
  FROM {q}.gateway_usage_daily
  WHERE usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE)
  GROUP BY requester
),
ranked AS (
  SELECT user_tokens, RANK() OVER (ORDER BY user_tokens DESC) AS rnk
  FROM user_totals
)
SELECT ROUND(
  SUM(CASE WHEN rnk <= 5 THEN user_tokens ELSE 0 END) * 100.0
  / NULLIF(SUM(user_tokens), 0), 1
) AS top5_share_percent
FROM ranked""",
        },
    ]


# =====================================================================
# Serialized-space builder (self-contained — no external deps)
# =====================================================================

def _new_id() -> str:
    return uuid4().hex


def build_serialized_space(catalog: str, schema: str) -> Dict[str, Any]:
    """Return the inner serialized_space dict (later JSON-stringified)."""
    table_ids = sorted(_tables(catalog, schema))

    space: Dict[str, Any] = {
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": t} for t in table_ids],
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": _new_id(),
                    "content": [INSTRUCTIONS.format(catalog=catalog, schema=schema)],
                }
            ],
        },
        "config": {
            "sample_questions": [
                {"id": _new_id(), "question": [q]} for q in SAMPLE_QUESTIONS
            ],
        },
    }

    if INCLUDE_BENCHMARKS:
        space["benchmarks"] = {
            "questions": [
                {
                    "id": _new_id(),
                    "question": [b["question"]],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": b["expected_sql"].splitlines(keepends=True),
                        }
                    ],
                }
                for b in _benchmark_sqls(catalog, schema)
            ],
        }

    return space


# =====================================================================
# API client
# =====================================================================

def _api(w, method: str, path: str, body: Optional[dict] = None) -> dict:
    """Call the Databricks REST API via the SDK's api_client."""
    if body is not None:
        return w.api_client.do(method, path, body=body)
    return w.api_client.do(method, path)


def create_space(
    w,
    title: str,
    description: str,
    parent_path: str,
    warehouse_id: str,
    serialized_space_json: str,
) -> dict:
    body = {
        "title": title,
        "description": description,
        "parent_path": parent_path,
        "warehouse_id": warehouse_id,
        "serialized_space": serialized_space_json,
    }
    return _api(w, "POST", "/api/2.0/genie/spaces", body)


def update_space(
    w,
    space_id: str,
    title: str,
    description: str,
    warehouse_id: str,
    serialized_space_json: str,
) -> dict:
    body = {
        "title": title,
        "description": description,
        "warehouse_id": warehouse_id,
        "serialized_space": serialized_space_json,
    }
    return _api(w, "PATCH", f"/api/2.0/genie/spaces/{space_id}", body)


# =====================================================================
# Main
# =====================================================================

def _resolve_parent_path(explicit: Optional[str], w) -> str:
    if explicit:
        return explicit
    try:
        me = w.current_user.me()
        return f"/Workspace/Users/{me.user_name}/genie"
    except Exception:
        raise SystemExit(
            "Could not determine parent_path. Pass --parent-path or set GENIE_PARENT_PATH."
        )


def _ensure_parent_path(w, parent_path: str) -> None:
    """Create the workspace folder if it doesn't exist (idempotent)."""
    try:
        w.workspace.mkdirs(parent_path)
    except Exception as exc:
        # Most workspace SDK shapes throw on existing path, but some surface it
        # as a no-op success. Swallow only the "already exists" variant.
        msg = str(exc).lower()
        if "exists" not in msg and "already" not in msg:
            print(f"  ⚠ could not mkdir {parent_path}: {exc}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create or update the ACP Genie space.")
    parser.add_argument("--catalog", default=os.environ.get("CATALOG"),
                        help="UC catalog with the analytical Delta tables")
    parser.add_argument("--schema", default=os.environ.get("SCHEMA", "control_plane"),
                        help="Schema (default: control_plane)")
    parser.add_argument("--warehouse-id", default=os.environ.get("GENIE_WAREHOUSE_ID"),
                        help="SQL warehouse ID that Genie should query against")
    parser.add_argument("--parent-path", default=os.environ.get("GENIE_PARENT_PATH"),
                        help="Workspace path for the space (defaults to /Workspace/Users/<you>/genie)")
    parser.add_argument("--space-id", default=os.environ.get("GENIE_SPACE_ID"),
                        help="Update an existing space instead of creating a new one")
    parser.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE"),
                        help="CLI profile (defaults to DEFAULT or env)")
    parser.add_argument("--print-payload", action="store_true",
                        help="Dump the serialized_space JSON to stdout without calling the API")
    args = parser.parse_args()

    if not args.catalog:
        print("ERROR: --catalog (or CATALOG env var) is required", file=sys.stderr)
        return 1

    serialized = build_serialized_space(args.catalog, args.schema)
    serialized_json = json.dumps(serialized, indent=2)

    if args.print_payload:
        print(serialized_json)
        return 0

    if not args.warehouse_id:
        print("ERROR: --warehouse-id (or GENIE_WAREHOUSE_ID env var) is required", file=sys.stderr)
        return 1

    # Lazy import so --print-payload works without the SDK installed
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()

    parent_path = _resolve_parent_path(args.parent_path, w)

    if args.space_id:
        print(f"Updating Genie space {args.space_id} ...")
        resp = update_space(
            w, args.space_id,
            title=SPACE_TITLE,
            description=SPACE_DESCRIPTION,
            warehouse_id=args.warehouse_id,
            serialized_space_json=serialized_json,
        )
        print(f"  ✅ updated.")
        sid = args.space_id
    else:
        _ensure_parent_path(w, parent_path)
        print(f"Creating new Genie space '{SPACE_TITLE}' in {parent_path} ...")
        resp = create_space(
            w,
            title=SPACE_TITLE,
            description=SPACE_DESCRIPTION,
            parent_path=parent_path,
            warehouse_id=args.warehouse_id,
            serialized_space_json=serialized_json,
        )
        sid = resp.get("space_id") or resp.get("id") or ""
        if not sid:
            print(f"  ⚠ no space_id in response — full response: {json.dumps(resp, indent=2)[:500]}")
            return 1
        print(f"  ✅ created. space_id: {sid}")
        host = (w.config.host or "").rstrip("/")
        if host:
            # /genie/rooms/{id} is the UI route; /genie/spaces/{id} is the REST API.
            print(f"  URL: {host}/genie/rooms/{sid}")
        print()
        print("Next steps:")
        print(f"  1. Add to your app's .env:   GENIE_SPACE_ID={sid}")
        print(f"  2. Set FEATURE_GENIE_ENABLED=true and redeploy the app.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
