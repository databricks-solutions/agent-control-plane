"""Multi-Workspace Federation — aggregate billing, discovery, and token data
per workspace from the Lakebase cache.

All reads hit Lakebase (fast) rather than system tables.
Data is populated by the billing and discovery refresh jobs.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.database import execute_query, DatabasePool
from backend.services.billing_service import (
    get_current_workspace_id,
    maybe_refresh_async,
)


# =====================================================================
# Composite: all workspace data in one round-trip
# =====================================================================

def get_workspaces_page_data(days: int = 30) -> Dict[str, Any]:
    """Return all data the Workspaces page needs in a single DB round-trip.

    Includes:
      • workspace_summaries — one row per workspace with costs, agents, tokens
      • cost_trend — daily cost trend per workspace (top 5 workspaces)
      • agent_type_breakdown — agent counts by type per workspace
      • top_endpoints — highest-cost endpoints across all workspaces
    """
    from psycopg2.extras import RealDictCursor

    maybe_refresh_async()

    current_ws = get_current_workspace_id()

    with DatabasePool.get_connection() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Workspace summaries — aggregate cost + token + agent data
        cur.execute(
            """
            WITH ws_cost AS (
                SELECT workspace_id,
                       SUM(total_cost_usd)::NUMERIC(18,2)  AS total_cost,
                       SUM(total_dbus)::NUMERIC(18,2)       AS total_dbus,
                       COUNT(DISTINCT endpoint_name)::INT   AS endpoint_count
                FROM billing_serving_daily
                WHERE usage_date >= CURRENT_DATE - %s
                GROUP BY workspace_id
            ),
            ws_tokens AS (
                SELECT workspace_id,
                       SUM(request_count)::BIGINT           AS total_requests,
                       SUM(input_tokens)::BIGINT            AS total_input_tokens,
                       SUM(output_tokens)::BIGINT           AS total_output_tokens
                FROM billing_token_daily
                WHERE usage_date >= CURRENT_DATE - %s
                GROUP BY workspace_id
            ),
            ws_agents AS (
                SELECT workspace_id,
                       COUNT(*)::INT                        AS agent_count,
                       COUNT(DISTINCT type)::INT            AS agent_type_count
                FROM discovered_agents
                GROUP BY workspace_id
            ),
            ws_products AS (
                SELECT workspace_id,
                       SUM(total_cost_usd)::NUMERIC(18,2)  AS total_all_product_cost
                FROM billing_product_daily
                WHERE usage_date >= CURRENT_DATE - %s
                GROUP BY workspace_id
            ),
            ws_cost_prev AS (
                SELECT workspace_id,
                       SUM(total_cost_usd)::NUMERIC(18,2)  AS prev_cost
                FROM billing_serving_daily
                WHERE usage_date >= CURRENT_DATE - (%s * 2)
                  AND usage_date < CURRENT_DATE - %s
                GROUP BY workspace_id
            )
            SELECT
                COALESCE(c.workspace_id, t.workspace_id, a.workspace_id, p.workspace_id)  AS workspace_id,
                COALESCE(c.total_cost, 0)             AS serving_cost,
                COALESCE(c.total_dbus, 0)             AS serving_dbus,
                COALESCE(c.endpoint_count, 0)         AS endpoint_count,
                COALESCE(t.total_requests, 0)         AS total_requests,
                COALESCE(t.total_input_tokens, 0)     AS total_input_tokens,
                COALESCE(t.total_output_tokens, 0)    AS total_output_tokens,
                COALESCE(a.agent_count, 0)            AS agent_count,
                COALESCE(a.agent_type_count, 0)       AS agent_type_count,
                COALESCE(p.total_all_product_cost, 0) AS total_all_product_cost,
                COALESCE(cp.prev_cost, 0)             AS prev_serving_cost
            FROM ws_cost c
            FULL OUTER JOIN ws_tokens t    USING (workspace_id)
            FULL OUTER JOIN ws_agents a    USING (workspace_id)
            FULL OUTER JOIN ws_products p  USING (workspace_id)
            LEFT JOIN ws_cost_prev cp      ON cp.workspace_id = COALESCE(c.workspace_id, t.workspace_id, a.workspace_id, p.workspace_id)
            ORDER BY COALESCE(c.total_cost, 0) DESC
            """,
            (days, days, days, days, days),
        )
        summaries = [dict(r) for r in cur.fetchall()]

        # 2. Daily cost trend for top 5 workspaces
        top_ws_ids = [s["workspace_id"] for s in summaries[:5] if s.get("workspace_id")]
        trend: List[Dict[str, Any]] = []
        if top_ws_ids:
            placeholders = ",".join(["%s"] * len(top_ws_ids))
            cur.execute(
                f"""SELECT usage_date::TEXT AS day,
                           workspace_id,
                           SUM(total_cost_usd)::NUMERIC(18,2) AS cost
                    FROM billing_serving_daily
                    WHERE usage_date >= CURRENT_DATE - %s
                      AND workspace_id IN ({placeholders})
                    GROUP BY usage_date, workspace_id
                    ORDER BY usage_date""",
                (days, *top_ws_ids),
            )
            trend = [dict(r) for r in cur.fetchall()]

        # 3. Agent type breakdown per workspace
        cur.execute(
            """SELECT workspace_id,
                      type           AS agent_type,
                      COUNT(*)::INT  AS count
               FROM discovered_agents
               GROUP BY workspace_id, type
               ORDER BY workspace_id, count DESC"""
        )
        type_breakdown = [dict(r) for r in cur.fetchall()]

        # 4. Top endpoints across workspaces
        cur.execute(
            """SELECT workspace_id,
                      endpoint_name,
                      SUM(total_cost_usd)::NUMERIC(18,2) AS total_cost,
                      SUM(total_dbus)::NUMERIC(18,2)     AS total_dbus
               FROM billing_serving_daily
               WHERE usage_date >= CURRENT_DATE - %s
               GROUP BY workspace_id, endpoint_name
               ORDER BY total_cost DESC
               LIMIT 20""",
            (days,),
        )
        top_endpoints = [dict(r) for r in cur.fetchall()]

        # 5. Product cost breakdown per workspace
        cur.execute(
            """SELECT workspace_id,
                      billing_origin_product,
                      SUM(total_cost_usd)::NUMERIC(18,2) AS total_cost
               FROM billing_product_daily
               WHERE usage_date >= CURRENT_DATE - %s
               GROUP BY workspace_id, billing_origin_product
               ORDER BY total_cost DESC""",
            (days,),
        )
        products_by_ws = [dict(r) for r in cur.fetchall()]

        # 6. Agents list (for workspace detail drill-down)
        cur.execute(
            """SELECT workspace_id, name, type, endpoint_name,
                      endpoint_status, model_name, creator, source
               FROM discovered_agents
               ORDER BY workspace_id, name"""
        )
        all_agents = [dict(r) for r in cur.fetchall()]

        cur.close()

    # Build global KPIs
    total_workspaces = len(summaries)
    total_cost = sum(float(s.get("serving_cost") or 0) for s in summaries)
    total_all_product_cost = sum(float(s.get("total_all_product_cost") or 0) for s in summaries)
    total_agents = sum(int(s.get("agent_count") or 0) for s in summaries)
    total_requests = sum(int(s.get("total_requests") or 0) for s in summaries)
    total_endpoints = sum(int(s.get("endpoint_count") or 0) for s in summaries)
    prev_cost = sum(float(s.get("prev_serving_cost") or 0) for s in summaries)
    cost_change_pct = ((total_cost - prev_cost) / prev_cost * 100) if prev_cost > 0 else 0.0

    return {
        "current_workspace_id": current_ws,
        "kpis": {
            "total_workspaces": total_workspaces,
            "total_serving_cost": round(total_cost, 2),
            "total_all_product_cost": round(total_all_product_cost, 2),
            "total_agents": total_agents,
            "total_requests": total_requests,
            "total_endpoints": total_endpoints,
            "cost_change_pct": round(cost_change_pct, 1),
        },
        "workspace_summaries": summaries,
        "cost_trend": trend,
        "agent_type_breakdown": type_breakdown,
        "top_endpoints": top_endpoints,
        "products_by_workspace": products_by_ws,
        "all_agents": all_agents,
    }
