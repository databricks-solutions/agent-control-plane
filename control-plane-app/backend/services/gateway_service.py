"""AI Gateway service — pulls REAL data from Databricks APIs and system tables.

Data sources:
  • Databricks SDK  → serving endpoints list, AI Gateway config, permissions
  • system.serving.endpoint_usage   → per-request usage (tokens, latency, status)
  • system.serving.served_entities  → endpoint ↔ entity mapping

Performance:
  All public functions are wrapped with an in-memory TTL cache so that
  repeated reads within a short window (default 120 s) return instantly
  instead of hitting the SDK / SQL warehouse every time.
"""
from __future__ import annotations

import time
import threading
import httpx
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from backend.config import (
    _get_workspace_client,
    get_databricks_host,
    get_databricks_headers,
    find_serverless_warehouse_id,
)

import logging

logger = logging.getLogger(__name__)

_TIMEOUT = 60.0

# =====================================================================
# In-memory TTL cache
# =====================================================================
_cache_lock = threading.Lock()
_cache: Dict[str, tuple] = {}  # key → (timestamp, value)
_DEFAULT_TTL = 600  # 10 minutes — system table data doesn't change rapidly

def _cache_get(key: str, ttl: int = _DEFAULT_TTL) -> Any:
    """Return cached value if present and fresh, else None."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry[0]) < ttl:
            return entry[1]
    return None

def _cache_set(key: str, value: Any) -> Any:
    """Store a value in the cache and return it (for chaining)."""
    with _cache_lock:
        _cache[key] = (time.time(), value)
    return value

def _cache_freshness() -> Optional[str]:
    """Return the ISO timestamp of the most recent cache entry, or None."""
    with _cache_lock:
        if not _cache:
            return None
        newest = max(entry[0] for entry in _cache.values())
    return datetime.fromtimestamp(newest, tz=timezone.utc).isoformat()

def clear_cache() -> None:
    """Clear the entire in-memory cache so the next request re-fetches fresh data."""
    with _cache_lock:
        _cache.clear()


# =====================================================================
# SQL helpers (reuse pattern from billing_service)
# =====================================================================

def _find_warehouse_id() -> Optional[str]:
    """Find the best SQL warehouse (prefers serverless)."""
    return find_serverless_warehouse_id()


def _execute_system_sql(sql: str, warehouse_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Execute SQL via the SQL Statement Execution API."""
    wh_id = warehouse_id or _find_warehouse_id()
    if not wh_id:
        return []

    path = "/api/2.0/sql/statements"
    body = {
        "warehouse_id": wh_id,
        "statement": sql,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }

    w = _get_workspace_client()
    resp_json: Optional[dict] = None
    if w:
        try:
            resp_json = w.api_client.do("POST", path, body=body)
        except Exception as exc:
            logger.warning("SDK SQL exec failed: %s", exc)

    if resp_json is None:
        base = get_databricks_host()
        if not base:
            return []
        try:
            resp = httpx.post(
                f"{base}{path}",
                headers=get_databricks_headers(),
                json=body,
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            resp_json = resp.json()
        except Exception as exc:
            logger.warning("httpx SQL exec failed: %s", exc)
            return []

    if not resp_json:
        return []

    # Handle PENDING state — poll until done
    status = resp_json.get("status", {}).get("state", "")
    if status == "PENDING":
        import time
        stmt_id = resp_json.get("statement_id")
        base = get_databricks_host()
        if stmt_id and base:
            for _ in range(30):
                time.sleep(2)
                try:
                    poll = httpx.get(
                        f"{base}/api/2.0/sql/statements/{stmt_id}",
                        headers=get_databricks_headers(),
                        timeout=_TIMEOUT,
                    )
                    resp_json = poll.json()
                    status = resp_json.get("status", {}).get("state", "")
                    if status in ("SUCCEEDED", "FAILED", "CANCELED"):
                        break
                except Exception:
                    break

    if resp_json.get("status", {}).get("state") != "SUCCEEDED":
        err = resp_json.get("status", {}).get("error", {})
        logger.warning("SQL failed: %s", err.get('message', status))
        return []

    manifest = resp_json.get("manifest", {})
    columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
    data_array = resp_json.get("result", {}).get("data_array", [])
    return [dict(zip(columns, row)) for row in data_array]


# =====================================================================
# SDK helpers — list endpoints, permissions, AI Gateway config
# =====================================================================

def _list_serving_endpoints() -> List[Dict[str, Any]]:
    """List all serving endpoints via the Databricks SDK."""
    w = _get_workspace_client()
    if not w:
        return []
    try:
        endpoints = list(w.serving_endpoints.list())
    except Exception as exc:
        logger.warning("Failed to list serving endpoints: %s", exc)
        return []

    results = []
    for ep in endpoints:
        state_ready = ep.state.ready.value if ep.state and ep.state.ready else "UNKNOWN"
        task = ep.task if hasattr(ep, "task") else None
        ep_type = ep.endpoint_type.value if hasattr(ep, "endpoint_type") and ep.endpoint_type else None
        creator = ep.creator if hasattr(ep, "creator") else None

        # Served entities
        served_entities = []
        if ep.config and ep.config.served_entities:
            for se in ep.config.served_entities:
                entity = {
                    "name": se.name or "",
                    "entity_name": se.entity_name or "",
                    "entity_version": se.entity_version or "",
                }
                if se.external_model:
                    entity["provider"] = se.external_model.name or ""
                    entity["external"] = True
                if hasattr(se, "foundation_model") and se.foundation_model:
                    entity["foundation_model"] = True
                served_entities.append(entity)

        # AI Gateway config
        gw_config = None
        if hasattr(ep, "ai_gateway") and ep.ai_gateway:
            gw = ep.ai_gateway
            gw_config = {
                "guardrails": _serialize_guardrails(gw.guardrails) if gw.guardrails else None,
                "rate_limits": _serialize_rate_limits(gw.rate_limits) if gw.rate_limits else [],
                "usage_tracking": {
                    "enabled": gw.usage_tracking_config.enabled if gw.usage_tracking_config else False,
                } if gw.usage_tracking_config else None,
                "inference_table": {
                    "enabled": gw.inference_table_config.enabled if gw.inference_table_config else False,
                    "catalog_name": gw.inference_table_config.catalog_name if gw.inference_table_config else None,
                    "schema_name": gw.inference_table_config.schema_name if gw.inference_table_config else None,
                    "table_name_prefix": gw.inference_table_config.table_name_prefix if gw.inference_table_config else None,
                } if gw.inference_table_config else None,
            }

        results.append({
            "endpoint_id": ep.id or "",
            "name": ep.name or "",
            "state": state_ready,
            "task": task or "",
            "endpoint_type": ep_type or "",
            "creator": creator or "",
            "served_entities": served_entities,
            "ai_gateway": gw_config,
            "tags": {t.key: t.value for t in ep.tags} if ep.tags else {},
            "creation_timestamp": ep.creation_timestamp,
        })
    return results


def _serialize_guardrails(guardrails) -> Dict[str, Any]:
    """Serialize AI Gateway guardrails config to a dict."""
    result: Dict[str, Any] = {}
    try:
        if guardrails.input:
            result["input"] = {
                "pii": {"behavior": guardrails.input.pii.behavior.value} if guardrails.input.pii else None,
                "safety": guardrails.input.safety,
                "valid_topics": guardrails.input.valid_topics,
                "invalid_keywords": guardrails.input.invalid_keywords,
            }
        if guardrails.output:
            result["output"] = {
                "pii": {"behavior": guardrails.output.pii.behavior.value} if guardrails.output.pii else None,
                "safety": guardrails.output.safety,
                "valid_topics": guardrails.output.valid_topics,
                "invalid_keywords": guardrails.output.invalid_keywords,
            }
    except Exception:
        result["_raw"] = str(guardrails)
    return result


def _serialize_rate_limits(rate_limits) -> List[Dict[str, Any]]:
    """Serialize AI Gateway rate limit configs to a list of dicts."""
    results = []
    if not rate_limits:
        return results
    for rl in rate_limits:
        try:
            results.append({
                "calls": rl.calls,
                "renewal_period": rl.renewal_period.value if rl.renewal_period else None,
                "key": rl.key.value if rl.key else None,
            })
        except Exception:
            results.append({"_raw": str(rl)})
    return results


def _get_endpoint_permissions(endpoint_id: str) -> List[Dict[str, Any]]:
    """Get permissions for a specific serving endpoint."""
    if not endpoint_id:
        return []  # FMAPI / system endpoints have no ID
    w = _get_workspace_client()
    if not w:
        return []
    try:
        perms = w.permissions.get("serving-endpoints", endpoint_id)
        results = []
        if perms.access_control_list:
            for acl in perms.access_control_list:
                principal = acl.user_name or acl.group_name or acl.service_principal_name or "unknown"
                principal_type = (
                    "user" if acl.user_name
                    else "group" if acl.group_name
                    else "service_principal" if acl.service_principal_name
                    else "unknown"
                )
                permissions = []
                if acl.all_permissions:
                    for p in acl.all_permissions:
                        permissions.append({
                            "permission_level": p.permission_level.value if p.permission_level else "",
                            "inherited": p.inherited or False,
                            "inherited_from_object": (
                                p.inherited_from_object[0] if p.inherited_from_object else None
                            ),
                        })
                results.append({
                    "principal": principal,
                    "principal_type": principal_type,
                    "permissions": permissions,
                })
        return results
    except Exception as exc:
        logger.warning("Failed to get permissions for %s: %s", endpoint_id, exc)
        return []


def _get_resource_permissions(resource_type: str, resource_id: str) -> List[Dict[str, Any]]:
    """Get permissions for any Databricks resource (apps, genie, etc.).

    Uses the generic ``w.permissions.get(resource_type, resource_id)`` call and
    returns the same shape as ``_get_endpoint_permissions``.
    """
    if not resource_id:
        return []
    w = _get_workspace_client()
    if not w:
        return []
    try:
        perms = w.permissions.get(resource_type, resource_id)
        results = []
        if perms.access_control_list:
            for acl in perms.access_control_list:
                principal = acl.user_name or acl.group_name or acl.service_principal_name or "unknown"
                principal_type = (
                    "user" if acl.user_name
                    else "group" if acl.group_name
                    else "service_principal" if acl.service_principal_name
                    else "unknown"
                )
                permissions = []
                if acl.all_permissions:
                    for p in acl.all_permissions:
                        permissions.append({
                            "permission_level": p.permission_level.value if p.permission_level else "",
                            "inherited": p.inherited or False,
                            "inherited_from_object": (
                                p.inherited_from_object[0] if p.inherited_from_object else None
                            ),
                        })
                results.append({
                    "principal": principal,
                    "principal_type": principal_type,
                    "permissions": permissions,
                })
        return results
    except Exception as exc:
        logger.warning("Failed to get %s permissions for %s: %s", resource_type, resource_id, exc)
        return []


def _get_app_permissions(app_name: str) -> List[Dict[str, Any]]:
    """Get permissions for a Databricks App."""
    return _get_resource_permissions("apps", app_name)


def _get_genie_permissions(space_id: str) -> List[Dict[str, Any]]:
    """Get permissions for a Genie space."""
    return _get_resource_permissions("genie", space_id)


def _infer_principal_type(principal: str) -> str:
    """Infer principal type from name pattern (UC grants don't include type)."""
    import re
    if "@" in principal:
        return "user"
    # UUIDs are typically service principals
    if re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', principal, re.IGNORECASE):
        return "service_principal"
    return "group"


def _get_fmapi_uc_model_name(endpoint_name: str, endpoints: Optional[List[Dict]] = None) -> str:
    """Derive the UC model name for an FMAPI endpoint."""
    if endpoints is None:
        endpoints = get_all_endpoints()
    for ep in endpoints:
        if ep["name"] == endpoint_name:
            for se in (ep.get("served_entities") or []):
                entity = se.get("entity_name", "")
                if "system.ai" in entity:
                    return entity
            break
    # Fallback: strip databricks- prefix
    model = endpoint_name.replace("databricks-", "", 1) if endpoint_name.startswith("databricks-") else endpoint_name
    return f"system.ai.{model}"


# =====================================================================
# PUBLIC API — called by the FastAPI routes
# =====================================================================

def get_all_endpoints() -> List[Dict[str, Any]]:
    """List all serving endpoints with their configurations (cached)."""
    cached = _cache_get("endpoints")
    if cached is not None:
        return cached
    return _cache_set("endpoints", _list_serving_endpoints())


def get_endpoint(name: str) -> Optional[Dict[str, Any]]:
    """Get a single endpoint by name."""
    eps = get_all_endpoints()
    for ep in eps:
        if ep["name"] == name or ep["endpoint_id"] == name:
            return ep
    return None


def get_overview() -> Dict[str, Any]:
    """KPI overview for the AI Gateway page (cached)."""
    cached = _cache_get("overview")
    if cached is not None:
        return cached

    eps = get_all_endpoints()

    total = len(eps)
    ready = sum(1 for e in eps if e["state"] == "READY")
    not_ready = total - ready
    has_gateway = sum(1 for e in eps if e.get("ai_gateway"))

    # Task distribution
    tasks: Dict[str, int] = {}
    for e in eps:
        t = e.get("task") or "unknown"
        tasks[t] = tasks.get(t, 0) + 1

    # Try to get recent usage stats from system tables
    usage_stats = _get_usage_overview_stats(days=1)

    result = {
        "total_endpoints": total,
        "ready_endpoints": ready,
        "not_ready_endpoints": not_ready,
        "gateway_enabled": has_gateway,
        "total_requests_24h": usage_stats.get("total_requests", 0),
        "total_input_tokens_24h": usage_stats.get("total_input_tokens", 0),
        "total_output_tokens_24h": usage_stats.get("total_output_tokens", 0),
        "error_count_24h": usage_stats.get("error_count", 0),
        "error_rate_24h": usage_stats.get("error_rate", 0),
        "unique_users_24h": usage_stats.get("unique_users", 0),
        "tasks": tasks,
    }
    return _cache_set("overview", result)


def get_permissions(endpoint_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get permissions across endpoints or for a specific one (cached)."""
    ck = f"permissions:{endpoint_name or '__all__'}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    eps = get_all_endpoints()
    if endpoint_name:
        eps = [e for e in eps if e["name"] == endpoint_name]

    results = []
    for ep in eps:
        eid = ep["endpoint_id"]
        if not eid:
            continue  # FMAPI / system endpoints have no ID — skip
        perms = _get_endpoint_permissions(eid)
        for p in perms:
            p["endpoint_name"] = ep["name"]
            p["endpoint_id"] = eid
        results.extend(perms)
    return _cache_set(ck, results)


def get_rate_limits(endpoint_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get rate limits from AI Gateway config on endpoints (cached)."""
    ck = f"rate_limits:{endpoint_name or '__all__'}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    eps = get_all_endpoints()
    if endpoint_name:
        eps = [e for e in eps if e["name"] == endpoint_name]

    results = []
    for ep in eps:
        gw = ep.get("ai_gateway")
        if gw and gw.get("rate_limits"):
            for rl in gw["rate_limits"]:
                results.append({
                    "endpoint_name": ep["name"],
                    "endpoint_id": ep["endpoint_id"],
                    "calls": rl.get("calls"),
                    "renewal_period": rl.get("renewal_period"),
                    "key": rl.get("key"),
                })
    return _cache_set(ck, results)


def get_guardrails(endpoint_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get guardrails config from AI Gateway on endpoints (cached)."""
    ck = f"guardrails:{endpoint_name or '__all__'}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    eps = get_all_endpoints()
    if endpoint_name:
        eps = [e for e in eps if e["name"] == endpoint_name]

    results = []
    for ep in eps:
        gw = ep.get("ai_gateway")
        if gw and gw.get("guardrails"):
            results.append({
                "endpoint_name": ep["name"],
                "endpoint_id": ep["endpoint_id"],
                "guardrails": gw["guardrails"],
            })
    return _cache_set(ck, results)


def get_inference_table_config(endpoint_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get inference table configs from AI Gateway on endpoints (cached)."""
    ck = f"inference_tbl_cfg:{endpoint_name or '__all__'}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    eps = get_all_endpoints()
    if endpoint_name:
        eps = [e for e in eps if e["name"] == endpoint_name]

    results = []
    for ep in eps:
        gw = ep.get("ai_gateway")
        if gw and gw.get("inference_table"):
            results.append({
                "endpoint_name": ep["name"],
                "endpoint_id": ep["endpoint_id"],
                "inference_table": gw["inference_table"],
            })
    return _cache_set(ck, results)


# ── Usage from system.serving.endpoint_usage ─────────────────────

def _get_usage_overview_stats(days: int = 1) -> Dict[str, Any]:
    """Get aggregate usage stats from Lakebase cache."""
    from backend.database import execute_query
    rows = execute_query(
        """SELECT COALESCE(SUM(request_count), 0) AS total_requests,
                  COALESCE(SUM(input_tokens), 0) AS total_input_tokens,
                  COALESCE(SUM(output_tokens), 0) AS total_output_tokens,
                  COALESCE(SUM(error_count), 0) AS error_count,
                  COUNT(DISTINCT NULLIF(requester, '')) AS unique_users
           FROM gateway_usage_daily
           WHERE usage_date >= CURRENT_DATE - INTERVAL '%s days'""",
        (days,),
    )
    if not rows:
        return {}
    r = rows[0]
    total = int(r.get("total_requests") or 0)
    errors = int(r.get("error_count") or 0)
    return {
        "total_requests": total,
        "total_input_tokens": int(r.get("total_input_tokens") or 0),
        "total_output_tokens": int(r.get("total_output_tokens") or 0),
        "error_count": errors,
        "error_rate": round(errors * 100.0 / total, 2) if total > 0 else 0,
        "unique_users": int(r.get("unique_users") or 0),
    }


def ensure_gateway_usage_columns() -> None:
    """Defensively add columns the app reads but the discovery workflow owns.

    The gateway_usage_* tables are created/populated by the discovery workflow,
    not the app. When the app ships a read for a new column (e.g.
    rate_limited_count) before the workflow's ALTER has run, the SELECT would
    fail and blank the whole usage view. Adding the column here (idempotent,
    no-op if the table doesn't exist yet) keeps reads self-healing.
    """
    from backend.database import execute_update
    for stmt in (
        "ALTER TABLE gateway_usage_daily  ADD COLUMN IF NOT EXISTS rate_limited_count BIGINT DEFAULT 0",
        "ALTER TABLE gateway_usage_hourly ADD COLUMN IF NOT EXISTS rate_limited_count BIGINT DEFAULT 0",
    ):
        try:
            execute_update(stmt)
        except Exception as exc:
            logger.warning("gateway_usage column ensure skipped: %s", exc)


def _row_int(r: Dict[str, Any], k: str) -> int:
    """Coerce a nullable Lakebase numeric column to int."""
    return int(r.get(k) or 0)


def _max_as_of(rows: List[Dict[str, Any]]) -> Optional[str]:
    """Latest max_event_time across rows — the cache's 'as of' timestamp."""
    return max((r.get("max_event_time") for r in rows if r.get("max_event_time")), default=None)


def get_uag_v2_usage() -> Dict[str, Any]:
    """Unity AI Gateway (v2) usage summary from `uag_usage_summary` (sourced from
    system.ai_gateway.usage — v2-routed endpoints only, ~20-min fresh).

    Returns {as_of, totals, endpoints}. Degrades to empty if the table isn't
    synced yet or the workflow couldn't read the (account-scoped) system table.
    """
    from backend.database import execute_query
    empty = {"as_of": None, "totals": {}, "endpoints": [], "breakdowns": {}}
    try:
        rows = execute_query(
            """SELECT endpoint_name, request_count, input_tokens, output_tokens,
                      cache_read_tokens, cache_creation_tokens,
                      p50_latency_ms, p90_latency_ms, p95_latency_ms, p99_latency_ms, p95_ttfb_ms,
                      error_count, unique_users, max_event_time
               FROM uag_usage_summary
               ORDER BY request_count DESC LIMIT 200"""
        )
    except Exception as exc:
        logger.warning("uag_usage_summary not available: %s", exc)
        return empty
    if not rows:
        return empty

    _i = _row_int
    total_req = sum(_i(r, "request_count") for r in rows)
    total_in = sum(_i(r, "input_tokens") for r in rows)
    total_out = sum(_i(r, "output_tokens") for r in rows)
    total_cache_read = sum(_i(r, "cache_read_tokens") for r in rows)
    total_cache_create = sum(_i(r, "cache_creation_tokens") for r in rows)
    cached = total_cache_read + total_cache_create
    cache_pct = round(100.0 * total_cache_read / total_in, 1) if total_in else 0.0
    as_of = _max_as_of(rows)

    # Additive breakdowns (agent-vs-human / by model / by api_type) — same source table.
    breakdowns: Dict[str, list] = {}
    try:
        bd = execute_query(
            """SELECT dimension, key, request_count, input_tokens, output_tokens, cached_tokens
               FROM uag_usage_breakdown ORDER BY request_count DESC"""
        )
        for r in bd:
            breakdowns.setdefault(r.get("dimension") or "unknown", []).append({
                "key": r.get("key", ""),
                "request_count": int(r.get("request_count") or 0),
                "input_tokens": int(r.get("input_tokens") or 0),
                "output_tokens": int(r.get("output_tokens") or 0),
                "cached_tokens": int(r.get("cached_tokens") or 0),
            })
    except Exception as exc:
        logger.warning("uag_usage_breakdown not available: %s", exc)

    return {
        "as_of": as_of,
        "breakdowns": breakdowns,
        "totals": {
            "request_count": total_req,
            "input_tokens": total_in,
            "output_tokens": total_out,
            "cached_tokens": cached,
            "cache_read_pct": cache_pct,
            "endpoints": len(rows),
        },
        # NOTE: p50/p90/p95/p99 are PER-ENDPOINT percentiles — they are non-additive,
        # so do not sum/average them into an account-level KPI (that would be wrong).
        "endpoints": [
            {
                "endpoint_name": r.get("endpoint_name", ""),
                "request_count": _i(r, "request_count"),
                "input_tokens": _i(r, "input_tokens"),
                "output_tokens": _i(r, "output_tokens"),
                "cache_read_tokens": _i(r, "cache_read_tokens"),
                "cache_creation_tokens": _i(r, "cache_creation_tokens"),
                "p50_latency_ms": _i(r, "p50_latency_ms"),
                "p90_latency_ms": _i(r, "p90_latency_ms"),
                "p95_latency_ms": _i(r, "p95_latency_ms"),
                "p99_latency_ms": _i(r, "p99_latency_ms"),
                "p95_ttfb_ms": _i(r, "p95_ttfb_ms"),
                "error_count": _i(r, "error_count"),
                "unique_users": _i(r, "unique_users"),
            }
            for r in rows
        ],
    }


def get_uag_budget_status() -> Dict[str, Any]:
    """Budget configuration inventory from `uag_budget_status` (sourced from the
    account Budgets API, /api/2.1/accounts/{id}/budgets).

    Read-only: surfaces each native budget's cap thresholds, whether it *enforces*
    (BLOCK_USAGE) vs only *alerts* (email), its filter, and AI-relevance — the
    fleet-wide view the native per-budget UI doesn't give. Enforcement stays
    entirely platform-side; the app never creates or enforces budgets.

    Returns {as_of, totals, budgets}. Degrades to empty when the table isn't
    synced yet or the discovery workflow had no account-level credentials to
    read the (account-scoped) Budgets API.

    NOTE: consumption (% of cap used) is not populated yet — the account Budgets
    API is config-only, so spend-vs-cap requires reproducing each budget's
    tag/workspace filter against system.billing.usage (a separate validated pass).
    """
    from backend.database import execute_query
    empty = {"as_of": None, "totals": {}, "budgets": []}
    try:
        # Totals aggregate the FULL table (not the limited list below), so KPIs
        # stay correct on accounts with more budgets than the display cap.
        agg = execute_query(
            """SELECT count(*) AS budget_count,
                      count(*) FILTER (WHERE enforce) AS enforcing_count,
                      count(*) FILTER (WHERE alerting) AS alerting_count,
                      count(*) FILTER (WHERE is_ai) AS ai_budget_count,
                      count(*) FILTER (WHERE pct_used >= 100) AS over_cap_count,
                      count(*) FILTER (WHERE pct_used >= 80 AND pct_used < 100) AS near_cap_count,
                      max(discovered_at) AS max_discovered
               FROM uag_budget_status"""
        )
        rows = execute_query(
            """SELECT budget_id, display_name, enforce, alerting,
                      min_threshold_usd, max_threshold_usd, time_period,
                      filter_summary, is_ai, spent_usd, pct_used, discovered_at
               FROM uag_budget_status
               ORDER BY pct_used DESC NULLS LAST, max_threshold_usd DESC NULLS LAST LIMIT 500"""
        )
    except Exception as exc:
        logger.warning("uag_budget_status not available: %s", exc)
        return empty
    if not agg or _row_int(agg[0], "budget_count") == 0:
        return empty

    a = agg[0]
    as_of = str(a.get("max_discovered")) if a.get("max_discovered") else None

    return {
        "as_of": as_of,
        "totals": {
            "budget_count": _row_int(a, "budget_count"),
            "enforcing_count": _row_int(a, "enforcing_count"),
            "alerting_count": _row_int(a, "alerting_count"),
            "ai_budget_count": _row_int(a, "ai_budget_count"),
            "over_cap_count": _row_int(a, "over_cap_count"),
            "near_cap_count": _row_int(a, "near_cap_count"),
        },
        "budgets": [
            {
                "budget_id": r.get("budget_id", ""),
                "display_name": r.get("display_name", ""),
                "enforce": bool(r.get("enforce")),
                "alerting": bool(r.get("alerting")),
                "min_threshold_usd": float(r.get("min_threshold_usd") or 0),
                "max_threshold_usd": float(r.get("max_threshold_usd") or 0),
                "time_period": r.get("time_period", ""),
                "filter_summary": r.get("filter_summary", ""),
                "is_ai": bool(r.get("is_ai")),
                # spent/pct are None (n/a) when the budget's filter shape isn't computable
                "spent_usd": float(r["spent_usd"]) if r.get("spent_usd") is not None else None,
                "pct_used": float(r["pct_used"]) if r.get("pct_used") is not None else None,
            }
            for r in rows
        ],
    }


def get_endpoint_inventory() -> Dict[str, Any]:
    """Account-wide served-entity inventory from `serving_endpoints_inventory`
    (system.serving.served_entities). Read-only fleet view across ALL workspaces in
    the metastore — the per-workspace serving API only sees the deploy workspace.
    Live management (ACL/config edits) stays per-workspace. Degrades to empty if the
    table isn't synced or served_entities wasn't readable at the discovery scope.
    """
    from backend.database import execute_query
    empty = {"as_of": None, "totals": {}, "endpoints": []}
    try:
        agg = execute_query(
            """SELECT count(*) AS served_entity_count,
                      count(DISTINCT endpoint_id) AS endpoint_count,
                      count(DISTINCT workspace_id) AS workspace_count,
                      count(*) FILTER (WHERE entity_type = 'FOUNDATION_MODEL') AS foundation_count,
                      count(*) FILTER (WHERE entity_type = 'CUSTOM_MODEL') AS custom_count,
                      count(*) FILTER (WHERE entity_type = 'EXTERNAL_MODEL') AS external_count,
                      max(discovered_at) AS max_discovered
               FROM serving_endpoints_inventory"""
        )
        rows = execute_query(
            """SELECT endpoint_name, workspace_id, entity_type, entity_name,
                      entity_version, provider, task, created_by, change_time
               FROM serving_endpoints_inventory
               ORDER BY change_time DESC NULLS LAST LIMIT 2000"""
        )
    except Exception as exc:
        logger.warning("serving_endpoints_inventory not available: %s", exc)
        return empty
    if not agg or _row_int(agg[0], "served_entity_count") == 0:
        return empty
    a = agg[0]
    return {
        "as_of": str(a.get("max_discovered")) if a.get("max_discovered") else None,
        "totals": {
            "served_entity_count": _row_int(a, "served_entity_count"),
            "endpoint_count": _row_int(a, "endpoint_count"),
            "workspace_count": _row_int(a, "workspace_count"),
            "foundation_count": _row_int(a, "foundation_count"),
            "custom_count": _row_int(a, "custom_count"),
            "external_count": _row_int(a, "external_count"),
        },
        "endpoints": [
            {
                "endpoint_name": r.get("endpoint_name", ""),
                "workspace_id": r.get("workspace_id", ""),
                "entity_type": r.get("entity_type", ""),
                "entity_name": r.get("entity_name", ""),
                "entity_version": r.get("entity_version", ""),
                "provider": r.get("provider", ""),
                "task": r.get("task", ""),
                "created_by": r.get("created_by", ""),
                "change_time": str(r["change_time"]) if r.get("change_time") else None,
            }
            for r in rows
        ],
    }


# ── Unity Gateway v3 — UC model services + grants (OBO, UC-enforced) ──

def _uc_call(method: str, path: str, user_token: str = "", json_body: Optional[dict] = None):
    """Call the workspace UC REST API. Uses the caller's OBO token when present
    (so UC enforces the user's own privileges — MANAGE required to edit grants);
    falls back to the app SP for reads when OBO isn't enabled."""
    from backend.config import get_databricks_host, _sdk_auth_headers
    host = get_databricks_host()
    if user_token:
        headers = {"Authorization": f"Bearer {user_token}"}
    else:
        headers = _sdk_auth_headers() or {}
    headers["Content-Type"] = "application/json"
    return httpx.request(method, f"{host}{path}", headers=headers, json=json_body, timeout=30)


def _uc_get_json(path: str, user_token: str = "") -> Optional[dict]:
    """GET a UC REST path, preferring the caller's OBO token but falling back to
    the app SP when OBO is rejected (the v3 UC APIs currently 403 downscoped OBO
    tokens; the app SP has the metastore read access, like the app's other UC
    calls). Returns parsed JSON or None."""
    for tok in ([user_token, ""] if user_token else [""]):
        try:
            resp = _uc_call("GET", path, tok)
            if resp.status_code == 200:
                return resp.json()
        except Exception as exc:
            logger.warning("UC GET %s failed: %s", path, exc)
    return None


def list_model_services(user_token: str = "") -> Dict[str, Any]:
    """v3 Unity Gateway UC model services from the `model_services_inventory`
    Lakebase cache (populated by the discovery workflow, which enumerates
    account-wide via its metastore-admin run-as — the list endpoint the app's
    OBO/SP identities can't reliably reach). Grants are read/edited live
    per-service (get_model_service_grants / set_model_service_grant)."""
    from backend.database import execute_query
    try:
        rows = execute_query(
            """SELECT full_name, owner, supported_api_types, create_time
               FROM model_services_inventory ORDER BY full_name LIMIT 2000"""
        )
    except Exception as exc:
        logger.warning("model_services_inventory not available: %s", exc)
        return {"services": []}
    return {
        "services": [
            {
                "full_name": r.get("full_name", ""),
                "owner": r.get("owner", ""),
                "supported_api_types": (r.get("supported_api_types") or "").split(",") if r.get("supported_api_types") else [],
                "create_time": r.get("create_time"),
            }
            for r in rows
        ]
    }


def get_model_service_grants(full_name: str, user_token: str = "") -> Dict[str, Any]:
    """UC grants on one model service (securable_type MODEL_SERVICE)."""
    body = _uc_get_json(f"/api/2.1/unity-catalog/permissions/model_service/{full_name}", user_token)
    if body is None:
        return {"grants": [], "error": "grants not readable (insufficient privileges)"}
    return {
        "grants": [
            {"principal": pa.get("principal", ""), "privileges": pa.get("privileges", [])}
            for pa in body.get("privilege_assignments", [])
        ]
    }


def set_model_service_grant(
    full_name: str, principal: str, add: Optional[List[str]] = None,
    remove: Optional[List[str]] = None, user_token: str = "",
) -> Dict[str, Any]:
    """Add/remove UC privileges for a principal on a model service. Prefers the
    caller's OBO token (UC-enforced), but Apps OBO can't carry the required
    `unity-catalog` scope today, so it falls back to the app SP (which has MANAGE).
    The route gates this on require_admin; when the SP path is used the authorization
    boundary is that admin gate, not per-user UC enforcement, and the write is
    SP-attributed. NOTE: currently dormant — the UI is read-only (see
    ideas/model-services-write-path.md for the planned user-MANAGE-checked design)."""
    body = {"changes": [{"principal": principal, "add": add or [], "remove": remove or []}]}
    path = f"/api/2.1/unity-catalog/permissions/model_service/{full_name}"
    # OBO can't carry the `unity-catalog` scope (not grantable to Apps OBO), so fall
    # back to the app SP — same pattern as v1 local permission writes. The route gates
    # this on require_admin; UC still enforces the SP has MANAGE on the securable.
    last = "no attempt"
    for tok in ([user_token, ""] if user_token else [""]):
        try:
            resp = _uc_call("PATCH", path, tok, json_body=body)
            if resp.status_code == 200:
                return {"ok": True, "via": "obo" if tok else "sp"}
            last = f"{resp.status_code}: {resp.text[:220]}"
        except Exception as exc:
            last = str(exc)[:220]
    return {"ok": False, "error": last}


def get_uag_mcp_tools() -> Dict[str, Any]:
    """Per-tool MCP activity from `uag_mcp_tool_daily` (sourced from
    system.ai_gateway.usage rows where service_type = MCP_SERVICE).

    Returns {as_of, totals, tools}. Degrades to empty when the table isn't
    synced or the workspace routes no MCP traffic through UAG v2.
    """
    from backend.database import execute_query
    empty = {"as_of": None, "totals": {}, "tools": []}
    # Account-wide totals from an unbounded aggregate — deriving them from the
    # capped list below would undercount services/tools/requests past the LIMIT.
    try:
        agg = execute_query(
            """SELECT COUNT(*) AS tools, COUNT(DISTINCT service_name) AS services,
                      COALESCE(SUM(request_count), 0) AS request_count,
                      COALESCE(SUM(error_count), 0) AS error_count,
                      MAX(max_event_time) AS as_of
               FROM uag_mcp_tool_daily"""
        )
    except Exception as exc:
        logger.warning("uag_mcp_tool_daily not available: %s", exc)
        return empty
    totals = agg[0] if agg else {}
    if not totals or _row_int(totals, "tools") == 0:
        return empty

    rows = execute_query(
        """SELECT service_name, tool_name, server_type, request_count,
                  error_count, unique_users
           FROM uag_mcp_tool_daily
           ORDER BY request_count DESC LIMIT 25"""
    )
    return {
        "as_of": totals.get("as_of"),
        "totals": {
            "request_count": _row_int(totals, "request_count"),
            "error_count": _row_int(totals, "error_count"),
            "services": _row_int(totals, "services"),
            "tools": _row_int(totals, "tools"),
        },
        "tools": [
            {
                "service_name": r.get("service_name", ""),
                "tool_name": r.get("tool_name") or "",
                "server_type": r.get("server_type") or "",
                "request_count": _row_int(r, "request_count"),
                "error_count": _row_int(r, "error_count"),
                "unique_users": _row_int(r, "unique_users"),
            }
            for r in rows
        ],
    }


def get_guardrail_coverage() -> Dict[str, Any]:
    """Guardrail COVERAGE / activity from `uag_guardrail_daily` — which endpoints
    have Unity AI Gateway v2 guardrails running, how often, and by which judge
    model(s).

    IMPORTANT: this is coverage/activity only, NOT block/mask outcomes. The
    guardrail verdict is not present in system.ai_gateway.usage (guardrail rows
    are the judge-model invocations, which succeed with 200); outcomes require
    the enrollment-gated UAG feature-results surface. Degrades to empty when the
    table is unsynced or no endpoint has guardrails active.
    """
    from backend.database import execute_query, execute_one
    empty: Dict[str, Any] = {"as_of": None, "totals": {}, "endpoints": []}
    # Totals from an unbounded aggregate (the row list below is capped for display).
    # No coverage ratio: a comparable "total guardable endpoints" denominator isn't
    # derivable here (uag_usage_summary counts MCP + judge endpoints too), so we
    # report the guarded count rather than a misleading "X of Y".
    try:
        agg = execute_one(
            """SELECT COUNT(*) AS guarded_endpoints,
                      COALESCE(SUM(checked_requests), 0) AS checked_requests,
                      MAX(max_event_time) AS as_of
               FROM uag_guardrail_daily"""
        )
    except Exception as exc:
        logger.warning("uag_guardrail_daily not available: %s", exc)
        return empty
    agg = dict(agg) if agg else {}
    if _row_int(agg, "guarded_endpoints") == 0:
        return empty

    rows = execute_query(
        """SELECT endpoint_name, checked_requests, unique_users, judge_models
           FROM uag_guardrail_daily
           ORDER BY checked_requests DESC LIMIT 200"""
    )
    return {
        "as_of": agg.get("as_of"),
        "totals": {
            "guarded_endpoints": _row_int(agg, "guarded_endpoints"),
            "checked_requests": _row_int(agg, "checked_requests"),
        },
        "endpoints": [
            {
                "endpoint_name": r.get("endpoint_name", ""),
                "checked_requests": _row_int(r, "checked_requests"),
                "unique_users": _row_int(r, "unique_users"),
                "judge_models": r.get("judge_models") or "",
            }
            for r in rows
        ],
    }


def get_throttling() -> Dict[str, Any]:
    """Throttling / reliability per endpoint from `uag_throttling_daily`: HTTP 429
    (rate-limited) and 5xx (server-error) counts vs total requests, over the
    discovery window. Answers "which endpoints are getting rate-limited/erroring".
    Degrades to empty when the table is unsynced or no endpoint saw 429/5xx.
    """
    from backend.database import execute_query, execute_one
    empty: Dict[str, Any] = {"as_of": None, "totals": {}, "endpoints": []}
    try:
        agg = execute_one(
            """SELECT COUNT(*) AS endpoints,
                      COALESCE(SUM(total_requests), 0)     AS total_requests,
                      COALESCE(SUM(throttled_count), 0)    AS throttled_count,
                      COALESCE(SUM(server_error_count), 0) AS server_error_count,
                      MAX(max_event_time) AS as_of
               FROM uag_throttling_daily"""
        )
    except Exception as exc:
        logger.warning("uag_throttling_daily not available: %s", exc)
        return empty
    agg = dict(agg) if agg else {}
    if _row_int(agg, "endpoints") == 0:
        return empty

    rows = execute_query(
        """SELECT endpoint_name, total_requests, throttled_count, server_error_count
           FROM uag_throttling_daily
           ORDER BY throttled_count DESC LIMIT 200"""
    )
    return {
        "as_of": agg.get("as_of"),
        "totals": {
            # NOTE: no blended fleet-wide throttle_rate — uag_throttling_daily is
            # pre-filtered to endpoints with ≥1 429/5xx, so SUM(throttled)/SUM(total)
            # would divide by erroring-endpoint traffic only and overstate fleet
            # throttling. The meaningful rate is per-endpoint (below); the headline
            # is the COUNT of affected endpoints.
            "endpoints": _row_int(agg, "endpoints"),
            "total_requests": _row_int(agg, "total_requests"),
            "throttled_count": _row_int(agg, "throttled_count"),
            "server_error_count": _row_int(agg, "server_error_count"),
        },
        "endpoints": [
            {
                "endpoint_name": r.get("endpoint_name", ""),
                "total_requests": _row_int(r, "total_requests"),
                "throttled_count": _row_int(r, "throttled_count"),
                "server_error_count": _row_int(r, "server_error_count"),
                "throttle_rate": (
                    _row_int(r, "throttled_count") / _row_int(r, "total_requests")
                    if _row_int(r, "total_requests") > 0 else None
                ),
            }
            for r in rows
        ],
    }


def get_fallback_routing() -> Dict[str, Any]:
    """Smart-routing fallback per endpoint from `uag_fallback_routing_daily`: how
    often the AI Gateway had to fall back to a backup model (>1 routing attempt),
    how many recovered (final attempt < 400), and which backup destinations were
    used. Reliability signal for AI Gateway smart-routing. Degrades to empty when
    the table is unsynced or no endpoint fell back.
    """
    from backend.database import execute_query, execute_one
    empty: Dict[str, Any] = {"as_of": None, "totals": {}, "endpoints": []}
    try:
        agg = execute_one(
            """SELECT COUNT(*) AS endpoints,
                      COALESCE(SUM(fallback_requests), 0)  AS fallback_requests,
                      COALESCE(SUM(fallback_recovered), 0) AS fallback_recovered,
                      MAX(max_event_time) AS as_of
               FROM uag_fallback_routing_daily"""
        )
    except Exception as exc:
        logger.warning("uag_fallback_routing_daily not available: %s", exc)
        return empty
    agg = dict(agg) if agg else {}
    if _row_int(agg, "endpoints") == 0:
        return empty

    rows = execute_query(
        """SELECT endpoint_name, total_requests, fallback_requests,
                  fallback_recovered, fallback_destinations
           FROM uag_fallback_routing_daily
           ORDER BY fallback_requests DESC LIMIT 200"""
    )
    return {
        "as_of": agg.get("as_of"),
        "totals": {
            "endpoints": _row_int(agg, "endpoints"),
            "fallback_requests": _row_int(agg, "fallback_requests"),
            "fallback_recovered": _row_int(agg, "fallback_recovered"),
        },
        "endpoints": [
            {
                "endpoint_name": r.get("endpoint_name", ""),
                "total_requests": _row_int(r, "total_requests"),
                "fallback_requests": _row_int(r, "fallback_requests"),
                "fallback_recovered": _row_int(r, "fallback_recovered"),
                "fallback_destinations": r.get("fallback_destinations") or "",
                # share of THIS endpoint's fallbacks that recovered (null if none)
                "recovery_rate": (
                    _row_int(r, "fallback_recovered") / _row_int(r, "fallback_requests")
                    if _row_int(r, "fallback_requests") > 0 else None
                ),
            }
            for r in rows
        ],
    }


def get_uag_v2_timeseries() -> Dict[str, Any]:
    """Daily UAG v2 usage series (requests + tokens) from `uag_usage_timeseries_daily`
    for trend charts on the v2 tab. Degrades to empty when unsynced / no v2 traffic."""
    from backend.database import execute_query
    try:
        rows = execute_query(
            """SELECT usage_date, request_count, input_tokens, output_tokens
               FROM uag_usage_timeseries_daily ORDER BY usage_date"""
        )
    except Exception as exc:
        logger.warning("uag_usage_timeseries_daily not available: %s", exc)
        return {"series": []}
    return {
        "series": [
            {
                "usage_date": r.get("usage_date", ""),
                "request_count": _row_int(r, "request_count"),
                "input_tokens": _row_int(r, "input_tokens"),
                "output_tokens": _row_int(r, "output_tokens"),
            }
            for r in rows
        ]
    }


def get_uag_coding_agents() -> Dict[str, Any]:
    """Coding-agent activity from `uag_coding_agent_usage` (classified from
    user_agent in system.ai_gateway.usage): Claude Code / Codex / Cursor / Gemini
    CLI, with requests, users, active days, tokens.

    Activity only — sessions / commits / lines-of-code are not in
    system.ai_gateway.usage. Degrades to empty when unsynced / no coding-agent traffic.
    """
    from backend.database import execute_query
    empty: Dict[str, Any] = {"as_of": None, "agents": []}
    try:
        rows = execute_query(
            """SELECT coding_agent, request_count, unique_users, active_days,
                      total_tokens, max_event_time
               FROM uag_coding_agent_usage ORDER BY request_count DESC"""
        )
    except Exception as exc:
        logger.warning("uag_coding_agent_usage not available: %s", exc)
        return empty
    if not rows:
        return empty
    return {
        "as_of": _max_as_of(rows),
        "agents": [
            {
                "coding_agent": r.get("coding_agent", ""),
                "request_count": _row_int(r, "request_count"),
                "unique_users": _row_int(r, "unique_users"),
                "active_days": _row_int(r, "active_days"),
                "total_tokens": _row_int(r, "total_tokens"),
            }
            for r in rows
        ],
    }


def get_usage_summary(days: int = 7) -> List[Dict[str, Any]]:
    """Per-endpoint usage summary from Lakebase cache."""
    from backend.database import execute_query
    rows = execute_query(
        """SELECT endpoint_name,
                  SUM(request_count) AS total_requests,
                  SUM(input_tokens) AS total_input_tokens,
                  SUM(output_tokens) AS total_output_tokens,
                  SUM(error_count) AS error_count,
                  COALESCE(SUM(rate_limited_count), 0) AS rate_limited_count,
                  COUNT(DISTINCT NULLIF(requester, '')) AS unique_users
           FROM gateway_usage_daily
           WHERE usage_date >= CURRENT_DATE - INTERVAL '%s days'
           GROUP BY endpoint_name ORDER BY total_requests DESC LIMIT 100""",
        (days,),
    )
    return [
        {
            "endpoint_name": r.get("endpoint_name", ""),
            "total_requests": int(r.get("total_requests") or 0),
            "total_input_tokens": int(r.get("total_input_tokens") or 0),
            "total_output_tokens": int(r.get("total_output_tokens") or 0),
            "error_count": int(r.get("error_count") or 0),
            "rate_limited_count": int(r.get("rate_limited_count") or 0),
            "unique_users": int(r.get("unique_users") or 0),
        }
        for r in rows
    ]


def get_usage_timeseries(days: int = 7, endpoint_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Hourly usage time series from Lakebase cache."""
    from backend.database import execute_query
    if endpoint_name:
        rows = execute_query(
            """SELECT hour, SUM(request_count) AS request_count,
                      SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens,
                      SUM(error_count) AS error_count,
                      COALESCE(SUM(rate_limited_count), 0) AS rate_limited_count
               FROM gateway_usage_hourly WHERE endpoint_name = %s
               GROUP BY hour ORDER BY hour""",
            (endpoint_name,),
        )
    else:
        rows = execute_query(
            """SELECT hour, SUM(request_count) AS request_count,
                      SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens,
                      SUM(error_count) AS error_count,
                      COALESCE(SUM(rate_limited_count), 0) AS rate_limited_count
               FROM gateway_usage_hourly
               GROUP BY hour ORDER BY hour""",
        )
    return [
        {
            "hour": r.get("hour", ""),
            "request_count": int(r.get("request_count") or 0),
            "input_tokens": int(r.get("input_tokens") or 0),
            "output_tokens": int(r.get("output_tokens") or 0),
            "error_count": int(r.get("error_count") or 0),
            "rate_limited_count": int(r.get("rate_limited_count") or 0),
        }
        for r in rows
    ]


def get_usage_by_user(days: int = 7) -> List[Dict[str, Any]]:
    """Per-user usage summary from Lakebase cache."""
    from backend.database import execute_query
    rows = execute_query(
        """SELECT requester, SUM(request_count) AS total_requests,
                  SUM(input_tokens) AS total_input_tokens, SUM(output_tokens) AS total_output_tokens,
                  SUM(error_count) AS error_count,
                  COALESCE(SUM(rate_limited_count), 0) AS rate_limited_count
           FROM gateway_usage_daily
           WHERE usage_date >= CURRENT_DATE - INTERVAL '%s days'
             AND requester != ''
           GROUP BY requester ORDER BY total_requests DESC LIMIT 50""",
        (days,),
    )
    return [
        {
            "requester": r.get("requester", ""),
            "total_requests": int(r.get("total_requests") or 0),
            "total_input_tokens": int(r.get("total_input_tokens") or 0),
            "total_output_tokens": int(r.get("total_output_tokens") or 0),
            "error_count": int(r.get("error_count") or 0),
            "rate_limited_count": int(r.get("rate_limited_count") or 0),
        }
        for r in rows
    ]


def get_inference_logs(
    limit: int = 50,
    endpoint_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Recent individual request logs from system tables (cached)."""
    ck = f"inference_logs:{limit}:{endpoint_name or '__all__'}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    endpoint_filter = ""
    if endpoint_name:
        endpoint_filter = f"AND se.endpoint_name = '{endpoint_name}'"

    rows = _execute_system_sql(f"""
        SELECT
            u.databricks_request_id,
            se.endpoint_name,
            u.requester,
            u.status_code,
            u.request_time,
            u.input_token_count,
            u.output_token_count,
            u.request_streaming
        FROM system.serving.endpoint_usage u
        JOIN system.serving.served_entities se
            ON u.served_entity_id = se.served_entity_id
        WHERE u.request_time >= date_sub(current_date(), 7)
          {endpoint_filter}
        ORDER BY u.request_time DESC
        LIMIT {limit}
    """)
    result = [
        {
            "request_id": r.get("databricks_request_id", ""),
            "endpoint_name": r.get("endpoint_name", ""),
            "requester": r.get("requester", ""),
            "status_code": int(r.get("status_code") or 0),
            "request_time": r.get("request_time", ""),
            "input_tokens": int(r.get("input_token_count") or 0),
            "output_tokens": int(r.get("output_token_count") or 0),
            "streaming": r.get("request_streaming") == "true",
        }
        for r in rows
    ]
    return _cache_set(ck, result)


def get_operational_metrics(hours: int = 24) -> Dict[str, Any]:
    """Aggregate operational metrics from system tables (cached)."""
    ck = f"ops_metrics:{hours}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    rows = _execute_system_sql(f"""
        SELECT
            COUNT(*)                                     AS total_requests,
            SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS total_errors,
            COALESCE(SUM(input_token_count), 0)          AS total_input_tokens,
            COALESCE(SUM(output_token_count), 0)         AS total_output_tokens,
            COUNT(DISTINCT requester)                     AS unique_users,
            COUNT(DISTINCT served_entity_id)              AS unique_entities
        FROM system.serving.endpoint_usage
        WHERE request_time >= current_timestamp() - INTERVAL {hours} HOURS
    """)
    agg = rows[0] if rows else {}
    total_req = int(agg.get("total_requests") or 0)
    total_err = int(agg.get("total_errors") or 0)

    # Per-task breakdown
    task_rows = _execute_system_sql(f"""
        SELECT
            se.task,
            COUNT(*)                                     AS requests,
            SUM(CASE WHEN u.status_code >= 400 THEN 1 ELSE 0 END) AS errors,
            COALESCE(SUM(u.input_token_count), 0)        AS input_tokens,
            COALESCE(SUM(u.output_token_count), 0)       AS output_tokens
        FROM system.serving.endpoint_usage u
        JOIN system.serving.served_entities se
            ON u.served_entity_id = se.served_entity_id
        WHERE u.request_time >= current_timestamp() - INTERVAL {hours} HOURS
        GROUP BY se.task
        ORDER BY requests DESC
    """)
    by_task = [
        {
            "task": r.get("task") or "unknown",
            "requests": int(r.get("requests") or 0),
            "errors": int(r.get("errors") or 0),
            "input_tokens": int(r.get("input_tokens") or 0),
            "output_tokens": int(r.get("output_tokens") or 0),
        }
        for r in task_rows
    ]

    result = {
        "total_requests": total_req,
        "total_errors": total_err,
        "error_rate": round(total_err * 100.0 / total_req, 2) if total_req > 0 else 0,
        "total_input_tokens": int(agg.get("total_input_tokens") or 0),
        "total_output_tokens": int(agg.get("total_output_tokens") or 0),
        "unique_users": int(agg.get("unique_users") or 0),
        "unique_entities": int(agg.get("unique_entities") or 0),
        "by_task": by_task,
    }
    return _cache_set(ck, result)


# =====================================================================
# Endpoint-level permissions — list / update / revoke
# =====================================================================

def get_endpoints_with_permissions() -> List[Dict[str, Any]]:
    """Return every serving endpoint with its current ACL.

    Each item contains the endpoint summary plus a flat ``acl`` list so
    the frontend can render one row per endpoint and expand to show
    individual grants.
    """
    ck = "endpoints_with_perms"
    cached = _cache_get(ck, ttl=120)
    if cached is not None:
        return cached

    from backend.services.access_service import _list_uc_grants

    eps = get_all_endpoints()
    results = []
    for ep in eps:
        eid = ep.get("endpoint_id")
        models = ", ".join(
            se.get("entity_name") or se.get("name") or ""
            for se in (ep.get("served_entities") or [])
        ) or "—"

        is_fmapi = not eid  # FMAPI / databricks-* endpoints have no endpoint_id
        uc_model_name = None

        if is_fmapi:
            uc_model_name = _get_fmapi_uc_model_name(ep["name"], eps)
            try:
                grants = _list_uc_grants("function", uc_model_name)
            except Exception as exc:
                logger.warning("Failed to fetch UC grants for %s: %s", uc_model_name, exc)
                grants = []
            acl = [
                {
                    "principal": grant["principal"],
                    "principal_type": _infer_principal_type(grant["principal"]),
                    "permissions": [{
                        "permission_level": grant["privilege"],
                        "inherited": grant.get("inherited", False),
                    }],
                }
                for grant in grants
            ]
        else:
            acl = _get_endpoint_permissions(eid)

        results.append({
            "endpoint_id": eid,
            "endpoint_name": ep["name"],
            "state": ep.get("state", "UNKNOWN"),
            "task": ep.get("task", ""),
            "endpoint_type": ep.get("endpoint_type", ""),
            "served_models": models,
            "acl": acl,
            "is_foundation_model": is_fmapi,
            "uc_model_name": uc_model_name,
        })
    return _cache_set(ck, results)


def update_endpoint_permission(
    endpoint_name: str,
    principal: str,
    principal_type: str,
    permission_level: str,
    resource_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Grant or update a permission on a serving endpoint, app, or genie space.

    ``resource_type`` is an optional hint (``"serving_endpoint"``,
    ``"app"``, ``"genie_space"``).  When provided the function skips
    auto-detection and directly targets the right Permissions API.

    For FMAPI endpoints (``databricks-*``), grants are applied via UC.
    """
    # ── FMAPI endpoints: grant via UC ──
    if endpoint_name.startswith("databricks-"):
        from backend.services.access_service import _grant_uc

        uc_model_name = _get_fmapi_uc_model_name(endpoint_name)
        ok = _grant_uc("function", uc_model_name, principal, ["EXECUTE"])
        if not ok:
            return {"error": f"UC grant failed on {uc_model_name} — check server logs"}
        _invalidate_perm_caches()
        return {
            "ok": True,
            "endpoint": endpoint_name,
            "principal": principal,
            "level": "EXECUTE",
            "acted_by": "sp",
            "is_foundation_model": True,
        }

    # ── Build ACR (shared by all resource types) ──
    from databricks.sdk.service.iam import (
        AccessControlRequest,
        PermissionLevel,
    )

    acr = AccessControlRequest(permission_level=PermissionLevel(permission_level))
    if principal_type == "user":
        acr.user_name = principal
    elif principal_type == "group":
        acr.group_name = principal
    elif principal_type == "service_principal":
        acr.service_principal_name = principal
    else:
        return {"error": f"Unknown principal_type '{principal_type}'"}

    w = _get_workspace_client()
    if not w:
        return {"error": "No workspace client"}

    # ── Direct grant when resource_type is known ──
    if resource_type == "genie_space":
        try:
            w.permissions.update("genie", endpoint_name, access_control_list=[acr])
            _invalidate_perm_caches()
            return {"ok": True, "endpoint": endpoint_name, "principal": principal, "level": permission_level, "resource_type": "genie_space"}
        except Exception as exc:
            return {"error": str(exc)}

    if resource_type == "app":
        try:
            w.permissions.update("apps", endpoint_name, access_control_list=[acr])
            _invalidate_perm_caches()
            return {"ok": True, "endpoint": endpoint_name, "principal": principal, "level": permission_level, "resource_type": "app"}
        except Exception as exc:
            return {"error": str(exc)}

    # ── Auto-detect: try app first ──
    if not resource_type:
        try:
            app = w.apps.get(endpoint_name)
            if app:
                w.permissions.update("apps", endpoint_name, access_control_list=[acr])
                _invalidate_perm_caches()
                return {"ok": True, "endpoint": endpoint_name, "principal": principal, "level": permission_level, "resource_type": "app"}
        except Exception:
            pass

    # ── Custom endpoints: grant via Permissions API ──
    ep = get_endpoint(endpoint_name)
    if not ep:
        return {"error": f"Resource '{endpoint_name}' not found (tried serving endpoint, app, genie space)"}

    eid = ep["endpoint_id"]

    try:
        w.permissions.update("serving-endpoints", eid, access_control_list=[acr])
        _invalidate_perm_caches()
        return {"ok": True, "endpoint": endpoint_name, "principal": principal, "level": permission_level, "resource_type": "serving_endpoint"}
    except Exception as exc:
        return {"error": str(exc)}


def remove_endpoint_permission(
    endpoint_name: str,
    principal: str,
    principal_type: str,
    resource_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Remove all direct grants for *principal* on a resource.

    ``resource_type`` is an optional hint (``"serving_endpoint"``,
    ``"app"``, ``"genie_space"``).  When provided skips auto-detection.

    For FMAPI endpoints (``databricks-*``), revokes via UC.
    """
    # ── FMAPI endpoints: revoke via UC ──
    if endpoint_name.startswith("databricks-"):
        from backend.services.access_service import _revoke_uc

        uc_model_name = _get_fmapi_uc_model_name(endpoint_name)
        ok = _revoke_uc("function", uc_model_name, principal, ["EXECUTE"])
        if not ok:
            return {"error": f"UC revoke failed on {uc_model_name} — check server logs"}
        _invalidate_perm_caches()
        return {"ok": True, "endpoint": endpoint_name, "removed": principal, "is_foundation_model": True}

    # ── Shared revoke helper (GET → filter → SET) ──
    from databricks.sdk.service.iam import (
        AccessControlRequest,
        PermissionLevel,
    )

    w = _get_workspace_client()
    if not w:
        return {"error": "No workspace client"}

    def _revoke_from_resource(rt: str, resource_id: str):
        current = w.permissions.get(rt, resource_id)
        keep: list[AccessControlRequest] = []
        if current.access_control_list:
            for acl in current.access_control_list:
                match_principal = (
                    (principal_type == "user" and acl.user_name == principal) or
                    (principal_type == "group" and acl.group_name == principal) or
                    (principal_type == "service_principal" and acl.service_principal_name == principal)
                )
                if match_principal:
                    continue
                direct_perms = [
                    p for p in (acl.all_permissions or []) if not p.inherited
                ]
                if not direct_perms:
                    continue
                perm_level = direct_perms[0].permission_level
                acr = AccessControlRequest(permission_level=perm_level)
                if acl.user_name:
                    acr.user_name = acl.user_name
                elif acl.group_name:
                    acr.group_name = acl.group_name
                elif acl.service_principal_name:
                    acr.service_principal_name = acl.service_principal_name
                keep.append(acr)
        w.permissions.set(rt, resource_id, access_control_list=keep)

    # ── Direct revoke when resource_type is known ──
    if resource_type == "genie_space":
        try:
            _revoke_from_resource("genie", endpoint_name)
            _invalidate_perm_caches()
            return {"ok": True, "endpoint": endpoint_name, "removed": principal, "resource_type": "genie_space"}
        except Exception as exc:
            return {"error": str(exc)}

    if resource_type == "app":
        try:
            _revoke_from_resource("apps", endpoint_name)
            _invalidate_perm_caches()
            return {"ok": True, "endpoint": endpoint_name, "removed": principal, "resource_type": "app"}
        except Exception as exc:
            return {"error": str(exc)}

    # ── Auto-detect: try app first ──
    if not resource_type:
        try:
            app = w.apps.get(endpoint_name)
            if app:
                _revoke_from_resource("apps", endpoint_name)
                _invalidate_perm_caches()
                return {"ok": True, "endpoint": endpoint_name, "removed": principal, "resource_type": "app"}
        except Exception:
            pass

    # ── Custom endpoints: revoke via Permissions API ──
    ep = get_endpoint(endpoint_name)
    if not ep:
        return {"error": f"Resource '{endpoint_name}' not found"}

    eid = ep["endpoint_id"]

    try:
        _revoke_from_resource("serving-endpoints", eid)
        _invalidate_perm_caches()
        return {"ok": True, "endpoint": endpoint_name, "removed": principal, "resource_type": "serving_endpoint"}
    except Exception as exc:
        return {"error": str(exc)}


# =====================================================================
# Cross-workspace permission management (via OBO token)
# =====================================================================

_RESOURCE_TYPE_MAP = {
    "serving_endpoint": "serving-endpoints",
    "app": "apps",
    # Databricks Permissions API uses "genie" not "genie-spaces" as the object type.
    # Verified: "genie-spaces" returns "not a supported object type" on all workspaces.
    "genie_space": "genie",
}


def _get_remote_headers_and_host(workspace_id: str, user_token: str = "") -> Optional[tuple]:
    """Resolve remote workspace host + auth headers for cross-workspace API calls.

    Uses SP M2M OAuth (client_credentials grant) to obtain a token scoped to
    the remote workspace.  Returns ``(host, headers)`` on success, ``None`` on
    failure, or ``(host, error_string)`` when the host is known but auth failed
    so callers can provide a specific error message.
    """
    import os
    from backend.services.workspace_registry import get_workspace_host
    host = get_workspace_host(str(workspace_id))
    if not host:
        return None

    # SP M2M OAuth — exchange credentials for a token on the remote workspace
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

    if client_id and client_secret:
        try:
            resp = httpx.post(
                f"{host}/oidc/v1/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": "all-apis",
                },
                timeout=30,
            )
            if resp.status_code == 200:
                token = resp.json().get("access_token", "")
                if token:
                    return (host, {"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
            logger.warning("SP token exchange for %s: HTTP %s — %s", host, resp.status_code, resp.text[:200])
        except Exception as exc:
            err = str(exc)
            logger.warning("SP token exchange for %s failed: %s", host, err)
            if "Cert validation" in err or "SSL" in err.upper() or "certificate" in err.lower():
                return (host, f"__ERROR__:Workspace has a TLS/certificate error — it may be deprovisioned or misconfigured.")

    return None


def _build_acl_entry(principal: str, principal_type: str, permission_level: str) -> Dict[str, Any]:
    """Build an ACL entry dict for the Permissions API."""
    entry: Dict[str, Any] = {"permission_level": permission_level}
    if principal_type == "user":
        entry["user_name"] = principal
    elif principal_type == "group":
        entry["group_name"] = principal
    elif principal_type == "service_principal":
        entry["service_principal_name"] = principal
    return entry


def _lookup_endpoint_id_from_db(endpoint_name: str, workspace_id: str) -> Optional[str]:
    """Look up endpoint_id from discovered_agents config, avoiding a live API call."""
    try:
        from backend.database import execute_one
        row = execute_one(
            "SELECT config->>'endpoint_id' AS endpoint_id FROM discovered_agents "
            "WHERE endpoint_name = %s AND workspace_id = %s",
            (endpoint_name, workspace_id),
        )
        if row and row.get("endpoint_id"):
            logger.info("   Resolved endpoint_id from DB: %s → %s", endpoint_name, row['endpoint_id'])
            return row["endpoint_id"]
    except Exception as exc:
        logger.warning("   DB endpoint_id lookup failed: %s", exc)
    return None


def _resolve_remote_endpoint_id(host: str, headers: dict, endpoint_name: str) -> Optional[str]:
    """Resolve a serving endpoint name or ID to the endpoint ID for the Permissions API.

    The agent data may store either the human-readable name or the endpoint ID.
    The serving-endpoints GET API accepts both name and ID, but the
    Permissions API needs the ID.
    """
    # Try GET by name first, then by ID if name fails
    for identifier in [endpoint_name]:
        try:
            resp = httpx.get(
                f"{host}/api/2.0/serving-endpoints/{identifier}",
                headers=headers, timeout=30,
            )
            if resp.status_code == 200:
                eid = resp.json().get("id", "")
                if eid:
                    return eid
        except Exception as exc:
            logger.warning("Failed to resolve endpoint '%s' in %s: %s", identifier, host, exc)

    # If the value looks like an ID already (hex, no dashes or dots), use it directly
    # and let the Permissions API validate it
    if endpoint_name and all(c in "0123456789abcdef" for c in endpoint_name.lower()):
        logger.info("   Using '%s' as endpoint ID directly (lookup failed)", endpoint_name)
        return endpoint_name

    return None


def update_remote_permission(
    workspace_id: str,
    endpoint_name: str,
    principal: str,
    principal_type: str,
    permission_level: str,
    resource_type: Optional[str] = None,
    user_token: str = "",
) -> Dict[str, Any]:
    """Grant a permission on a resource in a remote workspace.

    Uses the app SP's token via httpx to call the remote workspace's
    Permissions API.  OBO token is used for identity gating only.
    """
    remote = _get_remote_headers_and_host(workspace_id, user_token=user_token)
    if not remote:
        from backend.services.workspace_registry import get_workspace_host
        host = get_workspace_host(str(workspace_id))
        if not host:
            return {"error": f"Cannot connect to workspace {workspace_id} — workspace not in registry"}
        return {"error": f"Cannot authenticate to workspace {workspace_id} ({host}) — the app's service principal is not assigned to this workspace."}

    host, headers = remote
    # Check for error signal from token exchange (e.g. cert failure)
    if isinstance(headers, str) and headers.startswith("__ERROR__:"):
        return {"error": f"Workspace {workspace_id} ({host}): {headers[len('__ERROR__:'):]}"}

    acl_entry = _build_acl_entry(principal, principal_type, permission_level)
    db_resource_type = _RESOURCE_TYPE_MAP.get(resource_type or "", "")

    if db_resource_type in ("apps", "genie"):
        resource_id = endpoint_name
    else:
        resource_id = _lookup_endpoint_id_from_db(endpoint_name, workspace_id)
        if not resource_id:
            resource_id = _resolve_remote_endpoint_id(host, headers, endpoint_name)
        if not resource_id:
            return {"error": f"Endpoint '{endpoint_name}' not found in workspace {workspace_id}"}
        db_resource_type = "serving-endpoints"

    try:
        url = f"{host}/api/2.0/permissions/{db_resource_type}/{resource_id}"
        body = {"access_control_list": [acl_entry]}
        logger.info("   → PATCH %s (principal=%s, level=%s)", url, principal, permission_level)
        resp = httpx.patch(url, json=body, headers=headers, timeout=30)

        if resp.status_code in (200, 201):
            _invalidate_perm_caches()
            return {"ok": True, "endpoint": endpoint_name, "principal": principal,
                    "level": permission_level, "resource_type": resource_type,
                    "workspace_id": workspace_id, "cross_workspace": True}
        else:
            logger.warning("   Permissions PATCH failed: HTTP %s — %s", resp.status_code, resp.text[:300])
            return {"error": _friendly_remote_error(resp, workspace_id, host)}
    except Exception as exc:
        err_str = str(exc)
        if "Cert validation" in err_str or "SSL" in err_str.upper():
            return {"error": f"Workspace {workspace_id} ({host}) has a TLS/certificate error — it may be deprovisioned or misconfigured."}
        return {"error": f"Cannot reach workspace {workspace_id} ({host}): {err_str}"}


def remove_remote_permission(
    workspace_id: str,
    endpoint_name: str,
    principal: str,
    principal_type: str,
    resource_type: Optional[str] = None,
    user_token: str = "",
) -> Dict[str, Any]:
    """Remove a permission on a resource in a remote workspace.

    Reads the current ACL, removes the target principal, and sets the
    remaining ACL via the Permissions API.
    """
    remote = _get_remote_headers_and_host(workspace_id, user_token=user_token)
    if not remote:
        from backend.services.workspace_registry import get_workspace_host
        host = get_workspace_host(str(workspace_id))
        if not host:
            return {"error": f"Cannot connect to workspace {workspace_id} — workspace not in registry"}
        return {"error": f"Cannot authenticate to workspace {workspace_id} ({host}) — the app's service principal is not assigned to this workspace."}

    host, headers = remote
    if isinstance(headers, str) and headers.startswith("__ERROR__:"):
        return {"error": f"Workspace {workspace_id} ({host}): {headers[len('__ERROR__:'):]}"}

    db_resource_type = _RESOURCE_TYPE_MAP.get(resource_type or "", "")

    if db_resource_type in ("apps", "genie"):
        resource_id = endpoint_name
    else:
        resource_id = _lookup_endpoint_id_from_db(endpoint_name, workspace_id)
        if not resource_id:
            resource_id = _resolve_remote_endpoint_id(host, headers, endpoint_name)
        if not resource_id:
            return {"error": f"Endpoint '{endpoint_name}' not found in workspace {workspace_id}"}
        db_resource_type = "serving-endpoints"

    try:
        # Read current ACL
        get_url = f"{host}/api/2.0/permissions/{db_resource_type}/{resource_id}"
        resp = httpx.get(get_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            return {"error": _friendly_remote_error(resp, workspace_id, host)}

        current_acl = resp.json().get("access_control_list", [])
        keep: list = []
        for acl in current_acl:
            match = (
                (principal_type == "user" and acl.get("user_name") == principal) or
                (principal_type == "group" and acl.get("group_name") == principal) or
                (principal_type == "service_principal" and acl.get("service_principal_name") == principal)
            )
            if match:
                continue
            all_perms = acl.get("all_permissions", [])
            direct = [p for p in all_perms if not p.get("inherited")]
            if not direct:
                continue
            entry: Dict[str, Any] = {"permission_level": direct[0]["permission_level"]}
            if acl.get("user_name"):
                entry["user_name"] = acl["user_name"]
            elif acl.get("group_name"):
                entry["group_name"] = acl["group_name"]
            elif acl.get("service_principal_name"):
                entry["service_principal_name"] = acl["service_principal_name"]
            keep.append(entry)

        set_url = f"{host}/api/2.0/permissions/{db_resource_type}/{resource_id}"
        resp = httpx.put(set_url, json={"access_control_list": keep}, headers=headers, timeout=30)
        if resp.status_code in (200, 201):
            _invalidate_perm_caches()
            return {"ok": True, "endpoint": endpoint_name, "removed": principal,
                    "resource_type": resource_type, "workspace_id": workspace_id, "cross_workspace": True}
        else:
            return {"error": _friendly_remote_error(resp, workspace_id, host)}
    except Exception as exc:
        return {"error": str(exc)}


def _friendly_remote_error(resp, workspace_id: str, host: str) -> str:
    """Turn a remote API error response into a user-friendly message."""
    if resp.status_code in (400, 401, 403):
        try:
            detail = resp.json().get("message", resp.text[:200])
        except Exception:
            detail = resp.text[:200]
        # Detect cert/trust errors returned by Databricks API
        if "Cert validation" in detail or "snp system trusted" in detail:
            return (
                f"Workspace {workspace_id} has an internal certificate/trust error — "
                f"it may be deprovisioned, misconfigured, or undergoing maintenance. "
                f"This is a workspace-level issue, not a permissions issue."
            )
        return (
            f"Permission denied on workspace {workspace_id} ({host}): {detail}"
        )
    if resp.status_code == 404:
        return f"Resource not found in workspace {workspace_id}"
    try:
        detail = resp.json()
    except Exception:
        detail = resp.text
    return f"Remote API error {resp.status_code}: {detail}"


def _invalidate_perm_caches():
    """Clear permission-related cache entries after a mutation."""
    with _cache_lock:
        keys_to_drop = [k for k in _cache if "perm" in k.lower()]
        for k in keys_to_drop:
            _cache.pop(k, None)


# =====================================================================
# Composite endpoint — single request for initial page load
# =====================================================================

def get_page_data() -> Dict[str, Any]:
    """Return overview + endpoints in one call to avoid waterfall.

    This is what the frontend should call on first render.
    Internally it populates the per-key caches so that subsequent
    hook calls for individual pieces (permissions, rate-limits, etc.)
    hit the in-memory cache instead of going to the network.
    """
    endpoints = get_all_endpoints()
    overview = get_overview()
    return {
        "overview": overview,
        "endpoints": endpoints,
        "last_refreshed": _cache_freshness(),
    }


def prewarm_cache() -> None:
    """Pre-warm the in-memory cache with data for all gateway tabs.

    Called once at startup so that when users first visit the AI Gateway
    page, all tabs render instantly from cache instead of firing live
    SQL queries against system tables (which take 3-10 s each).
    """
    import time as _t
    start = _t.time()
    logger.info("AI Gateway: pre-warming cache …")
    try:
        get_all_endpoints()       # Overview tab (SDK)
        get_overview()            # Overview KPIs (SDK + 1 SQL)
        get_usage_summary(7)      # Usage tab (1 SQL)
        get_usage_timeseries(7)   # Usage tab chart (1 SQL)
        get_usage_by_user(7)      # Users tab (1 SQL)
        get_inference_logs(50)    # Request Logs tab (1 SQL)
        get_operational_metrics(24)  # Metrics tab (2 SQL)
        elapsed = round(_t.time() - start, 1)
        logger.info("AI Gateway cache pre-warmed in %ss", elapsed)
    except Exception as exc:
        elapsed = round(_t.time() - start, 1)
        logger.warning("AI Gateway cache pre-warm partial (%ss): %s", elapsed, exc)
