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
    """Get aggregate usage stats from system tables."""
    rows = _execute_system_sql(f"""
        SELECT
            COUNT(*)                                    AS total_requests,
            COALESCE(SUM(input_token_count), 0)         AS total_input_tokens,
            COALESCE(SUM(output_token_count), 0)        AS total_output_tokens,
            SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_count,
            COUNT(DISTINCT requester)                    AS unique_users
        FROM system.serving.endpoint_usage
        WHERE request_time >= date_sub(current_date(), {days})
    """)
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


def get_usage_summary(days: int = 7) -> List[Dict[str, Any]]:
    """Per-endpoint usage summary from system tables (cached)."""
    ck = f"usage_summary:{days}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    rows = _execute_system_sql(f"""
        SELECT
            se.endpoint_name,
            COUNT(*)                                     AS total_requests,
            COALESCE(SUM(u.input_token_count), 0)        AS total_input_tokens,
            COALESCE(SUM(u.output_token_count), 0)       AS total_output_tokens,
            SUM(CASE WHEN u.status_code >= 400 THEN 1 ELSE 0 END) AS error_count,
            COUNT(DISTINCT u.requester)                   AS unique_users
        FROM system.serving.endpoint_usage u
        JOIN system.serving.served_entities se
            ON u.served_entity_id = se.served_entity_id
        WHERE u.request_time >= date_sub(current_date(), {days})
        GROUP BY se.endpoint_name
        ORDER BY total_requests DESC
        LIMIT 100
    """)
    result = [
        {
            "endpoint_name": r.get("endpoint_name", ""),
            "total_requests": int(r.get("total_requests") or 0),
            "total_input_tokens": int(r.get("total_input_tokens") or 0),
            "total_output_tokens": int(r.get("total_output_tokens") or 0),
            "error_count": int(r.get("error_count") or 0),
            "unique_users": int(r.get("unique_users") or 0),
        }
        for r in rows
    ]
    return _cache_set(ck, result)


def get_usage_timeseries(days: int = 7, endpoint_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Hourly usage time series from system tables (cached)."""
    ck = f"usage_ts:{days}:{endpoint_name or '__all__'}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    endpoint_filter = ""
    if endpoint_name:
        endpoint_filter = f"AND se.endpoint_name = '{endpoint_name}'"

    rows = _execute_system_sql(f"""
        SELECT
            DATE_TRUNC('HOUR', u.request_time)           AS hour,
            COUNT(*)                                      AS request_count,
            COALESCE(SUM(u.input_token_count), 0)         AS input_tokens,
            COALESCE(SUM(u.output_token_count), 0)        AS output_tokens,
            SUM(CASE WHEN u.status_code >= 400 THEN 1 ELSE 0 END) AS error_count
        FROM system.serving.endpoint_usage u
        JOIN system.serving.served_entities se
            ON u.served_entity_id = se.served_entity_id
        WHERE u.request_time >= date_sub(current_date(), {days})
          {endpoint_filter}
        GROUP BY DATE_TRUNC('HOUR', u.request_time)
        ORDER BY hour
    """)
    result = [
        {
            "hour": r.get("hour", ""),
            "request_count": int(r.get("request_count") or 0),
            "input_tokens": int(r.get("input_tokens") or 0),
            "output_tokens": int(r.get("output_tokens") or 0),
            "error_count": int(r.get("error_count") or 0),
        }
        for r in rows
    ]
    return _cache_set(ck, result)


def get_usage_by_user(days: int = 7) -> List[Dict[str, Any]]:
    """Per-user usage summary (cached)."""
    ck = f"usage_by_user:{days}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    rows = _execute_system_sql(f"""
        SELECT
            u.requester,
            COUNT(*)                                     AS total_requests,
            COALESCE(SUM(u.input_token_count), 0)        AS total_input_tokens,
            COALESCE(SUM(u.output_token_count), 0)       AS total_output_tokens,
            SUM(CASE WHEN u.status_code >= 400 THEN 1 ELSE 0 END) AS error_count
        FROM system.serving.endpoint_usage u
        WHERE u.request_time >= date_sub(current_date(), {days})
        GROUP BY u.requester
        ORDER BY total_requests DESC
        LIMIT 50
    """)
    result = [
        {
            "requester": r.get("requester", ""),
            "total_requests": int(r.get("total_requests") or 0),
            "total_input_tokens": int(r.get("total_input_tokens") or 0),
            "total_output_tokens": int(r.get("total_output_tokens") or 0),
            "error_count": int(r.get("error_count") or 0),
        }
        for r in rows
    ]
    return _cache_set(ck, result)


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
