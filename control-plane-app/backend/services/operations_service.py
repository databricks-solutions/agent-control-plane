"""Real-time operations service — live agent health from Databricks APIs.

Fetches live status for ALL discovered agent types:
  • Serving endpoints  → GET /api/2.0/serving-endpoints (state, pending config)
  • Databricks Apps    → GET /api/2.0/apps (compute & deployment status)
  • Genie Spaces       → discovery metadata + basic active/inactive

Usage metrics from system.serving.endpoint_usage are wired into health
classification for serving-endpoint-backed agents.

Everything is cached for 30 seconds to balance freshness vs API load.
"""
from __future__ import annotations

import json
import time
import threading
import httpx
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.config import get_databricks_host, get_databricks_headers
from backend.database import execute_query
from backend.services.gateway_service import _execute_system_sql

import logging

logger = logging.getLogger(__name__)

# Short-lived in-memory cache (30 seconds)
_cache_lock = threading.Lock()
_cache: Dict[str, tuple] = {}  # key → (timestamp, value)
_CACHE_TTL = 30  # seconds


def _cache_get(key: str) -> Any:
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry[0]) < _CACHE_TTL:
            return entry[1]
    return None


def _cache_set(key: str, value: Any) -> Any:
    with _cache_lock:
        _cache[key] = (time.time(), value)
    return value


def _cache_freshness() -> Optional[str]:
    with _cache_lock:
        if not _cache:
            return None
        newest = max(entry[0] for entry in _cache.values())
    return datetime.fromtimestamp(newest, tz=timezone.utc).isoformat()


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()


# =====================================================================
# REST API helpers
# =====================================================================

_REST_TIMEOUT = 20.0


def _fetch_all_endpoints() -> List[Dict[str, Any]]:
    """Fetch all serving endpoints from the REST API."""
    base = get_databricks_host()
    headers = get_databricks_headers()
    try:
        resp = httpx.get(
            f"{base}/api/2.0/serving-endpoints",
            headers=headers,
            timeout=_REST_TIMEOUT,
        )
        if resp.status_code != 200:
            return []
        return resp.json().get("endpoints", [])
    except Exception:
        return []


def _fetch_all_apps() -> List[Dict[str, Any]]:
    """Fetch all Databricks Apps from the REST API."""
    base = get_databricks_host()
    headers = get_databricks_headers()
    try:
        resp = httpx.get(
            f"{base}/api/2.0/apps",
            headers=headers,
            timeout=_REST_TIMEOUT,
        )
        if resp.status_code != 200:
            return []
        return resp.json().get("apps", [])
    except Exception:
        return []


def _classify_endpoint_health(state: str, pending: bool, error_rate: float) -> str:
    """Classify serving endpoint health."""
    if state != "READY":
        return "down"
    if pending:
        return "pending"
    if error_rate > 5.0:
        return "degraded"
    return "healthy"


def _classify_app_health(compute_status: str, deployment_status: str) -> str:
    """Classify Databricks App health."""
    cs = (compute_status or "").upper()
    ds = (deployment_status or "").upper()
    if cs in ("ACTIVE", "RUNNING") and ds in ("SUCCEEDED", ""):
        return "healthy"
    if cs in ("STARTING", "STOPPING") or ds in ("IN_PROGRESS", "PENDING"):
        return "pending"
    if cs in ("STOPPED", "ERROR", "CRASHED") or ds == "FAILED":
        return "down"
    return "degraded"


# =====================================================================
# Usage metrics (for wiring error rates into health)
# =====================================================================

def _fetch_usage_metrics(hours: int = 1) -> Dict[str, Dict[str, Any]]:
    """Fetch per-endpoint usage from system.serving tables (account-wide).

    Uses the SQL Statement Execution API (Databricks system tables),
    NOT Lakebase, because ``system.serving.endpoint_usage`` only exists
    on the Databricks SQL warehouse.
    """
    try:
        rows = _execute_system_sql(f"""
            SELECT
                se.endpoint_name,
                COUNT(*) AS request_count,
                SUM(CASE WHEN u.status_code >= '400' THEN 1 ELSE 0 END) AS error_count,
                AVG(u.execution_time_ms) AS avg_latency_ms,
                PERCENTILE(u.execution_time_ms, 0.95) AS p95_latency_ms,
                PERCENTILE(u.execution_time_ms, 0.99) AS p99_latency_ms,
                SUM(u.input_token_count + u.output_token_count) AS total_tokens
            FROM system.serving.endpoint_usage u
            JOIN system.serving.served_entities se
                ON u.served_entity_id = se.served_entity_id
            WHERE u.request_time >= current_timestamp() - INTERVAL {hours} HOURS
            GROUP BY se.endpoint_name
            ORDER BY request_count DESC
        """)
        return {r["endpoint_name"]: dict(r) for r in rows if r.get("endpoint_name")}
    except Exception as exc:
        logger.warning("Usage metrics fetch failed: %s", exc)
        return {}


# =====================================================================
# PUBLIC API
# =====================================================================

def get_realtime_status() -> Dict[str, Any]:
    """Fetch real-time status of ALL discovered agents.

    Joins discovered agents from Lakebase with live API data:
    - Serving endpoints: full health + usage metrics
    - Databricks Apps: compute & deployment status
    - Genie Spaces: basic active/inactive from discovery

    Cached for 30 seconds.
    """
    cached = _cache_get("rt_status")
    if cached is not None:
        return cached

    # 1. Load discovered agents from Lakebase
    discovered = []
    try:
        rows = execute_query(
            "SELECT agent_id, name, type, endpoint_name, endpoint_status, "
            "model_name, creator, description, config, source "
            "FROM discovered_agents"
        )
        discovered = [dict(r) for r in rows]
    except Exception:
        pass

    # 2. Fetch live serving endpoint status
    raw_endpoints = _fetch_all_endpoints()
    ep_lookup: Dict[str, Dict] = {}
    for ep in raw_endpoints:
        ep_lookup[ep.get("name", "")] = ep

    # 3. Fetch live Databricks Apps status
    raw_apps = _fetch_all_apps()
    app_lookup: Dict[str, Dict] = {}
    for app in raw_apps:
        app_lookup[app.get("name", "")] = app

    # 4. Fetch usage metrics for error-rate-aware health classification
    usage_lookup = _fetch_usage_metrics(hours=1)

    # 5. Build agent status list
    agents: List[Dict[str, Any]] = []
    seen_endpoints: set = set()  # track which endpoints are covered by discovered agents

    for agent in discovered:
        agent_type = agent.get("type", "") or ""
        ep_name = agent.get("endpoint_name", "") or ""
        name = agent.get("name", "") or ""
        config_raw = agent.get("config") or ""
        config = {}
        if isinstance(config_raw, str) and config_raw:
            try:
                config = json.loads(config_raw)
            except Exception:
                pass
        elif isinstance(config_raw, dict):
            config = config_raw

        entry: Dict[str, Any] = {
            "agent_id": agent.get("agent_id", ""),
            "name": name,
            "endpoint_name": ep_name,
            "agent_type": agent_type,
            "model_name": agent.get("model_name", "") or "",
            "creator": agent.get("creator", "") or "",
            "description": agent.get("description", "") or "",
            "source": agent.get("source", "") or "",
            # defaults — overridden per type below
            "state": "",
            "health": "unknown",
            "has_pending_config": False,
            "pending_reason": "",
            "scale_to_zero": None,
            "workload_size": "",
            "served_entity_count": 0,
            "tags": {},
            "created_at": None,
            "updated_at": None,
            # usage metrics (populated if available)
            "request_count": None,
            "error_count": None,
            "avg_latency_ms": None,
            "p95_latency_ms": None,
            "error_rate": None,
        }

        if agent_type == "genie_space":
            continue  # no real-time API — skip

        if agent_type == "custom_app":
            # ── Databricks App ──────────────────────────────────
            live = app_lookup.get(name, {})
            compute = live.get("compute_status", {}) or {}
            active_deploy = live.get("active_deployment", {}) or {}
            deploy_status = (active_deploy.get("status", {}) or {}).get("state", "") or ""
            cs = compute.get("state", "") or agent.get("endpoint_status", "")

            entry["state"] = cs or deploy_status or "UNKNOWN"
            entry["health"] = _classify_app_health(cs, deploy_status)

            # App may link to serving endpoints — pull usage from those
            resources = config.get("resources", [])
            linked_eps = [r.get("endpoint_name", "") for r in resources if r.get("type") == "serving_endpoint" and r.get("endpoint_name")]
            total_req = 0
            total_err = 0
            total_lat = []
            for lep in linked_eps:
                u = usage_lookup.get(lep)
                if u:
                    total_req += int(u.get("request_count", 0) or 0)
                    total_err += int(u.get("error_count", 0) or 0)
                    lat = u.get("avg_latency_ms")
                    if lat is not None:
                        total_lat.append(float(lat))
                seen_endpoints.add(lep)
            if total_req > 0:
                entry["request_count"] = total_req
                entry["error_count"] = total_err
                entry["avg_latency_ms"] = round(sum(total_lat) / len(total_lat), 1) if total_lat else None
                entry["error_rate"] = round(total_err / total_req * 100, 2)

            if live:
                entry["created_at"] = live.get("create_time")
                entry["updated_at"] = live.get("update_time")
                app_url = live.get("url", "")
                if app_url:
                    entry["tags"] = {"app_url": app_url}

        else:
            # ── Serving endpoint agent ───────────────────────────
            live = ep_lookup.get(ep_name, {})
            seen_endpoints.add(ep_name)

            if live:
                state_obj = live.get("state", {}) or {}
                state = state_obj.get("ready", "") or state_obj.get("config_update", "") or "UNKNOWN"
                ep_config = live.get("config", {}) or {}
                pending_config = live.get("pending_config")
                served = ep_config.get("served_entities", []) or []
                has_pending = pending_config is not None

                # Tile metadata
                tile = live.get("tile_endpoint_metadata") or {}
                if not entry["model_name"] and tile:
                    entry["model_name"] = tile.get("tile_model_name", "") or ""

                # Served entity info
                se0 = served[0] if served else {}
                if not entry["model_name"]:
                    mn = se0.get("entity_name", "") or ""
                    if se0.get("foundation_model"):
                        mn = se0["foundation_model"].get("name", "") or mn
                    entry["model_name"] = mn

                entry["scale_to_zero"] = se0.get("scale_to_zero_enabled")
                entry["workload_size"] = se0.get("workload_size", "") or ""
                entry["served_entity_count"] = len(served)
                entry["state"] = state
                entry["has_pending_config"] = has_pending

                if has_pending:
                    ps = (pending_config or {}).get("served_entities", [])
                    if ps:
                        entry["pending_reason"] = f"Updating to {len(ps)} entities"

                # Tags
                tags_list = live.get("tags", [])
                entry["tags"] = {t.get("key", ""): t.get("value", "") for t in tags_list} if tags_list else {}
                entry["created_at"] = live.get("creation_timestamp")
                entry["updated_at"] = live.get("last_updated_timestamp")

                if not entry["creator"]:
                    entry["creator"] = live.get("creator", "") or ""

                # Usage & health
                u = usage_lookup.get(ep_name)
                error_rate = 0.0
                if u:
                    req = int(u.get("request_count", 0) or 0)
                    err = int(u.get("error_count", 0) or 0)
                    entry["request_count"] = req
                    entry["error_count"] = err
                    entry["avg_latency_ms"] = round(float(u.get("avg_latency_ms") or 0), 1)
                    entry["p95_latency_ms"] = round(float(u.get("p95_latency_ms") or 0), 1)
                    if req > 0:
                        error_rate = err / req * 100
                    entry["error_rate"] = round(error_rate, 2)

                entry["health"] = _classify_endpoint_health(state, has_pending, error_rate)
            else:
                # Cross-workspace agent — no live API data available.
                # Derive health from discovery status + usage metrics.
                disc_status = (agent.get("endpoint_status", "") or "").upper()
                entry["state"] = disc_status or "UNKNOWN"

                # Wire in usage metrics from system tables (account-wide)
                u = usage_lookup.get(ep_name)
                error_rate = 0.0
                if u:
                    req = int(u.get("request_count", 0) or 0)
                    err = int(u.get("error_count", 0) or 0)
                    entry["request_count"] = req
                    entry["error_count"] = err
                    entry["avg_latency_ms"] = round(float(u.get("avg_latency_ms") or 0), 1)
                    entry["p95_latency_ms"] = round(float(u.get("p95_latency_ms") or 0), 1)
                    if req > 0:
                        error_rate = err / req * 100
                    entry["error_rate"] = round(error_rate, 2)

                # Classify: if we have recent usage, the endpoint is alive
                if disc_status == "READY" or (u and int(u.get("request_count", 0) or 0) > 0):
                    entry["health"] = _classify_endpoint_health(
                        "READY", False, error_rate,
                    )
                elif disc_status in ("NOT_READY", "FAILED"):
                    entry["health"] = "down"
                elif disc_status in ("CUSTOM_MODEL", "EXTERNAL_MODEL", "FOUNDATION_MODEL"):
                    # System table discovery stores entity_type in endpoint_status
                    # — treat as healthy if recent usage, otherwise inferred
                    if u and int(u.get("request_count", 0) or 0) > 0:
                        entry["health"] = "healthy" if error_rate <= 5.0 else "degraded"
                    else:
                        entry["health"] = "healthy"
                    entry["state"] = "READY"
                else:
                    entry["health"] = "unknown"

        agents.append(entry)

    # Sort: down first, then degraded, then pending, then unknown, then healthy
    health_order = {"down": 0, "degraded": 1, "pending": 2, "unknown": 3, "healthy": 4}
    agents.sort(key=lambda e: (health_order.get(e["health"], 9), e["name"]))

    # Summary KPIs
    total = len(agents)
    result = {
        "agents": agents,
        "summary": {
            "total": total,
            "healthy": sum(1 for a in agents if a["health"] == "healthy"),
            "degraded": sum(1 for a in agents if a["health"] == "degraded"),
            "down": sum(1 for a in agents if a["health"] == "down"),
            "pending": sum(1 for a in agents if a["health"] == "pending"),
        },
        "last_refreshed": datetime.now(timezone.utc).isoformat(),
    }
    return _cache_set("rt_status", result)


def get_endpoint_detail(endpoint_name: str) -> Dict[str, Any]:
    """Fetch detailed status for a single serving endpoint (live, no cache)."""
    base = get_databricks_host()
    headers = get_databricks_headers()
    try:
        resp = httpx.get(
            f"{base}/api/2.0/serving-endpoints/{endpoint_name}",
            headers=headers,
            timeout=_REST_TIMEOUT,
        )
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}", "endpoint_name": endpoint_name}
        return resp.json()
    except Exception as e:
        return {"error": str(e), "endpoint_name": endpoint_name}


def get_recent_usage(hours: int = 1) -> Dict[str, Any]:
    """Get recent usage metrics from system.serving tables (cached 30s)."""
    cache_key = f"rt_usage_{hours}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    usage = _fetch_usage_metrics(hours)

    result = {
        "usage": list(usage.values()),
        "hours": hours,
        "last_refreshed": datetime.now(timezone.utc).isoformat(),
    }
    return _cache_set(cache_key, result)
