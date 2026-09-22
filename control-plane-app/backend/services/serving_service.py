"""Serving inventory helpers for the request path.

Currently exposes the list of serving endpoints (and running Apps) the app's
service principal can query — rendered as a live-resource card on the
Workspaces page. This is a live serving/apps inventory call (not a Delta /
system-table read), cached briefly so it stays off the hot path.

Extracted from the old playground service, which was removed. It deliberately
carries NONE of the removed chat/session code: no caller-supplied ``app_url``,
no SP-token forwarding to arbitrary hosts, no conversation persistence.
"""
from __future__ import annotations

import time
from typing import List, Dict, Any, Optional

from backend.config import _get_workspace_client
from backend.database import execute_query

import logging

logger = logging.getLogger(__name__)

# ── Queryable endpoint discovery ─────────────────────────────────

_queryable_cache: Optional[List[Dict[str, Any]]] = None
_queryable_ts: float = 0.0
_QUERYABLE_TTL = 120  # seconds


def _discover_queryable_apps() -> List[Dict[str, Any]]:
    """Return ACTIVE Databricks Apps from the cached discovered_agents table."""
    import json as _json_mod
    results: List[Dict[str, Any]] = []
    try:
        rows = execute_query("""
            SELECT name, endpoint_status, config
            FROM discovered_agents
            WHERE type = 'custom_app'
            ORDER BY name
        """)
        for r in rows:
            d = dict(r)
            cfg = d.get("config") or {}
            if isinstance(cfg, str):
                try:
                    cfg = _json_mod.loads(cfg)
                except Exception:
                    cfg = {}
            app_url = cfg.get("url", "")
            status = (d.get("endpoint_status") or "UNKNOWN").upper()
            # Only include apps that are actually running
            if status not in ("ACTIVE", "RUNNING", "ONLINE"):
                continue
            if not app_url:
                continue
            results.append({
                "endpoint_name": d["name"],
                "agent_name": d["name"],
                "type": "app",
                "kind": "app",
                "status": status,
                "model_name": "",
                "task": "",
                "creator": "",
                "app_url": app_url,
            })
    except Exception as exc:
        logger.warning("Could not list apps for serving inventory: %s", exc)
    return results


def _discover_queryable_serving_endpoints(w: Any) -> List[Dict[str, Any]]:
    """Return READY serving endpoints visible to the SP.

    The SDK's ``serving_endpoints.list()`` already pre-filters to endpoints
    the caller has at least ``CAN_VIEW`` on.  We include every READY endpoint
    here — if the SP lacks ``CAN_QUERY`` the error will surface at invocation
    time with a clear message rather than silently hiding the endpoint.
    """
    results: List[Dict[str, Any]] = []
    try:
        for ep in w.serving_endpoints.list():
            name = ep.name or ""

            # Only READY endpoints are invokable
            ready = ""
            if ep.state and hasattr(ep.state, "ready") and ep.state.ready:
                ready = (
                    ep.state.ready.value
                    if hasattr(ep.state.ready, "value")
                    else str(ep.state.ready)
                )
            if ready.upper() != "READY":
                continue

            # Classify the endpoint
            ep_type = "serving_endpoint"
            model_name = ""
            task = ""
            creator = getattr(ep, "creator", "") or ""
            if ep.config and ep.config.served_entities:
                se = ep.config.served_entities[0]
                model_name = getattr(se, "entity_name", "") or ""
                task = getattr(se, "task", "") or ""
                if getattr(se, "external_model", None):
                    ep_type = "external_model"
                elif getattr(se, "foundation_model", None):
                    ep_type = "foundation_model"
                else:
                    ep_type = "custom_model"

            results.append({
                "endpoint_name": name,
                "agent_name": name,
                "type": ep_type,
                "kind": "serving_endpoint",
                "status": ready,
                "model_name": model_name,
                "task": task,
                "creator": creator,
            })
    except Exception as exc:
        logger.warning("Could not list serving endpoints: %s", exc)
    return results


def list_queryable_endpoints(force: bool = False) -> List[Dict[str, Any]]:
    """Return serving endpoints + running Apps the app's SP can see.

    Results are cached for 2 minutes to keep this off the request hot path.
    """
    global _queryable_cache, _queryable_ts
    if not force and _queryable_cache is not None and (time.time() - _queryable_ts) < _QUERYABLE_TTL:
        return _queryable_cache

    w = _get_workspace_client()
    if w is None:
        return _queryable_cache or []

    queryable = _discover_queryable_serving_endpoints(w)
    apps = _discover_queryable_apps()
    queryable = queryable + apps

    _queryable_cache = queryable
    _queryable_ts = time.time()
    logger.info("Serving inventory: %s queryable endpoints (%s apps)", len(queryable), len(apps))
    return queryable
