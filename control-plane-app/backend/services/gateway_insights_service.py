"""Unity Gateway usage-dashboard parity: filterable usage, coding-agent sessions,
unit economics, spend by service tag, per-model performance and skills.

All reads come from Lakebase tables written by workflow 11
(``11_discover_ai_gateway_usage.py``) from ``system.ai_gateway.usage``,
``system.billing.usage`` (attributed via ``usage_metadata.ai_gateway.endpoint_id``)
and ``system.access.audit`` (skills).

Scoping:
  - ``uag_usage_daily_fact`` carries ``workspace_id`` → filtered to the caller's
    workspaces (so workspace admins see their own usage, not nothing).
  - Every other table here is account-wide with no workspace column → suppressed
    for scoped callers (``restricted: True``), same rule as the other uag_* views.
All functions fail open to an empty shape when a table isn't synced yet.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from backend.database import execute_query

logger = logging.getLogger(__name__)

FACT_DAYS = 30  # window workflow 11 keeps in uag_usage_daily_fact

_BREAKDOWN_DIMS = ("service_type", "destination_model", "source", "api_type",
                   "requester_type", "auth_mode", "workspace_id")


def _q(sql: str, params: Optional[dict] = None, label: str = "") -> List[Dict[str, Any]]:
    try:
        return execute_query(sql, params or {})
    except Exception as exc:
        logger.warning("%s unavailable: %s", label or "gateway insights query", exc)
        return []


def _num(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _int(v) -> int:
    return int(_num(v))


def _parse_date(v: Optional[str]) -> Optional[date]:
    if not v:
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


# ── Filterable usage (Usage tab) ─────────────────────────────────────────────

def get_usage_explorer(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    workspace_ids: Optional[List[str]] = None,
    tag_key: Optional[str] = None,
    tag_value: Optional[str] = None,
    allowed_workspace_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Usage KPIs, daily trend and breakdowns over ``uag_usage_daily_fact`` with
    date-range, workspace and service-tag filters (the built-in dashboard's filters).

    ``workspace_ids`` (the page selection) is intersected with the caller's scope,
    so it can narrow but never widen. Dates are clamped to the fact's 30-day window.
    """
    today = date.today()
    lo = max(_parse_date(start_date) or today - timedelta(days=7), today - timedelta(days=FACT_DAYS))
    hi = min(_parse_date(end_date) or today, today)
    if lo > hi:
        lo = hi

    selected = [w for w in (workspace_ids or []) if w]
    if allowed_workspace_ids is not None:
        allowed = set(allowed_workspace_ids)
        scope = [w for w in selected if w in allowed] if selected else list(allowed)
    else:
        scope = selected or None

    empty = {"filters": {"start_date": lo.isoformat(), "end_date": hi.isoformat(), "window_days": FACT_DAYS},
             "totals": {}, "trend": [], "breakdowns": {}, "top_services": [],
             "options": {"workspaces": [], "tag_keys": {}}, "scoped": allowed_workspace_ids is not None}
    if scope is not None and not scope:
        return empty

    where = ["f.usage_date BETWEEN %(lo)s AND %(hi)s"]
    params: Dict[str, Any] = {"lo": lo, "hi": hi}
    if scope is not None:
        where.append("f.workspace_id = ANY(%(ws)s)")
        params["ws"] = scope
    if tag_key:
        sub = "SELECT service_name FROM uag_service_tags WHERE tag_key = %(tk)s"
        params["tk"] = tag_key
        if tag_value:
            sub += " AND tag_value = %(tv)s"
            params["tv"] = tag_value
        where.append(f"f.service_name IN ({sub})")
    w = " AND ".join(where)

    totals = _q(f"""SELECT SUM(request_count) AS requests, SUM(error_count) AS errors,
                           SUM(throttled_count) AS throttled, SUM(input_tokens) AS input_tokens,
                           SUM(output_tokens) AS output_tokens, SUM(cached_tokens) AS cached_tokens,
                           COUNT(DISTINCT NULLIF(service_name, '')) AS services
                    FROM uag_usage_daily_fact f WHERE {w}""", params, "usage explorer totals")
    t = totals[0] if totals else {}
    req = _int(t.get("requests"))
    inp, cached = _int(t.get("input_tokens")), _int(t.get("cached_tokens"))
    out = {
        **empty,
        "totals": {
            "requests": req,
            "errors": _int(t.get("errors")),
            "throttled": _int(t.get("throttled")),
            "error_rate": round(_int(t.get("errors")) * 100.0 / req, 2) if req else None,
            "input_tokens": inp,
            "output_tokens": _int(t.get("output_tokens")),
            "cached_tokens": cached,
            "cache_share": round(cached * 100.0 / (inp + cached), 1) if (inp + cached) else None,
            "services": _int(t.get("services")),
        },
    }
    out["trend"] = [
        {"usage_date": str(r["usage_date"]), "requests": _int(r.get("requests")), "errors": _int(r.get("errors")),
         "input_tokens": _int(r.get("input_tokens")), "output_tokens": _int(r.get("output_tokens"))}
        for r in _q(f"""SELECT usage_date, SUM(request_count) AS requests, SUM(error_count) AS errors,
                               SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens
                        FROM uag_usage_daily_fact f WHERE {w} GROUP BY usage_date ORDER BY usage_date""",
                    params, "usage explorer trend")
    ]
    for dim in _BREAKDOWN_DIMS:  # fixed allow-list — safe to interpolate
        out["breakdowns"][dim] = [
            {"key": r.get("k") or "unknown", "requests": _int(r.get("requests")),
             "tokens": _int(r.get("tokens")), "errors": _int(r.get("errors"))}
            for r in _q(f"""SELECT f.{dim} AS k, SUM(request_count) AS requests,
                                   SUM(input_tokens + output_tokens) AS tokens, SUM(error_count) AS errors
                            FROM uag_usage_daily_fact f WHERE {w}
                            GROUP BY f.{dim} ORDER BY requests DESC LIMIT 15""", params, f"breakdown {dim}")
        ]
    out["top_services"] = [
        {"service_name": r.get("service_name"), "service_type": r.get("service_type"),
         "requests": _int(r.get("requests")), "errors": _int(r.get("errors")),
         "tokens": _int(r.get("tokens"))}
        for r in _q(f"""SELECT service_name, MAX(service_type) AS service_type, SUM(request_count) AS requests,
                               SUM(error_count) AS errors, SUM(input_tokens + output_tokens) AS tokens
                        FROM uag_usage_daily_fact f WHERE {w} AND service_name <> ''
                        GROUP BY service_name ORDER BY requests DESC LIMIT 20""", params, "top services")
    ]
    # Filter options: workspaces within the caller's scope; tag keys/values.
    opt_params: Dict[str, Any] = {}
    opt_where = ""
    if allowed_workspace_ids is not None:
        opt_where = "WHERE workspace_id = ANY(%(ws)s)"
        opt_params["ws"] = list(allowed_workspace_ids)
    out["options"]["workspaces"] = [
        r["workspace_id"] for r in _q(
            f"SELECT DISTINCT workspace_id FROM uag_usage_daily_fact {opt_where} ORDER BY workspace_id LIMIT 1000",
            opt_params, "workspace options") if r.get("workspace_id")
    ]
    keys: Dict[str, List[str]] = {}
    for r in _q("""SELECT tag_key, tag_value, COUNT(*) AS n FROM uag_service_tags
                   GROUP BY tag_key, tag_value ORDER BY tag_key, n DESC""", {}, "tag options"):
        vals = keys.setdefault(r["tag_key"], [])
        if len(vals) < 50 and r.get("tag_value") is not None:
            vals.append(r["tag_value"])
    out["options"]["tag_keys"] = keys
    return out


def get_daily_activity(allowed_workspace_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Daily active / MCP / coding-agent users (distinct counts; account-wide)."""
    if allowed_workspace_ids is not None:
        return {"restricted": True, "days": []}
    rows = _q("""SELECT usage_date, active_users, mcp_requests, mcp_users, coding_sessions, coding_users
                 FROM uag_daily_activity ORDER BY usage_date""", {}, "daily activity")
    return {"restricted": False, "days": [{**r, "usage_date": str(r["usage_date"])} for r in rows]}


# ── Coding agents ────────────────────────────────────────────────────────────

def get_coding_sessions(allowed_workspace_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Coding-agent sessions (session_metadata): daily trend, harness × surface mix,
    and the top sessions by estimated spend."""
    if allowed_workspace_ids is not None:
        return {"restricted": True, "totals": {}, "trend": [], "by_harness": [], "top_sessions": []}
    daily = _q("""SELECT usage_date, coding_agent, surface, sessions, users, subagents, requests,
                         input_tokens, output_tokens, reasoning_tokens, est_cost_usd
                  FROM uag_coding_sessions_daily""", {}, "coding sessions daily")
    trend: Dict[str, Dict[str, Any]] = {}
    harness: Dict[tuple, Dict[str, Any]] = {}
    for r in daily:
        d = str(r["usage_date"])
        t = trend.setdefault(d, {"usage_date": d, "sessions": 0, "requests": 0, "est_cost_usd": 0.0})
        t["sessions"] += _int(r.get("sessions"))
        t["requests"] += _int(r.get("requests"))
        t["est_cost_usd"] += _num(r.get("est_cost_usd"))
        key = (r.get("coding_agent") or "unknown", r.get("surface") or "unknown")
        h = harness.setdefault(key, {"coding_agent": key[0], "surface": key[1], "session_days": 0, "requests": 0,
                                     "input_tokens": 0, "output_tokens": 0, "reasoning_tokens": 0,
                                     "est_cost_usd": 0.0, "peak_daily_users": 0})
        h["session_days"] += _int(r.get("sessions"))
        h["requests"] += _int(r.get("requests"))
        h["input_tokens"] += _int(r.get("input_tokens"))
        h["output_tokens"] += _int(r.get("output_tokens"))
        h["reasoning_tokens"] += _int(r.get("reasoning_tokens"))
        h["est_cost_usd"] += _num(r.get("est_cost_usd"))
        h["peak_daily_users"] = max(h["peak_daily_users"], _int(r.get("users")))
    top = _q("""SELECT session_id, requester, coding_agent, surface, agent_version, reasoning_effort, models,
                       first_seen, last_seen, requests, subagents, input_tokens, output_tokens,
                       reasoning_tokens, est_cost_usd
                FROM uag_coding_sessions_top ORDER BY est_cost_usd DESC, requests DESC LIMIT 200""",
             {}, "top coding sessions")
    by_harness = sorted(harness.values(), key=lambda h: -h["requests"])
    for h in by_harness:
        h["est_cost_usd"] = round(h["est_cost_usd"], 2)
    trend_rows = sorted(trend.values(), key=lambda t: t["usage_date"])
    for t in trend_rows:
        t["est_cost_usd"] = round(t["est_cost_usd"], 2)
    return {
        "restricted": False,
        "totals": {
            "session_days": sum(h["session_days"] for h in by_harness),
            "requests": sum(h["requests"] for h in by_harness),
            "est_cost_usd": round(sum(h["est_cost_usd"] for h in by_harness), 2),
            "reasoning_tokens": sum(h["reasoning_tokens"] for h in by_harness),
            "harnesses": len({h["coding_agent"] for h in by_harness}),
        },
        "trend": trend_rows,
        "by_harness": by_harness,
        "top_sessions": [{**r, "est_cost_usd": round(_num(r.get("est_cost_usd")), 4)} for r in top],
    }


# ── Spend: unit economics + service-tag attribution ──────────────────────────

def get_unit_economics(allowed_workspace_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """$ per 1M tokens and per 1k requests per service (30d, attributed billing)."""
    if allowed_workspace_ids is not None:
        return {"restricted": True, "services": []}
    rows = _q("""SELECT service_name, service_type, cost_usd, requests, input_tokens, output_tokens,
                        usd_per_1m_tokens, usd_per_1k_requests
                 FROM uag_service_unit_cost ORDER BY cost_usd DESC LIMIT 200""", {}, "unit economics")
    return {"restricted": False, "services": rows}


def get_cost_by_service_tag(allowed_workspace_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Attributed spend rolled up by service tag key/value (30d)."""
    if allowed_workspace_ids is not None:
        return {"restricted": True, "tags": []}
    rows = _q("""SELECT tag_key, tag_value, cost_usd, services, requests
                 FROM uag_cost_by_service_tag ORDER BY cost_usd DESC LIMIT 500""", {}, "tag spend")
    return {"restricted": False, "tags": rows}


# ── Performance per model ────────────────────────────────────────────────────

def get_model_performance(allowed_workspace_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Per destination model (7d): latency, streaming TTFB, output tokens/sec,
    cache-read share, error / 429 rates."""
    if allowed_workspace_ids is not None:
        return {"restricted": True, "models": []}
    rows = _q("""SELECT destination_model, requests, error_count, throttled_count, streaming_requests,
                        p50_latency_ms, p95_latency_ms, p50_ttfb_ms, p95_ttfb_ms, p50_output_tps,
                        input_tokens, cache_read_tokens, reasoning_tokens
                 FROM uag_model_performance ORDER BY requests DESC LIMIT 200""", {}, "model performance")
    models = []
    for r in rows:
        req = _int(r.get("requests"))
        inp, cr = _int(r.get("input_tokens")), _int(r.get("cache_read_tokens"))
        models.append({
            **r,
            "error_rate": round(_int(r.get("error_count")) * 100.0 / req, 2) if req else None,
            "throttle_rate": round(_int(r.get("throttled_count")) * 100.0 / req, 2) if req else None,
            # cache_read_input_tokens are reported alongside input_tokens → share of total prompt
            "cache_read_share": round(cr * 100.0 / (inp + cr), 1) if (inp + cr) else None,
            "p50_output_tps": float(r["p50_output_tps"]) if r.get("p50_output_tps") is not None else None,
        })
    return {"restricted": False, "models": models}


# ── Skills ───────────────────────────────────────────────────────────────────

def get_skills(allowed_workspace_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Skill loads / bundle fetches / adopting users (30d, audit aiGatewaySkills)."""
    if allowed_workspace_ids is not None:
        return {"restricted": True, "skills": [], "users": []}
    skills = _q("""SELECT skill_name, loads, bundle_fetches, users, last_used
                   FROM uag_skill_usage ORDER BY loads DESC, users DESC LIMIT 500""", {}, "skills")
    users = _q("""SELECT user_name, COUNT(DISTINCT skill_name) AS skills, SUM(loads) AS loads, MAX(last_used) AS last_used
                  FROM uag_skill_users GROUP BY user_name ORDER BY loads DESC LIMIT 200""", {}, "skill users")
    return {"restricted": False, "skills": skills, "users": users}
