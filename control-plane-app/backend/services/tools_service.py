"""Service for Tools visibility — MCP servers, UC functions, and tool-call usage from traces.

Data sources:
  • UC Connections with `is_mcp_connection` option → managed & custom MCP servers
  • Databricks Apps with "mcp" in name/desc     → custom MCP servers via Apps
  • UC functions in agent-related catalogs (SDK)
  • MLflow traces parsed for tool call spans
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.config import _get_workspace_client, get_databricks_host, get_databricks_headers
from backend.database import execute_query, execute_one, execute_update, DatabasePool

import logging

logger = logging.getLogger(__name__)

_STALE_SECONDS = 3600

_refresh_lock = threading.Lock()
_refresh_in_progress = False


# =====================================================================
# DDL
# =====================================================================

def ensure_tools_tables():
    """Create the tool_registry table if it doesn't exist."""
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS tool_registry (
            tool_id         TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            type            TEXT NOT NULL,
            sub_type        TEXT,
            endpoint_name   TEXT,
            catalog_name    TEXT,
            schema_name     TEXT,
            description     TEXT,
            status          TEXT,
            config          JSONB,
            last_synced     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_tr_type ON tool_registry (type)",
    ]
    for stmt in ddl_statements:
        try:
            execute_update(stmt)
        except Exception as exc:
            logger.warning("Tools DDL warning: %s", exc)
    logger.info("Tools tables ensured")


# =====================================================================
# MCP server discovery — UC connections + Databricks Apps
# =====================================================================

def _is_databricks_app_url(url: str) -> bool:
    """Check if a URL points to a Databricks App."""
    return ".databricksapps.com" in (url or "").lower()


def _discover_mcp_connections() -> List[Dict[str, Any]]:
    """Discover MCP servers from UC connections with is_mcp_connection option.

    Classification:
      - sub_type='custom_app' → URL points to *.databricksapps.com (custom MCP via App)
      - sub_type='managed'    → external MCP service managed via UC connection
    """
    w = _get_workspace_client()
    if not w:
        return []

    tools: List[Dict[str, Any]] = []
    try:
        for conn in w.connections.list():
            conn_name = conn.name or ""
            opts = getattr(conn, "options", {}) or {}

            # Only include connections that are marked as MCP
            if not opts.get("is_mcp_connection"):
                continue

            url = getattr(conn, "url", "") or ""
            host = opts.get("host", "") or ""
            owner = getattr(conn, "owner", "") or ""
            comment = getattr(conn, "comment", "") or ""
            conn_type = str(getattr(conn, "connection_type", "")) or ""
            if "." in conn_type:
                conn_type = conn_type.rsplit(".", 1)[-1]

            # Classify: custom_app if URL points to a Databricks App
            is_app = _is_databricks_app_url(url) or _is_databricks_app_url(host)
            sub_type = "custom_app" if is_app else "managed"

            tool_id = f"mcp:conn:{conn_name}"
            tools.append({
                "tool_id": tool_id,
                "name": conn_name,
                "type": "mcp_server",
                "sub_type": sub_type,
                "endpoint_name": url or host,
                "catalog_name": "",
                "schema_name": "",
                "description": comment or f"MCP connection ({sub_type})",
                "status": "ACTIVE",
                "config": {
                    "url": url,
                    "host": host,
                    "owner": owner,
                    "connection_type": conn_type,
                    "sub_type": sub_type,
                    "is_databricks_app": is_app,
                },
            })
    except Exception as exc:
        logger.warning("MCP connections discovery error: %s", exc)
    return tools


def _discover_mcp_apps() -> List[Dict[str, Any]]:
    """Discover MCP servers deployed as Databricks Apps.

    Finds apps with 'mcp' in their name or description that weren't
    already captured via UC connections (de-duped by URL).
    """
    w = _get_workspace_client()
    if not w or not hasattr(w, "apps"):
        return []

    tools: List[Dict[str, Any]] = []
    try:
        for app in w.apps.list():
            name = getattr(app, "name", "") or ""
            desc = getattr(app, "description", "") or ""
            url = getattr(app, "url", "") or ""

            # Only include apps that are MCP servers
            if "mcp" not in name.lower() and "mcp" not in desc.lower():
                continue

            # Compute status
            status = ""
            cs = getattr(app, "compute_status", None)
            if cs:
                s = getattr(cs, "state", None)
                status = str(s).rsplit(".", 1)[-1] if s else ""
            if not status:
                ad = getattr(app, "active_deployment", None)
                if ad:
                    ds_obj = getattr(ad, "status", None)
                    if ds_obj:
                        ds = getattr(ds_obj, "state", None)
                        status = str(ds).rsplit(".", 1)[-1] if ds else ""

            creator = getattr(app, "creator", "") or ""
            app_id = getattr(app, "id", "") or ""

            tool_id = f"mcp:app:{name}"
            tools.append({
                "tool_id": tool_id,
                "name": name,
                "type": "mcp_server",
                "sub_type": "custom_app",
                "endpoint_name": url,
                "catalog_name": "",
                "schema_name": "",
                "description": desc or f"Custom MCP server (Databricks App)",
                "status": status or "UNKNOWN",
                "config": {
                    "url": url,
                    "app_id": app_id,
                    "creator": creator,
                    "sub_type": "custom_app",
                    "is_databricks_app": True,
                },
            })
    except Exception as exc:
        logger.warning("MCP Apps discovery error: %s", exc)
    return tools


# =====================================================================
# UC Function discovery
# =====================================================================

_SKIP_CATALOGS = {"system", "hive_metastore", "__databricks_internal"}
_SKIP_SCHEMAS  = {"information_schema", "__internal"}


def _discover_uc_functions() -> List[Dict[str, Any]]:
    """Discover UC functions that may be used as agent tools.

    Iterates ALL accessible catalogs and schemas (no hard caps).
    Skips system/internal catalogs and schemas only.
    Uses the full qualified name (catalog.schema.function) as the tool name
    so functions are uniquely identifiable in the UI.
    """
    w = _get_workspace_client()
    if not w:
        return []

    tools: List[Dict[str, Any]] = []
    try:
        catalogs = list(w.catalogs.list())
        logger.info("   → UC: found %s catalogs", len(catalogs))
        for cat in catalogs:
            cat_name = (cat.name or "").lower()
            if not cat_name:
                continue
            # Skip system/internal catalogs
            if any(cat_name == skip or cat_name.startswith(skip + "_") for skip in _SKIP_CATALOGS):
                continue
            if cat_name.startswith("__"):
                continue
            try:
                schemas = list(w.schemas.list(catalog_name=cat_name))
                for schema in schemas:
                    schema_name = schema.name or ""
                    if not schema_name:
                        continue
                    if schema_name.startswith("__") or schema_name.lower() in _SKIP_SCHEMAS:
                        continue
                    try:
                        funcs = list(w.functions.list(catalog_name=cat_name, schema_name=schema_name))
                        for fn in funcs:
                            fn_name = fn.name or ""
                            if not fn_name:
                                continue
                            full_name = f"{cat_name}.{schema_name}.{fn_name}"
                            comment = getattr(fn, "comment", "") or ""
                            tool_id = f"ucfn:{full_name}"
                            tools.append({
                                "tool_id": tool_id,
                                # Use full qualified name so functions are distinct in the UI
                                "name": full_name,
                                "type": "uc_function",
                                "sub_type": str(getattr(fn, "routine_type", "")) or "FUNCTION",
                                "endpoint_name": "",
                                "catalog_name": cat_name,
                                "schema_name": schema_name,
                                "description": comment or f"UC function {full_name}",
                                "status": "ACTIVE",
                                "config": {
                                    "full_name": full_name,
                                    "data_type": str(getattr(fn, "data_type", "")),
                                    "routine_type": str(getattr(fn, "routine_type", "")),
                                },
                            })
                        if funcs:
                            logger.info("   → UC: %s.%s: %s functions", cat_name, schema_name, len(funcs))
                    except Exception as exc:
                        logger.warning("   UC: skipping %s.%s: %s", cat_name, schema_name, exc)
            except Exception as exc:
                logger.warning("   UC: skipping catalog %s: %s", cat_name, exc)
    except Exception as exc:
        logger.warning("UC function discovery error: %s", exc)
    logger.info("   → UC functions total: %s", len(tools))
    return tools


# (System-table based endpoint discovery removed — MCP servers now come
# exclusively from UC connections + Databricks Apps, not from served_entities.)


# =====================================================================
# Tool call usage from MLflow traces
# =====================================================================

def _get_tool_call_usage(days: int = 7) -> List[Dict[str, Any]]:
    """Parse MLflow traces to extract tool call frequency/latency."""
    import httpx

    w = _get_workspace_client()
    host = get_databricks_host()
    if not host:
        return []

    usage: List[Dict[str, Any]] = []
    tool_stats: Dict[str, Dict] = {}

    try:
        # Get experiments first
        experiments: List[str] = []
        resp_json = None
        if w:
            try:
                resp_json = w.api_client.do("GET", "/api/2.0/mlflow/experiments/search", query={"max_results": "20"})
            except Exception:
                pass
        if resp_json is None:
            try:
                r = httpx.get(
                    f"{host}/api/2.0/mlflow/experiments/search",
                    headers=get_databricks_headers(),
                    params={"max_results": "20"},
                    timeout=30.0,
                )
                r.raise_for_status()
                resp_json = r.json()
            except Exception:
                pass

        if resp_json:
            for exp in resp_json.get("experiments", []):
                experiments.append(exp.get("experiment_id", ""))

        if not experiments:
            return []

        # Search traces for tool calls
        body = {
            "experiment_ids": experiments[:5],
            "max_results": 100,
        }
        traces_json = None
        if w:
            try:
                traces_json = w.api_client.do("POST", "/api/2.0/mlflow/traces", body=body)
            except Exception:
                pass
        if traces_json is None:
            try:
                r = httpx.post(
                    f"{host}/api/2.0/mlflow/traces",
                    headers=get_databricks_headers(),
                    json=body,
                    timeout=30.0,
                )
                r.raise_for_status()
                traces_json = r.json()
            except Exception:
                pass

        if not traces_json:
            return []

        for trace in traces_json.get("traces", []):
            trace_info = trace.get("info", {})
            trace_data = trace.get("data", {})

            for span in trace_data.get("spans", []):
                span_name = span.get("name", "")
                span_type = span.get("attributes", {}).get("mlflow.spanType", "")

                if span_type in ("TOOL", "FUNCTION", "RETRIEVER") or "tool" in span_name.lower():
                    start_ns = int(span.get("start_time_ns", 0) or 0)
                    end_ns = int(span.get("end_time_ns", 0) or 0)
                    latency_ms = (end_ns - start_ns) / 1e6 if end_ns > start_ns else 0

                    status = span.get("status", {}).get("status_code", "OK")
                    is_error = status != "OK"

                    if span_name not in tool_stats:
                        tool_stats[span_name] = {
                            "tool_name": span_name,
                            "span_type": span_type,
                            "call_count": 0,
                            "error_count": 0,
                            "total_latency_ms": 0,
                        }
                    tool_stats[span_name]["call_count"] += 1
                    tool_stats[span_name]["total_latency_ms"] += latency_ms
                    if is_error:
                        tool_stats[span_name]["error_count"] += 1

        for name, stats in tool_stats.items():
            count = stats["call_count"]
            usage.append({
                "tool_name": name,
                "span_type": stats["span_type"],
                "call_count": count,
                "error_count": stats["error_count"],
                "error_rate": round(stats["error_count"] / count * 100, 1) if count > 0 else 0,
                "avg_latency_ms": round(stats["total_latency_ms"] / count, 1) if count > 0 else 0,
            })

        usage.sort(key=lambda x: x["call_count"], reverse=True)
    except Exception as exc:
        logger.warning("Tool usage extraction error: %s", exc)

    return usage


# =====================================================================
# Cache management
# =====================================================================

def _upsert_tools(tools: List[Dict[str, Any]]):
    if not tools:
        return
    sql = """
        INSERT INTO tool_registry
            (tool_id, name, type, sub_type, endpoint_name,
             catalog_name, schema_name, description, status, config, last_synced)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (tool_id) DO UPDATE SET
            name = EXCLUDED.name,
            type = EXCLUDED.type,
            sub_type = EXCLUDED.sub_type,
            endpoint_name = EXCLUDED.endpoint_name,
            catalog_name = EXCLUDED.catalog_name,
            schema_name = EXCLUDED.schema_name,
            description = EXCLUDED.description,
            status = EXCLUDED.status,
            config = EXCLUDED.config,
            last_synced = NOW()
    """
    with DatabasePool.get_connection() as conn:
        with conn.cursor() as cur:
            for t in tools:
                cur.execute(sql, (
                    t["tool_id"],
                    t["name"],
                    t["type"],
                    t.get("sub_type", ""),
                    t.get("endpoint_name", ""),
                    t.get("catalog_name", ""),
                    t.get("schema_name", ""),
                    t.get("description", ""),
                    t.get("status", ""),
                    json.dumps(t.get("config") or {}),
                ))
            conn.commit()


def _is_stale() -> bool:
    row = execute_one("SELECT MAX(last_synced) AS last_synced FROM tool_registry")
    if not row or not row.get("last_synced"):
        return True
    last = row["last_synced"]
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - last).total_seconds()
    return age > _STALE_SECONDS


def refresh_tools():
    """Run full tools discovery and cache."""
    global _refresh_in_progress
    if not _refresh_lock.acquire(blocking=False):
        return
    try:
        _refresh_in_progress = True
        logger.info("Starting tools discovery …")

        # Clear old MCP entries (they may be stale serving-endpoint records)
        try:
            execute_update("DELETE FROM tool_registry WHERE type = 'mcp_server'")
        except Exception:
            pass

        # 1) Managed MCP — UC connections with is_mcp_connection
        mcp_conns = _discover_mcp_connections()
        _upsert_tools(mcp_conns)
        managed = sum(1 for t in mcp_conns if t["sub_type"] == "managed")
        custom_conn = sum(1 for t in mcp_conns if t["sub_type"] == "custom_app")
        logger.info("   → MCP connections: %s (managed=%s, custom_app=%s)", len(mcp_conns), managed, custom_conn)

        # 2) Custom MCP via Databricks Apps
        # De-duplicate against connection-discovered apps (by URL)
        known_urls = {t.get("endpoint_name", "").rstrip("/").lower() for t in mcp_conns}
        mcp_apps = _discover_mcp_apps()
        new_apps = [
            a for a in mcp_apps
            if a.get("endpoint_name", "").rstrip("/").lower() not in known_urls
        ]
        _upsert_tools(new_apps)
        logger.info("   → MCP apps (new, after dedup): %s", len(new_apps))

        # 3) UC functions
        funcs = _discover_uc_functions()
        _upsert_tools(funcs)
        logger.info("   → UC functions: %s", len(funcs))

        logger.info("Tools discovery complete")
    except Exception as exc:
        logger.warning("Tools refresh failed: %s", exc)
    finally:
        _refresh_in_progress = False
        _refresh_lock.release()


def maybe_refresh_async():
    if _refresh_in_progress:
        return
    try:
        if _is_stale():
            t = threading.Thread(target=refresh_tools, daemon=True)
            t.start()
    except Exception:
        pass


# =====================================================================
# PUBLIC API
# =====================================================================

def get_tools_overview() -> Dict[str, Any]:
    """KPI summary for the Tools page."""
    maybe_refresh_async()
    row = execute_one("""
        SELECT
            COUNT(*)                                            AS total,
            COUNT(*) FILTER (WHERE type = 'mcp_server')        AS mcp_count,
            COUNT(*) FILTER (WHERE type = 'uc_function')       AS uc_count,
            COUNT(*) FILTER (WHERE sub_type = 'managed')       AS managed_count,
            COUNT(*) FILTER (WHERE sub_type = 'custom_app')    AS custom_app_count,
            MAX(last_synced)                                    AS last_synced
        FROM tool_registry
    """)
    r = dict(row) if row else {}
    last = r.get("last_synced")
    return {
        "total_tools": int(r.get("total") or 0),
        "mcp_servers": int(r.get("mcp_count") or 0),
        "uc_functions": int(r.get("uc_count") or 0),
        "managed_count": int(r.get("managed_count") or 0),
        "custom_app_count": int(r.get("custom_app_count") or 0),
        "is_refreshing": _refresh_in_progress,
        "last_refreshed": last.isoformat() if last and hasattr(last, "isoformat") else None,
    }


def get_mcp_servers() -> List[Dict[str, Any]]:
    """List all MCP server entries from the tool registry."""
    maybe_refresh_async()
    rows = execute_query(
        "SELECT * FROM tool_registry WHERE type = 'mcp_server' ORDER BY name"
    )
    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("config"), str):
            try:
                d["config"] = json.loads(d["config"])
            except Exception:
                pass
        if d.get("last_synced") and hasattr(d["last_synced"], "isoformat"):
            d["last_synced"] = d["last_synced"].isoformat()
        result.append(d)
    return result


def get_uc_functions() -> List[Dict[str, Any]]:
    """List all UC function entries from the tool registry."""
    maybe_refresh_async()
    rows = execute_query(
        "SELECT * FROM tool_registry WHERE type = 'uc_function' ORDER BY catalog_name, schema_name, name"
    )
    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("config"), str):
            try:
                d["config"] = json.loads(d["config"])
            except Exception:
                pass
        if d.get("last_synced") and hasattr(d["last_synced"], "isoformat"):
            d["last_synced"] = d["last_synced"].isoformat()
        result.append(d)
    return result


def get_tool_usage(days: int = 7) -> List[Dict[str, Any]]:
    """Get tool call frequency from MLflow traces."""
    return _get_tool_call_usage(days)
