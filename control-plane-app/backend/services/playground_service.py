"""Agent Playground service – chat with Databricks serving endpoints.

Persists conversations in Lakebase PostgreSQL so users can revisit past sessions.
Proxies chat requests to serving endpoints using the SDK-first / httpx-fallback
pattern consistent with the rest of the codebase.
"""
from __future__ import annotations

import time
import uuid
import httpx
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from backend.config import (
    _get_workspace_client,
    get_databricks_host,
    get_databricks_headers,
)
from backend.database import execute_query, execute_one, execute_update

import logging

logger = logging.getLogger(__name__)

_TIMEOUT = 120.0  # serving endpoints can be slow on cold start


# =====================================================================
# DDL: ensure Lakebase tables exist
# =====================================================================

def ensure_playground_tables():
    """Create the playground tables in Lakebase if they don't already exist."""
    execute_update("""
        CREATE TABLE IF NOT EXISTS playground_sessions (
            session_id   TEXT PRIMARY KEY,
            endpoint_name TEXT NOT NULL,
            agent_name   TEXT,
            title        TEXT,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    execute_update("""
        CREATE TABLE IF NOT EXISTS playground_messages (
            message_id   TEXT PRIMARY KEY,
            session_id   TEXT NOT NULL REFERENCES playground_sessions(session_id) ON DELETE CASCADE,
            role         TEXT NOT NULL,
            content      TEXT NOT NULL,
            input_tokens  INTEGER,
            output_tokens INTEGER,
            total_tokens  INTEGER,
            latency_ms   INTEGER,
            model        TEXT,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    # Index for fast session lookup
    execute_update("""
        CREATE INDEX IF NOT EXISTS idx_playground_messages_session
        ON playground_messages(session_id, created_at)
    """)
    logger.info("Playground tables ensured")


# =====================================================================
# Queryable endpoint discovery
# =====================================================================

_queryable_cache: Optional[List[Dict[str, Any]]] = None
_queryable_ts: float = 0.0
_QUERYABLE_TTL = 120  # seconds


def _discover_queryable_apps() -> List[Dict[str, Any]]:
    """Return ACTIVE Databricks Apps from discovered_agents."""
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
        logger.warning("Could not list apps for playground: %s", exc)
    return results


def _get_app_url(app_name: str) -> Optional[str]:
    """Look up the URL for a Databricks App from discovered_agents."""
    import json as _json_mod
    row = execute_one(
        "SELECT config FROM discovered_agents WHERE type = 'custom_app' AND name = %s LIMIT 1",
        (app_name,),
    )
    if not row:
        return None
    cfg = row.get("config") or {}
    if isinstance(cfg, str):
        try:
            cfg = _json_mod.loads(cfg)
        except Exception:
            cfg = {}
    return cfg.get("url") or None


def _invoke_app_httpx(app_url: str, body: dict) -> Optional[dict]:
    """POST a chat request to a Databricks App.

    Tries common agent-app endpoint paths in order: /chat, /api/chat,
    /v1/chat/completions, then the app root.  Auth uses the same service
    principal token as all other Databricks API calls.
    """
    headers = get_databricks_headers()
    base = app_url.rstrip("/")
    for path in ("/chat", "/api/chat", "/v1/chat/completions", ""):
        url = base + path
        try:
            resp = httpx.post(url, headers=headers, json=body, timeout=_TIMEOUT)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                continue
            logger.warning("App invoke %s HTTP %s: %s", url, exc.response.status_code, exc.response.text[:300])
            return None
        except Exception as exc:
            logger.warning("App invoke %s failed: %s", url, exc)
            return None
    return None


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
        logger.warning("Could not list serving endpoints for playground: %s", exc)
    return results


def list_queryable_endpoints(force: bool = False) -> List[Dict[str, Any]]:
    """Return serving endpoints the app's SP can use.

    Only READY serving endpoints are returned.  Databricks Apps are excluded
    because they use OIDC browser-based auth and cannot be invoked via API
    tokens.  Agents deployed via Apps are reachable through their associated
    serving endpoint which *is* included here.

    Results are cached for 2 minutes.
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
    logger.info("Playground: %s queryable endpoints (%s apps)", len(queryable), len(apps))
    return queryable


# =====================================================================
# Endpoint invocation (SDK-first, httpx fallback)
# =====================================================================

def _invoke_endpoint_sdk(endpoint_name: str, body: dict) -> Optional[dict]:
    """Query a serving endpoint via the Databricks SDK high-level API."""
    w = _get_workspace_client()
    if w is None:
        return None
    try:
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
        # Use the SDK's high-level query method
        resp = w.serving_endpoints.query(
            name=endpoint_name,
            messages=[
                ChatMessage(
                    role=ChatMessageRole(m["role"]),
                    content=m["content"],
                )
                for m in body.get("messages", [])
            ],
            max_tokens=body.get("max_tokens"),
            temperature=body.get("temperature"),
        )
        # Convert SDK response to dict
        return resp.as_dict() if hasattr(resp, "as_dict") else resp.__dict__
    except ImportError:
        # Older SDK without ChatMessage – fall back to raw API call
        pass
    except Exception as exc:
        logger.warning("SDK high-level query %s failed: %s", endpoint_name, exc)

    # Fallback: raw API call (path must NOT have /api/2.0 prefix for invocations)
    try:
        raw = w.api_client.do(
            "POST",
            f"/serving-endpoints/{endpoint_name}/invocations",
            body=body,
        )
        return raw
    except Exception as exc:
        logger.warning("SDK raw invoke %s failed: %s", endpoint_name, exc)
        return None


def _invoke_endpoint_httpx(endpoint_name: str, body: dict) -> Optional[dict]:
    """POST to serving endpoint via raw httpx + PAT."""
    base = get_databricks_host()
    if not base:
        logger.warning("httpx invoke skipped: no Databricks host configured")
        return None
    url = f"{base}/serving-endpoints/{endpoint_name}/invocations"
    headers = get_databricks_headers()
    try:
        resp = httpx.post(url, headers=headers, json=body, timeout=_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "httpx invoke %s HTTP %s: %s",
            endpoint_name, exc.response.status_code, exc.response.text[:300],
        )
        return None
    except Exception as exc:
        logger.warning("httpx invoke %s failed: %s", endpoint_name, exc)
        return None


def _endpoint_exists(endpoint_name: str) -> bool:
    """Check whether the serving endpoint exists in the current workspace."""
    w = _get_workspace_client()
    if w is None:
        return True  # can't verify, assume it exists and let invocation fail
    try:
        w.serving_endpoints.get(name=endpoint_name)
        return True
    except Exception:
        return False


def query_endpoint(
    endpoint_name: str,
    messages: List[Dict[str, str]],
    max_tokens: int = 1024,
    temperature: float = 0.7,
    app_url: Optional[str] = None,
) -> Dict[str, Any]:
    """Send a chat-completion request to a serving endpoint or Databricks App.

    Returns dict with keys: content, input_tokens, output_tokens, total_tokens,
    model, latency_ms.  On failure, returns an error dict.

    If app_url is provided (or can be resolved from discovered_agents), the
    request is routed to the app over HTTP instead of the serving endpoint API.
    """
    start = time.time()

    body: Dict[str, Any] = {
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    # ── Databricks App invocation ─────────────────────────────────────────────
    resolved_app_url = app_url or _get_app_url(endpoint_name)
    if resolved_app_url:
        result = _invoke_app_httpx(resolved_app_url, body)
        latency_ms = int((time.time() - start) * 1000)
        if result is None:
            return {
                "error": (
                    f"App '{endpoint_name}' did not respond to chat requests. "
                    "Ensure the app is running and its service principal has CAN_USE permission."
                ),
                "latency_ms": latency_ms,
            }
        # Fall through to shared response parser below
    else:
        # ── Serving endpoint invocation ───────────────────────────────────────
        if not _endpoint_exists(endpoint_name):
            latency_ms = int((time.time() - start) * 1000)
            return {
                "error": (
                    f"Serving endpoint '{endpoint_name}' was not found in this workspace. "
                    "It may belong to a different workspace or have been deleted."
                ),
                "latency_ms": latency_ms,
            }

        last_error = ""
        try:
            result = _invoke_endpoint_sdk(endpoint_name, body)
        except Exception as exc:
            result = None
            last_error = str(exc)

        if result is None:
            try:
                result = _invoke_endpoint_httpx(endpoint_name, body)
            except Exception as exc:
                result = None
                last_error = str(exc)

        latency_ms = int((time.time() - start) * 1000)

        if result is None:
            detail = f"Failed to reach serving endpoint '{endpoint_name}'"
            if last_error:
                detail += f": {last_error}"
            return {"error": detail, "latency_ms": latency_ms}

    # Parse OpenAI-compatible response
    try:
        choice = result.get("choices", [{}])[0]
        message = choice.get("message", {})
        content = message.get("content", "")
        usage = result.get("usage", {})
        model = result.get("model", "")

        return {
            "content": content,
            "input_tokens": usage.get("prompt_tokens") or usage.get("input_tokens"),
            "output_tokens": usage.get("completion_tokens") or usage.get("output_tokens"),
            "total_tokens": usage.get("total_tokens"),
            "model": model,
            "latency_ms": latency_ms,
        }
    except Exception as exc:
        # Some endpoints return non-OpenAI format – try to extract what we can
        if isinstance(result, dict):
            # Check for Databricks agent-style response
            content = result.get("output", result.get("result", str(result)))
            if isinstance(content, dict):
                content = content.get("content", str(content))
            return {
                "content": str(content),
                "input_tokens": None,
                "output_tokens": None,
                "total_tokens": None,
                "model": result.get("model", ""),
                "latency_ms": latency_ms,
            }
        return {"error": f"Failed to parse response: {exc}", "latency_ms": latency_ms}


# =====================================================================
# Session CRUD
# =====================================================================

def create_session(endpoint_name: str, agent_name: Optional[str] = None) -> Dict[str, Any]:
    """Create a new chat session. Returns the session dict."""
    session_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    execute_update(
        """INSERT INTO playground_sessions
           (session_id, endpoint_name, agent_name, title, created_at, updated_at)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (session_id, endpoint_name, agent_name, "New conversation", now, now),
    )
    return {
        "session_id": session_id,
        "endpoint_name": endpoint_name,
        "agent_name": agent_name,
        "title": "New conversation",
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }


def list_sessions(limit: int = 50) -> List[Dict[str, Any]]:
    """Return recent sessions ordered by last activity."""
    rows = execute_query(
        """SELECT session_id, endpoint_name, agent_name, title,
                  created_at, updated_at
           FROM playground_sessions
           ORDER BY updated_at DESC
           LIMIT %s""",
        (limit,),
    )
    for r in rows:
        for k in ("created_at", "updated_at"):
            if r.get(k) and hasattr(r[k], "isoformat"):
                r[k] = r[k].isoformat()
    return rows


def get_session(session_id: str) -> Optional[Dict[str, Any]]:
    """Return a single session or None."""
    row = execute_one(
        """SELECT session_id, endpoint_name, agent_name, title,
                  created_at, updated_at
           FROM playground_sessions WHERE session_id = %s""",
        (session_id,),
    )
    if row:
        for k in ("created_at", "updated_at"):
            if row.get(k) and hasattr(row[k], "isoformat"):
                row[k] = row[k].isoformat()
    return row


def get_session_messages(session_id: str) -> List[Dict[str, Any]]:
    """Return all messages for a session, oldest first."""
    rows = execute_query(
        """SELECT message_id, session_id, role, content,
                  input_tokens, output_tokens, total_tokens,
                  latency_ms, model, created_at
           FROM playground_messages
           WHERE session_id = %s
           ORDER BY created_at ASC""",
        (session_id,),
    )
    for r in rows:
        if r.get("created_at") and hasattr(r["created_at"], "isoformat"):
            r["created_at"] = r["created_at"].isoformat()
    return rows


def save_message(
    session_id: str,
    role: str,
    content: str,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    total_tokens: Optional[int] = None,
    latency_ms: Optional[int] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist a message and update the session's updated_at timestamp."""
    message_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    execute_update(
        """INSERT INTO playground_messages
           (message_id, session_id, role, content, input_tokens, output_tokens,
            total_tokens, latency_ms, model, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (message_id, session_id, role, content,
         input_tokens, output_tokens, total_tokens, latency_ms, model, now),
    )
    # Touch session updated_at
    execute_update(
        "UPDATE playground_sessions SET updated_at = %s WHERE session_id = %s",
        (now, session_id),
    )
    return {
        "message_id": message_id,
        "session_id": session_id,
        "role": role,
        "content": content,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "latency_ms": latency_ms,
        "model": model,
        "created_at": now.isoformat(),
    }


def update_session_title(session_id: str, title: str) -> None:
    """Set session title (auto-generated from first user message)."""
    truncated = title[:60].rstrip() + ("…" if len(title) > 60 else "")
    execute_update(
        "UPDATE playground_sessions SET title = %s WHERE session_id = %s",
        (truncated, session_id),
    )


def delete_session(session_id: str) -> bool:
    """Delete a session and all its messages (CASCADE)."""
    affected = execute_update(
        "DELETE FROM playground_sessions WHERE session_id = %s",
        (session_id,),
    )
    return affected > 0
