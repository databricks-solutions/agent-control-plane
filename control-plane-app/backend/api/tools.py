"""API routes for Tools — MCP servers, UC functions, tool-call usage."""
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user, require_admin, UserInfo
from backend.utils.access_scope import get_allowed_workspace_ids, sees_deploy_workspace
from typing import Dict, Any, List
from backend.services.tools_service import (
    get_tools_overview,
    get_mcp_servers,
    get_mcp_activity,
    get_uc_functions,
    get_tool_usage,
    refresh_tools,
)

router = APIRouter(prefix="/tools", tags=["tools"], dependencies=[Depends(get_current_user)])


_EMPTY_OVERVIEW = {
    "total_tools": 0, "mcp_servers": 0, "uc_functions": 0,
    "managed_count": 0, "custom_app_count": 0,
    "is_refreshing": False, "last_refreshed": None,
}


def _live_ok(user: UserInfo) -> bool:
    return sees_deploy_workspace(get_allowed_workspace_ids(user))


@router.get("/overview")
def tools_overview(user: UserInfo = Depends(get_current_user)) -> Dict[str, Any]:
    """KPI summary: total tools, MCP server count, UC function count, etc."""
    if not _live_ok(user):
        return _EMPTY_OVERVIEW
    return get_tools_overview()


@router.get("/mcp-servers")
def list_mcp_servers(user: UserInfo = Depends(get_current_user)) -> List[Dict[str, Any]]:
    """List MCP server / serving endpoints with managed/custom classification."""
    if not _live_ok(user):
        return []
    return get_mcp_servers()


@router.get("/mcp-activity")
def mcp_activity(user: UserInfo = Depends(get_current_user)) -> Dict[str, Any]:
    """Server-grouped MCP tool activity from Unity Gateway v2 (managed vs
    UC-registered services, per-tool request/error/user counts)."""
    # uag_mcp_tool_daily is account-wide — only account admins (allowed is None).
    if get_allowed_workspace_ids(user) is not None:
        return {"as_of": None, "totals": {}, "servers": []}
    return get_mcp_activity()


@router.get("/functions")
def list_uc_functions(user: UserInfo = Depends(get_current_user)) -> List[Dict[str, Any]]:
    """List UC functions discovered as agent tools."""
    if not _live_ok(user):
        return []
    return get_uc_functions()


@router.get("/usage")
def tool_usage(
    days: int = Query(default=7, ge=1, le=90),
    user: UserInfo = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    """Tool call frequency + latency from MLflow traces."""
    if get_allowed_workspace_ids(user) is not None:
        return []
    return get_tool_usage(days)


@router.post("/sync")
def sync_tools(user: UserInfo = Depends(require_admin)):
    """Trigger a full tools discovery refresh."""
    refresh_tools()
    return {"status": "ok", "message": "Tools refresh complete"}


@router.get("/debug/uc")
def debug_uc_functions(user: UserInfo = Depends(get_current_user)) -> Dict[str, Any]:
    """Debug UC function discovery — runs raw discovery and returns what it finds."""
    if not _live_ok(user):
        return {"error": "No workspace access"}
    from backend.config import _get_workspace_client
    w = _get_workspace_client()
    if not w:
        return {"error": "No workspace client"}

    result: Dict[str, Any] = {"catalogs": [], "functions": [], "errors": []}
    try:
        cats = list(w.catalogs.list())
        result["catalog_count"] = len(cats)
        for cat in cats:
            cat_name = (cat.name or "").lower()
            result["catalogs"].append(cat_name)
            try:
                schemas = list(w.schemas.list(catalog_name=cat_name))
                for schema in schemas:
                    schema_name = schema.name or ""
                    try:
                        funcs = list(w.functions.list(catalog_name=cat_name, schema_name=schema_name))
                        for fn in funcs:
                            result["functions"].append(f"{cat_name}.{schema_name}.{fn.name or '?'}")
                    except Exception as e:
                        result["errors"].append(f"funcs {cat_name}.{schema_name}: {e}")
            except Exception as e:
                result["errors"].append(f"schemas {cat_name}: {e}")
    except Exception as e:
        result["errors"].append(f"catalogs: {e}")

    return result
