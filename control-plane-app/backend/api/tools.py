"""API routes for Tools — MCP servers, UC functions, tool-call usage."""
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user
from typing import Dict, Any, List
from backend.services.tools_service import (
    get_tools_overview,
    get_mcp_servers,
    get_uc_functions,
    get_tool_usage,
    refresh_tools,
)

router = APIRouter(prefix="/tools", tags=["tools"], dependencies=[Depends(get_current_user)])


@router.get("/overview")
def tools_overview() -> Dict[str, Any]:
    """KPI summary: total tools, MCP server count, UC function count, etc."""
    return get_tools_overview()


@router.get("/mcp-servers")
def list_mcp_servers() -> List[Dict[str, Any]]:
    """List MCP server / serving endpoints with managed/custom classification."""
    return get_mcp_servers()


@router.get("/functions")
def list_uc_functions() -> List[Dict[str, Any]]:
    """List UC functions discovered as agent tools."""
    return get_uc_functions()


@router.get("/usage")
def tool_usage(days: int = Query(default=7, ge=1, le=90)) -> List[Dict[str, Any]]:
    """Tool call frequency + latency from MLflow traces."""
    return get_tool_usage(days)


@router.post("/sync")
def sync_tools():
    """Trigger a full tools discovery refresh."""
    refresh_tools()
    return {"status": "ok", "message": "Tools refresh complete"}


@router.get("/debug/uc")
def debug_uc_functions() -> Dict[str, Any]:
    """Debug UC function discovery — runs raw discovery and returns what it finds."""
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
