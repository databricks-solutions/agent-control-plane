"""Service for UC-based access management — read/write grants on AI resources.

Uses WorkspaceClient to interact with the UC Grants API for:
  • Serving endpoints (CAN_QUERY, CAN_MANAGE)
  • UC functions (EXECUTE, ALL_PRIVILEGES)
  • Models (SELECT, ALL_PRIVILEGES)
"""
from __future__ import annotations

import re
import time
from typing import Any, Dict, List, Optional, Tuple
from backend.config import _get_workspace_client

import logging

logger = logging.getLogger(__name__)

# UUID pattern for service principal application IDs
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)
# Numeric ID (also common for service principals)
_NUMERIC_ID_RE = re.compile(r"^\d{10,}$")


def _classify_principal_type(name: str) -> str:
    """Infer principal type from name pattern.

    - Contains '@'           → user  (email address)
    - UUID or long numeric   → service_principal  (application/object ID)
    - Everything else        → group  (e.g. 'account users', 'admins')
    """
    if not name:
        return "unknown"
    if "@" in name:
        return "user"
    if _UUID_RE.match(name) or _NUMERIC_ID_RE.match(name):
        return "service_principal"
    return "group"


# =====================================================================
# Helpers
# =====================================================================

def _is_system_endpoint(name: str) -> bool:
    """Return True for FMAPI / system endpoints that don't support permissions."""
    return name.startswith("databricks-")


def _list_serving_endpoint_permissions(endpoint_name: str) -> List[Dict[str, Any]]:
    """Get permissions on a serving endpoint."""
    if _is_system_endpoint(endpoint_name):
        return []
    w = _get_workspace_client()
    if not w:
        return []
    try:
        perms = w.serving_endpoints.get_permissions(serving_endpoint_id=endpoint_name)
        result = []
        for acl in (perms.access_control_list or []):
            principal = ""
            principal_type = ""
            if acl.user_name:
                principal = acl.user_name
                principal_type = "user"
            elif acl.group_name:
                principal = acl.group_name
                principal_type = "group"
            elif acl.service_principal_name:
                principal = acl.service_principal_name
                principal_type = "service_principal"

            for perm in (acl.all_permissions or []):
                result.append({
                    "principal": principal,
                    "principal_type": principal_type,
                    "permission": perm.permission_level.value if perm.permission_level else "",
                    "inherited": bool(perm.inherited),
                    "inherited_from": (
                        perm.inherited_from_object[0].value
                        if perm.inherited_from_object
                        else None
                    ) if perm.inherited else None,
                })
        return result
    except Exception as exc:
        logger.warning("Could not get endpoint permissions for %s: %s", endpoint_name, exc)
        return []


def _set_serving_endpoint_permissions(
    endpoint_name: str,
    principal: str,
    permission_level: str,
    principal_type: str = "user",
) -> bool:
    """Grant a permission on a serving endpoint."""
    w = _get_workspace_client()
    if not w:
        return False
    try:
        from databricks.sdk.service.serving import ServingEndpointAccessControlRequest

        acr = ServingEndpointAccessControlRequest(
            permission_level=permission_level,
        )
        if principal_type == "user":
            acr.user_name = principal
        elif principal_type == "group":
            acr.group_name = principal
        elif principal_type == "service_principal":
            acr.service_principal_name = principal

        w.serving_endpoints.set_permissions(
            serving_endpoint_id=endpoint_name,
            access_control_list=[acr],
        )
        return True
    except Exception as exc:
        logger.warning("Could not set permissions on %s: %s", endpoint_name, exc)
        return False


def _list_uc_grants_rest(securable_type: str, full_name: str) -> List[Dict[str, Any]]:
    """Get UC grants via REST API (needed for 'function' type which SDK doesn't support)."""
    from backend.config import get_databricks_host, get_databricks_headers
    import httpx

    base = get_databricks_host()
    if not base:
        return []
    try:
        resp = httpx.get(
            f"{base}/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}",
            headers=get_databricks_headers(),
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
        result = []
        for g in (data.get("privilege_assignments") or []):
            for p in (g.get("privileges") or []):
                priv = p.get("privilege", str(p)) if isinstance(p, dict) else str(p)
                result.append({
                    "principal": g.get("principal", ""),
                    "privilege": priv,
                    "inherited": False,
                })
        return result
    except Exception as exc:
        logger.warning("REST UC grants GET failed for %s/%s: %s", securable_type, full_name, exc)
        return []


def _list_uc_grants(securable_type: str, full_name: str) -> List[Dict[str, Any]]:
    """Get UC grants on a securable (table, function, etc.)."""
    # The SDK doesn't support SecurableType.FUNCTION — use REST API directly
    if securable_type.lower() == "function":
        return _list_uc_grants_rest(securable_type, full_name)

    w = _get_workspace_client()
    if not w:
        return []
    try:
        from databricks.sdk.service.catalog import SecurableType

        type_map = {
            "table": SecurableType.TABLE,
            "schema": SecurableType.SCHEMA,
            "catalog": SecurableType.CATALOG,
            "volume": SecurableType.VOLUME,
            "registered_model": SecurableType.REGISTERED_MODEL,
        }
        st = type_map.get(securable_type.lower())
        if not st:
            return []

        grants = w.grants.get(securable_type=st, full_name=full_name)
        result = []
        for g in (grants.privilege_assignments or []):
            for p in (g.privileges or []):
                result.append({
                    "principal": g.principal,
                    "privilege": p.privilege.value if hasattr(p, "privilege") and p.privilege else str(p),
                    "inherited": False,
                })
        return result
    except Exception as exc:
        logger.warning("Could not get UC grants for %s: %s", full_name, exc)
        return []


def _mutate_uc_grants_rest(
    securable_type: str, full_name: str, principal: str,
    add: Optional[List[str]] = None, remove: Optional[List[str]] = None,
) -> bool:
    """Grant or revoke UC privileges via REST API (needed for 'function' type)."""
    from backend.config import get_databricks_host, get_databricks_headers
    import httpx

    base = get_databricks_host()
    if not base:
        return False
    change: Dict[str, Any] = {"principal": principal}
    if add:
        change["add"] = [p.upper() for p in add]
    if remove:
        change["remove"] = [p.upper() for p in remove]
    try:
        resp = httpx.patch(
            f"{base}/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}",
            headers=get_databricks_headers(),
            json={"changes": [change]},
            timeout=30.0,
        )
        resp.raise_for_status()
        return True
    except httpx.HTTPStatusError as exc:
        body = exc.response.text
        logger.warning("REST UC grant/revoke failed for %s/%s: %s — %s", securable_type, full_name, exc, body)
        return False
    except Exception as exc:
        logger.warning("REST UC grant/revoke failed for %s/%s: %s", securable_type, full_name, exc)
        return False


def _grant_uc(securable_type: str, full_name: str, principal: str, privileges: List[str]) -> bool:
    """Grant UC privileges."""
    # The SDK doesn't support SecurableType.FUNCTION — use REST API directly
    if securable_type.lower() == "function":
        return _mutate_uc_grants_rest(securable_type, full_name, principal, add=privileges)

    w = _get_workspace_client()
    if not w:
        return False
    try:
        from databricks.sdk.service.catalog import SecurableType, PermissionsChange, Privilege

        type_map = {
            "table": SecurableType.TABLE,
            "schema": SecurableType.SCHEMA,
            "catalog": SecurableType.CATALOG,
            "volume": SecurableType.VOLUME,
            "registered_model": SecurableType.REGISTERED_MODEL,
        }
        st = type_map.get(securable_type.lower())
        if not st:
            return False

        priv_objs = []
        for p in privileges:
            try:
                priv_objs.append(Privilege(p))
            except Exception:
                priv_objs.append(Privilege(p.upper()))

        w.grants.update(
            securable_type=st,
            full_name=full_name,
            changes=[
                PermissionsChange(
                    add=priv_objs,
                    principal=principal,
                )
            ],
        )
        return True
    except Exception as exc:
        logger.warning("UC grant failed: %s", exc)
        return False


def _revoke_uc(securable_type: str, full_name: str, principal: str, privileges: List[str]) -> bool:
    """Revoke UC privileges."""
    # The SDK doesn't support SecurableType.FUNCTION — use REST API directly
    if securable_type.lower() == "function":
        return _mutate_uc_grants_rest(securable_type, full_name, principal, remove=privileges)

    w = _get_workspace_client()
    if not w:
        return False
    try:
        from databricks.sdk.service.catalog import SecurableType, PermissionsChange, Privilege

        type_map = {
            "table": SecurableType.TABLE,
            "schema": SecurableType.SCHEMA,
            "catalog": SecurableType.CATALOG,
            "volume": SecurableType.VOLUME,
            "registered_model": SecurableType.REGISTERED_MODEL,
        }
        st = type_map.get(securable_type.lower())
        if not st:
            return False

        priv_objs = []
        for p in privileges:
            try:
                priv_objs.append(Privilege(p))
            except Exception:
                priv_objs.append(Privilege(p.upper()))

        w.grants.update(
            securable_type=st,
            full_name=full_name,
            changes=[
                PermissionsChange(
                    remove=priv_objs,
                    principal=principal,
                )
            ],
        )
        return True
    except Exception as exc:
        logger.warning("UC revoke failed: %s", exc)
        return False


# =====================================================================
# Principal search cache (in-memory with TTL)
# =====================================================================
_principal_cache: Dict[Tuple[str, Optional[str]], Tuple[float, List[Dict[str, Any]]]] = {}
_PRINCIPAL_CACHE_TTL = 60  # seconds


def search_principals(
    query: str,
    principal_type: Optional[str] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Search for users, groups, and service principals via SCIM API (cached 60s)."""
    cache_key = (query, principal_type)
    cached = _principal_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _PRINCIPAL_CACHE_TTL:
        return cached[1][:limit]

    w = _get_workspace_client()
    if not w:
        return []

    results: List[Dict[str, Any]] = []
    try:
        types_to_search = (
            [principal_type] if principal_type else ["user", "group", "service_principal"]
        )

        for pt in types_to_search:
            if len(results) >= limit:
                break
            try:
                if pt == "user":
                    for u in w.users.list(filter=f'displayName co "{query}" or userName co "{query}"'):
                        results.append({
                            "display_name": u.display_name or "",
                            "id": u.id or "",
                            "type": "user",
                            "email": u.user_name or "",
                        })
                        if len(results) >= limit:
                            break
                elif pt == "group":
                    for g in w.groups.list(filter=f'displayName co "{query}"'):
                        results.append({
                            "display_name": g.display_name or "",
                            "id": g.id or "",
                            "type": "group",
                            "email": None,
                        })
                        if len(results) >= limit:
                            break
                elif pt == "service_principal":
                    for sp in w.service_principals.list(filter=f'displayName co "{query}"'):
                        results.append({
                            "display_name": sp.display_name or "",
                            "id": sp.id or "",
                            "type": "service_principal",
                            "email": None,
                        })
                        if len(results) >= limit:
                            break
            except Exception as exc:
                logger.warning("Warning: Could not search %ss: %s", pt, exc)

    except Exception as exc:
        logger.warning("Warning: Principal search failed: %s", exc)

    results = results[:limit]
    _principal_cache[cache_key] = (time.time(), results)
    return results


def list_foundation_model_grants() -> Dict[str, Any]:
    """Get schema-level grants on system.ai (model grants fetched on-demand)."""
    schema_grants = _list_uc_grants("schema", "system.ai")
    return {"schema_grants": schema_grants, "model_grants": {}}


def get_uc_model_name(endpoint_name: str) -> str:
    """Resolve a serving endpoint name to its UC registered model name."""
    from backend.services import gateway_service

    endpoints = gateway_service.get_all_endpoints()
    for ep in endpoints:
        if ep.get("name") == endpoint_name:
            served = ep.get("served_entities") or []
            if served:
                entity_name = served[0].get("entity_name", "")
                if "system.ai" in entity_name:
                    return entity_name
            break

    # Fallback: derive from endpoint name
    base = endpoint_name.removeprefix("databricks-")
    return f"system.ai.{base}"


# =====================================================================
# PUBLIC API
# =====================================================================

def get_resource_permissions(
    resource_type: str,
    resource_name: str,
) -> List[Dict[str, Any]]:
    """Get permissions on a resource (serving endpoint or UC securable)."""
    if resource_type == "serving_endpoint":
        return _list_serving_endpoint_permissions(resource_name)
    else:
        return _list_uc_grants(resource_type, resource_name)


def grant_permission(
    resource_type: str,
    resource_name: str,
    principal: str,
    privileges: List[str],
    principal_type: str = "user",
) -> bool:
    """Grant permissions on a resource."""
    if resource_type == "serving_endpoint":
        # For serving endpoints, use the first privilege as permission level
        level = privileges[0] if privileges else "CAN_QUERY"
        return _set_serving_endpoint_permissions(resource_name, principal, level, principal_type)
    else:
        return _grant_uc(resource_type, resource_name, principal, privileges)


def revoke_permission(
    resource_type: str,
    resource_name: str,
    principal: str,
    privileges: List[str],
) -> bool:
    """Revoke permissions on a resource."""
    if resource_type == "serving_endpoint":
        # For serving endpoints we can't selectively revoke — set to CAN_VIEW or remove entirely
        return False  # not supported via SDK easily
    else:
        return _revoke_uc(resource_type, resource_name, principal, privileges)


def get_all_principals(days: int = 30) -> List[Dict[str, Any]]:
    """Aggregate all principals that have access to AI resources.

    Combines three sources:
      1. Serving endpoint permissions (current workspace, Permissions API)
      2. UC grants on system.ai (current workspace)
      3. Distinct requesters from system.serving.endpoint_usage (account-wide)

    The ``days`` parameter controls the lookback window for the system table
    query (source 3), which is the primary source for cross-workspace principals.
    """
    w = _get_workspace_client()
    principals: Dict[str, Dict[str, Any]] = {}

    # 1. Scan serving endpoint permissions (current workspace)
    if w:
        try:
            for ep in w.serving_endpoints.list():
                ep_name = ep.name or ""
                # FMAPI / system endpoints (e.g. databricks-*) have no ID and
                # don't support the permissions API — skip them.
                if not ep.id:
                    continue
                try:
                    perms = _list_serving_endpoint_permissions(ep_name)
                    for p in perms:
                        name = p.get("principal", "")
                        if not name:
                            continue
                        if name not in principals:
                            principals[name] = {
                                "principal": name,
                                "principal_type": p.get("principal_type", ""),
                                "resources": [],
                            }
                        principals[name]["resources"].append({
                            "resource_type": "serving_endpoint",
                            "resource_name": ep_name,
                            "permission": p.get("permission", ""),
                        })
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("Could not scan endpoint permissions: %s", exc)

    # 2. Scan FMAPI UC grants (registered models under system.ai)
    if w:
        try:
            from backend.services import gateway_service

            all_eps = gateway_service.get_all_endpoints()
            for ep in all_eps:
                ep_name = ep.get("name", "")
                if not ep_name.startswith("databricks-"):
                    continue
                try:
                    uc_model_name = get_uc_model_name(ep_name)
                    grants = _list_uc_grants("function", uc_model_name)
                    for g in grants:
                        name = g.get("principal", "")
                        if not name:
                            continue
                        if name not in principals:
                            principals[name] = {
                                "principal": name,
                                "principal_type": _classify_principal_type(name),
                                "resources": [],
                            }
                        principals[name]["resources"].append({
                            "resource_type": "function",
                            "resource_name": uc_model_name,
                            "permission": g.get("privilege", ""),
                        })
                except Exception:
                    pass

            # Also scan schema-level grants on system.ai
            try:
                schema_grants = _list_uc_grants("schema", "system.ai")
                for g in schema_grants:
                    name = g.get("principal", "")
                    if not name:
                        continue
                    if name not in principals:
                        principals[name] = {
                            "principal": name,
                            "principal_type": _classify_principal_type(name),
                            "resources": [],
                        }
                    principals[name]["resources"].append({
                        "resource_type": "schema",
                        "resource_name": "system.ai",
                        "permission": g.get("privilege", ""),
                    })
            except Exception:
                pass
        except Exception as exc:
            logger.warning("Could not scan FMAPI UC grants: %s", exc)

    # 3. Account-wide principals from system.serving.endpoint_usage
    #    This surfaces every user who has actually called any serving endpoint
    #    across all workspaces — the most reliable cross-workspace signal.
    try:
        from backend.services.gateway_service import _execute_system_sql

        rows = _execute_system_sql(f"""
            SELECT
                u.requester,
                COUNT(DISTINCT se.endpoint_name) AS endpoints_used,
                COUNT(*) AS request_count,
                MAX(u.request_time) AS last_active
            FROM system.serving.endpoint_usage u
            JOIN system.serving.served_entities se
                ON u.served_entity_id = se.served_entity_id
            WHERE u.request_time >= date_sub(current_date(), {int(days)})
              AND u.requester IS NOT NULL
            GROUP BY u.requester
        """)
        for r in rows:
            name = r.get("requester", "")
            if not name:
                continue
            endpoints_used = int(r.get("endpoints_used") or 0)
            request_count = int(r.get("request_count") or 0)
            if name not in principals:
                principals[name] = {
                    "principal": name,
                    "principal_type": _classify_principal_type(name),
                    "resources": [],
                }
            # Add a summary resource entry for cross-workspace usage
            principals[name]["resources"].append({
                "resource_type": "usage",
                "resource_name": f"{endpoints_used} endpoint(s)",
                "permission": f"{request_count} requests",
            })
            principals[name]["last_active"] = r.get("last_active", "")
            principals[name]["request_count"] = request_count
    except Exception as exc:
        logger.warning("Could not query system.serving.endpoint_usage for principals: %s", exc)

    return list(principals.values())
