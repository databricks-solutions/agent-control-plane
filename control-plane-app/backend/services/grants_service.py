"""Access management — read and change who can use an object, as the signed-in user.

Two families of objects, each changed with the caller's own OBO token so the
platform enforces *their* privileges and the audit log attributes the change to
them (the app's service principal is only used when OBO is disabled):

* **Unity Catalog securables** — via SQL ``SHOW GRANTS`` / ``GRANT`` / ``REVOKE``
  on the Statement Execution API. Apps OBO tokens can't carry the
  ``unity-catalog`` scope the UC permissions REST API requires, but the ``sql``
  scope covers these statements. The caller needs MANAGE or ownership (UC checks)
  and CAN_USE on the SQL warehouse.
* **Workspace objects** (apps, Genie spaces, Vector Search endpoints, Lakebase) —
  via the workspace Permissions API. OBO tokens are workspace-scoped, so only
  objects in the app's own workspace can be managed.

Agent services and skills have no SQL grant syntax yet and their REST API needs
the unavailable scope, so they're read-only here.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from backend.config import _sdk_auth_headers, find_serverless_warehouse_id, get_databricks_host

logger = logging.getLogger(__name__)

_TIMEOUT = 30.0

# kind → (SQL securable keyword, name depth, suggested privileges). The UI offers
# the suggestions; UC validates whatever is sent.
UC_KINDS: Dict[str, Tuple[str, int, List[str]]] = {
    "model_service": ("MODEL SERVICE", 3, ["EXECUTE", "MANAGE"]),
    "mcp_service": ("MCP SERVICE", 3, ["EXECUTE", "MANAGE"]),
    "model_provider_service": ("MODEL PROVIDER SERVICE", 3, ["EXECUTE", "MANAGE"]),
    "catalog": ("CATALOG", 1, ["USE CATALOG", "BROWSE", "SELECT", "MODIFY", "EXECUTE", "CREATE SCHEMA", "MANAGE", "ALL PRIVILEGES"]),
    "schema": ("SCHEMA", 2, ["USE SCHEMA", "SELECT", "MODIFY", "EXECUTE", "CREATE TABLE", "CREATE FUNCTION", "MANAGE", "ALL PRIVILEGES"]),
    # Vector Search indexes are UC tables.
    "table": ("TABLE", 3, ["SELECT", "MODIFY", "MANAGE", "ALL PRIVILEGES"]),
    "volume": ("VOLUME", 3, ["READ VOLUME", "WRITE VOLUME", "MANAGE", "ALL PRIVILEGES"]),
    "function": ("FUNCTION", 3, ["EXECUTE", "MANAGE", "ALL PRIVILEGES"]),
    "connection": ("CONNECTION", 1, ["USE CONNECTION", "MANAGE", "ALL PRIVILEGES"]),
}

# Read-only UC kinds (REST securable type). No SQL grant syntax exists for these.
UC_READ_ONLY_KINDS: Dict[str, Tuple[str, int]] = {
    "agent_service": ("agent_service", 3),
    "skill": ("skill", 3),
}

# kind → (Permissions API object types to try in order, suggested levels).
# Lakebase lists don't say whether a row is an Autoscaling project or a
# Provisioned instance, so both types are tried.
WS_KINDS: Dict[str, Tuple[Tuple[str, ...], List[str]]] = {
    "app": (("apps",), ["CAN_USE", "CAN_MANAGE"]),
    "genie_space": (("genie",), ["CAN_VIEW", "CAN_RUN", "CAN_EDIT", "CAN_MANAGE"]),
    "vector_search_endpoint": (("vector-search-endpoints",), ["CAN_USE", "CAN_MANAGE"]),
    "lakebase": (("database-projects", "database-instances"), ["CAN_USE", "CAN_MANAGE"]),
}

_NAME_PART = re.compile(r"^[^.`\s][^.`]*$")
_UC_PRIVILEGE = re.compile(r"^[A-Z][A-Z_ ]*$")
_WS_LEVEL = re.compile(r"^CAN_[A-Z_]+$")
_WS_OBJECT_ID = re.compile(r"^[\w\-]+$")
_REST_UC_PART = r"[\w\-]+"
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_NUMERIC_ID = re.compile(r"^\d{10,}$")


def kinds() -> Dict[str, Any]:
    """Supported kinds and their suggested privileges, for the UI."""
    out: Dict[str, Any] = {}
    for k, (_, _, privs) in UC_KINDS.items():
        out[k] = {"family": "uc", "writable": True, "privileges": privs}
    for k in UC_READ_ONLY_KINDS:
        out[k] = {"family": "uc", "writable": False, "privileges": []}
    for k, (_, levels) in WS_KINDS.items():
        out[k] = {"family": "workspace", "writable": True, "privileges": levels}
    return out


def _split_name(name: str, depth: int) -> Optional[List[str]]:
    parts = (name or "").split(".", depth - 1)
    if len(parts) != depth or not all(_NAME_PART.match(p) for p in parts):
        return None
    return parts


def _quote(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"


def _valid_principal(principal: str) -> bool:
    return bool(principal) and len(principal) <= 256 and not any(c in principal for c in "\r\n\x00")


def _principal_type(name: str) -> str:
    if "@" in name:
        return "user"
    if _UUID.match(name) or _NUMERIC_ID.match(name):
        return "service_principal"
    return "group"


def _headers(user_token: str) -> Dict[str, str]:
    h = {"Authorization": f"Bearer {user_token}"} if user_token else dict(_sdk_auth_headers() or {})
    h["Content-Type"] = "application/json"
    return h


def _via(user_token: str) -> str:
    return "user" if user_token else "app"


# ── Unity Catalog (SQL) ──────────────────────────────────────────────────────

def _run_sql(statement: str, user_token: str) -> Tuple[Optional[List[list]], Optional[str]]:
    """Run one statement on the app's SQL warehouse; (rows, None) or (None, error)."""
    wh_id = find_serverless_warehouse_id()
    if not wh_id:
        return None, "no SQL warehouse available"
    base = get_databricks_host()
    headers = _headers(user_token)
    try:
        resp = httpx.post(
            f"{base}/api/2.0/sql/statements", headers=headers, timeout=_TIMEOUT,
            json={"warehouse_id": wh_id, "statement": statement, "wait_timeout": "30s",
                  "disposition": "INLINE", "format": "JSON_ARRAY"},
        )
        if resp.status_code != 200:
            return None, f"{resp.status_code}: {resp.text[:300]}"
        data = resp.json()
        for _ in range(15):
            if data.get("status", {}).get("state") not in ("PENDING", "RUNNING"):
                break
            time.sleep(2)
            data = httpx.get(
                f"{base}/api/2.0/sql/statements/{data.get('statement_id')}",
                headers=headers, timeout=_TIMEOUT,
            ).json()
        status = data.get("status", {})
        if status.get("state") != "SUCCEEDED":
            return None, ((status.get("error") or {}).get("message") or status.get("state") or "unknown")[:400]
        return (data.get("result") or {}).get("data_array") or [], None
    except Exception as exc:
        return None, str(exc)[:300]


def _uc_object(kind: str, name: str) -> Optional[str]:
    keyword, depth, _ = UC_KINDS[kind]
    parts = _split_name(name, depth)
    return f"{keyword} {'.'.join(_quote(p) for p in parts)}" if parts else None


def _get_uc_grants(kind: str, name: str, user_token: str) -> Dict[str, Any]:
    obj = _uc_object(kind, name)
    if not obj:
        return {"grants": [], "error": "invalid name"}
    rows, err = _run_sql(f"SHOW GRANTS ON {obj}", user_token)
    if err is not None:
        return {"grants": [], "error": err}
    # Rows: [principal, privilege, object_type, object_key]. Grants on a parent
    # (e.g. the catalog) are inherited; only direct grants can be revoked here.
    by_principal: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        principal, privilege = (r + [None, None])[:2]
        obj_type, obj_key = (r + [None, None, None, None])[2:4]
        if not principal or not privilege:
            continue
        g = by_principal.setdefault(principal, {"principal": principal, "privileges": [], "inherited": []})
        if (obj_key or "").lower() == name.lower():
            g["privileges"].append(privilege)
        else:
            g["inherited"].append({"privilege": privilege, "from": f"{(obj_type or '').lower()} {obj_key or ''}".strip()})
    return {"grants": sorted(by_principal.values(), key=lambda g: (not g["privileges"], g["principal"]))}


def _update_uc_grants(kind: str, name: str, principal: str, add: List[str], remove: List[str], user_token: str) -> Optional[str]:
    if any(not _UC_PRIVILEGE.match(p or "") for p in add + remove):
        return "invalid privilege"
    obj = _uc_object(kind, name)
    if not obj:
        return "invalid name"
    who = _quote(principal)
    if add:
        _, err = _run_sql(f"GRANT {', '.join(add)} ON {obj} TO {who}", user_token)
        if err is not None:
            return err
    if remove:
        _, err = _run_sql(f"REVOKE {', '.join(remove)} ON {obj} FROM {who}", user_token)
        if err is not None:
            return err
    return None


def _get_uc_rest_grants(securable: str, depth: int, name: str, user_token: str) -> Dict[str, Any]:
    """UC permissions REST read. OBO tokens lack the `unity-catalog` scope, so this
    falls back to the app SP (the same metadata visibility the services list
    already has). Used for read-only kinds, and for SQL kinds when the warehouse
    can't be used."""
    # The name goes into a URL path unquoted, so only plain identifier parts pass.
    if not re.fullmatch(rf"{_REST_UC_PART}(\.{_REST_UC_PART}){{{depth - 1}}}", name or ""):
        return {"grants": [], "error": "invalid name"}
    base = get_databricks_host()
    for tok in ([user_token, ""] if user_token else [""]):
        try:
            resp = httpx.get(f"{base}/api/2.1/unity-catalog/permissions/{securable}/{name}",
                             headers=_headers(tok), timeout=_TIMEOUT)
            if resp.status_code == 200:
                return {"grants": [
                    {"principal": pa.get("principal", ""), "privileges": pa.get("privileges", []), "inherited": []}
                    for pa in resp.json().get("privilege_assignments", [])
                ]}
        except Exception as exc:
            logger.warning("UC grants GET %s/%s failed: %s", securable, name, exc)
    return {"grants": [], "error": "grants not readable (insufficient privileges)"}


# ── Workspace objects (Permissions API) ──────────────────────────────────────

def _ws_object_id(kind: str, name: str, user_token: str) -> Tuple[Optional[str], Optional[str]]:
    """Permissions API object id. Vector Search endpoints are addressed by id,
    so a name is resolved first (as the caller)."""
    if not _WS_OBJECT_ID.match(name or ""):
        return None, "invalid name"
    if kind != "vector_search_endpoint" or _UUID.match(name):
        return name, None
    try:
        resp = httpx.get(f"{get_databricks_host()}/api/2.0/vector-search/endpoints/{name}",
                         headers=_headers(user_token), timeout=_TIMEOUT)
        if resp.status_code != 200:
            return None, f"{resp.status_code}: {resp.text[:300]}"
        eid = resp.json().get("id")
        return (eid, None) if eid else (None, "endpoint id not found")
    except Exception as exc:
        return None, str(exc)[:300]


def _ws_get_acl(kind: str, object_id: str, user_token: str) -> Tuple[Optional[str], Optional[List[dict]], Optional[str]]:
    """(object type that answered, direct+inherited ACL, error)."""
    err = "not found"
    for obj_type in WS_KINDS[kind][0]:
        try:
            resp = httpx.get(f"{get_databricks_host()}/api/2.0/permissions/{obj_type}/{object_id}",
                             headers=_headers(user_token), timeout=_TIMEOUT)
            if resp.status_code == 200:
                return obj_type, resp.json().get("access_control_list") or [], None
            err = f"{resp.status_code}: {resp.text[:300]}"
            if resp.status_code not in (400, 404):
                break  # e.g. 403 — the object exists; another type won't help
        except Exception as exc:
            return None, None, str(exc)[:300]
    return None, None, err


_ACL_KEY = {"user": "user_name", "group": "group_name", "service_principal": "service_principal_name"}


def _acl_principal(entry: dict) -> Tuple[str, str]:
    for key, ptype in (("user_name", "user"), ("group_name", "group"), ("service_principal_name", "service_principal")):
        if entry.get(key):
            return entry[key], ptype
    return "", ""


def _get_ws_grants(kind: str, name: str, user_token: str) -> Dict[str, Any]:
    object_id, err = _ws_object_id(kind, name, user_token)
    if err:
        return {"grants": [], "error": err}
    _, acl, err = _ws_get_acl(kind, object_id, user_token)
    if err:
        return {"grants": [], "error": err}
    grants = []
    for entry in acl:
        principal, _ = _acl_principal(entry)
        if not principal:
            continue
        direct, inherited = [], []
        for p in entry.get("all_permissions") or []:
            level = p.get("permission_level")
            if not level:
                continue
            if p.get("inherited"):
                src = ", ".join(p.get("inherited_from_object") or []) or "parent"
                inherited.append({"privilege": level, "from": src})
            else:
                direct.append(level)
        grants.append({"principal": principal, "privileges": direct, "inherited": inherited})
    grants.sort(key=lambda g: (not g["privileges"], g["principal"]))
    return {"grants": grants}


def _update_ws_grants(kind: str, name: str, principal: str, add: List[str], remove: List[str], user_token: str) -> Optional[str]:
    if any(not _WS_LEVEL.match(p or "") for p in add + remove):
        return "invalid permission level"
    if len(add) > 1:
        return "workspace objects take one permission level per principal"
    object_id, err = _ws_object_id(kind, name, user_token)
    if err:
        return err
    obj_type, acl, err = _ws_get_acl(kind, object_id, user_token)
    if err:
        return err
    base = get_databricks_host()
    headers = _headers(user_token)
    key = _ACL_KEY[_principal_type(principal)]
    try:
        if remove:
            # The API can't drop one entry, so re-set the direct ACL without the
            # principal's removed levels. Everyone else's direct entries are kept.
            # Ownership isn't settable through this ACL, so IS_OWNER is left out
            # rather than sent back. A grant made by someone else between this
            # read and the PUT is lost — the API offers no single-entry delete.
            keep: List[dict] = []
            for entry in acl:
                p_name, p_type = _acl_principal(entry)
                if not p_name:
                    continue
                for p in entry.get("all_permissions") or []:
                    level = p.get("permission_level")
                    if p.get("inherited") or not level or level == "IS_OWNER":
                        continue
                    if p_name == principal and level in remove:
                        continue
                    keep.append({_ACL_KEY[p_type]: p_name, "permission_level": level})
            resp = httpx.put(f"{base}/api/2.0/permissions/{obj_type}/{object_id}", headers=headers,
                             json={"access_control_list": keep}, timeout=_TIMEOUT)
            if resp.status_code != 200:
                return f"{resp.status_code}: {resp.text[:300]}"
        if add:
            resp = httpx.patch(f"{base}/api/2.0/permissions/{obj_type}/{object_id}", headers=headers,
                               json={"access_control_list": [{key: principal, "permission_level": add[0]}]},
                               timeout=_TIMEOUT)
            if resp.status_code != 200:
                return f"{resp.status_code}: {resp.text[:300]}"
    except Exception as exc:
        return str(exc)[:300]
    return None


# ── Public API ───────────────────────────────────────────────────────────────

def get_grants(kind: str, name: str, user_token: str = "") -> Dict[str, Any]:
    """Grants on one object: ``{kind, name, writable, privileges, via, grants, error?}``.
    Each grant is ``{principal, privileges (direct), inherited: [{privilege, from}]}``."""
    meta = kinds().get(kind)
    if not meta:
        return {"kind": kind, "name": name, "grants": [], "error": f"unknown kind {kind!r}"}
    writable = meta["writable"]
    if kind in UC_KINDS:
        body = _get_uc_grants(kind, name, user_token)
        if "warehouse" in (body.get("error") or "").lower():
            # Grant changes need the warehouse, but reading can still go through
            # the REST API, so show the grants read-only rather than an error.
            rest = _get_uc_rest_grants(kind, UC_KINDS[kind][1], name, user_token)
            if not rest.get("error"):
                logger.info("grants: SQL read of %s %s failed (%s); read via REST", kind, name, body["error"])
                body = {**rest, "notice": "Read-only: changing access needs permission to use the app's SQL warehouse."}
                writable = False
    elif kind in UC_READ_ONLY_KINDS:
        body = _get_uc_rest_grants(*UC_READ_ONLY_KINDS[kind], name, user_token)
    else:
        body = _get_ws_grants(kind, name, user_token)
    return {"kind": kind, "name": name, "writable": writable,
            "privileges": meta["privileges"], "via": _via(user_token), **body}


def remove_principal(kind: str, name: str, principal: str, user_token: str = "") -> Dict[str, Any]:
    """Revoke every *direct* grant ``principal`` holds on the object (inherited
    ones can't be removed here). Same identity rules as ``update_grants``."""
    current = get_grants(kind, name, user_token)
    if current.get("error"):
        return {"ok": False, "via": _via(user_token), "error": current["error"]}
    direct = next((g["privileges"] for g in current["grants"] if g["principal"] == principal), [])
    if not direct:
        return {"ok": True, "via": _via(user_token)}  # nothing direct to remove
    return update_grants(kind, name, principal, remove=direct, user_token=user_token)


def update_grants(
    kind: str, name: str, principal: str,
    add: Optional[List[str]] = None, remove: Optional[List[str]] = None, user_token: str = "",
) -> Dict[str, Any]:
    """Grant ``add`` / revoke ``remove`` for ``principal``. Runs as the caller when
    ``user_token`` is set (a refusal is the platform's answer — there's no fallback
    to the app SP); otherwise as the app SP. Returns ``{ok, via, error?}``."""
    add, remove = list(add or []), list(remove or [])
    via = _via(user_token)
    if not add and not remove:
        return {"ok": False, "via": via, "error": "nothing to change"}
    if not _valid_principal(principal):
        return {"ok": False, "via": via, "error": "invalid principal"}
    if kind in UC_KINDS:
        err = _update_uc_grants(kind, name, principal, add, remove, user_token)
    elif kind in WS_KINDS:
        err = _update_ws_grants(kind, name, principal, add, remove, user_token)
    elif kind in UC_READ_ONLY_KINDS:
        err = f"{kind.replace('_', ' ')} grants can't be changed from the app yet"
    else:
        err = f"unknown kind {kind!r}"
    if err is None:
        logger.info("grants: %s %s on %s %s (+%s -%s)", via, principal, kind, name, add, remove)
        return {"ok": True, "via": via}
    return {"ok": False, "via": via, "error": err}
