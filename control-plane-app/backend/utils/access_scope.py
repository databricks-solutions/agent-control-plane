"""Workspace access scope — the single rule for "which workspaces can this
caller see", used by every page-data-style read endpoint.

Rule (deliberately simple — no workspace-member tier):
  • Real account admin           → None   (no restriction, sees everything)
  • Workspace admin (1+ places)  → [ids]   (scoped to exactly those workspaces)
  • Everyone else                → []      (empty — endpoints must return
                                             empty results, never fall back
                                             to "all workspaces")

``None`` vs ``[]`` is meaningful and must not be collapsed: ``None`` means
"do not filter", ``[]`` means "filter to nothing".
"""
from __future__ import annotations

from typing import List, Optional

from backend.utils.auth import UserInfo


def get_allowed_workspace_ids(user: UserInfo) -> Optional[List[str]]:
    """Return the workspace_ids ``user`` may see, or None for "all"."""
    if user.is_account_admin:
        return None

    if not user.username or user.username == "service-principal":
        # SP fallback identity — no per-user OBO token on this request
        # (see backend.utils.auth._SP_FALLBACK).
        from backend.config import settings
        if not settings.obo_enabled:
            # OBO is not configured for this deployment: there is no per-user
            # identity to scope by, so preserve the app's historical
            # single-identity "show everything" behaviour rather than
            # fail-closing every endpoint to empty. Opt in via OBO_ENABLED=false.
            return None
        # OBO IS enabled but this request carried no token — anomalous; the SP
        # fallback must never be treated as an admin of anything. Fail closed.
        return []

    # Runtime access mode (admin-toggleable, see settings_service). In "open"
    # mode any authenticated user gets unrestricted READ visibility — writes are
    # still gated by require_admin/require_account_admin, so this loosens
    # visibility only. "strict" mode applies the per-user scoping below.
    try:
        from backend.services.settings_service import get_access_mode
        if get_access_mode() == "open":
            return None
    except Exception:
        # Settings unavailable → fall through to strict scoping (fail closed).
        pass

    try:
        from backend.services.workspace_admins_service import get_admin_workspace_ids
        ids = set(get_admin_workspace_ids(user.username))
    except Exception:
        # Fail closed: an access-scope lookup that can't be resolved must
        # not silently grant access.
        ids = set()

    # A user whose OBO token resolved with workspace-admin rights is, by
    # definition, an admin of the workspace that authenticated them — the
    # deploy workspace this app runs in. Trust that directly so a workspace
    # admin always sees the deploy workspace, even when the cross-workspace
    # admins cache (populated via the account permission-assignments API) is
    # empty or unavailable. Cross-workspace admin rights still come from the
    # cache above.
    if user.is_admin:
        try:
            from backend.services.billing_service import get_current_workspace_id
            current = get_current_workspace_id()
            if current:
                ids.add(str(current))
        except Exception:
            pass

    return sorted(ids)


def sees_deploy_workspace(allowed_workspace_ids: Optional[List[str]]) -> bool:
    """True if the caller may see data that only exists on the *deploy*
    workspace (live serving-endpoint / MLflow / UC / tool lists with no
    ``workspace_id`` column).

    Account admin (``allowed is None``) → yes. Empty allow-list → no.
    Workspace admin → yes only when the deploy workspace is one of theirs.
    Fail closed if the deploy workspace id cannot be resolved.
    """
    if allowed_workspace_ids is None:
        return True
    if not allowed_workspace_ids:
        return False
    try:
        # Lazy import: billing_service imports this module at load time.
        from backend.services.billing_service import get_current_workspace_id
        current = get_current_workspace_id()
    except Exception:
        return False
    if not current:
        return False
    return str(current) in {str(x) for x in allowed_workspace_ids}


def workspace_is_allowed(
    workspace_id: Optional[str],
    allowed_workspace_ids: Optional[List[str]],
) -> bool:
    """True if ``workspace_id`` is inside the caller's allow-list.

    Unknown/empty workspace_id fails closed for scoped callers (we cannot
    prove it belongs to them). Account admin is always allowed.
    """
    if allowed_workspace_ids is None:
        return True
    if not workspace_id:
        return False
    return str(workspace_id) in {str(x) for x in allowed_workspace_ids}


class NoAccess(Exception):
    """Internal signal: the caller's access scope resolves to nothing.

    Raised by ``resolve_ws_ids`` so service functions can return an empty
    result without touching Lakebase, instead of accidentally falling back
    to an unfiltered ("all workspaces") query. Shared across
    billing_service / workspace_service / mlflow_service / vector_search_service
    so every read path enforces the exact same rule.
    """


def resolve_ws_ids(
    workspace_id: "Optional[str | List[str]]",
    allowed_workspace_ids: Optional[List[str]],
) -> Optional[List[str]]:
    """Combine a UI-requested workspace selection with the caller's access
    scope into one ``ws_ids`` list usable for ``= ANY(%s)`` filtering.

    ``workspace_id`` may be a single id (str), a list of ids (the multi-select
    workspace picker), or None/empty (no specific request). ``allowed_workspace_ids``:
    None = no restriction (real account admin — unchanged existing behaviour);
    [] or [ids...] = restrict to that scope. The effective result is the
    caller's selection intersected with their allow-list. Raises ``NoAccess``
    when the combination is empty (the caller has no workspace access at all,
    or asked only for workspaces they aren't scoped to).
    """
    # Normalize the UI request to a de-duped list of requested ids.
    if isinstance(workspace_id, (list, tuple)):
        requested = [str(w) for w in workspace_id if w]
    elif workspace_id:
        requested = [str(workspace_id)]
    else:
        requested = []

    if allowed_workspace_ids is not None:
        allowed_set = {str(x) for x in allowed_workspace_ids}
        if requested:
            # selection ∩ allow-list
            ws_ids = [w for w in requested if w in allowed_set]
        else:
            ws_ids = [str(x) for x in allowed_workspace_ids]
        if not ws_ids:
            raise NoAccess()
        return ws_ids
    # Account admin: no restriction — honour the requested selection as-is.
    return requested or None


def resolve_scope(user: UserInfo, requested_workspace_id: Optional[str]) -> Optional[List[str]]:
    """Combine the caller's allow-list with an optional single-workspace
    request from the UI (e.g. the workspace picker).

    Returns:
      • None        — no restriction; honor ``requested_workspace_id`` as-is
        (account admin — unchanged existing behaviour).
      • [ws_id]      — restrict to exactly the one requested workspace,
        because the caller is allowed to see it.
      • []           — restrict to nothing: either the caller has no
        workspace-admin access at all, or they asked for a specific
        workspace they are not scoped to.
      • [ids...]     — no specific workspace requested; restrict to every
        workspace the caller administers.
    """
    allowed = get_allowed_workspace_ids(user)
    if allowed is None:
        return None
    if requested_workspace_id:
        return [requested_workspace_id] if requested_workspace_id in allowed else []
    return allowed
