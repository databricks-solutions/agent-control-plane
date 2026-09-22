"""Tests for the workspace access-scope rule (backend/utils/access_scope.py).

Rule under test: account admin -> None (unrestricted); workspace admin ->
[ids...]; everyone else (including the SP fallback) -> [] (nothing).
"""
from unittest.mock import patch

from backend.utils.auth import UserInfo
from backend.utils.access_scope import get_allowed_workspace_ids, resolve_scope


def _user(**overrides) -> UserInfo:
    base = dict(
        username="user@databricks.com",
        display_name="User",
        user_id="1",
        is_admin=False,
        is_account_admin=False,
        groups=[],
        token="tok",
    )
    base.update(overrides)
    return UserInfo(**base)


class TestGetAllowedWorkspaceIds:
    def test_account_admin_has_no_restriction(self):
        user = _user(is_account_admin=True)
        assert get_allowed_workspace_ids(user) is None

    def test_sp_fallback_gets_empty_list(self):
        user = _user(username="service-principal")
        assert get_allowed_workspace_ids(user) == []

    def test_empty_username_gets_empty_list(self):
        user = _user(username="")
        assert get_allowed_workspace_ids(user) == []

    def test_workspace_admin_gets_their_workspaces(self):
        user = _user(username="admin@databricks.com")
        with patch(
            "backend.services.workspace_admins_service.get_admin_workspace_ids",
            return_value=["111", "222"],
        ):
            assert get_allowed_workspace_ids(user) == ["111", "222"]

    def test_non_admin_with_no_workspace_admin_rows_gets_empty_list(self):
        user = _user(username="plain-user@databricks.com")
        with patch(
            "backend.services.workspace_admins_service.get_admin_workspace_ids",
            return_value=[],
        ):
            assert get_allowed_workspace_ids(user) == []

    def test_lookup_failure_fails_closed(self):
        """If the workspace_admins lookup blows up, treat as no access —
        never fall back to "show everything"."""
        user = _user(username="user@databricks.com")
        with patch(
            "backend.services.workspace_admins_service.get_admin_workspace_ids",
            side_effect=RuntimeError("boom"),
        ):
            assert get_allowed_workspace_ids(user) == []

    def test_local_workspace_admin_is_not_automatically_account_admin(self):
        """Regression guard for the bug this replaces: being a workspace
        admin of the app's own workspace must NOT imply account-admin
        (i.e. "see everything") status."""
        user = _user(username="ws-admin@databricks.com", is_admin=True, is_account_admin=False)
        with patch(
            "backend.services.workspace_admins_service.get_admin_workspace_ids",
            return_value=["111"],
        ):
            allowed = get_allowed_workspace_ids(user)
        assert allowed == ["111"]  # scoped, not None ("everything")


class TestResolveScope:
    def test_account_admin_ignores_requested_workspace(self):
        user = _user(is_account_admin=True)
        assert resolve_scope(user, "999") is None

    def test_requested_workspace_within_scope_is_honored(self):
        user = _user(username="admin@databricks.com")
        with patch(
            "backend.services.workspace_admins_service.get_admin_workspace_ids",
            return_value=["111", "222"],
        ):
            assert resolve_scope(user, "111") == ["111"]

    def test_requested_workspace_outside_scope_is_denied(self):
        user = _user(username="admin@databricks.com")
        with patch(
            "backend.services.workspace_admins_service.get_admin_workspace_ids",
            return_value=["111", "222"],
        ):
            assert resolve_scope(user, "999") == []

    def test_no_requested_workspace_returns_full_scope(self):
        user = _user(username="admin@databricks.com")
        with patch(
            "backend.services.workspace_admins_service.get_admin_workspace_ids",
            return_value=["111", "222"],
        ):
            assert resolve_scope(user, None) == ["111", "222"]

    def test_non_admin_always_gets_empty_regardless_of_request(self):
        user = _user(username="plain-user@databricks.com")
        with patch(
            "backend.services.workspace_admins_service.get_admin_workspace_ids",
            return_value=[],
        ):
            assert resolve_scope(user, "111") == []
            assert resolve_scope(user, None) == []


class TestSeesDeployWorkspace:
    def test_account_admin_sees_deploy_workspace(self):
        from backend.utils.access_scope import sees_deploy_workspace
        assert sees_deploy_workspace(None) is True

    def test_empty_allow_list_does_not_see_deploy_workspace(self):
        from backend.utils.access_scope import sees_deploy_workspace
        assert sees_deploy_workspace([]) is False

    def test_workspace_admin_of_deploy_workspace_sees_it(self):
        from backend.utils.access_scope import sees_deploy_workspace
        with patch(
            "backend.services.billing_service.get_current_workspace_id",
            return_value="111",
        ):
            assert sees_deploy_workspace(["111", "222"]) is True

    def test_workspace_admin_of_other_workspace_does_not(self):
        from backend.utils.access_scope import sees_deploy_workspace
        with patch(
            "backend.services.billing_service.get_current_workspace_id",
            return_value="111",
        ):
            assert sees_deploy_workspace(["222"]) is False

    def test_unresolved_deploy_id_fails_closed(self):
        from backend.utils.access_scope import sees_deploy_workspace
        with patch(
            "backend.services.billing_service.get_current_workspace_id",
            return_value=None,
        ):
            assert sees_deploy_workspace(["111"]) is False


class TestWorkspaceIsAllowed:
    def test_account_admin_allowed_any_workspace(self):
        from backend.utils.access_scope import workspace_is_allowed
        assert workspace_is_allowed("999", None) is True

    def test_unknown_workspace_fails_closed_when_scoped(self):
        from backend.utils.access_scope import workspace_is_allowed
        assert workspace_is_allowed("", ["111"]) is False
        assert workspace_is_allowed(None, ["111"]) is False

    def test_membership(self):
        from backend.utils.access_scope import workspace_is_allowed
        assert workspace_is_allowed("111", ["111", "222"]) is True
        assert workspace_is_allowed("999", ["111", "222"]) is False


class TestFilterRtStatus:
    """Operations live status must keep empty-workspace_id rows for deploy-ws admins."""

    def _payload(self):
        return {
            "agents": [
                {"workspace_id": "111", "health": "healthy", "name": "a"},
                {"workspace_id": "", "health": "degraded", "name": "local"},
                {"workspace_id": "222", "health": "down", "name": "b"},
            ],
            "last_refreshed": "t",
        }

    def test_account_admin_unfiltered(self):
        from backend.services.operations_service import _filter_rt_status
        data = self._payload()
        assert _filter_rt_status(data, None) is data

    def test_no_access_returns_empty(self):
        from backend.services.operations_service import _filter_rt_status
        out = _filter_rt_status(self._payload(), [])
        assert out["agents"] == []
        assert out["summary"]["total"] == 0

    def test_deploy_admin_keeps_empty_workspace_id_rows(self):
        from backend.services.operations_service import _filter_rt_status
        with patch(
            "backend.services.operations_service.sees_deploy_workspace",
            return_value=True,
        ):
            out = _filter_rt_status(self._payload(), ["111"])
        names = {a["name"] for a in out["agents"]}
        assert names == {"a", "local"}
        assert out["summary"]["total"] == 2
        assert out["summary"]["healthy"] == 1
        assert out["summary"]["degraded"] == 1

    def test_other_workspace_admin_drops_empty_workspace_id_rows(self):
        from backend.services.operations_service import _filter_rt_status
        with patch(
            "backend.services.operations_service.sees_deploy_workspace",
            return_value=False,
        ):
            out = _filter_rt_status(self._payload(), ["222"])
        assert [a["name"] for a in out["agents"]] == ["b"]
        assert out["summary"]["down"] == 1


class TestDiscoveryStatusScope:
    def test_no_access_skips_query(self):
        with patch("backend.services.discovery_service.execute_one") as mock_one, \
             patch("backend.services.discovery_service.execute_query") as mock_q:
            from backend.services.discovery_service import get_discovery_status
            result = get_discovery_status(allowed_workspace_ids=[])
        mock_one.assert_not_called()
        mock_q.assert_not_called()
        assert result["total_discovered"] == 0
        assert result["by_type"] == {}
