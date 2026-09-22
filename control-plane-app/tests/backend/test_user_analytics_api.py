"""Tests for /user-analytics/page-data access scoping.

user_analytics_daily / user_analytics_heatmap have no workspace_id column
(account-wide rollups of system.serving.endpoint_usage), and get_all_principals
is explicitly account-wide too — so this endpoint must be suppressed entirely
for any caller who isn't a real account admin, rather than leaking
cross-workspace user activity to a workspace-scoped admin or being shown to
someone with no workspace access at all.
"""
from unittest.mock import patch

from backend.utils.auth import UserInfo


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


class TestUserAnalyticsPageDataScope:
    def test_account_admin_gets_real_data(self):
        from backend.api.user_analytics import user_analytics_page_data

        user = _user(is_account_admin=True)
        with patch("backend.api.user_analytics._get_user_kpis", return_value={"active_users_24h": 5}), \
             patch("backend.api.user_analytics._get_top_users", return_value=[{"user_id": "a"}]), \
             patch("backend.api.user_analytics._get_activity_heatmap", return_value=[]), \
             patch("backend.api.user_analytics._get_daily_active_users", return_value=[]), \
             patch("backend.api.user_analytics._get_user_agent_matrix", return_value=[]), \
             patch("backend.api.user_analytics._get_requests_per_user_distribution", return_value=[]), \
             patch("backend.api.user_analytics.get_all_principals", return_value=[{"principal": "p"}]):
            result = user_analytics_page_data(days=30, user=user)

        assert result["kpis"] == {"active_users_24h": 5}
        assert result["top_users"] == [{"user_id": "a"}]
        assert result["principals"] == [{"principal": "p"}]

    def test_workspace_admin_gets_suppressed_empty_data(self):
        """Scoped to specific workspaces — but this data can't be split by
        workspace, so it must be suppressed, not partially filtered."""
        from backend.api.user_analytics import user_analytics_page_data

        user = _user(username="wsadmin@databricks.com")
        with patch("backend.services.workspace_admins_service.get_admin_workspace_ids", return_value=["111"]), \
             patch("backend.api.user_analytics._get_user_kpis") as mock_kpis, \
             patch("backend.api.user_analytics.get_all_principals") as mock_principals:
            result = user_analytics_page_data(days=30, user=user)

        mock_kpis.assert_not_called()
        mock_principals.assert_not_called()
        assert result["kpis"]["active_users_24h"] == 0
        assert result["top_users"] == []
        assert result["principals"] == []

    def test_no_access_user_gets_suppressed_empty_data(self):
        from backend.api.user_analytics import user_analytics_page_data

        user = _user()  # not account admin, not a workspace admin anywhere
        with patch("backend.services.workspace_admins_service.get_admin_workspace_ids", return_value=[]), \
             patch("backend.api.user_analytics._get_user_kpis") as mock_kpis:
            result = user_analytics_page_data(days=30, user=user)

        mock_kpis.assert_not_called()
        assert result["principals"] == []
