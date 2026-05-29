"""Tests for backend.services.billing_service after the workflow refactor.

The billing data refresh now runs in workflows/09_discover_billing.py +
02_sync_to_lakebase.py (Phase 7). The app-side service is read-only.
"""
import pytest


class TestRefreshIsNowANoOp:
    """The old in-app refresh path has been removed in favor of the workflow."""

    def test_maybe_refresh_async_is_noop(self):
        from backend.services import billing_service
        # Must not raise, must return None, must not mutate _refresh_in_progress.
        before = billing_service._refresh_in_progress
        assert billing_service.maybe_refresh_async() is None
        assert billing_service._refresh_in_progress is before
        assert billing_service._refresh_in_progress is False

    def test_force_refresh_async_is_noop(self):
        from backend.services import billing_service
        # Accepts the legacy ``days`` kwarg for compat but ignores it.
        assert billing_service.force_refresh_async() is None
        assert billing_service.force_refresh_async(days=30) is None
        assert billing_service._refresh_in_progress is False

    def test_refresh_functions_no_longer_exported(self):
        """The old refresh_* helpers must not exist on the module — they live in the workflow."""
        from backend.services import billing_service
        for name in (
            "refresh_serving_daily",
            "refresh_token_daily",
            "refresh_product_daily",
            "refresh_user_endpoint_daily",
            "refresh_all",
            "_start_background_refresh",
            "_any_stale",
            "_is_stale",
            "_update_meta",
        ):
            assert not hasattr(billing_service, name), \
                f"{name} should have been removed (refresh now lives in workflows/09_discover_billing)"


class TestSharedSqlHelpersPreserved:
    """`_execute_system_sql` and `_find_warehouse_id` are general-purpose
    utilities reused by discovery_service. They must stay importable."""

    def test_shared_helpers_still_exported(self):
        from backend.services.billing_service import _execute_system_sql, _find_warehouse_id
        assert callable(_execute_system_sql)
        assert callable(_find_warehouse_id)


class TestReadFunctionsStillPresent:
    """All read-side functions still importable and callable (smoke test)."""

    @pytest.mark.parametrize("name", [
        "ensure_billing_tables",
        "get_serving_cost_summary",
        "get_serving_cost_trend",
        "get_serving_cost_by_sku",
        "get_serving_token_usage",
        "get_serving_daily_tokens",
        "get_serving_cost_by_user",
        "get_token_usage_by_user",
        "get_all_product_costs",
        "get_cache_status",
        "get_all_page_data",
        "get_available_workspaces",
        "get_current_workspace_id",
    ])
    def test_function_exported(self, name):
        from backend.services import billing_service
        assert hasattr(billing_service, name), f"{name} missing from billing_service"
        assert callable(getattr(billing_service, name))
