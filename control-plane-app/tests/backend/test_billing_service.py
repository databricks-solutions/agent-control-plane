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


class TestCostByServiceSavepoint:
    """cost_by_service joins uag_endpoint_service_map, which may not exist until the
    first workflow run after deploy. Its failure must roll back only its SAVEPOINT —
    not abort the shared transaction the rest of the Governance page reads with."""

    class _Cur:
        def __init__(self, fail_map: bool, map_rows=None):
            self.sql_log, self.fail_map, self.map_rows, self._last = [], fail_map, map_rows or [], ""

        def execute(self, sql, params=None):
            self.sql_log.append(" ".join(sql.split()))
            self.params_log = getattr(self, "params_log", []) + [params]
            self._last = sql
            if self.fail_map and "uag_endpoint_service_map" in sql:
                raise Exception('relation "uag_endpoint_service_map" does not exist')

        def fetchall(self):
            return self.map_rows if "uag_endpoint_service_map" in self._last else []

        def fetchone(self):
            return None

        def close(self):
            pass

    def _run(self, cur, allowed=None):
        from contextlib import contextmanager
        from unittest.mock import patch, MagicMock
        from backend.services import billing_service as b

        conn = MagicMock()
        conn.cursor.return_value = cur

        @contextmanager
        def _conn():
            yield conn

        with patch.object(b.DatabasePool, "get_connection", _conn), \
             patch.object(b, "maybe_refresh_async", lambda *a, **k: None):
            return b.get_all_page_data(days=30, allowed_workspace_ids=allowed)

    def test_missing_map_table_rolls_back_savepoint_only(self):
        cur = self._Cur(fail_map=True)
        out = self._run(cur)
        assert out["cost_by_service"] == [] and out["cost_by_user_service"] == []
        log = cur.sql_log
        assert "SAVEPOINT svc_cost" in log and "ROLLBACK TO SAVEPOINT svc_cost" in log
        # queries after the failed one still ran (transaction not left aborted)
        after = log[log.index("ROLLBACK TO SAVEPOINT svc_cost") + 1:]
        assert any("billing_user_endpoint_daily" in s for s in after)

    def test_rows_returned_when_map_present(self):
        rows = [{"service_name": "system.ai.claude-opus-4-8", "service_type": "MODEL_SERVICE",
                 "total_dbus": 1, "total_cost_usd": 3061.85, "users": 7}]
        cur = self._Cur(fail_map=False, map_rows=rows)
        out = self._run(cur)
        assert out["cost_by_service"][0]["service_name"] == "system.ai.claude-opus-4-8"
        assert "RELEASE SAVEPOINT svc_cost" in cur.sql_log


    def test_scoped_caller_filters_service_cost_by_workspace(self):
        """A workspace-scoped caller must only see gateway cost for their workspaces."""
        cur = self._Cur(fail_map=False)
        self._run(cur, allowed=["ws1"])
        svc = [(sql, prm) for sql, prm in zip(cur.sql_log, cur.params_log) if "uag_endpoint_service_map" in sql]
        assert len(svc) == 2
        for sql, prm in svc:
            assert "u.workspace_id = ANY(%s)" in sql
            assert any(p == ["ws1"] for p in (prm or ())), prm
