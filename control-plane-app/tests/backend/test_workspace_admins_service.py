"""Tests for workspace_admins_service — the Lakebase-backed cache of
"who is an ADMIN of which workspace", used to scope non-account-admins."""
import time
from unittest.mock import patch

from backend.services import workspace_admins_service as svc


class TestGetAdminWorkspaceIds:
    def setup_method(self):
        with svc._cache_lock:
            svc._admin_cache.clear()
        svc._cache_ts = time.time()

    def test_empty_username_returns_empty(self):
        assert svc.get_admin_workspace_ids("") == []

    def test_in_memory_cache_hit(self):
        with svc._cache_lock:
            svc._admin_cache["alice@databricks.com"] = {"111", "222"}
        assert svc.get_admin_workspace_ids("alice@databricks.com") == ["111", "222"]

    def test_cache_hit_is_case_insensitive(self):
        with svc._cache_lock:
            svc._admin_cache["alice@databricks.com"] = {"111"}
        assert svc.get_admin_workspace_ids("Alice@Databricks.com") == ["111"]

    def test_cache_miss_falls_back_to_lakebase(self):
        with patch(
            "backend.services.workspace_admins_service.execute_query",
            return_value=[{"workspace_id": "333"}],
        ):
            assert svc.get_admin_workspace_ids("bob@databricks.com") == ["333"]

    def test_lakebase_error_fails_closed(self):
        """A DB error must never be treated as 'has access' — fail closed."""
        with patch(
            "backend.services.workspace_admins_service.execute_query",
            side_effect=RuntimeError("db down"),
        ):
            assert svc.get_admin_workspace_ids("carol@databricks.com") == []

    def test_expired_cache_reloads_from_lakebase(self):
        """After TTL, a refresh that landed in Lakebase (possibly from another
        worker) must replace this process's stale in-memory map."""
        with svc._cache_lock:
            svc._admin_cache["alice@databricks.com"] = {"stale"}
        svc._cache_ts = time.time() - svc._CACHE_TTL - 1
        with patch(
            "backend.services.workspace_admins_service.execute_query",
            return_value=[{"workspace_id": "111", "user_name": "alice@databricks.com"}],
        ):
            assert svc.get_admin_workspace_ids("alice@databricks.com") == ["111"]


class TestWriteRowsInOneTransaction:
    """The write path must TRUNCATE + INSERT atomically — a failed INSERT
    must roll back rather than leave the table (and everyone's access)
    empty, mirroring the fix applied to the billing sync workflow."""

    def test_insert_failure_rolls_back_instead_of_committing_truncate(self):
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # Make the INSERT (execute_values) blow up.
        with patch("backend.services.workspace_admins_service.DatabasePool") as mock_pool, \
             patch("psycopg2.extras.execute_values", side_effect=RuntimeError("insert failed")):
            mock_pool.get_connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)

            try:
                svc._write_rows_in_one_transaction([("ws1", "someone@databricks.com")])
            except RuntimeError:
                pass

        mock_conn.commit.assert_not_called()
        mock_conn.rollback.assert_called_once()

    def test_empty_rows_still_truncates_and_commits(self):
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("backend.services.workspace_admins_service.DatabasePool") as mock_pool:
            mock_pool.get_connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)

            svc._write_rows_in_one_transaction([])

        mock_cur.execute.assert_called_once_with("TRUNCATE TABLE workspace_admins")
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()
