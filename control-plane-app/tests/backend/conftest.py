"""Shared fixtures for backend tests."""
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Set test env vars before importing backend modules
os.environ.setdefault("LAKEBASE_DNS", "test-host")
os.environ.setdefault("LAKEBASE_DATABASE", "test_db")
os.environ.setdefault("LAKEBASE_INSTANCE", "test-instance")


@pytest.fixture
def mock_db():
    """Mock all database functions."""
    with patch("backend.database.DatabasePool") as mock_pool:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.get_connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)
        yield {"pool": mock_pool, "conn": mock_conn, "cursor": mock_cursor}


@pytest.fixture
def mock_workspace_client():
    """Mock the Databricks WorkspaceClient."""
    with patch("backend.config._get_workspace_client") as mock:
        client = MagicMock()
        client.config.host = "https://test-workspace.cloud.databricks.com"
        client.api_client.do = MagicMock(return_value={})
        mock.return_value = client
        yield client


@pytest.fixture
def obo_token():
    """A fake OBO token for testing."""
    return "test-obo-token-12345"


@pytest.fixture
def mock_user_info():
    """A mock UserInfo for authenticated requests."""
    from backend.utils.auth import UserInfo
    return UserInfo(
        username="test@databricks.com",
        display_name="Test User",
        user_id="12345",
        is_admin=True,
        is_account_admin=False,
        groups=["admins", "users"],
        token="test-token",
    )


@pytest.fixture
def mock_sp_user():
    """The SP fallback user (non-admin)."""
    from backend.utils.auth import UserInfo
    return UserInfo(
        username="service-principal",
        display_name="Control Plane SP (read-only)",
        user_id="sp",
        is_admin=False,
        is_account_admin=False,
        groups=[],
        token="",
    )


@pytest.fixture
def app_client(mock_user_info):
    """FastAPI test client with mocked auth."""
    from fastapi.testclient import TestClient
    from backend.main import app
    from backend.utils.auth import get_current_user

    async def override_auth():
        return mock_user_info

    app.dependency_overrides[get_current_user] = override_auth
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


@pytest.fixture
def unauth_client():
    """FastAPI test client WITHOUT auth override (tests auth enforcement)."""
    from fastapi.testclient import TestClient
    from backend.main import app
    app.dependency_overrides.clear()
    return TestClient(app)
