"""Tests for OBO authentication and authorization."""
import pytest
from unittest.mock import patch, MagicMock
from fastapi import Request
from backend.utils.auth import (
    get_current_user,
    require_admin,
    require_account_admin,
    _resolve_user,
    _cache_key,
    _get_cached,
    _put_cache,
    _lookup_account_admin,
    _ACCOUNT_ADMIN_CACHE,
    _SP_FALLBACK,
    UserInfo,
)


class TestSPFallback:
    """SP fallback should be read-only (non-admin)."""

    def test_sp_fallback_is_not_admin(self):
        assert _SP_FALLBACK.is_admin is False

    def test_sp_fallback_is_not_account_admin(self):
        assert _SP_FALLBACK.is_account_admin is False

    def test_sp_fallback_has_no_groups(self):
        assert _SP_FALLBACK.groups == []

    def test_sp_fallback_has_no_token(self):
        assert _SP_FALLBACK.token == ""

    def test_sp_fallback_username(self):
        assert _SP_FALLBACK.username == "service-principal"


class TestTokenCache:
    """Token → UserInfo cache."""

    def test_cache_miss_returns_none(self):
        assert _get_cached("nonexistent-token") is None

    def test_cache_hit_after_put(self, mock_user_info):
        token = "cache-test-token"
        _put_cache(token, mock_user_info)
        cached = _get_cached(token)
        assert cached is not None
        assert cached.username == mock_user_info.username

    def test_cache_key_is_deterministic(self):
        assert _cache_key("abc") == _cache_key("abc")
        assert _cache_key("abc") != _cache_key("xyz")


class TestGetCurrentUser:
    """get_current_user dependency."""

    @pytest.mark.asyncio
    async def test_no_token_returns_sp_fallback(self):
        request = MagicMock(spec=Request)
        request.headers = {}
        user = await get_current_user(request)
        assert user.username == "service-principal"
        assert user.is_admin is False

    @pytest.mark.asyncio
    async def test_obo_token_resolves_user(self):
        request = MagicMock(spec=Request)
        request.headers = {"x-forwarded-access-token": "test-token-123"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "userName": "alice@databricks.com",
            "displayName": "Alice",
            "id": "99",
            "groups": [{"display": "users"}],
            "entitlements": [],
            "roles": [],
        }
        mock_response.raise_for_status = MagicMock()

        with patch("backend.utils.auth.httpx") as mock_httpx, \
             patch("backend.utils.auth.get_databricks_host", return_value="https://test.cloud.databricks.com"):
            mock_httpx.get.return_value = mock_response
            user = await get_current_user(request)

        assert user.username == "alice@databricks.com"
        assert user.is_admin is False  # not in admins group

    @pytest.mark.asyncio
    async def test_bearer_token_fallback(self):
        """Authorization: Bearer header works when no OBO token."""
        request = MagicMock(spec=Request)
        request.headers = {"authorization": "Bearer fallback-token"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "userName": "bob@databricks.com",
            "displayName": "Bob",
            "id": "100",
            "groups": [{"display": "admins"}],
            "entitlements": [],
            "roles": [],
        }
        mock_response.raise_for_status = MagicMock()

        with patch("backend.utils.auth.httpx") as mock_httpx, \
             patch("backend.utils.auth.get_databricks_host", return_value="https://test.cloud.databricks.com"):
            mock_httpx.get.return_value = mock_response
            user = await get_current_user(request)

        assert user.username == "bob@databricks.com"
        assert user.is_admin is True  # in admins group


class TestRequireAdmin:
    """require_admin raises 403 for non-admins."""

    @pytest.mark.asyncio
    async def test_admin_passes(self, mock_user_info):
        # require_admin receives the resolved user via Depends(get_current_user).
        user = await require_admin(mock_user_info)
        assert user.is_admin is True

    @pytest.mark.asyncio
    async def test_non_admin_raises_403(self, mock_sp_user):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await require_admin(mock_sp_user)
        assert exc_info.value.status_code == 403


class TestLookupAccountAdmin:
    """_lookup_account_admin resolves real account-admin status independently
    of local workspace-admin status (the bug this replaced: every workspace
    admin used to be treated as an account admin)."""

    def setup_method(self):
        _ACCOUNT_ADMIN_CACHE.clear()

    def test_empty_username_is_never_admin(self):
        assert _lookup_account_admin("") is False

    def test_no_account_id_fails_closed_without_network_call(self):
        with patch("backend.services.workspace_registry._get_account_id", return_value=None), \
             patch("backend.utils.auth.httpx") as mock_httpx:
            assert _lookup_account_admin("someone@databricks.com") is False
            mock_httpx.get.assert_not_called()

    def test_account_scim_hit_grants_admin(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "Resources": [{"groups": [{"display": "Account Admins"}], "entitlements": [], "roles": []}]
        }
        with patch("backend.services.workspace_registry._get_account_id", return_value="acct-1"), \
             patch("backend.utils.auth.httpx") as mock_httpx:
            mock_httpx.get.return_value = mock_resp
            assert _lookup_account_admin("real-admin@databricks.com") is True

    def test_account_scim_miss_is_not_admin(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"Resources": []}
        with patch("backend.services.workspace_registry._get_account_id", return_value="acct-1"), \
             patch("backend.utils.auth.httpx") as mock_httpx:
            mock_httpx.get.return_value = mock_resp
            assert _lookup_account_admin("nobody@databricks.com") is False

    def test_result_is_cached(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "Resources": [{"groups": [{"display": "account admins"}], "entitlements": [], "roles": []}]
        }
        with patch("backend.services.workspace_registry._get_account_id", return_value="acct-1"), \
             patch("backend.utils.auth.httpx") as mock_httpx:
            mock_httpx.get.return_value = mock_resp
            assert _lookup_account_admin("cached@databricks.com") is True
            assert _lookup_account_admin("cached@databricks.com") is True
            assert mock_httpx.get.call_count == 1  # second call served from cache

    def test_error_fails_closed(self):
        with patch("backend.services.workspace_registry._get_account_id", return_value="acct-1"), \
             patch("backend.utils.auth.httpx") as mock_httpx:
            mock_httpx.get.side_effect = RuntimeError("network down")
            assert _lookup_account_admin("erroring@databricks.com") is False


class TestRequireAccountAdmin:
    """require_account_admin raises 403 for non-account-admins."""

    @pytest.mark.asyncio
    async def test_non_account_admin_raises_403(self, mock_user_info):
        # mock_user_info has is_account_admin=False
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await require_account_admin(mock_user_info)
        assert exc_info.value.status_code == 403
