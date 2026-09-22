"""Workspace-registry host validation (security review finding #1c).

The registry host is used to mint the app service principal's OAuth token
(``client_credentials`` → ``{host}/oidc/v1/token``), so a caller-influenced or
malformed host would leak the SP client_id/secret. These tests pin the
allowlist and the store-time / read-time guards that prevent that.
"""
from unittest.mock import patch

from backend.services.workspace_registry import (
    is_valid_workspace_host,
    _upsert_workspace,
    get_workspace_host,
    _registry_cache,
)


class TestIsValidWorkspaceHost:
    def test_accepts_known_databricks_clouds(self):
        for host in [
            "https://fevm-serverless-b4nc10.cloud.databricks.com",
            "dbc-abc123.cloud.databricks.com",          # scheme optional
            "adb-1234567890.4.azuredatabricks.net",
            "x.gcp.databricks.com",
            "ws.cloud.databricks.us",                    # AWS GovCloud
            "ws.databricks.azure.us",                    # Azure Government
            "vanity.databricks.com",                     # any Databricks apex subdomain
        ]:
            assert is_valid_workspace_host(host) is True, host

    def test_rejects_non_databricks_and_malformed(self):
        for host in [
            "http://x.cloud.databricks.com",             # not https
            "https://evil.com",
            "https://cloud.databricks.com.evil.com",     # suffix-append trick
            "https://databricks.com.evil.com",           # apex suffix-append trick
            "https://attacker.com/x.cloud.databricks.com",  # path trick
            "https://ws.databricks.mycorp.com",          # custom domain, not allowlisted
            "not a url",
            "",
            None,
        ]:
            assert is_valid_workspace_host(host) is False, host

    def test_extra_suffixes_env_allows_custom_privatelink_domain(self, monkeypatch):
        host = "https://ws.databricks.mycorp.com"
        assert is_valid_workspace_host(host) is False
        # Operators onboard PrivateLink / vanity domains via env (with or
        # without the leading dot) instead of a code change.
        monkeypatch.setenv("EXTRA_WORKSPACE_HOST_SUFFIXES", "databricks.mycorp.com, .other.example.com")
        assert is_valid_workspace_host(host) is True
        assert is_valid_workspace_host("https://x.other.example.com") is True
        # Still https-only and still suffix-anchored.
        assert is_valid_workspace_host("http://ws.databricks.mycorp.com") is False
        assert is_valid_workspace_host("https://databricks.mycorp.com.evil.com") is False


class TestUpsertGuard:
    def test_invalid_host_is_not_stored(self):
        with patch("backend.services.workspace_registry.execute_update") as upd:
            _upsert_workspace("111", "https://evil.com", "Evil", "evil")
            upd.assert_not_called()

    def test_valid_host_is_stored(self):
        with patch("backend.services.workspace_registry.execute_update") as upd:
            _upsert_workspace("222", "https://ws.cloud.databricks.com", "Real", "real")
            upd.assert_called_once()


class TestGetWorkspaceHostGuard:
    def test_poisoned_cache_entry_is_not_returned(self):
        # Even if a bad host somehow sits in the in-memory cache, it must not be
        # handed to the SP-credential mint sites.
        _registry_cache["333"] = "https://evil.com"
        try:
            assert get_workspace_host("333") is None
        finally:
            _registry_cache.pop("333", None)

    def test_valid_cache_entry_is_returned(self):
        _registry_cache["444"] = "https://ws.cloud.databricks.com"
        try:
            assert get_workspace_host("444") == "https://ws.cloud.databricks.com"
        finally:
            _registry_cache.pop("444", None)
