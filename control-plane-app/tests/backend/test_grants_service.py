"""Access management (grants_service + /grants routes): UC grants via SQL and
workspace-object ACLs via the Permissions API, both as the caller."""
from types import SimpleNamespace
from unittest.mock import patch

import pytest


class _Resp(SimpleNamespace):
    def json(self):
        return self.body


class _Http:
    """Records httpx calls; ``routes`` maps (method, url-suffix) → (status, body)."""

    def __init__(self, routes=None, sql=None):
        self.routes, self.sql, self.calls = routes or {}, sql or {}, []

    def _hit(self, method, url, **kw):
        self.calls.append((method, url, kw))
        if url.endswith("/api/2.0/sql/statements"):
            stmt = kw["json"]["statement"]
            for prefix, (rows, err) in self.sql.items():
                if stmt.startswith(prefix):
                    if err:
                        return _Resp(status_code=200, text="", body={"status": {"state": "FAILED", "error": {"message": err}}})
                    return _Resp(status_code=200, text="", body={"status": {"state": "SUCCEEDED"}, "result": {"data_array": rows}})
            return _Resp(status_code=200, text="", body={"status": {"state": "SUCCEEDED"}, "result": {}})
        for (m, suffix), (status, body) in self.routes.items():
            if m == method and url.endswith(suffix):
                return _Resp(status_code=status, text=str(body), body=body)
        return _Resp(status_code=404, text="not found", body={})

    def post(self, url, **kw):
        return self._hit("POST", url, **kw)

    def get(self, url, **kw):
        return self._hit("GET", url, **kw)

    def put(self, url, **kw):
        return self._hit("PUT", url, **kw)

    def patch(self, url, **kw):
        return self._hit("PATCH", url, **kw)

    def statements(self):
        return [kw["json"]["statement"] for m, u, kw in self.calls if u.endswith("/sql/statements")]


@pytest.fixture
def http():
    from backend.services import grants_service as gs

    def _make(**kw):
        h = _Http(**kw)
        patches = [patch.object(gs, "httpx", h), patch.object(gs, "find_serverless_warehouse_id", return_value="wh"),
                   patch.object(gs, "get_databricks_host", return_value="https://ws"),
                   patch.object(gs, "_sdk_auth_headers", return_value={"Authorization": "Bearer SP"})]
        for p in patches:
            p.start()
        _make.patches.extend(patches)
        return h
    _make.patches = []
    yield _make
    for p in _make.patches:
        p.stop()


class TestUnityCatalog:
    def test_grant_runs_quoted_sql_as_the_caller(self, http):
        from backend.services import grants_service as gs
        h = http()
        out = gs.update_grants("model_service", "cat.sch.svc", "u@x.com", ["EXECUTE"], [], user_token="tok")
        assert out == {"ok": True, "via": "user"}
        assert h.statements() == ["GRANT EXECUTE ON MODEL SERVICE `cat`.`sch`.`svc` TO `u@x.com`"]
        assert h.calls[0][2]["headers"]["Authorization"] == "Bearer tok"

    def test_revoke_and_backtick_in_principal_is_escaped(self, http):
        from backend.services import grants_service as gs
        h = http()
        gs.update_grants("catalog", "main", "evil` TO `x", [], ["USE CATALOG"], user_token="tok")
        assert h.statements() == ["REVOKE USE CATALOG ON CATALOG `main` FROM `evil`` TO ``x`"]

    def test_refusal_is_final_no_sp_fallback(self, http):
        from backend.services import grants_service as gs
        h = http(sql={"GRANT": (None, "PERMISSION_DENIED: User does not have MANAGE")})
        out = gs.update_grants("table", "c.s.t", "g", ["SELECT"], [], user_token="tok")
        assert out["ok"] is False and "MANAGE" in out["error"] and out["via"] == "user"
        assert all(kw["headers"]["Authorization"] == "Bearer tok" for _, _, kw in h.calls)

    @pytest.mark.parametrize("privs", [["EXECUTE; DROP TABLE x"], ["select"], [""]])
    def test_invalid_privileges_never_reach_sql(self, http, privs):
        from backend.services import grants_service as gs
        h = http()
        assert gs.update_grants("function", "c.s.f", "u@x.com", privs, [], user_token="tok")["error"] == "invalid privilege"
        assert h.calls == []

    @pytest.mark.parametrize("kind,name", [("table", "c.s"), ("schema", "c"), ("catalog", "a.b"), ("table", "c..t"), ("table", "c.s.`t")])
    def test_wrong_depth_or_bad_names_rejected(self, http, kind, name):
        from backend.services import grants_service as gs
        h = http()
        assert gs.get_grants(kind, name, "tok")["error"] == "invalid name"
        assert h.calls == []

    def test_show_grants_splits_direct_and_inherited(self, http):
        from backend.services import grants_service as gs
        http(sql={"SHOW GRANTS": ([
            ["a@x.com", "EXECUTE", "MODEL_SERVICE", "cat.sch.svc"],
            ["a@x.com", "MANAGE", "CATALOG", "cat"],
            ["admins", "ALL PRIVILEGES", "CATALOG", "cat"],
        ], None)})
        out = gs.get_grants("model_service", "cat.sch.svc", "tok")
        assert out["writable"] is True and out["via"] == "user" and "EXECUTE" in out["privileges"]
        assert out["grants"] == [
            {"principal": "a@x.com", "privileges": ["EXECUTE"], "inherited": [{"privilege": "MANAGE", "from": "catalog cat"}]},
            {"principal": "admins", "privileges": [], "inherited": [{"privilege": "ALL PRIVILEGES", "from": "catalog cat"}]},
        ]

    def test_without_a_token_the_app_sp_acts(self, http):
        from backend.services import grants_service as gs
        h = http()
        assert gs.update_grants("schema", "c.s", "g", ["USE SCHEMA"], [], user_token="")["via"] == "app"
        assert h.calls[0][2]["headers"]["Authorization"] == "Bearer SP"


class TestWarehouseFallback:
    def test_unusable_warehouse_falls_back_to_rest_read_only(self, http):
        from backend.services import grants_service as gs
        http(sql={"SHOW GRANTS": (None, "User does not have CAN_USE on warehouse wh")},
             routes={("GET", "/unity-catalog/permissions/mcp_service/system.ai.slack"):
                     (200, {"privilege_assignments": [{"principal": "account users", "privileges": ["EXECUTE"]}]})})
        out = gs.get_grants("mcp_service", "system.ai.slack", "tok")
        assert out["writable"] is False and "warehouse" in out["notice"] and "error" not in out
        assert out["grants"] == [{"principal": "account users", "privileges": ["EXECUTE"], "inherited": []}]

    def test_other_sql_errors_are_not_masked(self, http):
        from backend.services import grants_service as gs
        h = http(sql={"SHOW GRANTS": (None, "PERMISSION_DENIED: no MANAGE")})
        out = gs.get_grants("table", "c.s.t", "tok")
        assert "MANAGE" in out["error"] and out["writable"] is True
        assert not any("/unity-catalog/permissions" in u for _, u, _ in h.calls)

    def test_catalog_depth_rest_name_check(self, http):
        from backend.services import grants_service as gs
        assert gs._get_uc_rest_grants("catalog", 1, "a/b", "tok")["error"] == "invalid name"


class TestReadOnlyKinds:
    def test_agent_service_writes_are_refused(self, http):
        from backend.services import grants_service as gs
        h = http()
        out = gs.update_grants("agent_service", "c.s.a", "u@x.com", ["EXECUTE"], [], user_token="tok")
        assert out["ok"] is False and "can't be changed" in out["error"]
        assert h.calls == []

    @pytest.mark.parametrize("bad", ["../../permissions/catalog/x", "cat/sch.x.y", "c.s.x?y=1", "a.b"])
    def test_rest_names_cannot_alter_the_path(self, http, bad):
        from backend.services import grants_service as gs
        h = http()
        assert gs.get_grants("skill", bad, "tok")["error"] == "invalid name"
        assert h.calls == []


class TestWorkspaceObjects:
    _ACL = {"access_control_list": [
        {"user_name": "owner@x.com", "all_permissions": [{"permission_level": "CAN_MANAGE", "inherited": False}]},
        {"group_name": "data", "all_permissions": [{"permission_level": "CAN_RUN", "inherited": False}]},
        {"group_name": "admins", "all_permissions": [{"permission_level": "CAN_MANAGE", "inherited": True,
                                                      "inherited_from_object": ["/directories/"]}]},
    ]}

    def test_reads_genie_acl_as_the_caller(self, http):
        from backend.services import grants_service as gs
        h = http(routes={("GET", "/permissions/genie/sp1"): (200, self._ACL)})
        out = gs.get_grants("genie_space", "sp1", "tok")
        assert out["grants"][0] == {"principal": "data", "privileges": ["CAN_RUN"], "inherited": []}
        assert {"principal": "admins", "privileges": [], "inherited": [{"privilege": "CAN_MANAGE", "from": "/directories/"}]} in out["grants"]
        assert h.calls[0][2]["headers"]["Authorization"] == "Bearer tok"

    def test_add_patches_one_entry_with_the_right_principal_key(self, http):
        from backend.services import grants_service as gs
        h = http(routes={("GET", "/permissions/apps/my-app"): (200, self._ACL),
                         ("PATCH", "/permissions/apps/my-app"): (200, {})})
        assert gs.update_grants("app", "my-app", "eng team", ["CAN_USE"], [], "tok")["ok"]
        patch_call = [kw for m, _, kw in h.calls if m == "PATCH"][0]
        assert patch_call["json"] == {"access_control_list": [{"group_name": "eng team", "permission_level": "CAN_USE"}]}

    def test_remove_resets_direct_acl_without_only_that_entry(self, http):
        from backend.services import grants_service as gs
        h = http(routes={("GET", "/permissions/genie/sp1"): (200, self._ACL),
                         ("PUT", "/permissions/genie/sp1"): (200, {})})
        assert gs.update_grants("genie_space", "sp1", "data", [], ["CAN_RUN"], "tok")["ok"]
        put = [kw for m, _, kw in h.calls if m == "PUT"][0]
        # Owner kept, removed entry dropped, inherited admins entry not re-set as direct.
        assert put["json"] == {"access_control_list": [{"user_name": "owner@x.com", "permission_level": "CAN_MANAGE"}]}

    def test_one_level_per_principal(self, http):
        from backend.services import grants_service as gs
        http()
        assert "one permission level" in gs.update_grants("app", "a", "u@x.com", ["CAN_USE", "CAN_MANAGE"], [], "tok")["error"]

    def test_lakebase_falls_back_from_project_to_instance(self, http):
        from backend.services import grants_service as gs
        h = http(routes={("GET", "/permissions/database-instances/db1"): (200, {"access_control_list": []})})
        assert gs.get_grants("lakebase", "db1", "tok")["grants"] == []
        paths = [u for _, u, _ in h.calls]
        assert paths == ["https://ws/api/2.0/permissions/database-projects/db1",
                         "https://ws/api/2.0/permissions/database-instances/db1"]

    def test_lakebase_forbidden_does_not_try_other_type(self, http):
        from backend.services import grants_service as gs
        h = http(routes={("GET", "/permissions/database-projects/db1"): (403, "forbidden")})
        assert gs.get_grants("lakebase", "db1", "tok")["error"].startswith("403")
        assert len(h.calls) == 1

    def test_vector_search_endpoint_name_resolves_to_id(self, http):
        from backend.services import grants_service as gs
        eid = "7d4d5b59-289a-4c11-9bd5-319c9f17aff0"
        h = http(routes={("GET", "/vector-search/endpoints/vs-ep"): (200, {"id": eid}),
                         ("GET", f"/permissions/vector-search-endpoints/{eid}"): (200, {"access_control_list": []})})
        assert gs.get_grants("vector_search_endpoint", "vs-ep", "tok")["grants"] == []
        assert h.calls[1][1].endswith(f"/permissions/vector-search-endpoints/{eid}")

    def test_object_ids_cannot_alter_the_path(self, http):
        from backend.services import grants_service as gs
        h = http()
        assert gs.get_grants("app", "../clusters/x", "tok")["error"] == "invalid name"
        assert h.calls == []


class TestRoutes:
    def test_get_passes_the_callers_token(self, app_client):
        with patch("backend.services.grants_service.get_grants", return_value={"grants": []}) as get:
            r = app_client.get("/api/v1/grants", params={"kind": "app", "name": "a"})
        assert r.status_code == 200
        assert get.call_args.kwargs["user_token"] == "test-token"

    def test_non_admin_owner_can_write(self, app_client, mock_user_info):
        # No app-level admin gate: the platform checks the caller's own privileges.
        mock_user_info.is_admin = False
        with patch("backend.services.grants_service.update_grants", return_value={"ok": True, "via": "user"}) as up:
            r = app_client.post("/api/v1/grants", json={"kind": "table", "name": "c.s.t", "principal": "g", "add": ["SELECT"]})
        assert r.status_code == 200 and r.json()["ok"]
        assert up.call_args.kwargs["user_token"] == "test-token"

    def test_sp_fallback_with_obo_enabled_is_refused(self, app_client, mock_user_info, monkeypatch):
        from backend.config import settings
        monkeypatch.setattr(settings, "obo_enabled", True)
        mock_user_info.username, mock_user_info.token = "service-principal", ""
        with patch("backend.services.grants_service.update_grants") as up:
            r = app_client.post("/api/v1/grants", json={"kind": "table", "name": "c.s.t", "principal": "g", "add": ["SELECT"]})
        assert r.status_code == 401
        up.assert_not_called()

    def test_sp_acts_when_obo_is_disabled(self, app_client, mock_user_info, monkeypatch):
        from backend.config import settings
        monkeypatch.setattr(settings, "obo_enabled", False)
        mock_user_info.username, mock_user_info.token = "service-principal", ""
        with patch("backend.services.grants_service.get_grants", return_value={"grants": []}) as get:
            assert app_client.get("/api/v1/grants", params={"kind": "app", "name": "a"}).status_code == 200
        assert get.call_args.kwargs["user_token"] == ""
