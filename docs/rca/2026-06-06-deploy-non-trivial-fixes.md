# Findings log: Non-trivial fixes during initial sandbox deployment

**Date:** 2026-06-04 → 2026-06-06
**Status:** Open log — items listed are deployment-blocking issues that exceeded the README/`docs/installation.md` instructions and required code or process changes.
**Workspace:** Sandbox (workspace and account identifiers redacted)

## Summary

The `agent-control-plane` README + `docs/installation.md` describe the happy-path setup: workspace admin runs `databricks bundle deploy`, runs `deploy.sh`, and the app + workflow start serving. On this sandbox that path was blocked at four points by environmental constraints not covered in the docs. Each blocker was resolved with a code or process change captured below so the next deployer does not have to re-discover them.

---

## 1. Workspace-admin alone is not sufficient — metastore admin / `CREATE CATALOG` is required

**Symptom.** `databricks bundle deploy --target dev` fails on the first task that creates the Unity Catalog catalog:

```
PERMISSION_DENIED: User does not have CREATE CATALOG on Metastore 'primary'
```

The user (a workspace admin) is a member of the `admins` group.

**Root cause.** Workspace admin is a *workspace-scoped* role; UC metastore admin is *account-level* and orthogonal. The README assumes the deployer has both but does not say so explicitly.

**Fix.** Request metastore-admin elevation on a privileged identity (a dedicated admin account, not the day-to-day developer account). Confirm with:

```sh
databricks api post /api/2.0/sql/statements -p <profile> --json \
  '{"warehouse_id":"<wh>","statement":"SELECT current_metastore() AS m, is_account_group_member(\"account admins\") AS is_acct_admin"}'
```

Then deploy with `-p <admin-profile>`. **Documentation gap:** `docs/installation.md` should list `CREATE CATALOG ON METASTORE` (or metastore-admin) as a hard prerequisite alongside workspace-admin.

---

## 2. SSO session reuse silently authenticates the wrong identity

**Symptom.** `databricks auth login -p admin-profile --host <url>` returned "successfully authenticated" without prompting for password — it had silently reused the existing Microsoft 365 / corporate SSO browser session for the day-to-day developer account. Every subsequent CLI call ran as the developer account, *not* the admin account, despite the profile name.

**Root cause.** The Databricks browser-OAuth flow honours an existing IdP cookie. If the browser is already signed in to Microsoft 365 (or any IdP) with another work account, the OIDC step doesn't re-prompt.

**Fix.**
1. Open an incognito window before the next `databricks auth login`.
2. After login, **always verify identity**:
   ```sh
   databricks current-user me -p <profile> -o json | jq .userName
   ```
3. If the wrong user surfaces, run `databricks auth logout -p <profile>` and retry in a private session.

**Documentation gap:** `docs/installation.md` should warn explicitly that profile names do not constrain identity — only the OAuth flow does — and add the `current-user me` verification step.

---

## 3. Lakebase Postgres rejects laptop connections from this workspace

**Symptom.** `deploy.sh` runs `grant_sp_lakebase.py` on the laptop, which connects to the Lakebase PG instance via psycopg2. Result:

```
psycopg2.OperationalError: External authorization failed.
Public access is not allowed for workspace <workspace-id>.
```

This persists even on the corporate VPN. The Lakebase endpoint resolves to a public IP; the rejection is server-side, based on a workspace-attached Network Policy that disallows public ingress to Lakebase.

**Root cause.** Workspace network policy blocks public PG access. There is no laptop-side fix — the connection has to originate from inside the workspace.

**Fix.** Run the grant logic as a one-shot Databricks Job using serverless compute. New in this repo:

| File | Role |
|---|---|
| `control-plane-app/grant_sp_lakebase_notebook.py` | Wrapper notebook: pip-installs deps, reads widget params, sets env vars, then `runpy.run_path`s `grant_sp_lakebase.py`. **Critical:** wraps in `try/except SystemExit` because the notebook framework treats `sys.exit(0)` (used by the script for normal completion) as a failure. |
| `control-plane-app/run_grant_sp_lakebase_job.sh` | Helper that uploads both files, submits a one-shot job (`notebook_task` + serverless `environments` spec with pinned `databricks-sdk>=0.40.0`, `psycopg2-binary`, `requests`). |
| `control-plane-app/deploy.sh` | Modified end-of-script: replaces the laptop-side `grant_sp_lakebase.py` invocation with a print pointing at `run_grant_sp_lakebase_job.sh`. |

**Sub-traps encountered while building the in-workspace fallback:**

- Submitting with `spark_python_task + environment_variables` failed: serverless `environments[].spec` does not honour `environment_variables`. Fixed by switching to `notebook_task` with `base_parameters` (widget reads).
- Default SDK on the serverless runtime did not have `databricks.sdk.service.postgres`. Fixed by pinning `databricks-sdk>=0.40.0` in the env spec **and** in a `%pip install --upgrade` cell at the top of the wrapper notebook.
- `sys.exit(0)` in `grant_sp_lakebase.py` reported as a task failure. Wrapper notebook catches `SystemExit` and only re-raises on non-zero exit codes.

**Documentation gap:** `docs/installation.md` mentions Lakebase prerequisites but does not warn that workspaces with public-PG blocked require running the SP-grant step in-workspace. The new helper script + this RCA should be referenced from `docs/installation.md`.

---

## 4. Sync workflow silently fails to create `gateway_usage_daily` and `gateway_usage_hourly`

**Symptom.** After successful end-to-end deploy:
- `databricks apps logs ai-control-plane` shows repeated:
  ```
  psycopg2.errors.UndefinedTable: relation "gateway_usage_daily" does not exist
  ```
- The Gateway page in the deployed app loads with empty metrics.
- Yet the `02_sync_to_lakebase` task reports `result=SUCCESS` on every recent run.
- The Delta source table `ai_control_plane.control_plane.gateway_usage_daily` exists and has rows.
- Direct in-workspace inspection of Lakebase `control_plane` confirms only the daily table is missing — every other expected table is present.

**Root cause.** `workflows/02_sync_to_lakebase.py` Phase 6 (gateway sync) ran a single transaction containing CREATEs and ALTERs interleaved as:

```python
[
  CREATE TABLE gateway_usage_daily (...),
  CREATE INDEX idx_gud_date,
  CREATE INDEX idx_gud_ep,
  ALTER TABLE gateway_usage_daily  ADD COLUMN IF NOT EXISTS rate_limited_count ...,
  ALTER TABLE gateway_usage_hourly ADD COLUMN IF NOT EXISTS rate_limited_count ...,  # <-- table doesn't exist yet
  CREATE TABLE gateway_usage_hourly (...),
]
```

The `ALTER TABLE gateway_usage_hourly` raises `UndefinedTable`. **psycopg2 marks the entire transaction aborted** on any error; subsequent `cur.execute(ddl)` calls in the same transaction silently no-op until rollback. The per-statement `try/except` swallowed the warning print but did not rollback, so the trailing `CREATE TABLE gateway_usage_hourly` and the prior `CREATE TABLE gateway_usage_daily`'s commit went through partially — and on a fresh database, *neither* table exists when the connection commits.

The observability section of the same file uses PostgreSQL `SAVEPOINT` per DDL, which contains failures correctly. The gateway section did not.

**Fix.** `workflows/02_sync_to_lakebase.py` (Phase 6 DDL block):
1. Reordered DDLs so all `CREATE TABLE`s come before any `CREATE INDEX` / `ALTER TABLE`.
2. Switched from one shared transaction to one transaction per DDL: each statement runs in its own `cursor()` context with its own `commit()`, and a failure does `rollback()` and continues. This isolates the "ALTER on a not-yet-existing table on the very first run" case from poisoning sibling DDLs.

**Verification step.** After the fix lands and the workflow re-runs, confirm tables exist with the in-workspace inspection notebook used during diagnosis (it queries `pg_tables` for the `control_plane` database via SDK-issued credentials):

```python
# minimal inspector — submits as a notebook_task on serverless
from databricks.sdk import WorkspaceClient
import psycopg2
w = WorkspaceClient()
me = w.current_user.me().user_name
token = w.database.generate_database_credential(instance_names=["ai-control-plane-db"]).token
conn = psycopg2.connect(host="<lakebase-dns>", dbname="control_plane",
                        user=me, password=token, sslmode="require")
cur = conn.cursor()
cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' "
            "AND tablename LIKE 'gateway%'")
print(cur.fetchall())
```

Expected: `[('gateway_inference_logs',), ('gateway_usage_daily',), ('gateway_usage_hourly',)]`.

**Documentation gap:** No documentation gap — this is a code bug. Captured in this RCA so a future contributor sees the pattern (interleaved CREATE/ALTER + per-statement try/except + shared transaction = silent partial DDL).

---

## Identity / permission audit (admin profile)

For reference, what the privileged admin account had on the sandbox workspace as of 2026-06-06:

| Layer | Status | Probe |
|---|---|---|
| Workspace admin | Yes — member of `admins` group | `current-user me` |
| Account admin | Yes — `is_account_admin=True` per app OBO log | App startup log |
| UC `SELECT` on `system.billing.usage` | Yes (43M rows in last 7d) | `/api/2.0/sql/statements` |
| UC `SELECT` on `system.serving.endpoint_usage` | Yes (71M rows / 7d) | same |
| UC `SELECT` on `system.ai_gateway.usage` | Yes (9M rows / 7d) | same |
| UC `SELECT` on `system.access.audit` | Yes (19B rows / 7d) | same |
| Lakebase `databricks_superuser` membership | Yes — confirmed via `pg_auth_members` | in-workspace inspector |
| SQL warehouse (Starter) | `CAN_MANAGE` via `admins` group | `warehouses get-permissions` |
| Account-level API (`/api/2.0/accounts/...`) | **No** — workspace OAuth profile returns `400 Unable to load OAuth Config` | direct curl |

**Conclusion of the permission probe.** The "not all views populated" symptom that triggered this audit was *not* a permissions issue. Every UC system-table grant is in place. The actual bug is the Phase 6 DDL transaction pattern in `02_sync_to_lakebase.py` (item 4 above). The lack of account-level API access via the workspace-scoped OAuth profile is expected and only matters for operations like `account workspaces list` (not used by the deployed app at runtime).

---

## Open follow-ups

- [x] Re-deploy and verify gateway tables populate after the Phase 6 fix (done 2026-06-06; 21,303 daily / 2,606 hourly rows).
- [x] Roll the reusable changes into a PR ([#18](https://github.com/databricks-solutions/agent-control-plane/pull/18)).
- [x] Add a Lakebase smoke check task to the discovery workflow so this regression class fails loud instead of silently leaving the app empty (`workflows/10_smoke_check_lakebase.py`, wired as `smoke_check_lakebase` task in `databricks.yml`, depends on `sync_to_lakebase`).
- [x] Author CI/CD deployment-pattern ADR for the production rollout ([2026-06-06-cicd-deployment-pattern.md](../decisions/2026-06-06-cicd-deployment-pattern.md)).
- [x] Root-cause and fix the empty `billing_user_cost_daily` / `billing_product_daily` tables (item 5 — missing SELECT on `system.billing.list_prices` + silent-failure path in `09_discover_billing.py`).
- [x] Root-cause and fix the empty Tools page (item 6 — `tool_registry` table never created because the app SP doesn't have Lakebase DDL privs and the daemon-thread startup hook ran past its timeout; refresh helper swallowed all errors).
- [x] Document the `databricks bundle deploy` parameter-regression footgun (item 7 — placeholder defaults silently overwrite a working job; bit me while verifying item 6).
- [ ] Update `docs/installation.md` to reference items 1, 2, 3, the `system.billing` schema-level grant from item 5, and the `--var=` requirements from item 7.

### New findings from the smoke check (2026-06-06)

The first end-to-end smoke run on the sandbox surfaced **two `REQUIRED` tables that are empty post-sync** despite their upstream feeds being populated. Both have now been root-caused and fixed.

## 5. Discovery silently writes 0 rows to all `system.billing.usage`-derived tables when the runtime identity lacks SELECT on `system.billing.list_prices`

**Symptom.** After the gateway DDL fix in item 4 lands and the workflow re-runs successfully (`result=SUCCESS`), three Lakebase tables are still empty:

- `billing_serving_daily` (0 rows in Lakebase) — drives Cost Overview by endpoint × SKU.
- `billing_product_daily` (0 rows in Lakebase) — drives the All Products breakdown.
- `billing_user_cost_daily` (0 rows in Lakebase) — drives per-user Endpoint Costs.

The other two billing tables (`billing_token_daily`, `billing_user_endpoint_daily`) populate correctly. Token usage IS rendering on the Governance page; only the cost-related sections are blank.

**Root cause.** `workflows/09_discover_billing.py` issues five SQL statements via `_execute_sql`. Three of them (queries 1, 3, 5) `LEFT JOIN system.billing.list_prices` to compute `total_cost_usd`. The deployer / workflow run-as identity had `SELECT ON SCHEMA system.billing` for *some* tables (granted ad-hoc) but **not** on `list_prices`. The JOIN fails with:

```
[INSUFFICIENT_PERMISSIONS] User does not have SELECT on Table 'system.billing.list_prices'. SQLSTATE: 42501
```

The `_execute_sql` helper in `09_discover_billing.py` swallowed the failure:

```python
if status != "SUCCEEDED":
    err = resp.get("status", {}).get("error", {})
    print(f"  SQL {status}: {err.get('message', '')[:300]}")
    return []   # <-- silent zero-row path
```

That `return []` propagated as `serving_rows = 0`, which the writer happily persisted as a 0-row Delta table, which the sync task happily synced to a 0-row Lakebase table. The discovery task reports `result_state: SUCCESS` because the helper caught the API error before the notebook could fail.

Critically, queries 2 and 4 (`token`, `user_endpoint`) hit `system.serving.endpoint_usage` only — no `list_prices` join, no permission gap, so they populated correctly. That's why token usage was showing while cost overviews were not.

**Fix (two parts):**

1. **Permission grant.** Granted `SELECT ON SCHEMA system.billing` to the discovery identity:
   ```sql
   GRANT SELECT ON SCHEMA system.billing TO `<discovery-identity>`
   ```
   Verified via `SHOW GRANTS ON TABLE system.billing.list_prices`. After the grant, the same JOIN over the last 7 days returns 771,370 rows.

2. **Make the silent path loud.** `workflows/09_discover_billing.py:170` — `_execute_sql` now `raise`s a `RuntimeError` with the API error code and message when the statement does not succeed, instead of returning `[]`. This converts future permission gaps from "0 rows in production with green CI" to "task FAILED with SQLSTATE in the error message". The smoke check would have caught this regardless, but failing the discovery task at the source is closer to the bug and gives a more actionable message than "EMPTY required table" three steps downstream.

**Verification.** After the grant + helper fix re-deployed and the workflow re-ran (run `415862563272753`):

| Table | Discovery rows | Lakebase rows |
|---|---:|---:|
| `billing_serving_daily` | 15,040 | 15,040 |
| `billing_product_daily` | 9,806 | 9,806 |
| `billing_user_cost_daily` | 4,472 | 4,472 |
| `billing_token_daily` | 3,932 | 3,932 |
| `billing_user_endpoint_daily` | 21,674 | 21,674 |

**Documentation gap.** `docs/installation.md` should list `SELECT ON SCHEMA system.billing` (not just `system.billing.usage`) as the discovery-identity grant — this is the difference between "Governance has token counts" and "Governance has actual cost numbers". Also worth noting: the `LEFT JOIN system.billing.list_prices` predicate means cost discovery is *all-or-nothing* on the join — partial grants produce silent zeros, not partial data.

---

## 6. Tools page renders empty / 500s when app SP lacks Lakebase DDL privileges

**Symptom.** After the deployment is otherwise healthy (Governance tabs working, Gateway tabs working, agents discovered), the **Tools** section in the app is broken across all four tabs:

- `/api/v1/tools/overview` — returns HTTP 500 `Internal Server Error`
- `/api/v1/tools/mcp-servers` — returns HTTP 500
- `/api/v1/tools/functions` — returns HTTP 500
- `/api/v1/tools/usage` — returns 200 with `[]`

The frontend Tools tabs all render empty.

The `/api/v1/tools/sync` endpoint (the only POST route on this router) returns HTTP 200 `{"status":"ok","message":"Tools refresh complete"}` regardless — it lies.

**Root cause.** Three layered failures masked each other:

1. **The `tool_registry` Lakebase table was never created.** App startup runs `_init_tools` in a daemon thread that calls `ensure_tools_tables()` (which `CREATE TABLE IF NOT EXISTS tool_registry ...`) followed by `maybe_refresh_async()`. The whole `_run_all_inits` fan-out is wrapped in `t.join(timeout=120)`, after which the server starts regardless. On this sandbox the daemon thread either crashed before `_init_tools` ran, ran past the 120 s budget, or hit a Lakebase auth issue under load — in all three cases the server boots without `tool_registry`.

2. **`ensure_tools_tables()` swallowed real DDL failures.** Each statement was wrapped in `try/except: logger.warning(...)`, so even when the DDL truly failed (e.g. SP doesn't have `CREATE` on the schema), the only signal was a warning line that was buried among others and rolled out of the app's short log retention.

3. **`refresh_tools()` swallowed the entire body.** The function was wrapped in `try/except Exception: logger.warning("Tools refresh failed: %s", exc)`. When `_upsert_tools` or `_discover_uc_functions` blew up against the missing table, the warning printed once and the lock released — and `/api/v1/tools/sync` happily returned 200. There was no traceback in logs and no error in the API response: a perfect silent break.

The dashboard route `/api/v1/tools/overview` raised the underlying `psycopg2.errors.UndefinedTable: relation "tool_registry" does not exist` to FastAPI's default 500 handler. That was the only externally visible signal that anything was wrong, and it took five minutes of `grep -B3 -A3 "GET /api/v1/tools/overview HTTP/1.1\\\" 500"` against `databricks apps logs` to find the underlying cause.

The same architectural issue exists for `request_logs` — it's another app-managed table created by the request-audit middleware, also fails silently if DDL isn't possible.

**Fix (three parts):**

1. **Move app-managed table DDL into the workflow.** Added Phase 7 to `workflows/02_sync_to_lakebase.py` that creates `tool_registry` and `request_logs` from the workflow run-as identity (which is `databricks_superuser` on the Lakebase PG instance). The app no longer needs DDL privileges to function — only `SELECT/INSERT/UPDATE/DELETE` on existing tables. This parallels the pattern for `discovered_agents`, `gateway_usage_*`, and 20+ other tables already created here.

2. **Make `tools_service._refresh_tools` self-heal and stop swallowing errors.**
   - `refresh_tools()` now calls `ensure_tools_tables()` at the top of its body, so if the workflow hasn't run yet (fresh deploy) the read path can recover on its own.
   - The catch-all in `refresh_tools()` now uses `logger.exception(...)` instead of `logger.warning(...)`, giving a full traceback when something fails.
   - Per-statement exception handlers in `ensure_tools_tables()` now use `logger.exception(...)` with the offending DDL statement included.

3. **Add `tool_registry` and `request_logs` to the smoke check.** Both are now in the `EXPECTED` bucket of `workflows/10_smoke_check_lakebase.py` — existence is asserted on every workflow run; 0 rows is a WARN not a failure (legitimate before any user activity), but the table being missing now flips `result_state` to FAILED with an actionable message.

**Why "EXPECTED" rather than "REQUIRED" for these.** Both tables are populated by user/app activity (the SP discovers MCP servers + UC functions on first request; request_logs accumulates as users interact with the app). On a freshly deployed sandbox with no activity yet, 0 rows is correct. The failure mode we're guarding against is "table missing entirely", which now correctly fires a smoke-check error instead of a 500 in the dashboard.

**Verification.** After the workflow re-ran with the Phase 7 patch (run `881046971716582`):

```sh
$ curl -sS -H "Authorization: Bearer $TOKEN" \
    https://<app-host>.databricksapps.com/api/v1/tools/overview
{"total_tools":3,"mcp_servers":3,"uc_functions":0,"managed_count":3,
 "custom_app_count":0,"is_refreshing":false,"last_refreshed":"2026-06-06T..."}
```

| Tab | Before | After |
|---|---|---|
| Overview     | HTTP 500 (UndefinedTable: tool_registry) | 3 MCP servers, 0 UC functions |
| MCP Servers  | HTTP 500                                  | 3 entries: Atlassian / Google Drive / SharePoint (system-managed) |
| UC Functions | HTTP 500                                  | Empty (legitimate — no functions in `ai_control_plane.control_plane`; SP also only sees 6 catalogs) |
| Usage        | `[]` (worked because no Lakebase touch)   | `[]` (legitimate — no MLflow traces with TOOL/FUNCTION spans yet) |

The remaining "empty" UC Functions and Usage tabs are correct given the current sandbox state — there are zero UC functions in the project's catalog, and no MLflow traces with tool spans. Both will populate naturally as agents using UC function tools are deployed.

**Documentation gap.** `docs/installation.md` does not currently call out that the app SP needs read access to `ai_control_plane.control_plane`'s UC functions (and any other catalogs the deployer wants surfaced in the Tools tab). This is a workspace-by-workspace concern (catalog grants are deployer-discretion); should be flagged in the Tools page as "no functions found — grant `USE CATALOG` to the app SP" rather than silently empty.

---

## 7. `databricks bundle deploy` without explicit `--var` flags actively breaks a working job

**Symptom.** While verifying the item-6 fix, ran `databricks bundle deploy --target dev -p <profile>` from the `workflows/` directory to register the newly-added `smoke_check_lakebase` task. The CLI reported `Deployment complete!`. The next workflow run then failed every task with:

```
[PARSE_SYNTAX_ERROR] Syntax error at or near '<'. SQLSTATE: 42601
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
                                          ^^^^^^^^^^^^^^^^
```

Inspecting the deployed job revealed `base_parameters` had been overwritten with the literal placeholder strings:

```json
"base_parameters": {
  "billing_retention_days": "90",
  "catalog": "<your-catalog>",
  "schema": "control_plane",
  "warehouse_id": "<your-warehouse-id>"
}
```

**Root cause.** `workflows/databricks.yml` defines the `dev` target with template-style placeholder defaults:

```yaml
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: <your-catalog>                   # e.g. main, my_catalog
      lakebase_dns: <your-lakebase-dns>         # e.g. ep-xxxx.database.cloud.databricks.com
      warehouse_id: <your-warehouse-id>         # SQL warehouse ID
      account_id: <your-account-id>             # Databricks account ID
```

These are not Bundle interpolation tokens; they are literal string defaults. A subsequent `databricks bundle deploy --target dev` without `--var=` overrides happily writes those literal values into the running job's parameters. The CLI does not warn that you are about to overwrite a working configuration with a placeholder.

The previous (working) deploy was done by someone who knew to pass `--var="catalog=ai_control_plane" --var="warehouse_id=..."`. There is no record of those values in the repo and no `.databricks-bundle.local.yml` overlay — the parameters were ephemeral to that one CLI invocation.

**Fix (immediate).** Re-deploy with all variables supplied:

```sh
cd workflows
databricks bundle deploy --target dev -p <profile> \
  --var="catalog=<your-catalog>" \
  --var="schema=control_plane" \
  --var="lakebase_dns=<your-lakebase-dns>" \
  --var="lakebase_endpoint_path=" \
  --var="lakebase_instance=<your-lakebase-instance>" \
  --var="warehouse_id=<your-warehouse-id>" \
  --var="account_id=<your-account-id>"
```

Confirmed end-to-end (run `430345618113642`): all 11 tasks SUCCESS, smoke check passed with 12/12 REQUIRED tables OK and `tool_registry` + `request_logs` present in the EXPECTED bucket.

**Fix (durable).** Two complementary changes worth doing in a follow-up PR:

1. Replace the `<your-...>` placeholder defaults with values that *fail loud* if not overridden — e.g. `default: __UNSET__` plus a CI-time check in `deploy.sh` / the GitHub Actions workflow that grep-rejects any value matching `__UNSET__` after rendering. The current behaviour silently overwrites prod-like configurations.

2. Persist target-specific values to a `.databricks-bundle.<target>.local.yml` (gitignored) overlay loaded automatically by the CLI, so a `databricks bundle deploy` from any workstation without explicit `--var=` flags is either a no-op (correctly resolved from the overlay) or fails fast on missing overlay.

**Documentation gap.** `docs/installation.md` should explicitly enumerate the required `--var=` flags for `databricks bundle deploy`, OR direct deployers to author a local overlay file with the values pinned. Right now a fresh deployer who follows the README steps will succeed *only if* they happen to pass the right flags — and a successful re-deploy by someone who has those flags is actively destructive to a job configured by anyone who didn't.
