# Findings log: Non-trivial fixes during initial Ecolab sandbox deployment

**Date:** 2026-06-04 → 2026-06-06
**Status:** Open log — items listed are deployment-blocking issues that exceeded the README/`docs/installation.md` instructions and required code or process changes.
**Workspace:** Ecolab sandbox (`adb-6239133969168510.10.azuredatabricks.net`, account `7edf83f2-6ac4-4461-94ed-48f0e96724b1`)

## Summary

The `agent-control-plane` README + `docs/installation.md` describe the happy-path setup: workspace admin runs `databricks bundle deploy`, runs `deploy.sh`, and the app + workflow start serving. In the Ecolab sandbox this path is blocked at four points by environmental constraints not covered in the docs. Each blocker was resolved with a code or process change captured below so the next deployer (us or someone else) does not have to re-discover them.

---

## 1. Workspace-admin alone is not sufficient — metastore admin / `CREATE CATALOG` is required

**Symptom.** `databricks bundle deploy --target dev` fails on the first task that creates the Unity Catalog catalog:

```
PERMISSION_DENIED: User does not have CREATE CATALOG on Metastore 'primary'
```

The user (`thijs.hakkenberg@ecolab.com`) is a workspace `admins` group member.

**Root cause.** Workspace admin is a *workspace-scoped* role; UC metastore admin is *account-level* and orthogonal. The README assumes the deployer has both but does not say so explicitly.

**Fix.** Request metastore-admin elevation on a privileged identity (in our case the `a-hakketh@ecolab.com` admin account). Confirm with:

```sh
databricks api post /api/2.0/sql/statements -p <profile> --json \
  '{"warehouse_id":"<wh>","statement":"SELECT current_metastore() AS m, is_account_group_member(\"account admins\") AS is_acct_admin"}'
```

Then deploy with `-p <admin-profile>`. **Documentation gap:** `docs/installation.md` should list `CREATE CATALOG ON METASTORE` (or metastore-admin) as a hard prerequisite alongside workspace-admin.

---

## 2. SSO session reuse silently authenticates the wrong identity

**Symptom.** `databricks auth login -p a-hakketh --host <url>` returned "successfully authenticated" without prompting for password — it had silently reused the existing Microsoft / Ecolab SSO browser session for `thijs.hakkenberg@ecolab.com`. Every subsequent CLI call ran as `thijs`, *not* `a-hakketh`, despite the profile name.

**Root cause.** The Databricks browser-OAuth flow honours an existing IdP cookie. If the browser is already signed in to Microsoft 365 with another work account, the OIDC step doesn't re-prompt.

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
Public access is not allowed for workspace 6239133969168510.
```

This persists even on the Ecolab corporate VPN. The Lakebase endpoint resolves to a public IP; the rejection is server-side, based on a workspace-attached Network Policy that disallows public ingress to Lakebase.

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

## Identity / permission audit (a-hakketh)

For reference, what `a-hakketh@ecolab.com` actually has in this workspace as of 2026-06-06:

| Layer | Status | Probe |
|---|---|---|
| Workspace admin | Yes — member of `admins` group | `current-user me` |
| Account admin | Yes — `is_account_admin=True` per app OBO log | App startup log |
| UC `SELECT` on `system.billing.usage` | Yes (43M rows in last 7d) | `/api/2.0/sql/statements` |
| UC `SELECT` on `system.serving.endpoint_usage` | Yes (71M rows / 7d) | same |
| UC `SELECT` on `system.ai_gateway.usage` | Yes (9M rows / 7d) | same |
| UC `SELECT` on `system.access.audit` | Yes (19B rows / 7d) | same |
| Lakebase `databricks_superuser` membership | Yes — confirmed via `pg_auth_members` | in-workspace inspector |
| SQL warehouse `e372b03bb75f880e` (Starter) | `CAN_MANAGE` via `admins` group | `warehouses get-permissions` |
| Account-level API (`/api/2.0/accounts/...`) | **No** — workspace OAuth profile returns `400 Unable to load OAuth Config` | direct curl |

**Conclusion of the permission probe.** The "not all views populated" symptom that triggered this audit was *not* a permissions issue. Every UC system-table grant is in place. The actual bug is the Phase 6 DDL transaction pattern in `02_sync_to_lakebase.py` (item 4 above). The lack of account-level API access via the workspace-scoped OAuth profile is expected and only matters for operations like `account workspaces list` (not used by the deployed app at runtime).

---

## Open follow-ups

- [x] Re-deploy and verify gateway tables populate after the Phase 6 fix (done 2026-06-06; 21,303 daily / 2,606 hourly rows).
- [x] Roll the reusable changes into a PR ([#18](https://github.com/databricks-solutions/agent-control-plane/pull/18)).
- [x] Add a Lakebase smoke check task to the discovery workflow so this regression class fails loud instead of silently leaving the app empty (`workflows/10_smoke_check_lakebase.py`, wired as `smoke_check_lakebase` task in `databricks.yml`, depends on `sync_to_lakebase`).
- [x] Author CI/CD deployment-pattern ADR for the production rollout ([2026-06-06-cicd-deployment-pattern.md](../decisions/2026-06-06-cicd-deployment-pattern.md)).
- [x] Root-cause and fix the empty `billing_user_cost_daily` / `billing_product_daily` tables (item 5 — missing SELECT on `system.billing.list_prices` + silent-failure path in `09_discover_billing.py`).
- [ ] Update `docs/installation.md` to reference items 1, 2, 3, and the `system.billing` schema-level grant from item 5.

### New findings from the smoke check (2026-06-06)

The first end-to-end smoke run on the Ecolab sandbox surfaced **two `REQUIRED` tables that are empty post-sync** despite their upstream feeds being populated. Both have now been root-caused and fixed.

## 5. Discovery silently writes 0 rows to all `system.billing.usage`-derived tables when the runtime identity lacks SELECT on `system.billing.list_prices`

**Symptom.** After the gateway DDL fix in item 4 lands and the workflow re-runs successfully (`result=SUCCESS`), three Lakebase tables are still empty:

- `billing_serving_daily` (0 rows in Lakebase) — drives Cost Overview by endpoint × SKU.
- `billing_product_daily` (0 rows in Lakebase) — drives the All Products breakdown.
- `billing_user_cost_daily` (0 rows in Lakebase) — drives per-user Endpoint Costs.

The other two billing tables (`billing_token_daily`, `billing_user_endpoint_daily`) populate correctly. Token usage IS rendering on the Governance page; only the cost-related sections are blank.

**Root cause.** `workflows/09_discover_billing.py` issues five SQL statements via `_execute_sql`. Three of them (queries 1, 3, 5) `LEFT JOIN system.billing.list_prices` to compute `total_cost_usd`. The deployer / workflow run-as identity (`a-hakketh@ecolab.com`) had `SELECT ON SCHEMA system.billing` for *some* tables (granted ad-hoc) but **not** on `list_prices`. The JOIN fails with:

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
   GRANT SELECT ON SCHEMA system.billing TO `a-hakketh@ecolab.com`
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
