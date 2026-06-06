# ADR: CI/CD deployment pattern for Ecolab production rollout

**Status:** Proposed (2026-06-06)
**Related:**
- [Findings log: deploy non-trivial fixes](../rca/2026-06-06-deploy-non-trivial-fixes.md) — the four issues that motivated the smoke gate.
- [PR #18 — gateway DDL transaction fix](https://github.com/databricks-solutions/agent-control-plane/pull/18) — introduces the smoke check this ADR depends on.

## Context

We just landed an Ecolab sandbox deployment of `agent-control-plane`. The deployment surfaced a regression class that the existing pipeline does not catch: the `02_sync_to_lakebase` Phase 6 transaction bug created Delta tables successfully, reported `result=SUCCESS`, but silently failed to materialise `gateway_usage_daily` / `gateway_usage_hourly` in Lakebase. The deployed app then logged a continuous stream of `UndefinedTable` errors and rendered the Gateway page empty. The job result and the dashboard state disagreed.

The fix (PR #18) now lives on a fork because we don't have push access to `databricks-solutions/agent-control-plane`. To roll the same stack into Ecolab production responsibly, we want every future update to clear two gates before reaching prod: build-time tests of the code, and post-deploy smoke checks of the actual data plane (Lakebase tables exist with rows, app endpoints return non-empty payloads). This ADR defines that deployment pattern.

## Decision

**Adopt a three-environment promotion pipeline (sandbox → stage → prod) with a Lakebase smoke gate as the post-deploy quality bar in every environment.** Drive promotion from a CI/CD platform (proposal: GitHub Actions) using Databricks Asset Bundles and the Databricks CLI.

### Environment matrix

| Environment | Workspace | Trigger | Promotion criteria |
|---|---|---|---|
| `sandbox` | Ecolab sandbox `adb-6239133969168510` | Every push to `main` of our prod-tracking fork | Smoke task passes; manual sign-off optional |
| `stage` | A second Ecolab workspace (TBD — request from platform team) | Manual dispatch after sandbox smoke is green for ≥24h | Smoke + Playwright regression both pass |
| `prod` | Ecolab production workspace (TBD — request from platform team) | Manual dispatch with two-person approval | Stage has been green for ≥24h; smoke + regression pass against prod after deploy |

We do not propose merging to `databricks-solutions/agent-control-plane`'s `main` as the prod trigger. The upstream is shared with other customers. Ecolab tracks its own internal repo (or fork) and pulls in upstream `main` periodically.

### Pipeline shape

For each environment:

```
[lint+unit]  →  [bundle deploy --target <env>]  →  [app deploy]
       ↓
[run discovery workflow once]
       ↓
[smoke_check_lakebase task]  ←  the gate
       ↓
[Playwright regression — stage/prod only]
       ↓
[mark deployment green; promote]
```

Key choice: **the smoke check runs as a task inside the discovery workflow, not as a separate CI step.** This keeps the regression test next to the data it checks (same identity, same network, same Lakebase credentials), which means the smoke check actually exercises the production data plane rather than a CI-only proxy of it. The CI job's role is to *trigger* the workflow run, *poll* for completion, and *fail* if the smoke task fails. The bundle config wires `smoke_check_lakebase` to depend on `sync_to_lakebase`, so the existing Databricks job machinery handles ordering, retries, and email notifications.

### Smoke gate spec

`workflows/10_smoke_check_lakebase.py` (introduced alongside this ADR) categorises every Lakebase table the app reads from into three buckets:

| Bucket | Behaviour |
|---|---|
| `REQUIRED` | Must exist AND have ≥1 row. Failure raises and the workflow goes red. |
| `EXPECTED` | Must exist. 0 rows is allowed (legitimate in quiet workspaces) but logged at WARN. |
| `OPTIONAL` | App-managed (created lazily on user action). Existence not asserted. |

The bucketing is a contract derived from the actual `FROM <table>` references in `control-plane-app/backend/services/`. When a service starts reading from a new table, the smoke contract must be updated alongside the service change — that pairing is the unit of review.

This catches three regression classes that the existing job result does not:
1. **Silent CREATE skips** like the gateway DDL transaction bug.
2. **Sync-side query that returns 0 rows** for what should be a populated table (e.g., a cron schedule that doesn't trigger, a role missing SELECT on a system table).
3. **Schema drift** — a column rename in the sync code that produces an empty INSERT clause.

### Why GitHub Actions specifically

| Option | Pro | Con |
|---|---|---|
| **GitHub Actions** | Already authenticated for the fork; community-supported `databricks/setup-cli` action; secrets handled by GH; easy fork→main promotion gate via PR | Requires a GH-side OAuth secret per target workspace |
| Azure DevOps Pipelines | Aligns with Ecolab's existing ADO-centric tooling | Crosses repo boundaries (GH for source, ADO for CI); harder to configure with a public-fork PR model |
| Databricks Workflows-only | Self-contained, no external CI | Can't run `bundle deploy` from inside Databricks (chicken-and-egg); poor model for blocking promotion on cross-environment results |
| Run smoke ad-hoc on laptop | Zero infra | Easy to skip; no audit trail of what shipped where; doesn't scale beyond one engineer |

GitHub Actions is the path of least resistance for the public-fork model and gives us a documented audit trail.

### Secrets layout

For each environment (sandbox/stage/prod), GH repo secrets:
- `DATABRICKS_HOST_<ENV>` — workspace URL
- `DATABRICKS_CLIENT_ID_<ENV>` — service-principal applicationId
- `DATABRICKS_CLIENT_SECRET_<ENV>` — service-principal OAuth secret
- `DATABRICKS_BUNDLE_VAR_*` — workspace-specific bundle variables (lakebase_dns, instance, warehouse_id, account_id)

The deploying service principal needs:
- Workspace admin (deploy app + workflow)
- Metastore admin / `CREATE CATALOG` on metastore (or scope catalogs to a metastore-managed parent the SP already owns)
- `CAN_USE` on the SQL warehouse the workflow uses
- `databricks_superuser` role in Lakebase (created automatically when the SP first connects via `generate_database_credential`)

### Playwright regression in stage/prod

The existing `tests/governance-workspace-filter.spec.ts` is the seed. Extend it to walk all primary tabs (Governance, Observability, Gateway, Knowledge Bases, User Analytics, Agents) and assert that each has at least one populated chart/table. Run headless with a service-principal-issued OAuth token for the test user. Skip in sandbox to keep iteration fast; require in stage and prod.

## Alternatives considered

**Run the smoke check as a CI-side script that hits the deployed app's `/api/v1/*/page-data` endpoints.** This would catch *frontend* rendering issues that pure Lakebase checks miss. Rejected as the *primary* gate because (a) endpoint behaviour depends on the signed-in user's UC grants, which makes "is this empty because of permissions or because of missing data?" hard to answer from CI; (b) the SSO loop is brittle in CI; (c) it's still on the table as a *secondary* gate (Playwright in stage/prod fills this role).

**Promote via merge to upstream `main`.** Rejected — upstream is multi-tenant; we'd be coupling Ecolab's release cadence to the upstream maintainers'. Pull from upstream weekly into the fork's `main`; promote from there.

**One smoke notebook per page (gateway, governance, observability, …).** Rejected — over-engineered for the current surface area. One contract file in one notebook is auditable in one diff. We can split later if it grows past a screen.

## Consequences

**Positive:**
- Future regressions of the silent-empty-table class fail loud at the workflow level, before users see empty screens.
- A single artefact (the smoke notebook + its contract) is the source of truth for "what does the app expect to find in Lakebase."
- Promotion criteria are written down; releasing to prod stops being an "is it working?" judgement call and becomes a checklist.
- The bundle stays the source of truth for both *what* deploys (job + app) and *what counts as a successful deploy* (smoke task green).

**Negative:**
- Adds ~30s to every discovery run (smoke task overhead).
- Smoke contract has to stay in sync with the backend's `FROM <table>` set. A new dashboard tab that queries a new table needs the contract updated in the same PR — easy to forget.
- Stage/prod environments don't exist yet; this ADR depends on the platform team provisioning them.
- The CI-side OAuth secrets need rotation discipline (proposal: 90-day rotation, tracked in `docs/operations.md` once written).

**Code-level impact:**
- New file: `workflows/10_smoke_check_lakebase.py` (added in PR #18 follow-up).
- New task in `workflows/databricks.yml`: `smoke_check_lakebase`, depending on `sync_to_lakebase`.
- New: `.github/workflows/deploy-sandbox.yml`, `.github/workflows/deploy-stage.yml`, `.github/workflows/deploy-prod.yml` (sandbox first; stage/prod after platform provisions the workspaces).
- New: `docs/operations.md` covering rotation, SP onboarding, runbook for smoke failures (separate PR).

## Trigger conditions for revisiting

- The smoke contract drifts from reality more than once. Replace the manual contract with introspection (e.g., a generated list from the backend's SQL strings).
- Stage/prod workspaces materialise. Update the environment matrix with their actual workspace IDs and reassess the SP grant requirements.
- Frontend regressions slip through the backend smoke gate. Promote Playwright from "stage/prod only" to the sandbox path too.
