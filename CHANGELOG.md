# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.3] - 2026-08-26

This release grows the **Unity Gateway** (v3) story and moves the app's read
layer onto cached Lakebase tables where it was still doing live per-load calls.
Two threads drove it: give native platform primitives (budgets, endpoints, UC
model services) fleet-wide read-only visibility the per-object UIs don't, and
stop the remaining Observability drill-downs from querying live on every click.

### Added
- **Budget status + consumption** (F5). Fleet-wide inventory of native account
  budgets — cap thresholds, whether each *enforces* (BLOCK_USAGE) vs only *alerts*,
  its filter, and AI-relevance — plus month-to-date spend vs cap (%-used), sortable
  and paginated. Read-only: enforcement stays platform-side. New discovery task
  `workflows/13_discover_budgets.py` → `uag_budget_status`; backend
  `get_uag_budget_status()` and `GET /gateway/uag-budget-status`, degrading to empty
  without account credentials.
- **Account-wide endpoint inventory.** Read-only served-entity fleet view across
  every workspace in the metastore from `system.serving.served_entities` — the view
  the per-workspace serving API can't give. New `workflows/14_discover_endpoint_inventory.py`
  → `serving_endpoints_inventory`, surfaced on the Unity Gateway tab.
- **v3 Unity Gateway UC model services** (read-only). Metastore-wide list via the UC
  REST API (workflow run-as a metastore admin — the list endpoint the app's OBO/SP
  identities can't reach) plus live per-service grant reads. New
  `workflows/15_discover_model_services.py` → `model_services_inventory`.
- **Model Registry version cache.** Every version per registered model is cached to
  Lakebase (`mlflow_model_versions`, from `04_discover_observability`) so the model
  drill-down reads cache-first instead of a live REST search on each expand;
  `GET /mlflow/models/{name}/versions` falls back to live only on a cold cache.
- **MLflow trace-detail write-through.** SP-fetched trace details are persisted to
  `observability_trace_details` on a cache miss, so repeat opens read from Lakebase.

### Changed
- **AI Gateway page reorganized and rebranded "Unity Gateway"**; Beta and version
  labels removed (GA). v1 per-endpoint views consolidated into a **"Legacy AI
  Gateway"** tab; Budgets moved next to Unity Gateway; Requests/Tokens-per-day charts
  moved directly under the KPI cards.
- **Tools → MCP Usage** is now sortable and paginated.
- **Budget list is uncapped.** `GET /gateway/uag-budget-status` returns the full
  budget list (totals already aggregate the whole table; the frontend paginates),
  replacing the prior 500-row cap.

### Removed
- Pruned dead tabs: Observability **Gateway Requests** and **Quality & Evals**, and
  Governance **Guardrails** (plus their now-unused panels and imports).

### Fixed
- `10_smoke_check_lakebase` `authenticate()` bug that crashed every run.
- Budget month-to-date total now includes NULL-workspace (account-level) usage
  instead of under-counting it.
- Model-versions discovery orders by `last_updated_timestamp DESC` so the per-model
  cap keeps the newest versions (with a retry-without-order_by fallback so an
  unsupported order_by never silently zeroes a model); single quotes in the name
  filter are escaped; and a transient full-enumeration failure no longer overwrites
  the good `mlflow_model_versions` table with empty.

### Security
- **Trace-detail write-through is scoped to SP-authority fetches only.**
  `observability_trace_details` is a shared cache served to every app user with no
  per-user authorization recheck. Persisting a user-scoped (OBO) fetch could expose a
  trace to users who lack access to it, so write-through now fires only when the fetch
  used the app service principal — keeping the shared cache ⊆ SP-visible data, the
  invariant the discovery workflow already maintains. Cross-workspace trace detail
  (always OBO) stays live-per-request.

## [0.1.2] - 2026-06-02

This release builds the Unity AI Gateway **v2** story and steps the app back from
enforcement it shouldn't own. Two design decisions drove it (detail below): keep
v2 on its own tab rather than replacing the existing gateway view, and remove
in-app budgeting in favor of the platform's native control.

### Added
- **Unity AI Gateway v2 (Beta) tab.** A dedicated tab surfacing data that exists
  *only* in `system.ai_gateway.usage` (v2-routed traffic, ~20-min fresh): cached
  tokens, p50/p95 latency, p95 time-to-first-byte, and a per-endpoint table with a
  freshness ("as of") badge and a scope tooltip. The `(Beta)` marker is a label
  pill, not plain text. New discovery task `workflows/11_discover_ai_gateway_usage.py`
  → Delta → Lakebase (`uag_usage_summary`); backend `get_uag_v2_usage()` and
  `GET /gateway/uag-v2-usage`, both degrading gracefully to empty when the
  (account-scoped) system table is unreadable or unsynced.
- **v2 usage breakdowns** on that tab — three additive cuts of
  `system.ai_gateway.usage`: **Agent vs. Human** (`requester_type`), **Top Models**
  (`destination_model`), and **By API Type** (`api_type`), each rendered as a
  proportion-bar card. New `uag_usage_breakdown` Delta table + Lakebase mirror; the
  backend response gains a `breakdowns` object keyed by dimension.
- **Actual per-user cost** via Unity AI Gateway v2 attribution, from
  `system.billing.usage` joined to v2 usage (replaces estimated splits where v2
  data exists).
- **Rate-limit (429) visibility** per endpoint and per user, sourced from
  `system.serving.endpoint_usage`.

### Changed
- The AI Gateway top KPI row (Total Endpoints / Ready / Gateway Enabled /
  Requests 24h / Unique Users 24h / Error Rate 24h) now renders **only on the
  Overview tab**, so the Metrics, Permissions, Rate Limits, and v2 tabs aren't
  fronted by Overview-scoped numbers.

### Removed
- **In-app per-user / per-group token budgeting** (`FEATURE_BUDGETS_ENABLED`, the
  Budgets tab, `/gateway/budgets*` routes, `budgets_service.py`, and the related
  config). Unity AI Gateway v2 ships native budgets / spend controls; the app
  should not run a competing enforcement path. The Lakebase `gateway_budgets`
  table is left intact (orphaned, non-destructive). We will integrate the native
  budgets once a public v2 management API is available.

### Security
- Frontend dependency advisories cleared: axios, postcss, follow-redirects,
  picomatch bumps; Vite 5 → 6 (esbuild dev-server advisory); lodash override
  → 4.18.1.

### Design decisions

- **Why v2 is a separate tab, not a replacement.** `system.ai_gateway.usage` only
  covers traffic routed through v2-enabled endpoints — a subset of all serving
  requests — and carries **no dollar cost** and almost no 429s. The
  broad/authoritative sources remain
  `system.serving.endpoint_usage` (all serving + rate-limit hits) and
  `system.billing.usage` (billed cost). Folding v2 into — or replacing — the main
  view would contaminate broad metrics with a partial-coverage subset and drop
  cost/429 coverage. So v2 is **additive**: its own clearly-Beta tab for the things
  only it has (cached-token %, TTFB, requester/model/API breakdowns, ~20-min
  freshness). When v2 eventually supersedes the legacy gateway, we expand this tab
  rather than re-plumb the others.
- **Why in-app budgeting was removed.** Enforcement belongs in the request path,
  which is the gateway — not in an app reading mirrored tables after the fact.
  Running our own budgets alongside v2's native budgets would create two sources of
  truth and a split-brain control. The v2 policy/budget management APIs are not yet
  public, so anything built now would target unstable internals and need rebuilding.
  The app stays in its lane — **observability**, not enforcement — and will surface
  native budgets once their API lands.

## [0.1.1] - 2026-05-29

### Added
- **Ask Genie** chat overlay (behind `FEATURE_GENIE_ENABLED`, defaults off; requires `GENIE_SPACE_ID`). Floating bottom-right FAB on every page; ⌘/Ctrl+K to toggle. Sends questions to the Databricks Genie REST API on behalf of the signed-in user, polls until completion, and renders Genie's prose answer alongside a chart or table derived from the SQL it ran. The bootstrap script `setup_genie_space.py` provisions an ACP Analytics space with 11 Delta tables and 30 sample questions.
- **Auto-detected result charts** in the overlay: line (date × numeric), bar with top-15 + "Other" bucket (string × numeric), multi-series line (date × category × numeric), stacked bar with top-8 series + "Other" bucket (string × category × numeric), and donut for ≤6 categories. Y-axis uses compact formatting (1.38B, $12K); hover shows full precision; column-name hints route currency/percent formatting automatically. Date cells render as "May 16" with full ISO on hover.
- **Per-route starter chips** in the empty state. Routes covered: governance, agents, agent detail, ai-gateway, observability, workspaces, tools, knowledge bases, admin.
- **In-chat retry** on errors (re-asks the previous question), **cancel button** while Genie is thinking (AbortController-aware poll loop), and **conversation persistence** across hard refresh via sessionStorage.
- Per-user / per-group **token budgets** (behind `FEATURE_BUDGETS_ENABLED`, defaults off). Admins set a token cap per principal scoped optionally to an endpoint and a period (day / month / quarter / year). The new "Budgets" sub-tab on AI Gateway shows real-time spent vs cap with `ok` / `warning` / `breached` status pills, a breached-count header banner, and a token-amount input that accepts K / M / B suffixes. Spend is computed on read from `gateway_usage_daily` (input + output tokens) — no separate cache, no dollar conversion. For authoritative billed dollars, the Governance tab remains the source of truth.
- New Lakebase table `gateway_budgets` (created by `setup_lakebase_tables.py`); new REST surface `GET/POST/PATCH/DELETE /api/v1/gateway/budgets` plus `GET /api/v1/gateway/budgets/alerts`; admin gating via the existing `require_admin` dependency.

### Changed
- **Billing data refresh moved from app startup to the discovery workflow.** New task `workflows/09_discover_billing.py` pulls `system.billing.usage`, `system.billing.list_prices`, and `system.serving.endpoint_usage` every 30 min into four Delta tables (`billing_serving_daily`, `billing_token_daily`, `billing_product_daily`, `billing_user_endpoint_daily`). `02_sync_to_lakebase.py` (Phase 7) mirrors them to Lakebase and stamps `billing_cache_meta.last_refreshed`. The app's `billing_service.py` is now a read-only layer over the Lakebase cache — `maybe_refresh_async()`/`force_refresh_async()` are kept as no-op stubs for backward compatibility. Brings the billing pipeline in line with the project's discovery → Delta → Lakebase pattern and exposes per-endpoint cost data to Genie/analytics consumers via Delta.

## [0.1.0] - 2026-04-09

### Added
- Agent discovery from serving endpoints, Databricks Apps, Genie Spaces, and Agent Bricks
- Cross-workspace agent discovery via system tables (`system.serving.served_entities`)
- Governance dashboard with billing/cost attribution from `system.billing.usage`
- MLflow observability with cross-workspace experiments and runs via `system.mlflow.*`
- AI Gateway management with permissions, rate limits, and request logs
- Agent dependency topology graph
- Interactive agent playground (chat)
- MCP server and UC function registry
- User analytics with activity heatmap and RBAC matrix
- Multi-workspace federation overview
- Scheduled discovery workflows (Databricks Asset Bundles)
- Lakebase (PostgreSQL) caching for fast dashboard reads
- OBO (On-Behalf-Of) authentication via Databricks Apps
- Parameterized deployment script (`deploy.sh`)
