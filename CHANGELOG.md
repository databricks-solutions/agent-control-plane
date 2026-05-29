# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
