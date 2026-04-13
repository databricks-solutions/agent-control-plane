# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-04-13

### Added
- Knowledge Bases page: unified Vector Search + Lakebase monitoring with cost attribution
- Vector Search: endpoint/index discovery, sync status, health history, workload cost breakdown
- Lakebase: instance inventory, compute vs storage cost, per-workspace breakdown
- Gateway usage caching: all system.serving queries cached via workflow (zero live queries)
- User analytics caching: activity data cached via workflow
- 6 parallel discovery workflow tasks (agents, observability, knowledge bases, user analytics, gateway usage)
- All billing data cached in `kb_billing_daily` Lakebase table
- Recharts line charts for cost trends on Knowledge Bases page

### Changed
- Knowledge Bases billing reads from Lakebase cache (was live system table queries)
- Gateway usage reads from Lakebase cache (was live system table queries)
- User analytics reads from Lakebase cache (was live system table queries)
- Sidebar nav reordered: Agents → AI Gateway → Knowledge Bases → Tools → ...

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
