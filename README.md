# Agent Control Plane

A management and observability platform for AI agents deployed on Databricks — purpose-built for teams operating production-grade agent infrastructure at scale.

📺 **[View the interactive demo →](https://databricks-solutions.github.io/agent-control-plane/product-demo.html)** (walkthrough of the app; no install required)

## The Problem

As enterprises deploy more AI agents, a new operational challenge emerges: **who is using what, how is it performing, and who has access?**

Agents are created across multiple workspaces by different teams — some via Agent Bricks, some as custom serving endpoints, some as Databricks Apps. There's no single view of what's running, what it costs, or who can access it.

## The Solution

Agent Control Plane gives platform and ML teams a **single pane of glass** over all AI agents running across Databricks workspaces. It auto-discovers agents, tracks costs, surfaces MLflow observability data, and manages access — all from one app.

Built natively on Databricks: **Lakebase**, **system tables**, **MLflow**, **Unity Catalog**, and **Databricks Apps**.

## Features

### Governance

Cost attribution and billing analytics powered by `system.billing.usage`. Track DBU spend per endpoint, token usage trends, and cost breakdown by SKU across all workspaces in the account. Drill into daily cost trends, product-level breakdown, and top consumers.

### Agents

Auto-discovered agent registry across all workspaces. Finds serving endpoints, Databricks Apps, Genie Spaces, and Agent Bricks (Knowledge Assistants, Multi-Agent Supervisors, Knowledge Inference Engines). View endpoint status, operations metrics, and an interactive dependency topology.

### Unity Gateway

Unified view of all model serving endpoints with usage analytics, token volume charts, per-endpoint and per-user breakdowns. Manage Unity Catalog permissions directly from the UI. Monitor operational metrics (requests, errors, latency), view individual request logs, and inspect rate limits and safety guardrails configured via Unity Gateway.

### Knowledge Bases

Combined monitoring for Vector Search and Lakebase. Overview tab shows total cost, daily cost trends by product, and top workspaces by spend. Vector Search tab provides endpoint/index inventory, sync status, health history, and cost attribution by workload type (ingest, serving, storage). Lakebase tab shows instance inventory, compute vs storage cost, and per-workspace breakdown.

### Observability

Cross-workspace MLflow observability — experiments, evaluation runs, and agent traces — for account-wide visibility from `system.mlflow.*` and Unity Catalog. Filter by workspace, drill into trace detail, and see which source each record came from. Trace discovery pulls from multiple backends (local MLflow, serving inference logs, and UC-stored traces) so coverage doesn't hinge on a single setup. See the [installation guide](docs/installation.md#how-trace-discovery-works) for how it works and what to enable on your agents.

### Tools

Registry of Unity Catalog functions and MCP servers available to agents. Browse function signatures, descriptions, and catalog locations.

### Workspaces

Multi-workspace federation dashboard. See agent inventory, cost breakdown, cloud provider, and deployment region per workspace. Drill into individual workspaces for detailed agent and cost views.

### Admin

Identity and access management with all principals, builders/users breakdown, RBAC matrix, and per-agent permission management. User activity tab shows top users, request distribution, daily active user trends, activity heatmap (24h UTC, Monday-first), and per-user agent usage. An **App Settings** tab lets workspace or account admins choose the access mode — **open** (any authenticated user sees all workspaces read-only) or **strict** (per-user workspace scoping: account admins see everything, workspace admins see only the workspaces they administer).

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Databricks APIs + System Tables                    │
│  (serving, billing, mlflow, access, apps, genie)    │
└──────────────────────┬──────────────────────────────┘
                       │  Scheduled workflow (every 30 min)
                       ▼
┌─────────────────────────────────────────────────────┐
│  Delta Tables → Lakebase (PostgreSQL)               │
│  (agents, experiments, runs, traces, billing cache) │
└──────────────────────┬──────────────────────────────┘
                       │  Sub-100ms reads
                       ▼
┌─────────────────────────────────────────────────────┐
│  FastAPI Backend (20 routers, 19 services)          │
│  → React Frontend (TanStack Query + Tailwind)       │
│  → Databricks App (OBO authentication)              │
└─────────────────────────────────────────────────────┘
```

**Key data sources:**
- `system.serving.served_entities` — cross-workspace agent discovery
- `system.serving.endpoint_usage` — per-endpoint request and token metrics
- `system.billing.usage` + `system.billing.list_prices` — cost attribution
- `system.mlflow.experiments_latest` / `runs_latest` — observability
- `system.access.audit` — user activity and access patterns
- Databricks REST APIs — endpoints, apps, genie spaces, traces

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/databricks-solutions/agent-control-plane.git
cd agent-control-plane
make setup

# 2. Edit .env with your Lakebase and workspace details
vi control-plane-app/.env

# 3. Deploy the app
make deploy

# 4. Deploy discovery workflows
make deploy-workflows TARGET=dev

# 5. Trigger first discovery run
make run-workflows TARGET=dev
```

See the full **[Installation Guide](docs/installation.md)** for detailed setup including Lakebase creation, OBO configuration, and system table grants.

## Prerequisites

- **Databricks workspace** with Unity Catalog enabled
- **Lakebase instance** (PostgreSQL) — for fast dashboard reads
- **SQL warehouse** (serverless preferred) — for system table queries
- **Databricks App** with User Authorization (OBO) enabled
- **Node.js 18+** and **Python 3.11+** — for building and deploying

## Project Structure

```
agent-control-plane/
├── control-plane-app/          # The Databricks App
│   ├── backend/                # FastAPI (Python)
│   │   ├── api/                # 20 route modules
│   │   ├── services/           # Business logic
│   │   ├── models/             # Pydantic schemas
│   │   └── utils/auth.py       # OBO authentication
│   ├── frontend/               # React 18 + TypeScript
│   ├── tests/                  # pytest + Playwright
│   ├── deploy.sh               # Parameterized deploy script
│   ├── grant_sp_permissions.py # SP workspace permission setup
│   └── propagate-sp.sh         # Cross-workspace SP propagation
├── workflows/                  # Databricks Asset Bundles (15 discovery tasks)
│   ├── 01_discover_agents.py          # Agent discovery → Delta
│   ├── 02_sync_to_lakebase.py         # All Delta → Lakebase + billing cache
│   ├── 03_discover_knowledge_bases.py # Vector Search + Lakebase + billing → Delta
│   ├── 04_discover_observability.py   # Cross-workspace traces → Delta
│   ├── 05_discover_user_analytics.py  # User activity → Delta
│   ├── 06_discover_gateway_usage.py   # Gateway usage → Delta
│   ├── ...                            # 07–15: UC OTel traces, inference logs,
│   │                                  #   billing, smoke check, gateway usage,
│   │                                  #   model classify, budgets, endpoint
│   │                                  #   inventory, model services
│   ├── 10_smoke_check_lakebase.py     # Post-sync smoke check (fails CI on empty)
│   └── databricks.yml                 # Bundle configuration
├── docs/                       # Documentation
│   ├── installation.md         # Setup guide
│   └── configuration.md        # Config reference
├── setup_lakebase_tables.py    # One-time Lakebase schema setup
└── Makefile                    # Common operations
```

## Documentation

| Document | Description |
|----------|-------------|
| **[Installation Guide](docs/installation.md)** | Step-by-step setup (Lakebase, App, workflows) |
| **[Configuration Reference](docs/configuration.md)** | All env vars, workflow targets, finding your values |
| **[Contributing](CONTRIBUTING.md)** | Development setup, code style, PR process |
| **[Security](SECURITY.md)** | Vulnerability reporting, security model |
| **[Changelog](CHANGELOG.md)** | Version history |
| **[Releasing](docs/releasing.md)** | How to create a release |

## Development

```bash
# Start backend (hot reload)
make backend

# Start frontend dev server
make frontend

# Run tests
make test

# Run all checks (Python + TypeScript)
make check

# See all available commands
make help
```

## API

All endpoints are versioned under `/api/v1/`. Interactive documentation available at `/api/v1/docs` when the app is running.

Key endpoints:
- `GET /api/v1/agents` — discovered agent registry
- `GET /api/v1/billing/page-data` — cost attribution dashboard
- `GET /api/v1/mlflow/experiments` — MLflow experiments (cross-workspace)
- `GET /api/v1/mlflow/traces` — MLflow traces
- `GET /api/v1/gateway/overview` — Unity Gateway analytics
- `POST /api/v1/agents/sync` — trigger agent discovery refresh

## License

Apache License 2.0. See [LICENSE](LICENSE).
