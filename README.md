# Agent Control Plane

A management and observability platform for AI agents deployed on Databricks — purpose-built for teams operating production-grade agent infrastructure at scale.

<!-- Screenshots: Replace these placeholders with actual screenshots of your deployment -->
<!-- ![Dashboard](docs/screenshots/governance.png) -->

## The Problem

As enterprises deploy more AI agents, a new operational challenge emerges: **who is using what, how is it performing, and who has access?**

Agents are created across multiple workspaces by different teams — some via Agent Bricks, some as custom serving endpoints, some as Databricks Apps. There's no single view of what's running, what it costs, or who can access it.

## The Solution

Agent Control Plane gives platform and ML teams a **single pane of glass** over all AI agents running across Databricks workspaces. It auto-discovers agents, tracks costs, surfaces MLflow observability data, and manages access — all from one app.

Built natively on Databricks: **Lakebase**, **system tables**, **MLflow**, **Unity Catalog**, and **Databricks Apps**.

## Features

### Agents
Auto-discovered registry across all workspaces. Finds serving endpoints, Databricks Apps, Genie Spaces, and Agent Bricks (Knowledge Assistants, Multi-Agent Supervisors). Includes operations metrics, interactive dependency topology graph, and embedded playground for testing.

<!-- ![Agents](docs/screenshots/agents.png) -->

### Governance
Billing and cost attribution powered by `system.billing.usage`. Tracks DBU spend per endpoint, token usage trends, and cost breakdown by SKU — across all workspaces in the account.

<!-- ![Governance](docs/screenshots/governance.png) -->

### Observability
Cross-workspace MLflow experiments, evaluation runs, and traces. Queries `system.mlflow.experiments_latest` and `system.mlflow.runs_latest` for account-wide visibility. Each row tagged with its data source (`system_table` or `rest_api`).

<!-- ![Observability](docs/screenshots/observability.png) -->

### AI Gateway
Usage analytics, request logs, permissions, and rate limits for all model serving endpoints. Manage Unity Catalog grants directly from the UI.

### Knowledge Bases
Unified monitoring for Vector Search and Lakebase. Overview with combined cost trends, per-workspace drill-down. Vector Search tab shows endpoint/index inventory, sync status, health history, and cost by workload type (ingest/serving/storage). Lakebase tab shows instance inventory, compute vs storage cost, and per-workspace breakdown.

### Workspaces
Multi-workspace federation dashboard. Agent inventory, cost breakdown, and health status per workspace.

### Tools
Registry of MCP servers and Unity Catalog functions available to agents.

### Admin
User analytics with activity heatmap, RBAC matrix, and access management. See who accessed which endpoints, when, and how often.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Databricks APIs + System Tables                     │
│  (serving, billing, mlflow, access, apps, genie)     │
└──────────────────────┬──────────────────────────────┘
                       │  Scheduled workflow (every 30 min)
                       ▼
┌─────────────────────────────────────────────────────┐
│  Delta Tables → Lakebase (PostgreSQL)                │
│  (agents, experiments, runs, traces, billing cache)  │
└──────────────────────┬──────────────────────────────┘
                       │  Sub-100ms reads
                       ▼
┌─────────────────────────────────────────────────────┐
│  FastAPI Backend (17 routers, 16 services)          │
│  → React Frontend (TanStack Query + Tailwind)        │
│  → Databricks App (OBO authentication)               │
└─────────────────────────────────────────────────────┘
```

**Key data sources:**
- `system.serving.served_entities` — cross-workspace agent discovery
- `system.billing.usage` — cost attribution
- `system.mlflow.experiments_latest` / `runs_latest` — observability
- MLflow Tracking API — traces (per-workspace)
- Databricks REST APIs — endpoints, apps, genie spaces

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/databrickslabs/agent-control-plane.git
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
- **Node.js 18+** and **Python 3.10+** — for building and deploying

## Project Structure

```
agent-control-plane/
├── control-plane-app/          # The Databricks App
│   ├── backend/                # FastAPI (Python)
│   │   ├── api/                # 17 route modules
│   │   ├── services/           # Business logic
│   │   ├── models/             # Pydantic schemas
│   │   └── utils/auth.py       # OBO authentication
│   ├── frontend/               # React 18 + TypeScript
│   ├── tests/                  # pytest + Playwright
│   ├── deploy.sh               # Parameterized deploy script
│   ├── grant_sp_permissions.py # SP workspace permission setup
│   └── propagate-sp.sh         # Cross-workspace SP propagation
├── workflows/                  # Databricks Asset Bundles
│   ├── 01_discover_agents.py          # Agent discovery → Delta
│   ├── 02_sync_to_lakebase.py         # All Delta → Lakebase + billing cache
│   ├── 03_discover_knowledge_bases.py # Vector Search + Lakebase → Delta
│   ├── 04_discover_observability.py   # Cross-workspace traces → Delta
│   ├── 05_discover_user_analytics.py  # User activity → Delta
│   ├── 06_discover_gateway_usage.py   # Gateway usage → Delta
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
- `GET /api/v1/gateway/overview` — AI Gateway analytics
- `POST /api/v1/agents/sync` — trigger agent discovery refresh

## License

Apache License 2.0. See [LICENSE](LICENSE).
