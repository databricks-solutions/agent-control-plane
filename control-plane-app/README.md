# Control Plane App

The Databricks App — FastAPI backend + React frontend.

See the **[root README](../README.md)** for project overview and **[Installation Guide](../docs/installation.md)** for setup.

## Backend

FastAPI server with 17 API routers and 16 service modules.

```
backend/
├── api/            # Route handlers (agents, billing, mlflow, gateway, etc.)
├── services/       # Business logic, Databricks SDK integration, caching
├── models/         # Pydantic request/response schemas
├── utils/auth.py   # OBO authentication (x-forwarded-access-token)
├── config.py       # Settings from environment variables
├── database.py     # Lakebase (PostgreSQL) connection pool
└── main.py         # App entrypoint, middleware, route registration
```

All API endpoints require authentication (`Depends(get_current_user)`). Routes are versioned under `/api/v1/`.

## Frontend

React 18 SPA with TypeScript, TanStack Query, Tailwind CSS, and Recharts.

```
frontend/src/
├── pages/          # One page per feature (Agents, Governance, Observability, etc.)
├── components/     # Reusable UI components
├── api/
│   ├── client.ts   # Axios HTTP client
│   └── hooks.ts    # TanStack Query hooks (100+)
├── context/        # Theme context (dark/light mode)
└── lib/            # Constants, utilities, branding
```

## Scripts

| Script | Purpose |
|--------|---------|
| `deploy.sh` | Build frontend + deploy to Databricks Apps (reads `.env`) |
| `grant_sp_permissions.py` | Grant the app's SP permissions on workspace resources |
| `propagate-sp.sh` | Add the app's SP to all workspaces in the account |

## Local Development

```bash
# Backend (hot reload on :8000)
uvicorn backend.main:app --reload --port 8000

# Frontend (Vite dev server on :3000)
cd frontend && npm run dev

# Set CORS_ORIGINS=http://localhost:3000 in .env for local dev
```

## Tests

```bash
# Install test deps
pip install pytest pytest-asyncio pytest-cov

# Run tests
python -m pytest tests/backend/ -v

# With coverage
python -m pytest tests/backend/ -v --cov=backend --cov-report=term-missing
```
