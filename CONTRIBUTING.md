# Contributing to Agent Control Plane

We welcome contributions to the Agent Control Plane project. This document provides guidelines for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Create a feature branch: `git checkout -b feature/my-change`
4. Follow the [installation guide](docs/installation.md) to set up your development environment

## Development Setup

### Backend (FastAPI)

```bash
cd control-plane-app
pip install -r requirements.txt
# For local development, set CORS_ORIGINS in .env:
# CORS_ORIGINS=http://localhost:3000
uvicorn backend.main:app --reload --port 8000
```

### Frontend (React + Vite)

```bash
cd control-plane-app/frontend
npm install
npm run dev   # starts on http://localhost:3000
```

### Workflows (Databricks Asset Bundles)

```bash
cd workflows
# Edit databricks.yml with your target config
databricks bundle deploy --target dev
databricks bundle run agent_discovery --target dev
```

## Code Style

### Python (Backend)
- Use type hints for function signatures
- Use `logging` module (not `print()`) for output
- Follow existing patterns for new API routes:
  - Router in `backend/api/`
  - Service logic in `backend/services/`
  - Pydantic models in `backend/models/`
- All routers must include `dependencies=[Depends(get_current_user)]`

### TypeScript (Frontend)
- Use TanStack Query hooks for data fetching (in `frontend/src/api/hooks.ts`)
- One page component per file in `frontend/src/pages/`
- Reusable components in `frontend/src/components/`
- Use Tailwind CSS for styling

## Pull Request Process

1. Ensure your code compiles:
   - Backend: `python -c "import py_compile; py_compile.compile('backend/main.py', doraise=True)"`
   - Frontend: `cd frontend && npx tsc --noEmit`
2. Test your changes locally
3. Update documentation if you changed behavior
4. Submit a PR against `main` with a clear description of what changed and why

## Reporting Issues

- Use GitHub Issues for bug reports and feature requests
- Include steps to reproduce for bugs
- Include your Databricks workspace type (AWS/Azure/GCP) and runtime version

## Security

If you discover a security vulnerability, please report it responsibly. See [SECURITY.md](SECURITY.md) for details.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
