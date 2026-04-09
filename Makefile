# Agent Control Plane — common operations
# Usage: make <target>

.PHONY: help setup deploy deploy-workflows run-workflows check frontend backend clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Setup ─────────────────────────────────────────────────────

setup: ## First-time setup: install dependencies, copy .env
	@echo "Installing backend dependencies..."
	pip install -r control-plane-app/requirements.txt
	@echo "Installing frontend dependencies..."
	cd control-plane-app/frontend && npm install
	@if [ ! -f control-plane-app/.env ]; then \
		cp control-plane-app/.env.example control-plane-app/.env; \
		echo "Created .env from .env.example — edit it with your values"; \
	else \
		echo ".env already exists"; \
	fi

# ── Deploy ────────────────────────────────────────────────────

deploy: ## Deploy the app (reads config from control-plane-app/.env)
	cd control-plane-app && bash deploy.sh

deploy-profile: ## Deploy with a specific CLI profile (usage: make deploy-profile PROFILE=my-profile)
	cd control-plane-app && bash deploy.sh --profile $(PROFILE)

deploy-workflows: ## Deploy discovery workflows (usage: make deploy-workflows TARGET=dev)
	cd workflows && databricks bundle deploy --target $(or $(TARGET),dev)

run-workflows: ## Trigger a workflow run (usage: make run-workflows TARGET=dev)
	cd workflows && databricks bundle run agent_discovery --target $(or $(TARGET),dev) --no-wait

# ── Development ───────────────────────────────────────────────

backend: ## Start backend locally (hot reload)
	cd control-plane-app && uvicorn backend.main:app --reload --port 8000

frontend: ## Start frontend dev server
	cd control-plane-app/frontend && npm run dev

# ── Quality ───────────────────────────────────────────────────

test: ## Run backend tests with coverage
	cd control-plane-app && python -m pytest tests/backend/ -v --tb=short

test-cov: ## Run backend tests with coverage report
	cd control-plane-app && python -m pytest tests/backend/ -v --cov=backend --cov-report=term-missing --tb=short

lint: ## Run Python linter (ruff)
	cd control-plane-app && ruff check backend/ --select E,F,W --ignore E501,E402

check: ## Run all checks (Python compile + TypeScript type check + lint)
	@echo "Checking Python..."
	@python3 -c "import py_compile, glob; [py_compile.compile(f, doraise=True) for f in glob.glob('control-plane-app/backend/**/*.py', recursive=True)]"
	@echo "Checking TypeScript..."
	@cd control-plane-app/frontend && npx tsc --noEmit
	@echo "All checks passed"

# ── Cleanup ───────────────────────────────────────────────────

clean: ## Remove build artifacts
	rm -rf control-plane-app/dist
	rm -rf control-plane-app/frontend/node_modules/.vite
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
