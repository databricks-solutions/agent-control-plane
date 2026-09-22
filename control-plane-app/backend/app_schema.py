"""Single source of truth for the app-managed *registry* tables.

`agent_registry`, `model_registry`, and `gateway_budgets` are created by the app
(on startup, in backend.main) and by the standalone `setup_lakebase_tables.py`
provisioning script. Historically each kept its own copy of the CREATE TABLE
statements, which could silently drift. Both now import `APP_REGISTRY_DDL` from
here so there is exactly one definition.

Note: the discovery workflow (`workflows/02_sync_to_lakebase.py`, Phase 7) runs as
a Databricks notebook on a separate cluster and cannot import this package, so it
keeps an inline copy — kept intentionally identical to the statements below (see
the "keep in sync with backend/app_schema.py" note there). Every statement is
idempotent (CREATE TABLE / INDEX IF NOT EXISTS), so running them from more than
one place is safe.
"""

# Ordered list of idempotent DDL statements: each CREATE TABLE followed by its
# indexes. Applied by iterating and executing each statement in turn.
APP_REGISTRY_DDL = [
    """
    CREATE TABLE IF NOT EXISTS agent_registry (
        agent_id        VARCHAR(255) PRIMARY KEY,
        name            VARCHAR(255) NOT NULL,
        type            VARCHAR(50)  NOT NULL,
        description     TEXT,
        endpoint_name   VARCHAR(255),
        endpoint_type   VARCHAR(50),
        endpoint_status VARCHAR(50),
        app_id          VARCHAR(255),
        app_url         VARCHAR(500),
        version         VARCHAR(50),
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by      VARCHAR(255),
        tags            JSONB,
        config          JSONB,
        is_active       BOOLEAN DEFAULT TRUE
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_agent_registry_type   ON agent_registry(type)",
    "CREATE INDEX IF NOT EXISTS idx_agent_registry_status ON agent_registry(endpoint_status)",
    "CREATE INDEX IF NOT EXISTS idx_agent_registry_active ON agent_registry(is_active)",
    """
    CREATE TABLE IF NOT EXISTS model_registry (
        model_id      VARCHAR(255) PRIMARY KEY,
        name          VARCHAR(255) NOT NULL,
        version       VARCHAR(50)  NOT NULL,
        model_uri     VARCHAR(500),
        model_type    VARCHAR(50),
        endpoint_name VARCHAR(255),
        endpoint_type VARCHAR(50),
        status        VARCHAR(50),
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metrics       JSONB,
        tags          JSONB
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_model_registry_name ON model_registry(name)",
    """
    CREATE TABLE IF NOT EXISTS gateway_budgets (
        budget_id        VARCHAR(36) PRIMARY KEY,
        principal        VARCHAR(255) NOT NULL,
        principal_type   VARCHAR(32)  NOT NULL,
        endpoint_name    VARCHAR(255),
        workspace_id     VARCHAR(64),
        budget_tokens    BIGINT NOT NULL,
        period           VARCHAR(16) NOT NULL DEFAULT 'month',
        alert_at_percent INTEGER NOT NULL DEFAULT 80,
        is_active        BOOLEAN NOT NULL DEFAULT TRUE,
        created_by       VARCHAR(255) NOT NULL,
        created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_gateway_budgets_principal ON gateway_budgets(principal)",
    "CREATE INDEX IF NOT EXISTS idx_gateway_budgets_endpoint  ON gateway_budgets(endpoint_name) WHERE endpoint_name IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_gateway_budgets_active    ON gateway_budgets(is_active) WHERE is_active = TRUE",
]
"""The app-managed registry tables, in dependency order. See module docstring."""
