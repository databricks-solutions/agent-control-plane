#!/usr/bin/env python3
"""
Create tables in Lakebase PostgreSQL.

Usage:
    1. Set environment variables (or copy .env.example to .env and fill in):
       - LAKEBASE_DNS
       - LAKEBASE_DATABASE
       - LAKEBASE_INSTANCE

    2. Authenticate:
       databricks auth login --host https://<your-workspace>.cloud.databricks.com

    3. Run:
       python setup_lakebase_tables.py
"""

import os
import sys
import uuid
import psycopg2

# Import the canonical app-registry DDL so this script and the app's startup hook
# (backend.main) never drift. The backend package lives under control-plane-app/.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "control-plane-app"))
from backend.app_schema import APP_REGISTRY_DDL

# Read config from environment
LAKEBASE_DNS = os.environ.get("LAKEBASE_DNS", "")
DATABASE = os.environ.get("LAKEBASE_DATABASE", "control_plane")
LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE", "")
LAKEBASE_ENDPOINT_PATH = os.environ.get("LAKEBASE_ENDPOINT_PATH", "")
PORT = 5432

if not LAKEBASE_DNS:
    print("Error: LAKEBASE_DNS environment variable is required.")
    sys.exit(1)
if not LAKEBASE_ENDPOINT_PATH and not LAKEBASE_INSTANCE:
    print("Error: Set LAKEBASE_ENDPOINT_PATH (Autoscaling) or LAKEBASE_INSTANCE (Provisioned).")
    sys.exit(1)


def get_lakebase_credentials():
    """Generate Lakebase credentials using the Databricks SDK."""
    try:
        from databricks.sdk import WorkspaceClient
        import requests
        w = WorkspaceClient()
        me = w.current_user.me()
        pg_user = me.user_name

        header_factory = w.config.authenticate
        auth_headers = header_factory()
        token = auth_headers.get("Authorization", "").replace("Bearer ", "")
        host = w.config.host.rstrip("/")

        pg_password = None

        # Autoscaling: POST /api/2.0/postgres/credentials with endpoint path
        if LAKEBASE_ENDPOINT_PATH:
            resp = requests.post(
                f"{host}/api/2.0/postgres/credentials",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"endpoint": LAKEBASE_ENDPOINT_PATH},
            )
            resp.raise_for_status()
            pg_password = resp.json().get("token", "")
            if pg_password:
                print(f"Credential generated via Autoscaling API")

        # Provisioned fallback: SDK then REST
        if not pg_password and LAKEBASE_INSTANCE and hasattr(w, "database"):
            try:
                creds = w.database.generate_database_credential(instance_names=[LAKEBASE_INSTANCE])
                pg_password = creds.token
                print("Credential generated via Provisioned SDK")
            except Exception as e:
                print(f"Provisioned SDK credential generation failed: {e}")

        if not pg_password and LAKEBASE_INSTANCE:
            resp = requests.post(
                f"{host}/api/2.0/database/credentials",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"instance_names": [LAKEBASE_INSTANCE], "request_id": str(uuid.uuid4())},
            )
            resp.raise_for_status()
            pg_password = resp.json().get("token", "")
            print("Credential generated via Provisioned REST API")

        if not pg_password:
            print("Error: could not generate Lakebase credentials")
            sys.exit(1)

        return pg_user, pg_password
    except Exception as e:
        print(f"Failed to get credentials: {e}")
        print("Make sure you have authenticated: databricks auth login --host <your-workspace-url>")
        sys.exit(1)


def create_tables():
    """Create all tables in Lakebase PostgreSQL."""
    pg_user, pg_password = get_lakebase_credentials()

    try:
        print(f"Connecting to Lakebase: {LAKEBASE_DNS}")
        conn = psycopg2.connect(
            host=LAKEBASE_DNS,
            port=PORT,
            database=DATABASE,
            user=pg_user,
            password=pg_password,
            sslmode="require"
        )
        print("Connected successfully")

        cur = conn.cursor()

        # agent_registry, model_registry, gateway_budgets — from the shared
        # backend.app_schema.APP_REGISTRY_DDL (single source; keeps this script in
        # lockstep with the app's startup hook). All statements are idempotent.
        print("Creating app registry tables (agent_registry, model_registry, gateway_budgets)...")
        for stmt in APP_REGISTRY_DDL:
            cur.execute(stmt)

        print("Creating request_logs table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS request_logs (
              request_id VARCHAR(255) PRIMARY KEY,
              agent_id VARCHAR(255),
              model_id VARCHAR(255),
              user_id VARCHAR(255),
              timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              query_text TEXT,
              response_text TEXT,
              latency_ms INTEGER,
              status_code INTEGER,
              input_tokens INTEGER,
              output_tokens INTEGER,
              cost_usd DECIMAL(10,4),
              error_message TEXT,
              endpoint_type VARCHAR(50)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_timestamp ON request_logs(timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_agent_id ON request_logs(agent_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_user_id ON request_logs(user_id)")

        # gateway_budgets is created above via APP_REGISTRY_DDL.

        conn.commit()
        print("All tables created successfully")

        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' ORDER BY table_name
        """)
        tables = cur.fetchall()
        print(f"Tables: {', '.join(t[0] for t in tables)}")

        cur.close()
        conn.close()
        print("Setup complete.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    create_tables()
