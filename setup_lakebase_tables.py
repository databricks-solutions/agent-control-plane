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

# Read config from environment
LAKEBASE_DNS = os.environ.get("LAKEBASE_DNS", "")
DATABASE = os.environ.get("LAKEBASE_DATABASE", "control_plane")
LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE", "")
PORT = 5432

if not LAKEBASE_DNS or not LAKEBASE_INSTANCE:
    print("Error: LAKEBASE_DNS and LAKEBASE_INSTANCE environment variables are required.")
    print("Set them or copy .env.example to .env and fill in your values.")
    sys.exit(1)


def get_lakebase_credentials():
    """Generate Lakebase credentials using the Databricks SDK."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        me = w.current_user.me()
        pg_user = me.user_name

        pg_password = None
        if hasattr(w, "database"):
            try:
                creds = w.database.generate_database_credential(
                    instance_names=[LAKEBASE_INSTANCE]
                )
                pg_password = creds.token
            except Exception as e:
                print(f"SDK credential generation failed: {e}")

        if not pg_password:
            import requests
            header_factory = w.config.authenticate
            auth_headers = header_factory()
            token = auth_headers.get("Authorization", "").replace("Bearer ", "")
            host = w.config.host.rstrip("/")
            resp = requests.post(
                f"{host}/api/2.0/database/credentials",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"instance_names": [LAKEBASE_INSTANCE], "request_id": str(uuid.uuid4())},
            )
            resp.raise_for_status()
            pg_password = resp.json().get("token", "")

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

        print("Creating agent_registry table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_registry (
              agent_id VARCHAR(255) PRIMARY KEY,
              name VARCHAR(255) NOT NULL,
              type VARCHAR(50) NOT NULL,
              description TEXT,
              endpoint_name VARCHAR(255),
              endpoint_type VARCHAR(50),
              endpoint_status VARCHAR(50),
              app_id VARCHAR(255),
              app_url VARCHAR(500),
              version VARCHAR(50),
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              created_by VARCHAR(255),
              tags JSONB,
              config JSONB,
              is_active BOOLEAN DEFAULT TRUE
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_registry_type ON agent_registry(type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_registry_status ON agent_registry(endpoint_status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_registry_active ON agent_registry(is_active)")

        print("Creating model_registry table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS model_registry (
              model_id VARCHAR(255) PRIMARY KEY,
              name VARCHAR(255) NOT NULL,
              version VARCHAR(50) NOT NULL,
              model_uri VARCHAR(500),
              model_type VARCHAR(50),
              endpoint_name VARCHAR(255),
              endpoint_type VARCHAR(50),
              status VARCHAR(50),
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              metrics JSONB,
              tags JSONB
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_model_registry_name ON model_registry(name)")

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
