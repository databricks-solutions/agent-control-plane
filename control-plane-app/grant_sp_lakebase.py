#!/usr/bin/env python3
"""
Register the app's service principal as a Lakebase Postgres role and grant it
the privileges needed by the control plane tables.

Runs as the current user (Lakebase admin). Idempotent — safe to re-run.

Required env vars:
  APP_NAME              — the Databricks App name (to look up its SP client ID)
  LAKEBASE_DNS          — Lakebase endpoint hostname
  LAKEBASE_DATABASE     — database name (e.g. control_plane)

Plus exactly one of:
  LAKEBASE_ENDPOINT_PATH — Autoscaling, e.g. projects/<name>/branches/<branch>/endpoints/<endpoint>
  LAKEBASE_INSTANCE      — Provisioned instance name

Optional:
  DATABRICKS_CONFIG_PROFILE — CLI profile to use
"""

import os
import sys
import uuid

import psycopg2
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Role,
    RoleRoleSpec,
    RoleAuthMethod,
    RoleIdentityType,
)


def main() -> int:
    app_name = os.environ.get("APP_NAME", "")
    lakebase_dns = os.environ.get("LAKEBASE_DNS", "")
    lakebase_database = os.environ.get("LAKEBASE_DATABASE", "")
    endpoint_path = os.environ.get("LAKEBASE_ENDPOINT_PATH", "")
    instance_name = os.environ.get("LAKEBASE_INSTANCE", "")

    if not app_name:
        print("Error: APP_NAME is required")
        return 1
    if not lakebase_dns or not lakebase_database:
        print("Error: LAKEBASE_DNS and LAKEBASE_DATABASE are required")
        return 1
    if not endpoint_path and not instance_name:
        print("Error: set LAKEBASE_ENDPOINT_PATH (Autoscaling) or LAKEBASE_INSTANCE (Provisioned)")
        return 1

    w = WorkspaceClient()

    app = w.apps.get(name=app_name)
    sp_client_id = app.service_principal_client_id
    if not sp_client_id:
        print(f"Error: could not determine service principal for app '{app_name}'")
        return 1
    print(f"App SP: {sp_client_id}")

    # 1. Register the SP as a Lakebase role (if not already registered)
    if endpoint_path:
        parent = "/".join(endpoint_path.split("/")[:4])  # projects/<p>/branches/<b>
        existing = [
            r for r in w.postgres.list_roles(parent=parent)
            if r.status and r.status.postgres_role == sp_client_id
        ]
        if existing:
            print(f"SP role already registered: {existing[0].name}")
        else:
            print(f"Registering SP as Lakebase role under {parent} ...")
            w.postgres.create_role(
                parent=parent,
                role=Role(spec=RoleRoleSpec(
                    postgres_role=sp_client_id,
                    identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                    auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                )),
            )
            print("  OK")
    else:
        # Provisioned mode: the Permissions API handles SP access grants
        print("Provisioned mode — use the Databricks UI or Permissions API to grant the SP access to the Lakebase instance")

    # 2. Generate a credential for the current user (Lakebase admin)
    me = w.current_user.me()
    pg_user = me.user_name
    token = w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
    host = w.config.host.rstrip("/")

    if endpoint_path:
        r = requests.post(
            f"{host}/api/2.0/postgres/credentials",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"endpoint": endpoint_path},
            timeout=15,
        )
    else:
        r = requests.post(
            f"{host}/api/2.0/database/credentials",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"instance_names": [instance_name], "request_id": str(uuid.uuid4())},
            timeout=15,
        )
    r.raise_for_status()
    pg_password = r.json().get("token", "")

    # 3. Grant Postgres privileges to the SP
    print(f"Granting Postgres privileges to {sp_client_id} on {lakebase_database} ...")
    conn = psycopg2.connect(
        host=lakebase_dns, port=5432, database=lakebase_database,
        user=pg_user, password=pg_password, sslmode="require", connect_timeout=15,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'GRANT CONNECT ON DATABASE {lakebase_database} TO "{sp_client_id}"')
        cur.execute(f'GRANT USAGE, CREATE ON SCHEMA public TO "{sp_client_id}"')
        cur.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{sp_client_id}"')
        cur.execute(f'GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO "{sp_client_id}"')
        cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{sp_client_id}"')
        cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "{sp_client_id}"')
    conn.close()
    print("  OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
