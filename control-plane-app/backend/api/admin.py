"""Admin probe endpoints — for ad-hoc diagnostics, not for steady-state use."""
from typing import Optional
from fastapi import APIRouter, Query
import httpx
from databricks.sdk import WorkspaceClient

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/probe-cross-workspace")
async def probe_cross_workspace(
    target_host: str = Query(
        "https://dbc-c1ba6c29-075e.cloud.databricks.com",
        description="Target workspace host (https://...)",
    ),
    secret_scope: str = Query("acp-discovery"),
    client_id_key: str = Query("client_id"),
    client_secret_key: str = Query("client_secret"),
):
    """Probe whether App Compute can call /api/2.0/mlflow/* on another workspace.

    Tests three things in sequence and reports each independently:
      1. Can the app SP read the discovery SP creds from the secret scope?
      2. Can we exchange those creds for a workspace token at the target host?
      3. Can we call /api/2.0/mlflow/experiments/search on the target as the SP?
    """
    out = {"target_host": target_host.rstrip("/"), "steps": {}}

    # 1. Read SP creds from secret scope
    try:
        w = WorkspaceClient()
        cid = w.secrets.get_secret(scope=secret_scope, key=client_id_key).value
        csec = w.secrets.get_secret(scope=secret_scope, key=client_secret_key).value
        # SDK returns base64-encoded value
        import base64
        client_id = base64.b64decode(cid).decode()
        client_secret = base64.b64decode(csec).decode()
        out["steps"]["secret_read"] = {
            "ok": True,
            "client_id_length": len(client_id),
            "client_secret_length": len(client_secret),
        }
    except Exception as exc:
        out["steps"]["secret_read"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        return out

    # 2. OIDC token exchange at target workspace
    try:
        r = httpx.post(
            f"{out['target_host']}/oidc/v1/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "all-apis",
            },
            timeout=30,
        )
        if r.status_code == 200:
            sp_token = r.json().get("access_token", "")
            out["steps"]["oidc_exchange"] = {
                "ok": bool(sp_token),
                "status": r.status_code,
                "token_length": len(sp_token),
            }
        else:
            out["steps"]["oidc_exchange"] = {
                "ok": False,
                "status": r.status_code,
                "body": r.text[:300],
            }
            return out
    except Exception as exc:
        out["steps"]["oidc_exchange"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        return out

    # 3. Cross-workspace MLflow API call as SP
    try:
        r = httpx.post(
            f"{out['target_host']}/api/2.0/mlflow/experiments/search",
            headers={"Authorization": f"Bearer {sp_token}", "Content-Type": "application/json"},
            json={"max_results": 5},
            timeout=30,
        )
        body = r.text[:500]
        if r.status_code == 200:
            data = r.json()
            exps = data.get("experiments", [])
            out["steps"]["mlflow_call"] = {
                "ok": True,
                "status": 200,
                "experiment_count": len(exps),
                "first_experiment_name": (exps[0].get("name", "") if exps else None),
            }
        else:
            out["steps"]["mlflow_call"] = {
                "ok": False,
                "status": r.status_code,
                "body": body,
            }
    except Exception as exc:
        out["steps"]["mlflow_call"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    return out
