"""FastAPI routes that proxy Databricks MLflow REST APIs.

When OBO is enabled (User Authorization on the app), MLflow queries run as
the logged-in user so they can see experiments across workspaces they have
access to.  Without OBO, queries fall back to the app's service principal.

Cross-workspace support: pass ``workspace_id`` query param to target a
specific remote workspace, or ``workspace_id=all`` to fan out across all
registered workspaces.  Omitting it preserves backward-compatible behaviour
(current workspace only).

Cross-workspace queries prefer the Lakebase cache (populated by the scheduled
workflow) for speed and reliability.  Live fan-out is attempted as a fallback
when the cache is empty.
"""
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from typing import Optional
from backend.utils.auth import get_current_user
from backend.services import mlflow_service
from backend.database import execute_update

router = APIRouter(prefix="/mlflow", tags=["mlflow"], dependencies=[Depends(get_current_user)])


def _obo_token(request: Request) -> Optional[str]:
    """Extract OBO token from the request if present."""
    return request.headers.get("x-forwarded-access-token") or None


# ── Experiments ─────────────────────────────────────────────────

@router.get("/experiments")
async def list_experiments(
    request: Request,
    max_results: int = Query(10000, le=100000),
    workspace_id: Optional[str] = Query(None, description="Workspace ID, 'all' for all workspaces"),
):
    """List MLflow experiments, optionally cross-workspace."""
    try:
        token = _obo_token(request)
        if workspace_id == "all":
            # Read from Lakebase cache (populated by scheduled workflow)
            cached = mlflow_service.get_cached_experiments(None, max_results)
            # Also include current workspace live data
            current = mlflow_service.search_experiments(200, user_token=token)
            seen = set()
            merged = []
            # Cached cross-workspace data first, then current workspace
            for e in cached + current:
                eid = e.get("experiment_id", "")
                if eid and eid not in seen:
                    seen.add(eid)
                    merged.append(e)
            merged.sort(key=lambda e: int(e.get("last_update_time") or 0), reverse=True)
            return merged
        elif workspace_id:
            return mlflow_service.get_cached_experiments(workspace_id, max_results)
        else:
            return mlflow_service.search_experiments(max_results, user_token=token)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLflow API error: {e}")


@router.get("/experiments/{experiment_id}")
async def get_experiment(experiment_id: str):
    """Get a single experiment."""
    try:
        exp = mlflow_service.get_experiment(experiment_id)
        if not exp:
            raise HTTPException(status_code=404, detail="Experiment not found")
        return exp
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLflow API error: {e}")


# ── Runs ────────────────────────────────────────────────────────

@router.get("/runs")
async def list_runs(
    request: Request,
    experiment_ids: Optional[str] = Query(None, description="Comma-separated experiment IDs"),
    filter_string: str = Query("", description="MLflow filter string"),
    max_results: int = Query(10000, le=100000),
    workspace_id: Optional[str] = Query(None, description="Workspace ID, 'all' for all workspaces"),
):
    """Search MLflow runs, optionally cross-workspace."""
    try:
        token = _obo_token(request)
        if workspace_id == "all":
            # Read from Lakebase cache (populated by scheduled workflow)
            return mlflow_service.get_cached_runs(None, max_results)
        elif workspace_id:
            return mlflow_service.get_cached_runs(workspace_id, max_results)
        else:
            exp_list = experiment_ids.split(",") if experiment_ids else None
            return mlflow_service.search_runs(exp_list, filter_string, max_results)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLflow API error: {e}")


# ── Traces ──────────────────────────────────────────────────────

@router.get("/traces")
async def list_traces(
    request: Request,
    experiment_ids: Optional[str] = Query(None),
    filter_string: str = Query(""),
    max_results: int = Query(10000, le=100000),
    workspace_id: Optional[str] = Query(None, description="Workspace ID, 'all' for all workspaces"),
):
    """Search MLflow traces. All data comes from Lakebase cache (populated by scheduled workflow)."""
    try:
        if workspace_id == "all" or not workspace_id:
            # All workspaces (including current) — read everything from cache
            return mlflow_service.get_cached_traces(None, max_results)
        else:
            # Specific workspace
            return mlflow_service.get_cached_traces(workspace_id, max_results)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLflow API error: {e}")


@router.get("/traces/{request_id}")
async def get_trace_detail(
    request_id: str,
    request: Request,
    workspace_id: Optional[str] = Query(None, description="Workspace ID for cross-workspace trace lookup"),
):
    """Get full trace detail with parsed metadata."""
    try:
        token = _obo_token(request)
        if workspace_id and token:
            detail = mlflow_service.get_trace_detail_for_workspace(
                request_id, workspace_id, user_token=token,
            )
        else:
            detail = mlflow_service.get_trace_detail(
                request_id, user_token=token,
            )
        if not detail:
            raise HTTPException(status_code=404, detail="Trace not found")
        return detail
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLflow API error: {e}")


# ── Models ──────────────────────────────────────────────────────

@router.get("/models")
async def list_models(
    request: Request,
    max_results: int = Query(500, le=10000),
    workspace_id: Optional[str] = Query(None, description="Workspace ID, 'all' for all workspaces"),
):
    """Search Unity Catalog registered models, optionally cross-workspace."""
    try:
        token = _obo_token(request)
        if workspace_id == "all" or workspace_id:
            # Models are only available on the current workspace via REST
            # Cross-workspace model registry not supported (no system table, OBO scope too narrow)
            return mlflow_service.search_registered_models(max_results)
        else:
            return mlflow_service.search_registered_models(max_results)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLflow API error: {e}")


@router.get("/models/{name:path}/versions")
async def list_model_versions(name: str, max_results: int = Query(20, le=100)):
    """Search versions for a registered model."""
    try:
        return mlflow_service.search_model_versions(name, max_results)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLflow API error: {e}")


# ── Cross-workspace: metadata & cache ───────────────────────────

@router.get("/workspaces")
async def list_observability_workspaces():
    """Return workspaces that have cached MLflow observability data."""
    try:
        return mlflow_service.get_observability_workspaces()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLflow API error: {e}")


@router.post("/refresh-cache")
async def refresh_cache(request: Request):
    """Trigger a cross-workspace observability cache refresh via system tables.

    This is the only path that queries system.mlflow.* from the app.
    Regular page loads read from Lakebase cache (populated by the scheduled workflow).
    """
    try:
        counts = {"experiments": 0, "runs": 0}

        # Query system tables and upsert to Lakebase cache
        experiments = mlflow_service.search_experiments_system_tables(5000)
        if experiments:
            for exp in experiments:
                try:
                    execute_update(
                        """INSERT INTO observability_experiments
                           (experiment_id, workspace_id, name, lifecycle_stage, last_update_time, artifact_location, data_source, last_synced)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                           ON CONFLICT (workspace_id, experiment_id) DO UPDATE SET
                               name = EXCLUDED.name, lifecycle_stage = EXCLUDED.lifecycle_stage,
                               last_update_time = EXCLUDED.last_update_time, artifact_location = EXCLUDED.artifact_location,
                               data_source = EXCLUDED.data_source, last_synced = NOW()""",
                        (exp.get("experiment_id"), exp.get("workspace_id"), exp.get("name"),
                         exp.get("lifecycle_stage"), int(exp.get("last_update_time") or 0),
                         exp.get("artifact_location"), exp.get("data_source", "system_table")),
                    )
                    counts["experiments"] += 1
                except Exception:
                    pass

        runs = mlflow_service.search_runs_system_tables(5000)
        if runs:
            for r in runs:
                try:
                    execute_update(
                        """INSERT INTO observability_runs
                           (run_id, workspace_id, experiment_id, status, start_time, end_time, user_id, run_name, data_source, last_synced)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                           ON CONFLICT (workspace_id, run_id) DO UPDATE SET
                               experiment_id = EXCLUDED.experiment_id, status = EXCLUDED.status,
                               start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time,
                               user_id = EXCLUDED.user_id, run_name = EXCLUDED.run_name,
                               data_source = EXCLUDED.data_source, last_synced = NOW()""",
                        (r.get("run_id"), r.get("workspace_id"), r.get("experiment_id"),
                         r.get("status"), int(r.get("start_time") or 0),
                         int(r.get("end_time") or 0) if r.get("end_time") else None,
                         r.get("user_id"), r.get("run_name"), r.get("data_source", "system_table")),
                    )
                    counts["runs"] += 1
                except Exception:
                    pass

        return {"status": "ok", "cached": counts}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Cache refresh error: {e}")
