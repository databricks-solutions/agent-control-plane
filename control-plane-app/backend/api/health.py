"""API routes for system health."""
from fastapi import APIRouter
from backend.database import execute_one

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/status")
async def get_status():
    """Get system health status."""
    from datetime import datetime
    # Check database connection
    try:
        result = execute_one("SELECT 1 as check")
        db_status = "healthy" if result else "unhealthy"
    except Exception:
        db_status = "unhealthy"
    
    return {
        "status": "healthy" if db_status == "healthy" else "degraded",
        "database": db_status,
        "timestamp": datetime.now().isoformat()
    }


@router.get("/errors")
async def get_errors(limit: int = 50):
    """Get recent errors."""
    query = """
        SELECT request_id, agent_id, timestamp, status_code, error_message
        FROM request_logs
        WHERE status_code >= 400
        ORDER BY timestamp DESC
        LIMIT %s
    """
    from backend.database import execute_query
    errors = execute_query(query, (limit,))
    return {"data": errors, "meta": {"count": len(errors)}}
