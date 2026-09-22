"""WebSocket endpoint for real-time updates."""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import asyncio
from datetime import datetime

from backend.services.query_service import get_recent_requests
from backend.services.agent_service import get_all_agents
from backend.utils.auth import _SP_FALLBACK, _get_cached, _resolve_user
from backend.utils.access_scope import get_allowed_workspace_ids, sees_deploy_workspace

router = APIRouter()


class ConnectionManager:
    """Manages WebSocket connections."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients."""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)

        for conn in disconnected:
            self.disconnect(conn)


manager = ConnectionManager()


def _user_from_websocket(websocket: WebSocket):
    """Resolve the caller the same way HTTP routes do (OBO header, else SP)."""
    token = websocket.headers.get("x-forwarded-access-token", "")
    if not token:
        auth = websocket.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth[7:]
    if not token:
        return _SP_FALLBACK
    cached = _get_cached(token)
    if cached:
        return cached
    return _resolve_user(token)


@router.websocket("/ws/updates")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await manager.connect(websocket)
    try:
        user = _user_from_websocket(websocket)
        allowed = get_allowed_workspace_ids(user)
        live_ok = sees_deploy_workspace(allowed)
        while True:
            await asyncio.sleep(5)

            if live_ok:
                recent_requests = get_recent_requests(10)
                agents = get_all_agents(
                    active_only=True, allowed_workspace_ids=allowed,
                )
                agent_payload = [
                    {"agent_id": a.agent_id, "endpoint_status": a.endpoint_status}
                    for a in agents
                ]
            else:
                recent_requests = []
                agent_payload = []

            await websocket.send_json({
                "type": "update",
                "timestamp": datetime.now().isoformat(),
                "data": {
                    "recent_requests": recent_requests,
                    "agents": agent_payload,
                },
            })
    except WebSocketDisconnect:
        manager.disconnect(websocket)
