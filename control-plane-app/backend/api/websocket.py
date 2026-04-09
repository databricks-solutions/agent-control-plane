"""WebSocket endpoint for real-time updates."""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import json
import asyncio
from datetime import datetime
from backend.services.query_service import get_recent_requests
from backend.services.agent_service import get_all_agents

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


@router.websocket("/ws/updates")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await manager.connect(websocket)
    try:
        while True:
            # Send updates every 5 seconds
            await asyncio.sleep(5)
            
            # Get recent requests
            recent_requests = query_service.get_recent_requests(10)
            
            # Get agent status
            agents = agent_service.get_all_agents(active_only=True)
            
            # Send update
            await websocket.send_json({
                "type": "update",
                "timestamp": datetime.now().isoformat(),
                "data": {
                    "recent_requests": recent_requests,
                    "agents": [{"agent_id": a.agent_id, "endpoint_status": a.endpoint_status} for a in agents]
                }
            })
    except WebSocketDisconnect:
        manager.disconnect(websocket)
