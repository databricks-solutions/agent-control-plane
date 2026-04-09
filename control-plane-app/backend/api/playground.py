"""API routes for the Agent Playground – chat with serving endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query
from backend.utils.auth import get_current_user
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from backend.services import playground_service

router = APIRouter(prefix="/playground", tags=["playground"], dependencies=[Depends(get_current_user)])


# ── Request / response models ────────────────────────────────────

class ChatRequest(BaseModel):
    endpoint_name: str
    agent_name: Optional[str] = None
    session_id: Optional[str] = None
    message: str
    max_tokens: int = 1024
    temperature: float = 0.7
    app_url: Optional[str] = None  # set for Databricks App agents


class ChatResponse(BaseModel):
    session_id: str
    response: str
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    latency_ms: Optional[int] = None
    model: Optional[str] = None
    error: Optional[str] = None


# ── Endpoints ────────────────────────────────────────────────────

@router.get("/endpoints")
def list_queryable_endpoints():
    """Return serving endpoints the app can query (READY + CAN_QUERY permission)."""
    return playground_service.list_queryable_endpoints()


@router.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    """Send a message to a serving endpoint and persist the conversation.

    If session_id is omitted a new session is created automatically.
    """
    # 1. Resolve or create session
    if req.session_id:
        session = playground_service.get_session(req.session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        session_id = req.session_id
    else:
        session = playground_service.create_session(req.endpoint_name, req.agent_name)
        session_id = session["session_id"]

    # 2. Persist user message
    playground_service.save_message(session_id, "user", req.message)

    # Auto-title from first user message
    existing_msgs = playground_service.get_session_messages(session_id)
    user_msgs = [m for m in existing_msgs if m["role"] == "user"]
    if len(user_msgs) == 1:
        playground_service.update_session_title(session_id, req.message)

    # 3. Build conversation history for the endpoint
    messages: List[Dict[str, str]] = []
    for m in existing_msgs:
        if m["role"] in ("user", "assistant"):
            messages.append({"role": m["role"], "content": m["content"]})

    # 4. Call serving endpoint or app
    result = playground_service.query_endpoint(
        endpoint_name=req.endpoint_name,
        messages=messages,
        max_tokens=req.max_tokens,
        temperature=req.temperature,
        app_url=req.app_url,
    )

    if "error" in result:
        # Persist error as a message too for debugging
        playground_service.save_message(
            session_id, "error", result["error"],
            latency_ms=result.get("latency_ms"),
        )
        return ChatResponse(
            session_id=session_id,
            response="",
            latency_ms=result.get("latency_ms"),
            error=result["error"],
        )

    # 5. Persist assistant response
    playground_service.save_message(
        session_id,
        "assistant",
        result["content"],
        input_tokens=result.get("input_tokens"),
        output_tokens=result.get("output_tokens"),
        total_tokens=result.get("total_tokens"),
        latency_ms=result.get("latency_ms"),
        model=result.get("model"),
    )

    return ChatResponse(
        session_id=session_id,
        response=result["content"],
        input_tokens=result.get("input_tokens"),
        output_tokens=result.get("output_tokens"),
        total_tokens=result.get("total_tokens"),
        latency_ms=result.get("latency_ms"),
        model=result.get("model"),
    )


@router.get("/sessions")
def list_sessions(limit: int = Query(default=50, ge=1, le=200)):
    """List recent playground sessions."""
    return playground_service.list_sessions(limit)


@router.get("/sessions/{session_id}")
def get_session(session_id: str):
    """Get a session with all its messages."""
    session = playground_service.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    messages = playground_service.get_session_messages(session_id)
    return {**session, "messages": messages}


@router.delete("/sessions/{session_id}")
def delete_session(session_id: str):
    """Delete a session and all its messages."""
    deleted = playground_service.delete_session(session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"status": "ok", "message": f"Session {session_id} deleted"}
