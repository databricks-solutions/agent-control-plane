"""Pydantic models for Agent entities."""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime


class AgentIn(BaseModel):
    """Input model for creating/updating an agent."""
    name: str
    type: str
    description: Optional[str] = None
    endpoint_name: Optional[str] = None
    endpoint_type: Optional[str] = None
    app_id: Optional[str] = None
    app_url: Optional[str] = None
    version: Optional[str] = "1.0"
    tags: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None
    is_active: bool = True


class AgentOut(BaseModel):
    """Complete output model for an agent."""
    agent_id: str
    name: str
    type: str
    description: Optional[str] = None
    endpoint_name: Optional[str] = None
    endpoint_type: Optional[str] = None
    endpoint_status: Optional[str] = None
    app_id: Optional[str] = None
    app_url: Optional[str] = None
    version: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None
    is_active: bool
    
    class Config:
        from_attributes = True


class AgentListOut(BaseModel):
    """Summary model for agent list (performance optimized)."""
    agent_id: str
    name: str
    type: str
    endpoint_type: Optional[str] = None
    endpoint_status: Optional[str] = None
    app_url: Optional[str] = None
    is_active: bool
    
    class Config:
        from_attributes = True


class AgentUpdate(BaseModel):
    """Model for updating agent fields."""
    name: Optional[str] = None
    description: Optional[str] = None
    endpoint_status: Optional[str] = None
    is_active: Optional[bool] = None
    tags: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None
