from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FlowNode(BaseModel):
    id: str
    type: str
    position: dict[str, float]
    data: dict[str, Any] = Field(default_factory=dict)


class FlowEdge(BaseModel):
    id: str
    source: str
    target: str
    animated: bool | None = None


class FlowPayload(BaseModel):
    nodes: list[FlowNode] = Field(default_factory=list)
    edges: list[FlowEdge] = Field(default_factory=list)
    target_node_id: str | None = None


class PostgresSourceConfig(BaseModel):
    host: str | None = None
    port: int = 5432
    db: str | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = None
    schema_name: str = Field(default='public', alias='schema')
    table: str
    limit: int = 200

    model_config = {
        'populate_by_name': True,
    }
