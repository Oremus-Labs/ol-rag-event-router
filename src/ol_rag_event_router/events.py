from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class DocsDiscoveredEvent(BaseModel):
    event_id: UUID
    event_type: str = Field(default="docs.discovered")
    document_id: str
    source: str
    source_uri: str
    content_fingerprint: str
    pipeline_version: str
    discovered_at: datetime
    hints: Optional[dict[str, Any]] = None


def prefect_idempotency_key(event: DocsDiscoveredEvent) -> str:
    return f"{event.pipeline_version}:{event.document_id}:{event.content_fingerprint}"

