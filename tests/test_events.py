from datetime import datetime, timezone
from uuid import uuid4

from ol_rag_event_router.events import DocsDiscoveredEvent, prefect_idempotency_key


def test_idempotency_key_is_stable() -> None:
    ev = DocsDiscoveredEvent(
        event_id=uuid4(),
        document_id="doc1",
        source="nextcloud",
        source_uri="https://example/doc1.pdf",
        content_fingerprint="abc123",
        pipeline_version="v1",
        discovered_at=datetime.now(timezone.utc),
    )
    assert prefect_idempotency_key(ev) == "v1:doc1:abc123"

