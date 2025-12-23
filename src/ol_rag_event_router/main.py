from __future__ import annotations

import asyncio
import json
import logging

from nats.aio.client import Client as NATS

from ol_rag_event_router.config import load_settings
from ol_rag_event_router.db import PostgresConfig, ensure_schema, mark_trigger_result, try_insert_receipt
from ol_rag_event_router.events import DocsDiscoveredEvent, prefect_idempotency_key
from ol_rag_event_router.prefect_api import resolve_deployment_id, safe_error, create_flow_run


def _setup_logger() -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return logging.getLogger("ol_rag_event_router")


async def main() -> None:
    log = _setup_logger()
    settings = load_settings()

    pg = PostgresConfig(
        dsn=settings.pg_dsn,
        host=settings.postgres_host,
        port=settings.postgres_port,
        db=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )
    pg_dsn = pg.build_dsn()

    ensure_schema(pg_dsn)
    deployment = resolve_deployment_id(settings.prefect_api_url, settings.prefect_deployment_name)
    log.info("Router starting: subject=%s deployment=%s", settings.nats_subject, deployment.name)

    nc = NATS()
    await nc.connect(servers=[settings.nats_url])

    async def on_msg(msg) -> None:  # noqa: ANN001
        subject = msg.subject
        raw = msg.data.decode("utf-8", errors="replace")

        def handle() -> None:
            try:
                event = DocsDiscoveredEvent.model_validate_json(raw)
            except Exception as e:  # noqa: BLE001
                log.error("Invalid event payload: %s", safe_error(e))
                return

            payload_json = json.dumps(event.model_dump(mode="json"))
            inserted = try_insert_receipt(
                pg_dsn,
                event_id=str(event.event_id),
                event_type=event.event_type,
                subject=subject,
                document_id=event.document_id,
                pipeline_version=event.pipeline_version,
                content_fingerprint=event.content_fingerprint,
                source=event.source,
                source_uri=event.source_uri,
                payload_json=payload_json,
                prefect_deployment_name=deployment.name,
            )
            if not inserted:
                log.info("Duplicate event_id ignored: %s", event.event_id)
                return

            idem = prefect_idempotency_key(event)
            try:
                run = create_flow_run(
                    settings.prefect_api_url,
                    deployment_id=deployment.id,
                    idempotency_key=idem,
                    parameters={
                        "event": event.model_dump(mode="json"),
                        "event_id": str(event.event_id),
                        "document_id": event.document_id,
                        "pipeline_version": event.pipeline_version,
                        "content_fingerprint": event.content_fingerprint,
                    },
                )
                flow_run_id = run.get("id")
                try:
                    mark_trigger_result(
                        pg_dsn,
                        event_id=str(event.event_id),
                        prefect_deployment_id=deployment.id,
                        prefect_flow_run_id=flow_run_id,
                        trigger_error=None,
                    )
                except Exception as e:  # noqa: BLE001
                    log.error("Failed to record trigger result: %s", safe_error(e))
                log.info("Triggered Prefect run: event_id=%s flow_run_id=%s", event.event_id, flow_run_id)
            except Exception as e:  # noqa: BLE001
                err = safe_error(e)
                try:
                    mark_trigger_result(
                        pg_dsn,
                        event_id=str(event.event_id),
                        prefect_deployment_id=deployment.id,
                        prefect_flow_run_id=None,
                        trigger_error=err,
                    )
                except Exception as e:  # noqa: BLE001
                    log.error("Failed to record trigger error: %s", safe_error(e))
                log.error("Failed to trigger Prefect: event_id=%s err=%s", event.event_id, err)

        await asyncio.to_thread(handle)

    await nc.subscribe(settings.nats_subject, cb=on_msg)
    log.info("Subscribed to %s", settings.nats_subject)
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
