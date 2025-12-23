from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import psycopg
from pydantic import SecretStr


DDL = """
create table if not exists event_receipts (
  event_id uuid primary key,
  event_type text not null,
  subject text not null,
  received_at timestamptz not null default now(),
  document_id text not null,
  pipeline_version text not null,
  content_fingerprint text not null,
  source text not null,
  source_uri text not null,
  payload jsonb not null,
  prefect_deployment_name text,
  prefect_deployment_id uuid,
  prefect_flow_run_id uuid,
  triggered_at timestamptz,
  trigger_error text
);

create index if not exists idx_event_receipts_doc_fingerprint
  on event_receipts (document_id, content_fingerprint);
"""


@dataclass(frozen=True)
class PostgresConfig:
    dsn: Optional[str]
    host: Optional[str]
    port: int
    db: Optional[str]
    user: Optional[str]
    password: Optional[SecretStr]

    def build_dsn(self) -> str:
        if self.dsn:
            return self.dsn
        missing = []
        if not self.host:
            missing.append("POSTGRES_HOST")
        if not self.db:
            missing.append("POSTGRES_DB")
        if not self.user:
            missing.append("POSTGRES_USER")
        if not self.password:
            missing.append("POSTGRES_PASSWORD")
        if missing:
            raise ValueError(f"Missing Postgres config: {', '.join(missing)} (or set PG_DSN)")
        return (
            f"postgresql://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.db}"
        )


def ensure_schema(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        conn.execute(DDL)
        conn.commit()


def try_insert_receipt(
    dsn: str,
    *,
    event_id: str,
    event_type: str,
    subject: str,
    document_id: str,
    pipeline_version: str,
    content_fingerprint: str,
    source: str,
    source_uri: str,
    payload_json: str,
    prefect_deployment_name: str,
) -> bool:
    sql = """
    insert into event_receipts (
      event_id, event_type, subject,
      document_id, pipeline_version, content_fingerprint,
      source, source_uri, payload,
      prefect_deployment_name
    ) values (
      %(event_id)s, %(event_type)s, %(subject)s,
      %(document_id)s, %(pipeline_version)s, %(content_fingerprint)s,
      %(source)s, %(source_uri)s, %(payload)s::jsonb,
      %(prefect_deployment_name)s
    )
    on conflict (event_id) do nothing
    """
    with psycopg.connect(dsn) as conn:
        cur = conn.execute(
            sql,
            {
                "event_id": event_id,
                "event_type": event_type,
                "subject": subject,
                "document_id": document_id,
                "pipeline_version": pipeline_version,
                "content_fingerprint": content_fingerprint,
                "source": source,
                "source_uri": source_uri,
                "payload": payload_json,
                "prefect_deployment_name": prefect_deployment_name,
            },
        )
        inserted = cur.rowcount == 1
        conn.commit()
        return inserted


def mark_trigger_result(
    dsn: str,
    *,
    event_id: str,
    prefect_deployment_id: Optional[str],
    prefect_flow_run_id: Optional[str],
    trigger_error: Optional[str],
) -> None:
    sql = """
    update event_receipts
    set
      prefect_deployment_id = %(prefect_deployment_id)s::uuid,
      prefect_flow_run_id = %(prefect_flow_run_id)s::uuid,
      triggered_at = case when %(prefect_flow_run_id)s is not null then now() else null end,
      trigger_error = %(trigger_error)s::text
    where event_id = %(event_id)s::uuid
    """
    with psycopg.connect(dsn) as conn:
        conn.execute(
            sql,
            {
                "event_id": event_id,
                "prefect_deployment_id": prefect_deployment_id,
                "prefect_flow_run_id": prefect_flow_run_id,
                "trigger_error": trigger_error,
            },
        )
        conn.commit()
