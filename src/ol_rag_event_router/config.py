from __future__ import annotations

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pipeline_version: str = Field(alias="PIPELINE_VERSION")

    nats_url: str = Field(alias="NATS_URL")

    prefect_api_url: str = Field(alias="PREFECT_API_URL")

    pg_dsn: Optional[str] = Field(default=None, alias="PG_DSN")


def load_settings() -> Settings:
    return Settings()

