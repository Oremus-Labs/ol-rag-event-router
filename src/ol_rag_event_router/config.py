from __future__ import annotations

from typing import Optional

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pipeline_version: str = Field(alias="PIPELINE_VERSION")

    nats_url: str = Field(alias="NATS_URL")
    nats_subject: str = Field(default="docs.discovered", alias="NATS_SUBJECT")

    prefect_api_url: str = Field(alias="PREFECT_API_URL")
    prefect_deployment_name: str = Field(alias="PREFECT_DEPLOYMENT_NAME")
    prefect_deployment_names: Optional[str] = Field(default=None, alias="PREFECT_DEPLOYMENT_NAMES")

    pg_dsn: Optional[str] = Field(default=None, alias="PG_DSN")
    postgres_host: Optional[str] = Field(default=None, alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: Optional[str] = Field(default=None, alias="POSTGRES_DB")
    postgres_user: Optional[str] = Field(default=None, alias="POSTGRES_USER")
    postgres_password: Optional[SecretStr] = Field(default=None, alias="POSTGRES_PASSWORD")


def load_settings() -> Settings:
    return Settings()
