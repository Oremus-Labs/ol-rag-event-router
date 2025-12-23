from ol_rag_event_router.config import Settings


def test_settings_parses_required_fields() -> None:
    settings = Settings.model_validate(
        {
            "PIPELINE_VERSION": "v1",
            "NATS_URL": "nats://localhost:4222",
            "PREFECT_API_URL": "http://localhost:4200/api",
        }
    )
    assert settings.pipeline_version == "v1"

