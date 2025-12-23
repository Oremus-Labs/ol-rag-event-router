from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import httpx


@dataclass(frozen=True)
class PrefectDeployment:
    id: str
    name: str


def resolve_deployment_id(prefect_api_url: str, deployment_name: str) -> PrefectDeployment:
    url = prefect_api_url.rstrip("/") + "/deployments/filter"
    with httpx.Client(timeout=10.0) as client:
        r = client.post(url, json={"limit": 200})
        r.raise_for_status()
        deployments = r.json()

    for dep in deployments:
        if dep.get("name") == deployment_name:
            return PrefectDeployment(id=dep["id"], name=dep["name"])
    raise RuntimeError(f"Prefect deployment not found: {deployment_name}")


def create_flow_run(
    prefect_api_url: str,
    *,
    deployment_id: str,
    idempotency_key: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    url = prefect_api_url.rstrip("/") + f"/deployments/{deployment_id}/create_flow_run"
    with httpx.Client(timeout=15.0) as client:
        r = client.post(
            url,
            json={
                "idempotency_key": idempotency_key,
                "parameters": parameters,
            },
        )
        r.raise_for_status()
        return r.json()


def safe_error(exc: Exception) -> str:
    msg = str(exc).replace("\n", " ").strip()
    return msg[:500]

