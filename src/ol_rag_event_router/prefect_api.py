from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import httpx


@dataclass(frozen=True)
class PrefectDeployment:
    id: str
    name: str


def resolve_deployment_id(prefect_api_url: str, deployment_name: str) -> PrefectDeployment:
    deployments = resolve_deployments(prefect_api_url, [deployment_name])
    return deployments[0]


def resolve_deployments(
    prefect_api_url: str,
    deployment_names: list[str],
) -> list[PrefectDeployment]:
    names = [name.strip() for name in deployment_names if name.strip()]
    if not names:
        raise RuntimeError("No Prefect deployment names supplied")

    url = prefect_api_url.rstrip("/") + "/deployments/filter"
    with httpx.Client(timeout=10.0) as client:
        r = client.post(url, json={"limit": 200})
        r.raise_for_status()
        deployments = r.json()

    by_name = {}
    for dep in deployments:
        name = dep.get("name")
        if not name:
            continue
        by_name[name] = PrefectDeployment(id=dep["id"], name=name)

    missing = [name for name in names if name not in by_name]
    if missing:
        raise RuntimeError(f"Prefect deployments not found: {', '.join(missing)}")

    return [by_name[name] for name in names]


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
