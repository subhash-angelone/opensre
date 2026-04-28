from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import Environment
from app.webapp import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_health_ok_returns_200_and_payload(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("app.webapp._graph_loaded", lambda: True)
    monkeypatch.setattr("app.webapp._llm_configured", lambda: True)
    monkeypatch.setattr("app.webapp.get_version", lambda: "0.1.0")
    monkeypatch.setattr("app.webapp.get_environment", lambda: Environment.PRODUCTION)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "version": "0.1.0",
        "graph_loaded": True,
        "llm_configured": True,
        "env": "production",
    }


def test_health_unhealthy_returns_503_and_payload(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("app.webapp._graph_loaded", lambda: False)
    monkeypatch.setattr("app.webapp._llm_configured", lambda: True)
    monkeypatch.setattr("app.webapp.get_version", lambda: "0.1.0")
    monkeypatch.setattr("app.webapp.get_environment", lambda: Environment.DEVELOPMENT)

    response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "ok": False,
        "version": "0.1.0",
        "graph_loaded": False,
        "llm_configured": True,
        "env": "development",
    }


def test_health_payload_has_stable_keys(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("app.webapp._graph_loaded", lambda: True)
    monkeypatch.setattr("app.webapp._llm_configured", lambda: False)
    monkeypatch.setattr("app.webapp.get_version", lambda: "0.1.0")
    monkeypatch.setattr("app.webapp.get_environment", lambda: Environment.PRODUCTION)

    response = client.get("/health")

    assert sorted(response.json().keys()) == [
        "env",
        "graph_loaded",
        "llm_configured",
        "ok",
        "version",
    ]


def test_debug_logs_api_search_uses_effective_integration_defaults(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "app.webapp.resolve_effective_integrations",
        lambda: {
            "logs_api": {
                "source": "local store",
                "config": {
                    "base_url": "https://logs-api.example.invalid",
                    "bearer_token": "secret-token",
                    "logs_topic": "aws-prod-ecs-infinitrade-portal",
                    "application_name": "infinitrade-portal-masters-prod",
                    "max_results": 25,
                    "timeout_seconds": 12.0,
                    "integration_id": "logs-api-1",
                },
            }
        },
    )

    captured: dict[str, object] = {}

    def _fake_query_logs_api_rawlogs(**kwargs):
        captured.update(kwargs)
        return {"available": True, "lines": [{"message": "ok"}]}

    monkeypatch.setattr("app.webapp.query_logs_api_rawlogs", _fake_query_logs_api_rawlogs)

    response = client.post(
        "/debug/logs-api/search",
        json={"query": "bhav copy uploaded", "time_range_minutes": 180, "limit": 10},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["resolved"]["integration_source"] == "local store"
    assert body["resolved"]["base_url"] == "https://logs-api.example.invalid"
    assert body["resolved"]["logs_topic"] == "aws-prod-ecs-infinitrade-portal"
    assert body["resolved"]["application_name"] == "infinitrade-portal-masters-prod"
    assert captured["query"] == "bhav copy uploaded"
    assert captured["time_range_minutes"] == 180
    assert captured["limit"] == 10
    assert captured["integration_id"] == "logs-api-1"


def test_debug_logs_api_search_requires_configured_credentials(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("app.webapp.resolve_effective_integrations", lambda: {})

    response = client.post("/debug/logs-api/search", json={"query": "bhav copy uploaded"})

    assert response.status_code == 400
    assert response.json() == {"detail": "Missing logs_api base_url."}
