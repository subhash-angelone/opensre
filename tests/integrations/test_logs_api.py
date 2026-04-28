from __future__ import annotations

import os
import sys
from datetime import UTC, datetime, timedelta

import pytest

from app.integrations.logs_api import RawLogsApiBackend
from app.integrations.verify import resolve_effective_integrations


def test_resolve_effective_integrations_normalizes_logs_api_store_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.catalog.load_integrations",
        lambda: [
            {
                "id": "logs-api-local",
                "service": "logs_api",
                "status": "active",
                "credentials": {
                    "base_url": " https://logs-api.example.invalid/ ",
                    "bearer_token": "Bearer store-token",
                    "logs_topic": "payments",
                    "application_name": "payments-api",
                    "timeout_seconds": 15,
                    "max_results": 25,
                },
            }
        ],
    )

    effective = resolve_effective_integrations()

    assert effective["logs_api"]["source"] == "local store"
    assert effective["logs_api"]["config"] == {
        "base_url": "https://logs-api.example.invalid",
        "bearer_token": "store-token",
        "logs_topic": "payments",
        "application_name": "payments-api",
        "timeout_seconds": 15.0,
        "max_results": 25,
        "integration_id": "logs-api-local",
    }


def _required_logs_api_env() -> dict[str, str]:
    keys = (
        "LOGS_API_BASE_URL",
        "LOGS_API_BEARER_TOKEN",
        "LOGS_API_TOPIC",
        "LOGS_API_APPLICATION_NAME",
    )
    values = {key: (os.getenv(key) or "").strip() for key in keys}
    missing = [key for key, value in values.items() if not value]
    if missing:
        pytest.skip(f"Live logs API test requires env vars: {', '.join(missing)}")
    return values


@pytest.mark.integration
@pytest.mark.e2e
def test_raw_logs_api_backend_live_call_returns_parseable_lines_or_data() -> None:
    env = _required_logs_api_env()
    query = (os.getenv("LOGS_API_QUERY") or "error").strip() or "error"
    backend = RawLogsApiBackend(
        base_url=env["LOGS_API_BASE_URL"],
        bearer_token=env["LOGS_API_BEARER_TOKEN"],
        timeout_seconds=float((os.getenv("LOGS_API_TIMEOUT_SECONDS") or "15").strip() or "15"),
    )
    end = datetime.now(UTC)
    start = end - timedelta(
        minutes=int((os.getenv("LOGS_API_TIME_RANGE_MINUTES") or "60").strip() or "60")
    )

    result = backend.search(
        logs_topic=env["LOGS_API_TOPIC"],
        application_name=env["LOGS_API_APPLICATION_NAME"],
        start=start,
        end=end,
        free_text_query=query,
        limit=int((os.getenv("LOGS_API_LIMIT") or "20").strip() or "20"),
    )

    if result.get("error") and any(
        marker in str(result["error"]).lower()
        for marker in (
            "nodename nor servname provided",
            "name or service not known",
            "temporary failure in name resolution",
            "connection refused",
            "network is unreachable",
            "timed out",
        )
    ):
        pytest.skip(f"Live logs API request could not reach endpoint: {result['error']}")

    assert "error" not in result, result.get("error")
    assert result["logs_topic"] == env["LOGS_API_TOPIC"]
    assert result["application_name"] == env["LOGS_API_APPLICATION_NAME"]
    assert result["query"] == query
    assert isinstance(result.get("search_queries_attempted"), list)
    assert result.get("search_attempt_count") == len(result.get("search_queries_attempted", []))
    assert "window_start" in result
    assert "window_end" in result
    assert isinstance(result.get("lines"), list)
    assert all(isinstance(line, dict) for line in result["lines"])


def test_raw_logs_api_backend_accepts_nested_data_response_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx
    from app.integrations import logs_api as logs_api_module

    real_httpx_client = httpx.Client

    def _handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": {"response": [{"message": "nested-line"}]}},
        )

    def _fake_client(*args: object, **kwargs: object) -> httpx.Client:
        return real_httpx_client(
            transport=httpx.MockTransport(_handler),
            timeout=kwargs.get("timeout"),
        )

    monkeypatch.setattr(logs_api_module.httpx, "Client", _fake_client)

    backend = RawLogsApiBackend(
        base_url="https://logs-api.example.invalid",
        bearer_token="secret-token",
    )
    end = datetime.now(UTC)

    result = backend.search(
        logs_topic="payments",
        application_name="payments-api",
        start=end - timedelta(minutes=15),
        end=end,
        free_text_query="error",
        limit=10,
    )

    assert "error" not in result, result.get("error")
    assert len(result["lines"]) == 1
    assert result["lines"][0]["message"] == "nested-line"
    assert result["lines"][0]["message_text"] == "nested-line"
    assert result["lines"][0]["message_json"] is None
    assert result["lines"][0]["tags_json"] is None


def test_raw_logs_api_backend_normalizes_embedded_message_and_tags_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx
    from app.integrations import logs_api as logs_api_module

    real_httpx_client = httpx.Client

    def _handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "response": [
                        {
                            "time": "2026-04-28T00:00:00.086Z",
                            "message": (
                                '{"level":"info","time":"2026-04-28T05:30:00+05:30",'
                                '"caller":"/go/src/app/masters/bhav_copy/task.go:118",'
                                '"message":"records already uploaded for, , NSE, FO"}'
                            ),
                            "tags": (
                                '{"application_name":"infinitrade-portal-masters-prod",'
                                '"container_id":"c-1","topic":"aws-prod-ecs-infinitrade-portal"}'
                            ),
                        }
                    ]
                }
            },
        )

    def _fake_client(*args: object, **kwargs: object) -> httpx.Client:
        return real_httpx_client(
            transport=httpx.MockTransport(_handler),
            timeout=kwargs.get("timeout"),
        )

    monkeypatch.setattr(logs_api_module.httpx, "Client", _fake_client)

    backend = RawLogsApiBackend(
        base_url="https://logs-api.example.invalid",
        bearer_token="secret-token",
    )
    end = datetime.now(UTC)

    result = backend.search(
        logs_topic="payments",
        application_name="payments-api",
        start=end - timedelta(minutes=15),
        end=end,
        free_text_query="error",
        limit=10,
    )

    line = result["lines"][0]
    assert line["message_text"] == "records already uploaded for, , NSE, FO"
    assert line["log_level"] == "info"
    assert line["log_time"] == "2026-04-28T05:30:00+05:30"
    assert line["caller"] == "/go/src/app/masters/bhav_copy/task.go:118"
    assert line["application_name"] == "infinitrade-portal-masters-prod"
    assert line["topic"] == "aws-prod-ecs-infinitrade-portal"
    assert line["container_id"] == "c-1"
    assert line["message_json"]["message"] == "records already uploaded for, , NSE, FO"
    assert line["tags_json"]["application_name"] == "infinitrade-portal-masters-prod"


def test_raw_logs_api_backend_treats_null_response_container_as_empty_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx
    from app.integrations import logs_api as logs_api_module

    real_httpx_client = httpx.Client

    def _handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"response": None}})

    def _fake_client(*args: object, **kwargs: object) -> httpx.Client:
        return real_httpx_client(
            transport=httpx.MockTransport(_handler),
            timeout=kwargs.get("timeout"),
        )

    monkeypatch.setattr(logs_api_module.httpx, "Client", _fake_client)

    backend = RawLogsApiBackend(
        base_url="https://logs-api.example.invalid",
        bearer_token="secret-token",
    )
    end = datetime.now(UTC)

    result = backend.search(
        logs_topic="payments",
        application_name="payments-api",
        start=end - timedelta(minutes=15),
        end=end,
        free_text_query="error",
        limit=10,
    )

    assert "error" not in result, result.get("error")
    assert result["lines"] == []


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-s", "-v"], plugins=[]))
