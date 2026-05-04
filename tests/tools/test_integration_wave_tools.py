"""Focused tests for integration-wave tool slices."""

from __future__ import annotations

from typing import Any

from app.tools.AzureMonitorLogsTool import query_azure_monitor_logs
from app.tools.BitbucketSearchCodeTool import _resolve_config
from app.tools.LogsApiRawLogsTool import query_logs_api_rawlogs
from app.tools.OpenObserveLogsTool import query_openobserve_logs
from app.tools.OpenSearchAnalyticsTool import query_opensearch_analytics
from app.tools.SnowflakeQueryHistoryTool import query_snowflake_history

_LOGS_API_SAMPLE_QUERY = "Was nse bhav copy uploaded on time today?"


class _MockResponse:
    def __init__(self, payload: Any, *, json_error: Exception | None = None) -> None:
        self._payload = payload
        self._json_error = json_error

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        if self._json_error is not None:
            raise self._json_error
        return self._payload


class _MockHttpClient:
    def __init__(
        self,
        captured: dict[str, Any],
        payloads: list[Any] | None = None,
        exception: Exception | None = None,
        json_error: Exception | None = None,
    ) -> None:
        self._captured = captured
        self._payloads = list(payloads or [])
        self._exception = exception
        self._json_error = json_error

    def __enter__(self) -> _MockHttpClient:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        _ = (exc_type, exc, tb)

    def post(
        self,
        url: str,
        json: dict[str, Any],
        headers: dict[str, str],
    ) -> _MockResponse:
        self._captured["url"] = url
        self._captured.setdefault("payloads", []).append(json)
        self._captured["headers"] = headers
        if self._exception is not None:
            raise self._exception
        payload = self._payloads.pop(0) if self._payloads else {"lines": []}
        return _MockResponse(payload, json_error=self._json_error)


def test_bitbucket_resolve_config_accepts_routed_instance_metadata() -> None:
    config = _resolve_config(
        "acme",
        "bb-user",
        "bb-pass",
        "https://api.bitbucket.org/2.0/",
        40,
        "bb-1",
    )

    assert config is not None
    assert config.workspace == "acme"
    assert config.base_url == "https://api.bitbucket.org/2.0"
    assert config.max_results == 40
    assert config.integration_id == "bb-1"


def test_snowflake_tool_enforces_bounded_limit(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_post(
        url: str, headers: dict[str, str], json: dict[str, Any], timeout: float
    ) -> _MockResponse:
        captured["url"] = url
        captured["statement"] = json["statement"]
        captured["timeout"] = timeout
        return _MockResponse({"data": [{"id": idx} for idx in range(20)]})

    monkeypatch.setattr("app.tools.SnowflakeQueryHistoryTool.httpx.post", _fake_post)

    result = query_snowflake_history(
        account_identifier="xy12345.us-east-1",
        token="sf-token",
        query="SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())",
        limit=500,
        max_results=6,
    )

    assert "LIMIT 6" in captured["statement"].upper()
    assert result["available"] is True
    assert len(result["rows"]) == 6


def test_snowflake_tool_requires_token() -> None:
    result = query_snowflake_history(
        account_identifier="xy12345.us-east-1",
        user="service-user",
        password="secret",
    )

    assert result["available"] is False
    assert result["error"] == "Missing Snowflake token."


def test_azure_tool_enforces_bounded_take_clause(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_post(
        url: str, headers: dict[str, str], json: dict[str, Any], timeout: float
    ) -> _MockResponse:
        captured["url"] = url
        captured["query"] = json["query"]
        return _MockResponse(
            {
                "tables": [
                    {
                        "columns": [{"name": "TimeGenerated"}, {"name": "Message"}],
                        "rows": [[f"t{idx}", f"message-{idx}"] for idx in range(10)],
                    }
                ]
            }
        )

    monkeypatch.setattr("app.tools.AzureMonitorLogsTool.httpx.post", _fake_post)

    result = query_azure_monitor_logs(
        workspace_id="workspace-1",
        access_token="azure-token",
        query="AppTraces | order by TimeGenerated desc",
        limit=999,
        max_results=3,
    )

    assert "take 3" in captured["query"].lower()
    assert result["available"] is True
    assert len(result["rows"]) == 3


def test_openobserve_tool_caps_size_and_output(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_post(
        url: str, headers: dict[str, str], json: dict[str, Any], timeout: float
    ) -> _MockResponse:
        captured["url"] = url
        captured["size"] = json["size"]
        captured["sql"] = json["query"]["sql"]
        return _MockResponse({"hits": [{"message": f"m{idx}"} for idx in range(12)]})

    monkeypatch.setattr("app.tools.OpenObserveLogsTool.httpx.post", _fake_post)

    result = query_openobserve_logs(
        base_url="https://openobserve.example.invalid",
        org="acme",
        api_token="oo-token",
        limit=1000,
        max_results=4,
    )

    assert captured["size"] == 4
    assert (
        captured["sql"]
        == "SELECT * FROM \"default\" WHERE level = 'error' ORDER BY _timestamp DESC"
    )
    assert result["available"] is True
    assert len(result["records"]) == 4


def test_opensearch_tool_caps_limit_before_client_query(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_search_logs(
        self: Any,
        query: str = "*",
        time_range_minutes: int = 60,
        limit: int = 50,
        index_pattern: str | None = None,
        timestamp_field: str = "@timestamp",
    ) -> dict[str, Any]:
        _ = (query, time_range_minutes, index_pattern, timestamp_field)
        captured["limit"] = limit
        return {"success": True, "logs": [{"message": f"log-{idx}"} for idx in range(12)]}

    monkeypatch.setattr(
        "app.tools.OpenSearchAnalyticsTool.ElasticsearchClient.search_logs",
        _fake_search_logs,
    )

    result = query_opensearch_analytics(
        url="https://opensearch.example.invalid",
        query="error",
        limit=500,
        max_results=5,
    )

    assert captured["limit"] == 5
    assert result["available"] is True
    assert len(result["logs"]) == 5


def test_logs_api_tool_uses_normalized_query_attempts(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_client(*args: Any, **kwargs: Any) -> _MockHttpClient:
        _ = (args, kwargs)
        return _MockHttpClient(
            captured,
            payloads=[
                {"lines": []},
                {"lines": [{"message": f"m{idx}"} for idx in range(10)]},
            ],
        )

    monkeypatch.setattr("app.integrations.logs_api.httpx.Client", _fake_client)

    result = query_logs_api_rawlogs(
        base_url="https://logs-api.example.invalid",
        bearer_token="secret-token",
        logs_topic="payments",
        application_name="payments-api",
        query=_LOGS_API_SAMPLE_QUERY,
        limit=1000,
        max_results=4,
    )

    payloads = captured["payloads"]

    assert captured["url"] == "https://logs-api.example.invalid/api/v1/rawlogs"
    assert captured["headers"]["Authorization"] == "Bearer secret-token"
    assert len(payloads) == 2
    assert isinstance(payloads[0]["from"], str)
    assert isinstance(payloads[0]["to"], str)
    assert payloads[0]["topic"] == "payments"
    assert payloads[0]["application_name"] == "payments-api"
    assert payloads[0]["search_keyword"] == "bnNlIEFORCBiaGF2IEFORCB1cGxvYWQ="
    assert payloads[0]["mode"] == "NORMAL"
    assert payloads[1]["search_keyword"] == "bnNlIEFORCBiaGF2"
    assert result["available"] is True
    assert result["query"] == _LOGS_API_SAMPLE_QUERY
    assert result["search_query_used"] == "nse AND bhav"
    assert result["search_queries_attempted"] == ["nse AND bhav AND upload", "nse AND bhav"]
    assert result["search_attempt_count"] == 2
    assert result["search_fallback_applied"] is True
    assert len(result["lines"]) == 4


def test_logs_api_tool_skips_fallback_when_first_attempt_has_lines(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_client(*args: Any, **kwargs: Any) -> _MockHttpClient:
        _ = (args, kwargs)
        return _MockHttpClient(captured, payloads=[{"lines": [{"message": "m1"}]}])

    monkeypatch.setattr("app.integrations.logs_api.httpx.Client", _fake_client)

    result = query_logs_api_rawlogs(
        base_url="https://logs-api.example.invalid",
        bearer_token="secret-token",
        logs_topic="payments",
        application_name="payments-api",
        query=_LOGS_API_SAMPLE_QUERY,
    )

    assert len(captured["payloads"]) == 1
    assert result["available"] is True
    assert result["query"] == _LOGS_API_SAMPLE_QUERY


def test_logs_api_tool_skips_fallback_on_transport_error(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_client(*args: Any, **kwargs: Any) -> _MockHttpClient:
        _ = (args, kwargs)
        return _MockHttpClient(captured, exception=RuntimeError("boom"))

    monkeypatch.setattr("app.integrations.logs_api.httpx.Client", _fake_client)

    result = query_logs_api_rawlogs(
        base_url="https://logs-api.example.invalid",
        bearer_token="secret-token",
        query=_LOGS_API_SAMPLE_QUERY,
    )

    assert len(captured["payloads"]) == 1
    assert result["available"] is False
    assert result["error"] == "boom"


def test_logs_api_tool_skips_fallback_on_malformed_response(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_client(*args: Any, **kwargs: Any) -> _MockHttpClient:
        _ = (args, kwargs)
        return _MockHttpClient(captured, payloads=[{"unexpected": []}])

    monkeypatch.setattr("app.integrations.logs_api.httpx.Client", _fake_client)

    result = query_logs_api_rawlogs(
        base_url="https://logs-api.example.invalid",
        bearer_token="secret-token",
        query=_LOGS_API_SAMPLE_QUERY,
    )

    assert len(captured["payloads"]) == 1
    assert result["available"] is False
    assert "Malformed logs API response" in result["error"]


def test_logs_api_tool_skips_fallback_on_malformed_list_entries(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_client(*args: Any, **kwargs: Any) -> _MockHttpClient:
        _ = (args, kwargs)
        return _MockHttpClient(captured, payloads=[{"lines": ["bad-entry"]}])

    monkeypatch.setattr("app.integrations.logs_api.httpx.Client", _fake_client)

    result = query_logs_api_rawlogs(
        base_url="https://logs-api.example.invalid",
        bearer_token="secret-token",
        query=_LOGS_API_SAMPLE_QUERY,
    )

    assert len(captured["payloads"]) == 1
    assert result["available"] is False
    assert "Malformed logs API response" in result["error"]


def test_logs_api_tool_prefers_explicit_window_over_relative_minutes(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_client(*args: Any, **kwargs: Any) -> _MockHttpClient:
        _ = (args, kwargs)
        return _MockHttpClient(captured, payloads=[{"lines": [{"message": "m1"}]}])

    monkeypatch.setattr("app.integrations.logs_api.httpx.Client", _fake_client)

    result = query_logs_api_rawlogs(
        base_url="https://logs-api.example.invalid",
        bearer_token="secret-token",
        logs_topic="payments",
        application_name="payments-api",
        query=_LOGS_API_SAMPLE_QUERY,
        time_range_minutes=60,
        window_start="2026-04-19T18:30:00+00:00",
        window_end="2026-04-20T18:30:00+00:00",
    )

    payload = captured["payloads"][0]
    assert payload["from"] == "1776623400"
    assert payload["to"] == "1776709800"
    assert result["available"] is True


def test_logs_api_tool_prefers_high_signal_identifier_over_generic_terms(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _fake_client(*args: Any, **kwargs: Any) -> _MockHttpClient:
        _ = (args, kwargs)
        return _MockHttpClient(captured, payloads=[{"lines": [{"message": "m1"}]}])

    monkeypatch.setattr("app.integrations.logs_api.httpx.Client", _fake_client)

    query = "why the trading balance was not updated on 20th april for 889F5943F00EBA7"
    result = query_logs_api_rawlogs(
        base_url="https://logs-api.example.invalid",
        bearer_token="secret-token",
        logs_topic="ledger-topic",
        application_name="ledger-service-prod",
        query=query,
    )

    payload = captured["payloads"][0]
    assert payload["search_keyword"] == "ODg5RjU5NDNGMDBFQkE3"
    assert result["search_query_used"] == "889F5943F00EBA7"
    assert result["search_queries_attempted"] == ["889F5943F00EBA7"]
    assert result["available"] is True
