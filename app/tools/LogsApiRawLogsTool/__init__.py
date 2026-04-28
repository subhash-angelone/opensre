"""Raw logs API search tool."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from app.integrations.logs_api import RawLogsApiBackend
from app.tools.tool_decorator import tool

_DEFAULT_MAX_RESULTS = 100
_MAX_HARD_LIMIT = 500
logger = logging.getLogger(__name__)


def _bounded_limit(limit: int, max_results: int) -> int:
    safe_max = max(1, min(max_results, _MAX_HARD_LIMIT))
    return max(1, min(limit, safe_max))


def _logs_api_available(sources: dict[str, dict[str, Any]]) -> bool:
    logs_api = sources.get("logs_api", {})
    return bool(
        logs_api.get("connection_verified")
        and str(logs_api.get("base_url", "")).strip()
        and str(logs_api.get("bearer_token", "")).strip()
    )


def _logs_api_extract_params(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    logs_api = sources["logs_api"]
    return {
        "base_url": str(logs_api.get("base_url", "")).strip(),
        "bearer_token": str(logs_api.get("bearer_token", "")).strip(),
        "logs_topic": str(logs_api.get("logs_topic", "")).strip(),
        "application_name": str(logs_api.get("application_name", "")).strip(),
        "query": str(logs_api.get("query", "")).strip(),
        "time_range_minutes": int(logs_api.get("time_range_minutes", 60) or 60),
        "limit": 50,
        "max_results": int(
            logs_api.get("max_results", _DEFAULT_MAX_RESULTS) or _DEFAULT_MAX_RESULTS
        ),
        "timeout_seconds": float(logs_api.get("timeout_seconds", 10.0) or 10.0),
        "integration_id": str(logs_api.get("integration_id", "")).strip(),
    }


@tool(
    name="query_logs_api_rawlogs",
    description="Search a rawlogs HTTP API for bounded error evidence.",
    source="logs_api",
    surfaces=("investigation", "chat"),
    requires=["base_url", "bearer_token"],
    input_schema={
        "type": "object",
        "properties": {
            "base_url": {"type": "string"},
            "bearer_token": {"type": "string"},
            "logs_topic": {"type": "string"},
            "application_name": {"type": "string"},
            "query": {"type": "string"},
            "time_range_minutes": {"type": "integer", "default": 60},
            "limit": {"type": "integer", "default": 50},
            "max_results": {"type": "integer", "default": 100},
            "timeout_seconds": {"type": "number", "default": 10.0},
            "integration_id": {"type": "string"},
        },
        "required": ["base_url", "bearer_token"],
    },
    is_available=_logs_api_available,
    extract_params=_logs_api_extract_params,
)
def query_logs_api_rawlogs(
    base_url: str,
    bearer_token: str,
    logs_topic: str = "",
    application_name: str = "",
    query: str = "",
    time_range_minutes: int = 60,
    limit: int = 50,
    max_results: int = _DEFAULT_MAX_RESULTS,
    timeout_seconds: float = 10.0,
    integration_id: str = "",
    **_kwargs: Any,
) -> dict[str, Any]:
    """Fetch bounded evidence from a raw logs API."""
    normalized_base = base_url.strip().rstrip("/")
    normalized_token = bearer_token.strip()
    if not normalized_base:
        return {"source": "logs_api", "available": False, "error": "Missing base_url.", "lines": []}
    if not normalized_token:
        return {
            "source": "logs_api",
            "available": False,
            "error": "Missing bearer_token.",
            "lines": [],
        }

    effective_limit = _bounded_limit(limit, max_results)
    end = datetime.now(UTC)
    start = end - timedelta(minutes=max(1, time_range_minutes))

    logger.info(
        "logs_api_tool_call base_url=%s topic=%s app=%s window_minutes=%d limit=%d timeout_seconds=%.1f integration_id=%s query=%s",
        normalized_base,
        logs_topic.strip(),
        application_name.strip(),
        max(1, time_range_minutes),
        effective_limit,
        max(1.0, timeout_seconds),
        integration_id,
        query.strip(),
    )

    backend = RawLogsApiBackend(
        base_url=normalized_base,
        bearer_token=normalized_token,
        timeout_seconds=max(1.0, timeout_seconds),
    )
    result = backend.search(
        logs_topic=logs_topic.strip(),
        application_name=application_name.strip(),
        start=start,
        end=end,
        free_text_query=query.strip(),
        limit=effective_limit,
    )

    if result.get("error"):
        logger.warning(
            "logs_api_tool_error base_url=%s topic=%s app=%s integration_id=%s error=%s",
            normalized_base,
            logs_topic.strip(),
            application_name.strip(),
            integration_id,
            result.get("error"),
        )
        return {
            "source": "logs_api",
            "available": False,
            "integration_id": integration_id,
            "logs_topic": logs_topic.strip(),
            "application_name": application_name.strip(),
            "query": query.strip(),
            "error": str(result.get("error")),
            "lines": [],
        }

    lines = [line for line in result.get("lines", []) if isinstance(line, dict)]
    logger.info(
        "logs_api_tool_success topic=%s app=%s integration_id=%s returned_lines=%d",
        logs_topic.strip(),
        application_name.strip(),
        integration_id,
        len(lines),
    )
    return {
        "source": "logs_api",
        "available": True,
        "integration_id": integration_id,
        "logs_topic": logs_topic.strip(),
        "application_name": application_name.strip(),
        "query": query.strip(),
        "search_query_used": result.get("search_query_used"),
        "search_queries_attempted": result.get("search_queries_attempted", []),
        "search_attempt_count": result.get("search_attempt_count"),
        "search_fallback_applied": bool(result.get("search_fallback_applied")),
        "window_start": result.get("window_start"),
        "window_end": result.get("window_end"),
        "total_returned": len(lines),
        "lines": lines,
    }
