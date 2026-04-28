from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field

from app.config import LLMSettings, get_environment
from app.integrations.catalog import resolve_effective_integrations
from app.tools.LogsApiRawLogsTool import query_logs_api_rawlogs
from app.version import get_version


class HealthResponse(BaseModel):
    ok: bool
    version: str
    graph_loaded: bool
    llm_configured: bool
    env: str


class LogsApiDebugRequest(BaseModel):
    query: str = ""
    logs_topic: str | None = None
    application_name: str | None = None
    time_range_minutes: int = Field(default=60, ge=1, le=24 * 60)
    limit: int = Field(default=50, ge=1, le=500)
    max_results: int | None = Field(default=None, ge=1, le=500)
    timeout_seconds: float | None = Field(default=None, gt=0, le=120)
    base_url: str | None = None
    bearer_token: str | None = None


class LogsApiDebugResponse(BaseModel):
    resolved: dict[str, Any]
    result: dict[str, Any]


app = FastAPI()


def _graph_loaded() -> bool:
    return "app.graph_pipeline" in sys.modules


def _llm_configured() -> bool:
    try:
        LLMSettings.from_env()
    except Exception:
        return False
    return True


def get_health_response() -> HealthResponse:
    graph_loaded = _graph_loaded()
    llm_configured = _llm_configured()

    return HealthResponse(
        ok=graph_loaded and llm_configured,
        version=get_version(),
        graph_loaded=graph_loaded,
        llm_configured=llm_configured,
        env=get_environment().value,
    )


@app.get("/health", response_model=HealthResponse)
def health(response: Response) -> HealthResponse:
    health_response = get_health_response()
    response.status_code = (
        status.HTTP_200_OK if health_response.ok else status.HTTP_503_SERVICE_UNAVAILABLE
    )
    return health_response


@app.post("/debug/logs-api/search", response_model=LogsApiDebugResponse)
def debug_logs_api_search(request: LogsApiDebugRequest) -> LogsApiDebugResponse:
    effective = resolve_effective_integrations()
    logs_api = effective.get("logs_api", {})
    config = logs_api.get("config", {}) if isinstance(logs_api, dict) else {}
    if not isinstance(config, dict):
        config = {}

    base_url = str(request.base_url or config.get("base_url", "")).strip()
    bearer_token = str(request.bearer_token or config.get("bearer_token", "")).strip()
    logs_topic = str(request.logs_topic or config.get("logs_topic", "")).strip()
    application_name = str(
        request.application_name or config.get("application_name", "")
    ).strip()
    max_results = int(request.max_results or config.get("max_results", 100) or 100)
    timeout_seconds = float(request.timeout_seconds or config.get("timeout_seconds", 10.0) or 10.0)

    if not base_url:
        raise HTTPException(status_code=400, detail="Missing logs_api base_url.")
    if not bearer_token:
        raise HTTPException(status_code=400, detail="Missing logs_api bearer_token.")

    # Use the same path as the investigation tool so breakpointing here mirrors production behavior.
    result = query_logs_api_rawlogs(
        base_url=base_url,
        bearer_token=bearer_token,
        logs_topic=logs_topic,
        application_name=application_name,
        query=request.query.strip(),
        time_range_minutes=request.time_range_minutes,
        limit=request.limit,
        max_results=max_results,
        timeout_seconds=timeout_seconds,
        integration_id=str(config.get("integration_id", "")).strip(),
    )

    resolved = {
        "integration_source": str(logs_api.get("source", "unknown")).strip()
        if isinstance(logs_api, dict)
        else "unknown",
        "base_url": base_url,
        "logs_topic": logs_topic,
        "application_name": application_name,
        "time_range_minutes": request.time_range_minutes,
        "limit": request.limit,
        "max_results": max_results,
        "timeout_seconds": timeout_seconds,
        "window_start": (datetime.now(UTC) - timedelta(minutes=request.time_range_minutes)).isoformat(),
        "window_end": datetime.now(UTC).isoformat(),
    }
    return LogsApiDebugResponse(resolved=resolved, result=result)
