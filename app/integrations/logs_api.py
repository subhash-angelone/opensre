"""Raw logs API integration helpers.

This module centralizes config normalization and a lightweight backend client
for APIs that expose ``/api/v1/rawlogs`` and accept the expected rawlogs
query payload shape.
"""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from pydantic import Field, field_validator

from app.strict_config import StrictConfigModel

logger = logging.getLogger(__name__)

RAWLOGS_MAX_QUERY_HOURS = 24
DEFAULT_LOGS_API_TIMEOUT_SECONDS = 10.0
DEFAULT_LOGS_API_MAX_RESULTS = 100
_MAX_HARD_LIMIT = 500


def _utc_epoch_seconds(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp())


def normalize_rawlogs_url(base: str) -> str:
    """Normalize a base URL to the rawlogs endpoint."""
    normalized = base.strip().rstrip("/")
    if normalized.endswith("/api/v1/rawlogs"):
        return normalized
    return f"{normalized}/api/v1/rawlogs"


class LogsApiConfig(StrictConfigModel):
    """Normalized credentials/config for a raw logs API endpoint."""

    base_url: str
    bearer_token: str
    logs_topic: str = ""
    application_name: str = ""
    timeout_seconds: float = Field(default=DEFAULT_LOGS_API_TIMEOUT_SECONDS, gt=0)
    max_results: int = Field(default=DEFAULT_LOGS_API_MAX_RESULTS, ge=1, le=_MAX_HARD_LIMIT)
    integration_id: str = ""

    @field_validator("base_url", mode="before")
    @classmethod
    def _normalize_base_url(cls, value: object) -> str:
        return str(value or "").strip().rstrip("/")

    @field_validator("bearer_token", mode="before")
    @classmethod
    def _normalize_token(cls, value: object) -> str:
        token = str(value or "").strip()
        if token.lower().startswith("bearer "):
            token = token.split(None, 1)[1].strip()
        return token

    @field_validator("logs_topic", "application_name", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str:
        return str(value or "").strip()


def build_logs_api_config(raw: dict[str, Any] | None) -> LogsApiConfig:
    """Build normalized logs API config from store/env data."""
    return LogsApiConfig.model_validate(raw or {})


@dataclass
class RawLogsApiBackend:
    """Thin client for searching a raw logs HTTP API."""

    base_url: str
    bearer_token: str
    timeout_seconds: float = DEFAULT_LOGS_API_TIMEOUT_SECONDS

    def search(
        self,
        *,
        logs_topic: str,
        application_name: str,
        start: datetime,
        end: datetime,
        free_text_query: str,
        limit: int = 50,
    ) -> dict[str, Any]:
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)

        window_clamped = False
        if end - start > timedelta(hours=RAWLOGS_MAX_QUERY_HOURS):
            start = end - timedelta(hours=RAWLOGS_MAX_QUERY_HOURS)
            window_clamped = True

        _ = limit
        payload: dict[str, Any] = {
            "from": str(_utc_epoch_seconds(start)),
            "to": str(_utc_epoch_seconds(end)),
            "topic": logs_topic,
            "application_name": application_name,
            "search_keyword": base64.b64encode(free_text_query.encode()).decode("ascii"),
            "mode": "NORMAL",
        }
        
        logger.info(f"logs_api: window_clamped={window_clamped}, params={payload}")

        endpoint = normalize_rawlogs_url(self.base_url)
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.post(endpoint, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
        except Exception as exc:  # noqa: BLE001
            logger.warning("rawlogs_request_failed %s", exc)
            return {
                "logs_topic": logs_topic,
                "application_name": application_name,
                "query": free_text_query,
                "error": str(exc),
                "lines": [],
            }

        lines: list[dict[str, Any]] = []
        if isinstance(data, dict):
            raw_lines = data.get("lines") or data.get("data") or []
            if isinstance(raw_lines, list):
                lines = [line for line in raw_lines if isinstance(line, dict)]

        return {
            "logs_topic": logs_topic,
            "application_name": application_name,
            "query": free_text_query,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "lines": lines[: max(1, int(limit))],
            "stub": False,
        }
