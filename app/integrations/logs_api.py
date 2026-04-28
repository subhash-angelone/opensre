"""Raw logs API integration helpers.

This module centralizes config normalization and a lightweight backend client
for APIs that expose ``/api/v1/rawlogs`` and accept the expected rawlogs
query payload shape.
"""

from __future__ import annotations

import base64
import json
import logging
import re
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
_QUERY_STOPWORDS = frozenset(
    {
        "was",
        "is",
        "the",
        "on",
        "in",
        "at",
        "today",
        "yesterday",
        "did",
        "does",
        "do",
        "a",
        "an",
        "of",
        "for",
        "to",
        "time",
        "timely",
        "copy",
    }
)
_TERM_NORMALIZATION = {
    "uploaded": "upload",
}


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


def _normalize_query_token(token: str) -> str:
    normalized = _TERM_NORMALIZATION.get(token, token)
    if len(normalized) > 3 and normalized.endswith("s") and not normalized.endswith("ss"):
        normalized = normalized[:-1]
    return normalized


def _meaningful_query_terms(query: str) -> list[str]:
    raw_tokens = re.sub(r"[^\w]+", " ", query.lower()).split()
    terms: list[str] = []
    seen: set[str] = set()
    for raw_token in raw_tokens:
        if raw_token in _QUERY_STOPWORDS:
            continue
        token = _normalize_query_token(raw_token)
        if not token or token in _QUERY_STOPWORDS or token in seen:
            continue
        seen.add(token)
        terms.append(token)
    return terms


def _build_query_attempts(query: str) -> list[str]:
    trimmed = query.strip()
    if not trimmed:
        return [""]

    terms = _meaningful_query_terms(trimmed)
    if not terms:
        return [trimmed]

    strict_terms = terms[:3]
    attempts = [" AND ".join(strict_terms)]
    if len(strict_terms) >= 3:
        attempts.append(" AND ".join(strict_terms[:-1]))
    return attempts


def _build_payload(
    *,
    logs_topic: str,
    application_name: str,
    start: datetime,
    end: datetime,
    search_query: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "from": str(_utc_epoch_seconds(start)),
        "to": str(_utc_epoch_seconds(end)),
        "topic": logs_topic,
        "application_name": application_name,
        "search_keyword": base64.b64encode(search_query.encode()).decode("ascii"),
        "mode": "NORMAL",
    }
    return payload


def _extract_lines(data: Any) -> tuple[list[dict[str, Any]] | None, str | None]:
    if not isinstance(data, dict):
        return None, "Malformed logs API response: expected a JSON object."

    container = _find_line_container(data)
    if container is None:
        if _has_known_line_container_key(data):
            return [], None
        return None, "Malformed logs API response: missing 'lines'/'data' container."
    if not isinstance(container, list):
        return None, "Malformed logs API response: expected 'lines'/'data' to be a list."

    if any(not isinstance(line, dict) for line in container):
        return None, "Malformed logs API response: expected 'lines'/'data' entries to be objects."

    return [_normalize_line_entry(line) for line in container], None


def _find_line_container(data: dict[str, Any]) -> Any:
    """Locate the first list-like line container in a response envelope.

    Some deployments return `{"lines": [...]}` or `{"data": [...]}`, while
    others wrap the list one level deeper, e.g. `{"data": {"lines": [...]}}`.
    """
    direct_keys = ("lines", "data")
    for key in direct_keys:
        if key not in data:
            continue
        candidate = data[key]
        if isinstance(candidate, list):
            return candidate
        if isinstance(candidate, dict):
            nested = _find_nested_line_container(candidate)
            if nested is not None:
                return nested
            if _has_known_nested_line_container_key(candidate):
                return None
        return candidate
    return None


def _find_nested_line_container(data: dict[str, Any]) -> list[dict[str, Any]] | Any | None:
    for key in ("lines", "data", "response", "items", "results", "records", "hits"):
        candidate = data.get(key)
        if isinstance(candidate, list):
            return candidate
        if isinstance(candidate, dict):
            nested = _find_nested_line_container(candidate)
            if nested is not None:
                return nested
    return None


def _has_known_line_container_key(data: dict[str, Any]) -> bool:
    for key in ("lines", "data"):
        if key not in data:
            continue
        candidate = data.get(key)
        if candidate is None:
            return True
        if isinstance(candidate, dict):
            return _has_known_nested_line_container_key(candidate)
        return False
    return False


def _has_known_nested_line_container_key(data: dict[str, Any]) -> bool:
    for key in ("lines", "data", "response", "items", "results", "records", "hits"):
        if key not in data:
            continue
        candidate = data.get(key)
        if candidate is None:
            return True
        if isinstance(candidate, dict) and _has_known_nested_line_container_key(candidate):
            return True
    return False


def _normalize_line_entry(line: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(line)
    normalized["message_json"] = _parse_embedded_json(line.get("message"))
    normalized["tags_json"] = _parse_embedded_json(line.get("tags"))

    message_json = normalized["message_json"]
    tags_json = normalized["tags_json"]
    if isinstance(message_json, dict):
        normalized.setdefault("message_text", str(message_json.get("message", "")).strip())
        normalized.setdefault("log_time", str(message_json.get("time", "")).strip())
        normalized.setdefault("log_level", str(message_json.get("level", "")).strip())
        normalized.setdefault("caller", str(message_json.get("caller", "")).strip())
    else:
        normalized.setdefault("message_text", str(line.get("message", "")).strip())

    if isinstance(tags_json, dict):
        normalized.setdefault(
            "application_name",
            str(tags_json.get("application_name", line.get("application_name", ""))).strip(),
        )
        normalized.setdefault(
            "topic",
            str(tags_json.get("topic", line.get("topic", ""))).strip(),
        )
        normalized.setdefault(
            "container_id",
            str(tags_json.get("container_id", line.get("container_id", ""))).strip(),
        )

    return normalized


def _parse_embedded_json(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or not text.startswith("{"):
        return None
    try:
        parsed = json.loads(text)
    except Exception:  # noqa: BLE001
        return None
    return parsed if isinstance(parsed, dict) else None


def _error_result(logs_topic: str, application_name: str, query: str, error: str) -> dict[str, Any]:
    return {
        "logs_topic": logs_topic,
        "application_name": application_name,
        "query": query,
        "error": error,
        "lines": [],
    }


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

        if end - start > timedelta(hours=RAWLOGS_MAX_QUERY_HOURS):
            start = end - timedelta(hours=RAWLOGS_MAX_QUERY_HOURS)

        endpoint = normalize_rawlogs_url(self.base_url)
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        attempts = _build_query_attempts(free_text_query)
        chosen_query = attempts[0]
        executed_attempts = 0
        lines: list[dict[str, Any]] = []

        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                for index, candidate_query in enumerate(attempts):
                    chosen_query = candidate_query
                    executed_attempts = index + 1
                    payload = _build_payload(
                        logs_topic=logs_topic,
                        application_name=application_name,
                        start=start,
                        end=end,
                        search_query=candidate_query,
                    )
                    logger.warning("logs_api_payload attempt=%d params=%s", index + 1, payload)
                    response = client.post(endpoint, json=payload, headers=headers)
                    response.raise_for_status()
                    try:
                        data = response.json()
                    except Exception as exc:  # noqa: BLE001
                        return _error_result(
                            logs_topic,
                            application_name,
                            free_text_query,
                            f"Malformed logs API response: {exc}",
                        )

                    extracted_lines, parse_error = _extract_lines(data)
                    if parse_error:
                        return _error_result(
                            logs_topic,
                            application_name,
                            free_text_query,
                            parse_error,
                        )

                    lines = extracted_lines or []
                    if lines or index == len(attempts) - 1:
                        break
        except Exception as exc:  # noqa: BLE001
            logger.warning("rawlogs_request_failed %s", exc)
            return _error_result(logs_topic, application_name, free_text_query, str(exc))

        return {
            "logs_topic": logs_topic,
            "application_name": application_name,
            "query": free_text_query,
            "search_query_used": chosen_query,
            "search_queries_attempted": attempts,
            "search_attempt_count": executed_attempts,
            "search_fallback_applied": len(attempts) > 1 and chosen_query != attempts[0],
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "lines": lines[: max(1, int(limit))],
            "stub": False,
        }
