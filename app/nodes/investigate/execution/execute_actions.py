"""Investigation action execution."""

import logging
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from app.tools.registered_tool import RegisteredTool as InvestigationAction

logger = logging.getLogger(__name__)


def _redact_params(value: Any) -> Any:
    """Redact sensitive fields from tool-call parameters before logging."""
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            lowered = str(key).lower()
            if any(
                marker in lowered
                for marker in (
                    "token",
                    "api_key",
                    "apikey",
                    "password",
                    "secret",
                    "authorization",
                    "auth",
                    "bearer",
                )
            ):
                redacted[str(key)] = "***REDACTED***"
            else:
                redacted[str(key)] = _redact_params(item)
        return redacted
    if isinstance(value, list):
        return [_redact_params(item) for item in value]
    return value


@dataclass
class ActionExecutionResult:
    """Result of executing an investigation action."""

    action_name: str
    success: bool
    data: dict
    error: str | None = None


def _is_transient_error(exception: Exception) -> bool:
    """Check if exception is likely a transient AWS error."""
    error_str = str(exception).lower()
    transient_indicators = [
        "throttling",
        "rate exceeded",
        "timeout",
        "connection",
        "service unavailable",
        "internal error",
        "503",
        "500",
    ]
    return any(indicator in error_str for indicator in transient_indicators)


def _execute_with_retry(
    call_index: int,
    action_name: str,
    action: Any,
    available_sources: dict[str, dict],
    max_attempts: int = 3,
) -> ActionExecutionResult:
    """Execute action with exponential backoff retry for transient failures."""
    last_error = None

    for attempt in range(max_attempts):
        try:
            kwargs = action.extract_params(available_sources)
            if attempt == 0:
                logger.info(
                    "Tool Call #%d tool_name=%s parameters=%s",
                    call_index,
                    action_name,
                    _redact_params(kwargs),
                )
            logger.info(
                "action_execute_start action=%s attempt=%d",
                action_name,
                attempt + 1,
            )
            data = action.run(**kwargs)

            if isinstance(data, dict):
                # Actions that use "available" field (e.g. Grafana) are successful
                # when available=True, even if they contain an "error" key for
                # context. All other actions succeed when no "error" key is present.
                if "available" in data:
                    is_success = bool(data.get("available"))
                else:
                    is_success = "error" not in data

                if is_success:
                    logger.info(
                        "action_execute_success action=%s attempt=%d",
                        action_name,
                        attempt + 1,
                    )
                    return ActionExecutionResult(
                        action_name=action_name,
                        success=True,
                        data=data,
                        error=None,
                    )
                else:
                    logger.warning(
                        "action_execute_failed action=%s attempt=%d error=%s",
                        action_name,
                        attempt + 1,
                        data.get("error", "Unknown error"),
                    )
                    return ActionExecutionResult(
                        action_name=action_name,
                        success=False,
                        data=data,
                        error=data.get("error", "Unknown error"),
                    )
            else:
                logger.warning(
                    "action_execute_invalid_response action=%s attempt=%d",
                    action_name,
                    attempt + 1,
                )
                return ActionExecutionResult(
                    action_name=action_name,
                    success=False,
                    data={},
                    error="Invalid response",
                )
        except Exception as e:
            last_error = e
            logger.warning(
                "action_execute_exception action=%s attempt=%d error=%s",
                action_name,
                attempt + 1,
                e,
            )
            if attempt < max_attempts - 1 and _is_transient_error(e):
                backoff_seconds = 2**attempt
                time.sleep(backoff_seconds)
                continue
            break

    available_source_keys = list(available_sources.keys()) if available_sources else []
    error_detail = f"{type(last_error).__name__}: {str(last_error)} | Available sources: {available_source_keys}"
    logger.error("action_execute_terminal_failure action=%s error=%s", action_name, error_detail)
    return ActionExecutionResult(
        action_name=action_name,
        success=False,
        data={},
        error=error_detail,
    )


def _execute_single_action(
    call_index: int,
    action_name: str,
    action: Any,
    available_sources: dict[str, dict],
) -> ActionExecutionResult:
    """Execute a single investigation action with error handling and retry."""
    return _execute_with_retry(call_index, action_name, action, available_sources)


def execute_actions(
    action_names: list[str],
    available_actions: dict[str, InvestigationAction] | Iterable[InvestigationAction],
    available_sources: dict[str, dict] | None = None,
) -> dict[str, ActionExecutionResult]:
    """
    Execute investigation actions in parallel.

    Args:
        action_names: List of action names to execute
        available_actions: Mapping or iterable of available actions
        available_sources: Optional dictionary of available data sources

    Returns:
        Dictionary mapping action names to execution results
    """
    if available_sources is None:
        available_sources = {}

    if isinstance(available_actions, dict):
        available_actions_map = available_actions
    else:
        available_actions_map = {action.name: action for action in available_actions}

    results: dict[str, ActionExecutionResult] = {}

    actions_to_execute: list[tuple[int, str, InvestigationAction]] = []
    for index, action_name in enumerate(action_names, start=1):
        if action_name not in available_actions_map:
            results[action_name] = ActionExecutionResult(
                action_name=action_name,
                success=False,
                data={},
                error=f"Unknown action: {action_name}",
            )
            continue

        action = available_actions_map[action_name]

        if not action.is_available(available_sources):
            results[action_name] = ActionExecutionResult(
                action_name=action_name,
                success=False,
                data={},
                error="Action not available: required data sources not found",
            )
            continue

        actions_to_execute.append((index, action_name, action))

    if not actions_to_execute:
        return results

    with ThreadPoolExecutor(max_workers=min(5, len(actions_to_execute))) as executor:
        future_to_action = {
            executor.submit(
                _execute_single_action,
                call_index,
                action_name,
                action,
                available_sources,
            ): action_name
            for call_index, action_name, action in actions_to_execute
        }

        for future in as_completed(future_to_action):
            action_name = future_to_action[future]
            try:
                results[action_name] = future.result()
            except Exception as e:
                results[action_name] = ActionExecutionResult(
                    action_name=action_name,
                    success=False,
                    data={},
                    error=f"Execution failed: {type(e).__name__}: {str(e)}",
                )

    return results
