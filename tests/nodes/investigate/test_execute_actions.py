from __future__ import annotations

from app.nodes.investigate.execution.execute_actions import execute_actions
from app.tools.registered_tool import REGISTERED_TOOL_ATTR
from app.tools.run_diagnostic_code import run_diagnostic_code


def _registered_run_diagnostic_code():
    tool = getattr(run_diagnostic_code, REGISTERED_TOOL_ATTR, None)
    assert tool is not None
    return tool


def test_execute_actions_reports_missing_required_params_without_exception() -> None:
    results = execute_actions(
        ["run_diagnostic_code"],
        {"run_diagnostic_code": _registered_run_diagnostic_code()},
        available_sources={"logs_api": {"base_url": "https://logs-api.example.invalid"}},
    )

    result = results["run_diagnostic_code"]
    assert result.success is False
    assert result.data == {}
    assert result.error == "Missing required parameters: code"
