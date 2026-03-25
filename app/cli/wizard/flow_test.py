from __future__ import annotations

from unittest.mock import MagicMock

from app.cli.wizard import flow
from app.cli.wizard.probes import ProbeResult
from app.cli.wizard.validation import ValidationResult


def test_run_wizard_advanced_remote_falls_back_to_local(monkeypatch, tmp_path, capsys) -> None:
    select_responses = iter(["advanced", "remote", "anthropic", "claude-opus-4-20250514"])
    confirm_responses = iter([True])

    def _mock_select(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = next(select_responses)
        return m

    def _mock_confirm(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = next(confirm_responses)
        return m

    def _mock_checkbox(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = []
        return m

    def _mock_password(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = "secret-key"
        return m

    saved: dict[str, object] = {}

    monkeypatch.setattr(flow.questionary, "select", _mock_select)
    monkeypatch.setattr(flow.questionary, "confirm", _mock_confirm)
    monkeypatch.setattr(flow.questionary, "checkbox", _mock_checkbox)
    monkeypatch.setattr(flow.questionary, "password", _mock_password)
    monkeypatch.setattr(flow, "get_store_path", lambda: tmp_path / "opensre.json")
    monkeypatch.setattr(flow, "probe_local_target", lambda _path: ProbeResult("local", True, "ok"))
    monkeypatch.setattr(flow, "probe_remote_target", lambda: ProbeResult("remote", True, "remote ok"))
    monkeypatch.setattr(
        flow,
        "validate_provider_credentials",
        lambda **_kwargs: ValidationResult(ok=True, detail="validated", sample_response="OpenSRE ready"),
    )
    monkeypatch.setattr(
        flow,
        "build_demo_action_response",
        lambda: {"success": True, "topics": ["recovery_remediation"], "guidance": [{"topic": "recovery_remediation"}]},
    )

    def _save_local_config(**kwargs):
        saved.update(kwargs)
        return tmp_path / "opensre.json"

    monkeypatch.setattr(flow, "save_local_config", _save_local_config)
    monkeypatch.setattr(flow, "sync_provider_env", lambda **_kwargs: tmp_path / ".env")

    exit_code = flow.run_wizard()

    assert exit_code == 0
    assert saved["wizard_mode"] == "advanced"
    assert saved["provider"] == "anthropic"
    assert saved["model"] == "claude-opus-4-20250514"
    assert saved["api_key"] == "secret-key"

    output = capsys.readouterr().out
    assert "Provider sample: OpenSRE ready" in output
    assert "Demo action response" in output
    assert "Saved configuration" in output


def test_run_wizard_retries_invalid_api_key(monkeypatch, tmp_path, capsys) -> None:
    validations = iter(
        [
            ValidationResult(ok=False, detail="bad key"),
            ValidationResult(ok=True, detail="validated", sample_response="OpenSRE ready"),
        ]
    )

    select_responses = iter(["quickstart", "anthropic", "claude-opus-4-20250514"])

    def _mock_select(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = next(select_responses)
        return m

    def _mock_checkbox(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = []
        return m

    def _mock_password(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = "secret-key"
        return m

    monkeypatch.setattr(flow.questionary, "select", _mock_select)
    monkeypatch.setattr(flow.questionary, "checkbox", _mock_checkbox)
    monkeypatch.setattr(flow.questionary, "password", _mock_password)
    monkeypatch.setattr(flow, "get_store_path", lambda: tmp_path / "opensre.json")
    monkeypatch.setattr(flow, "probe_local_target", lambda _path: ProbeResult("local", True, "ok"))
    monkeypatch.setattr(
        flow,
        "validate_provider_credentials",
        lambda **_kwargs: next(validations),
    )
    monkeypatch.setattr(flow, "save_local_config", lambda **_kwargs: tmp_path / "opensre.json")
    monkeypatch.setattr(flow, "sync_provider_env", lambda **_kwargs: tmp_path / ".env")
    monkeypatch.setattr(
        flow,
        "build_demo_action_response",
        lambda: {"success": True, "topics": ["recovery_remediation"], "guidance": []},
    )

    exit_code = flow.run_wizard()

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Validation failed: bad key" in output
    assert "Paste the API key again to retry" in output
    assert "validated" in output


def test_run_wizard_configures_optional_integrations(monkeypatch, tmp_path, capsys) -> None:
    select_responses = iter(["quickstart", "anthropic", "claude-opus-4-20250514", "role"])
    saved_integrations: list[tuple[str, dict]] = []
    synced_env_values: list[dict[str, str]] = []

    def _mock_select(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = next(select_responses)
        return m

    def _mock_checkbox(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = ["grafana", "slack"]
        return m

    password_responses = iter([
        "llm-secret",
        "grafana-token",
        "https://hooks.slack.com/services/T000/B000/abc",
    ])
    text_responses = iter(["https://grafana.example.com"])

    def _mock_password(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = next(password_responses)
        return m

    def _mock_text(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = next(text_responses)
        return m

    monkeypatch.setattr(flow.questionary, "select", _mock_select)
    monkeypatch.setattr(flow.questionary, "checkbox", _mock_checkbox)
    monkeypatch.setattr(flow.questionary, "password", _mock_password)
    monkeypatch.setattr(flow.questionary, "text", _mock_text)
    monkeypatch.setattr(flow, "get_store_path", lambda: tmp_path / "opensre.json")
    monkeypatch.setattr(flow, "probe_local_target", lambda _path: ProbeResult("local", True, "ok"))
    monkeypatch.setattr(
        flow,
        "validate_provider_credentials",
        lambda **_kwargs: ValidationResult(ok=True, detail="validated", sample_response="OpenSRE ready"),
    )
    monkeypatch.setattr(
        flow,
        "validate_grafana_integration",
        lambda **_kwargs: flow.IntegrationHealthResult(ok=True, detail="Grafana ok"),
    )
    monkeypatch.setattr(
        flow,
        "validate_slack_webhook",
        lambda **_kwargs: flow.IntegrationHealthResult(ok=True, detail="Slack ok"),
    )
    monkeypatch.setattr(flow, "save_local_config", lambda **_kwargs: tmp_path / "opensre.json")
    monkeypatch.setattr(flow, "sync_provider_env", lambda **_kwargs: tmp_path / ".env")

    def _sync_env_values(values: dict[str, str], **_kwargs):
        synced_env_values.append(values)
        return tmp_path / ".env"

    monkeypatch.setattr(flow, "sync_env_values", _sync_env_values)
    monkeypatch.setattr(
        flow,
        "upsert_integration",
        lambda service, payload: saved_integrations.append((service, payload)),
    )
    monkeypatch.setattr(
        flow,
        "build_demo_action_response",
        lambda: {"success": True, "topics": ["recovery_remediation"], "guidance": []},
    )

    exit_code = flow.run_wizard()

    assert exit_code == 0
    assert saved_integrations == [
        (
            "grafana",
            {
                "credentials": {
                    "endpoint": "https://grafana.example.com",
                    "api_key": "grafana-token",
                }
            },
        )
    ]
    assert synced_env_values == [
        {
            "GRAFANA_INSTANCE_URL": "https://grafana.example.com",
            "GRAFANA_READ_TOKEN": "grafana-token",
        },
        {
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/T000/B000/abc",
        },
    ]

    output = capsys.readouterr().out
    assert "Grafana, Slack" in output


def test_run_wizard_configures_github_mcp_and_sentry(monkeypatch, tmp_path, capsys) -> None:
    select_responses = iter([
        "quickstart",
        "anthropic",
        "claude-opus-4-20250514",
        flow.DEFAULT_GITHUB_MCP_MODE,
    ])
    text_responses = iter([
        flow.DEFAULT_GITHUB_MCP_URL,
        "repos,issues,pull_requests,actions",
        flow.DEFAULT_SENTRY_URL,
        "demo-org",
        "payments",
    ])
    password_responses = iter([
        "llm-secret",
        "ghp_test",
        "sntrys_test",
    ])
    saved_integrations: list[tuple[str, dict]] = []
    synced_env_values: list[dict[str, str]] = []

    def _mock_select(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = next(select_responses)
        return m

    def _mock_checkbox(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = ["github", "sentry"]
        return m

    def _mock_password(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = next(password_responses)
        return m

    def _mock_text(*_args, **_kwargs):
        m = MagicMock()
        m.ask.return_value = next(text_responses)
        return m

    monkeypatch.setattr(flow.questionary, "select", _mock_select)
    monkeypatch.setattr(flow.questionary, "checkbox", _mock_checkbox)
    monkeypatch.setattr(flow.questionary, "password", _mock_password)
    monkeypatch.setattr(flow.questionary, "text", _mock_text)
    monkeypatch.setattr(flow, "get_store_path", lambda: tmp_path / "opensre.json")
    monkeypatch.setattr(flow, "probe_local_target", lambda _path: ProbeResult("local", True, "ok"))
    monkeypatch.setattr(
        flow,
        "validate_provider_credentials",
        lambda **_kwargs: ValidationResult(ok=True, detail="validated", sample_response="OpenSRE ready"),
    )
    monkeypatch.setattr(
        flow,
        "validate_github_mcp_integration",
        lambda **_kwargs: flow.IntegrationHealthResult(ok=True, detail="GitHub MCP ok"),
    )
    monkeypatch.setattr(
        flow,
        "validate_sentry_integration",
        lambda **_kwargs: flow.IntegrationHealthResult(ok=True, detail="Sentry ok"),
    )
    monkeypatch.setattr(flow, "save_local_config", lambda **_kwargs: tmp_path / "opensre.json")
    monkeypatch.setattr(flow, "sync_provider_env", lambda **_kwargs: tmp_path / ".env")

    def _sync_env_values(values: dict[str, str], **_kwargs):
        synced_env_values.append(values)
        return tmp_path / ".env"

    monkeypatch.setattr(
        flow,
        "sync_env_values",
        _sync_env_values,
    )
    monkeypatch.setattr(
        flow,
        "upsert_integration",
        lambda service, payload: saved_integrations.append((service, payload)),
    )
    monkeypatch.setattr(
        flow,
        "build_demo_action_response",
        lambda: {"success": True, "topics": ["recovery_remediation"], "guidance": []},
    )

    exit_code = flow.run_wizard()

    assert exit_code == 0
    assert saved_integrations == [
        (
            "github",
            {
                "credentials": {
                    "url": flow.DEFAULT_GITHUB_MCP_URL,
                    "mode": flow.DEFAULT_GITHUB_MCP_MODE,
                    "auth_token": "ghp_test",
                    "command": "",
                    "args": [],
                    "toolsets": ["repos", "issues", "pull_requests", "actions"],
                }
            },
        ),
        (
            "sentry",
            {
                "credentials": {
                    "base_url": flow.DEFAULT_SENTRY_URL,
                    "organization_slug": "demo-org",
                    "auth_token": "sntrys_test",
                    "project_slug": "payments",
                }
            },
        ),
    ]
    assert synced_env_values == [
        {
            "GITHUB_MCP_URL": flow.DEFAULT_GITHUB_MCP_URL,
            "GITHUB_MCP_MODE": flow.DEFAULT_GITHUB_MCP_MODE,
            "GITHUB_MCP_COMMAND": "",
            "GITHUB_MCP_ARGS": "",
            "GITHUB_MCP_AUTH_TOKEN": "ghp_test",
            "GITHUB_MCP_TOOLSETS": "repos,issues,pull_requests,actions",
        },
        {
            "SENTRY_URL": flow.DEFAULT_SENTRY_URL,
            "SENTRY_ORG_SLUG": "demo-org",
            "SENTRY_PROJECT_SLUG": "payments",
            "SENTRY_AUTH_TOKEN": "sntrys_test",
        },
    ]

    output = capsys.readouterr().out
    assert "GitHub MCP" in output
    assert "Sentry" in output
