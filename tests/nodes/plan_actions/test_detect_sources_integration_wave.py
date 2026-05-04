"""Tests for integration-wave source routing in detect_sources."""

from __future__ import annotations

from datetime import UTC, datetime

from app.nodes.plan_actions.detect_sources import detect_sources


def test_detect_sources_routes_bitbucket_from_repo_url() -> None:
    alert = {
        "alert_name": "Deployment failed",
        "annotations": {
            "repo_url": "https://bitbucket.org/acme/backend-service/src/main.py",
            "file_path": "src/main.py",
        },
    }
    integrations = {
        "bitbucket": {
            "workspace": "acme",
            "username": "bb-user",
            "app_password": "bb-pass",
            "integration_id": "bb-1",
        }
    }

    sources = detect_sources(alert, {}, integrations)
    bitbucket = sources.get("bitbucket")

    assert bitbucket is not None
    assert bitbucket["workspace"] == "acme"
    assert bitbucket["repo_slug"] == "backend-service"
    assert bitbucket["path"] == "src/main.py"
    assert bitbucket["integration_id"] == "bb-1"
    assert bitbucket["connection_verified"] is True


def test_detect_sources_routes_bitbucket_from_self_hosted_server_url() -> None:
    alert = {
        "alert_name": "Deployment failed",
        "annotations": {
            "repo_url": "https://bitbucket.acme.internal/projects/OPS/repos/backend-service/browse/src/main.py",
        },
    }
    integrations = {
        "bitbucket": {
            "workspace": "ops-default",
            "username": "bb-user",
            "app_password": "bb-pass",
            "integration_id": "bb-1",
        }
    }

    sources = detect_sources(alert, {}, integrations)
    bitbucket = sources.get("bitbucket")

    assert bitbucket is not None
    assert bitbucket["workspace"] == "ops-default"
    assert bitbucket["repo_slug"] == "backend-service"
    assert bitbucket["connection_verified"] is True


def test_detect_sources_does_not_treat_substring_host_as_bitbucket_cloud() -> None:
    alert = {
        "alert_name": "Deployment failed",
        "annotations": {
            "repo_url": "https://evil-bitbucket.org.example/projects/OPS/repos/backend-service/browse/src/main.py",
        },
    }
    integrations = {
        "bitbucket": {
            "workspace": "ops-default",
            "username": "bb-user",
            "app_password": "bb-pass",
            "integration_id": "bb-1",
        }
    }

    sources = detect_sources(alert, {}, integrations)
    bitbucket = sources.get("bitbucket")

    assert bitbucket is not None
    assert bitbucket["workspace"] == "ops-default"
    assert bitbucket["repo_slug"] == "backend-service"


def test_detect_sources_does_not_route_bitbucket_without_repo_context() -> None:
    alert = {"alert_name": "Deployment failed", "annotations": {}}
    integrations = {
        "bitbucket": {
            "workspace": "acme",
            "username": "bb-user",
            "app_password": "bb-pass",
            "integration_id": "bb-1",
        }
    }

    sources = detect_sources(alert, {}, integrations)
    assert "bitbucket" not in sources


def test_detect_sources_routes_new_integration_wave_sources() -> None:
    alert = {"alert_name": "Latency spike", "annotations": {}}
    integrations = {
        "snowflake": {
            "account_identifier": "xy12345.us-east-1",
            "token": "sf-token",
            "integration_id": "sf-1",
        },
        "azure": {
            "workspace_id": "az-workspace",
            "access_token": "az-token",
            "integration_id": "az-1",
        },
        "openobserve": {
            "base_url": "https://oo.example.invalid",
            "org": "acme",
            "api_token": "oo-token",
            "integration_id": "oo-1",
        },
        "opensearch": {
            "url": "https://os.example.invalid",
            "api_key": "os-key",
            "index_pattern": "logs-*",
            "integration_id": "os-1",
        },
        "logs_api": {
            "base_url": "https://logs-api.example.invalid",
            "bearer_token": "logs-token",
            "logs_topic": "payments",
            "application_name": "payments-api",
            "integration_id": "la-1",
        },
    }

    sources = detect_sources(alert, {}, integrations)

    assert sources["snowflake"]["integration_id"] == "sf-1"
    assert sources["azure"]["integration_id"] == "az-1"
    assert sources["openobserve"]["integration_id"] == "oo-1"
    assert sources["opensearch"]["integration_id"] == "os-1"
    assert sources["logs_api"]["integration_id"] == "la-1"
    assert sources["snowflake"]["connection_verified"] is True
    assert sources["azure"]["connection_verified"] is True
    assert sources["openobserve"]["connection_verified"] is True
    assert sources["opensearch"]["connection_verified"] is True
    assert sources["logs_api"]["connection_verified"] is True


def test_detect_sources_skips_snowflake_without_token() -> None:
    alert = {"alert_name": "Latency spike", "annotations": {}}
    integrations = {
        "snowflake": {
            "account_identifier": "xy12345.us-east-1",
            "user": "service-user",
            "password": "secret",
            "integration_id": "sf-1",
        }
    }

    sources = detect_sources(alert, {}, integrations)

    assert "snowflake" not in sources


def test_detect_sources_uses_explicit_date_window_for_logs_api(monkeypatch) -> None:
    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            base = cls(2026, 4, 28, 10, 0, 0)
            return base.replace(tzinfo=tz) if tz is not None else base

    monkeypatch.setattr("app.nodes.plan_actions.detect_sources.datetime", _FixedDatetime)

    alert = {
        "alert_name": "why the trading balance was not updated on 20th april for 889F5943F00EBA7",
        "service": "ledger",
        "environment": "prod",
    }
    integrations = {
        "logs_api": {
            "base_url": "https://logs-api.example.invalid",
            "bearer_token": "logs-token",
            "integration_id": "la-1",
        }
    }

    sources = detect_sources(alert, {}, integrations)

    assert sources["logs_api"]["window_start"] == "2026-04-19T18:30:00+00:00"
    assert sources["logs_api"]["window_end"] == "2026-04-20T18:30:00+00:00"
