from __future__ import annotations

from pathlib import Path

from app.domain import ServiceCatalog


def test_service_catalog_loads_real_domain_directory() -> None:
    catalog = ServiceCatalog.from_directory(Path("app/domain"))

    masters = catalog.find_service("masters")
    ledger = catalog.find_service("ledger")

    assert masters is not None
    assert ledger is not None
    assert masters.default_environment == "prod"
    assert ledger.default_environment == "prod"
    assert masters.environments["prod"].logs_topic == "aws-prod-ecs-infinitrade-portal"
    assert ledger.environments["prod"].logs_topic == "aws-prod-ecs-ledger-service"
