from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import re
from typing import Any
import yaml


class ServiceCatalogError(ValueError):
    """Raised when the service catalog cannot be loaded or validated."""


@dataclass(frozen=True)
class ServiceEnvironmentConfig:
    name: str
    deployment_repo: str
    cloudwatch_account_id: str
    region: str
    logs_topic: str
    logs_application_name: str


@dataclass(frozen=True)
class ServiceDefinition:
    service_id: str
    group_id: str
    group_name: str
    display_name: str | None
    description: str
    triage_hints: str | None
    code_repo: str
    default_environment: str | None
    dependencies: tuple[str, ...]
    environments: dict[str, ServiceEnvironmentConfig]


@dataclass(frozen=True)
class ServiceGroup:
    group_id: str
    group_name: str
    description: str | None
    services: tuple[ServiceDefinition, ...]


@dataclass(frozen=True)
class ClassifierServiceHint:
    service_id: str
    display_name: str | None
    description_excerpt: str
    time_window_hint: str | None = None


class ServiceCatalog:
    def __init__(
        self,
        *,
        groups: tuple[ServiceGroup, ...],
        strict_dependencies: bool = False,
    ):
        self.groups = groups
        self.strict_dependencies = strict_dependencies
        self.services_by_id: dict[str, ServiceDefinition] = {}
        self.services_by_display_name: dict[str, ServiceDefinition] = {}
        for group in groups:
            for service in group.services:
                if service.service_id in self.services_by_id:
                    raise ServiceCatalogError(
                        f"duplicate service id: {service.service_id}"
                    )
                self.services_by_id[service.service_id] = service
                if service.display_name:
                    display_key = service.display_name.strip().lower()
                    if display_key in self.services_by_display_name:
                        raise ServiceCatalogError(
                            f"duplicate service display name: {service.display_name}"
                        )
                    self.services_by_display_name[display_key] = service
        self._validate_dependencies()

    @classmethod
    def from_directory(
        cls,
        directory: str | Path,
        *,
        strict_dependencies: bool = False,
    ) -> "ServiceCatalog":
        root = Path(directory)
        if not root.is_dir():
            raise ServiceCatalogError(f"service directory not found: {root}")

        groups: list[ServiceGroup] = []
        for path in sorted(root.glob("*.yml")):
            groups.append(_load_group(path))
        if not groups:
            raise ServiceCatalogError(f"no service YAML files found in {root}")
        return cls(
            groups=tuple(groups),
            strict_dependencies=strict_dependencies,
        )

    def get_service(self, service_id: str) -> ServiceDefinition:
        try:
            return self.services_by_id[service_id]
        except KeyError as exc:
            raise ServiceCatalogError(f"unknown service id: {service_id}") from exc

    def find_service(self, name_or_id: str) -> ServiceDefinition | None:
        normalized = name_or_id.strip().lower()
        if not normalized:
            return None
        return self.services_by_id.get(normalized) or self.services_by_display_name.get(
            normalized
        )

    def list_services(self) -> tuple[ServiceDefinition, ...]:
        return tuple(self.services_by_id.values())

    def build_classifier_context(
        self,
        *,
        raw_query: str,
        max_services: int = 5,
    ) -> tuple[dict[str, Any], ...]:
        scored: list[tuple[int, ServiceDefinition]] = []
        query_tokens = _query_tokens(raw_query)
        for service in self.services_by_id.values():
            score = _service_match_score(service, query_tokens)
            if score > 0:
                scored.append((score, service))

        scored.sort(key=lambda item: (-item[0], item[1].service_id))
        hints = [
            ClassifierServiceHint(
                service_id=service.service_id,
                display_name=service.display_name,
                description_excerpt=_description_excerpt(
                    service.description,
                    query_tokens=query_tokens,
                ),
                time_window_hint=_extract_time_window_hint(
                    service.description,
                    query_tokens=query_tokens,
                ),
            )
            for _, service in scored[:max_services]
        ]
        return tuple(asdict(hint) for hint in hints)

    def unknown_dependencies(self) -> dict[str, tuple[str, ...]]:
        known_ids = set(self.services_by_id.keys())
        unknown: dict[str, list[str]] = {}
        for service in self.services_by_id.values():
            missing = [dep for dep in service.dependencies if dep not in known_ids]
            if missing:
                unknown[service.service_id] = missing
        return {service_id: tuple(values) for service_id, values in unknown.items()}

    def _validate_dependencies(self) -> None:
        known_ids = set(self.services_by_id.keys())
        for service in self.services_by_id.values():
            for dependency in service.dependencies:
                if dependency not in known_ids:
                    if self.strict_dependencies:
                        raise ServiceCatalogError(
                            f"unknown dependency '{dependency}' referenced by '{service.service_id}'"
                        )


def _load_group(path: Path) -> ServiceGroup:
    raw = _parse_yaml(path)
    if not isinstance(raw, dict):
        raise ServiceCatalogError(f"{path} must contain a top-level mapping")

    group = raw.get("group")
    services = raw.get("services")
    if not isinstance(group, dict):
        raise ServiceCatalogError(f"{path} is missing group metadata")
    if not isinstance(services, dict):
        raise ServiceCatalogError(f"{path} is missing services mapping")

    group_id = _require_non_empty_string(group.get("id"), f"{path} group.id")
    group_name = _require_non_empty_string(group.get("name"), f"{path} group.name")
    group_description = _optional_string(group.get("description"))

    parsed_services: list[ServiceDefinition] = []
    for service_id, payload in services.items():
        if not isinstance(payload, dict):
            raise ServiceCatalogError(
                f"{path} service '{service_id}' must be a mapping"
            )
        normalized_id = _require_non_empty_string(service_id, f"{path} service id")
        parsed_services.append(
            _build_service_definition(
                path=path,
                service_id=normalized_id,
                group_id=group_id,
                group_name=group_name,
                payload=payload,
            )
        )

    return ServiceGroup(
        group_id=group_id,
        group_name=group_name,
        description=group_description,
        services=tuple(parsed_services),
    )


def _build_service_definition(
    *,
    path: Path,
    service_id: str,
    group_id: str,
    group_name: str,
    payload: dict[str, Any],
) -> ServiceDefinition:
    description = _require_non_empty_string(
        payload.get("description"), f"{path} service '{service_id}' description"
    )
    code_repo = _require_non_empty_string(
        payload.get("code_repo"), f"{path} service '{service_id}' code_repo"
    )
    display_name = _optional_string(payload.get("display_name"))
    default_environment = _optional_string(payload.get("default_environment"))
    triage_hints = _optional_string(payload.get("triage_hints"))

    dependencies_value = payload.get("dependencies", [])
    if not isinstance(dependencies_value, list):
        raise ServiceCatalogError(
            f"{path} service '{service_id}' dependencies must be a list"
        )
    dependencies = tuple(
        _require_non_empty_string(dep, f"{path} service '{service_id}' dependency")
        for dep in dependencies_value
    )

    environments_value = payload.get("environments")
    if not isinstance(environments_value, dict) or not environments_value:
        raise ServiceCatalogError(
            f"{path} service '{service_id}' must define at least one environment"
        )

    environments: dict[str, ServiceEnvironmentConfig] = {}
    for env_name, env_payload in environments_value.items():
        if not isinstance(env_payload, dict):
            raise ServiceCatalogError(
                f"{path} service '{service_id}' environment '{env_name}' must be a mapping"
            )
        normalized_env_name = _require_non_empty_string(
            env_name, f"{path} service '{service_id}' environment name"
        )
        aws_payload = env_payload.get("aws")
        logs_payload = env_payload.get("logs")
        if not isinstance(aws_payload, dict):
            raise ServiceCatalogError(
                f"{path} service '{service_id}' environment '{env_name}' is missing aws config"
            )
        if not isinstance(logs_payload, dict):
            raise ServiceCatalogError(
                f"{path} service '{service_id}' environment '{env_name}' is missing logs config"
            )

        environments[normalized_env_name] = ServiceEnvironmentConfig(
            name=normalized_env_name,
            deployment_repo=_require_non_empty_string(
                env_payload.get("deployment_repo"),
                f"{path} service '{service_id}' environment '{env_name}' deployment_repo",
            ),
            cloudwatch_account_id=_require_non_empty_string(
                aws_payload.get("cloudwatch_account_id"),
                f"{path} service '{service_id}' environment '{env_name}' cloudwatch_account_id",
            ),
            region=_require_non_empty_string(
                aws_payload.get("region"),
                f"{path} service '{service_id}' environment '{env_name}' aws.region",
            ),
            logs_topic=_require_non_empty_string(
                logs_payload.get("topic"),
                f"{path} service '{service_id}' environment '{env_name}' logs.topic",
            ),
            logs_application_name=_require_non_empty_string(
                logs_payload.get("application_name"),
                f"{path} service '{service_id}' environment '{env_name}' logs.application_name",
            ),
        )

    if default_environment is not None and default_environment not in environments:
        raise ServiceCatalogError(
            f"{path} service '{service_id}' default_environment '{default_environment}' is not defined in environments"
        )

    return ServiceDefinition(
        service_id=service_id,
        group_id=group_id,
        group_name=group_name,
        display_name=display_name,
        description=description,
        triage_hints=triage_hints,
        code_repo=code_repo,
        default_environment=default_environment,
        dependencies=dependencies,
        environments=environments,
    )


def _parse_yaml(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise ServiceCatalogError(f"failed to parse YAML {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ServiceCatalogError(f"{path} must contain a top-level mapping")
    return data


def _require_non_empty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ServiceCatalogError(f"{label} must be a non-empty string")
    return value.strip()


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ServiceCatalogError("optional string field must be a string when provided")
    stripped = value.strip()
    return stripped or None


__all__ = [
    "ClassifierServiceHint",
    "ServiceCatalog",
    "ServiceCatalogError",
    "ServiceDefinition",
    "ServiceEnvironmentConfig",
    "ServiceGroup",
]


def _query_tokens(raw_query: str) -> set[str]:
    tokens = {
        token
        for token in re.findall(r"[a-z0-9]+", raw_query.lower())
        if len(token) >= 3
    }
    filtered = {token for token in tokens if token not in _STOPWORDS}
    return filtered or tokens


def _service_match_score(service: ServiceDefinition, query_tokens: set[str]) -> int:
    if not query_tokens:
        return 0
    haystacks = [
        service.service_id.lower(),
        (service.display_name or "").lower(),
        service.description.lower(),
    ]
    score = 0
    for token in query_tokens:
        if any(token in haystack for haystack in haystacks):
            score += 1
    return score


def _description_excerpt(description: str, *, query_tokens: set[str]) -> str:
    lines = [line.strip() for line in description.splitlines() if line.strip()]
    for line in lines:
        lowered = line.lower()
        if any(token in lowered for token in query_tokens):
            return line[:500]
    if not lines:
        return ""
    return lines[0][:500]


def _extract_time_window_hint(description: str, query_tokens: set[str]) -> str | None:
    """Extract time window information from service description for query tokens.
    
    Looks for documented time windows in the service description that match the query.
    Returns the time window hint if found, None otherwise.
    """
    import re
    
    lines = description.lower().splitlines()
    for line in lines:
        # Check if line contains query tokens
        if not any(token in line for token in query_tokens):
            continue
        
        # Look for time window patterns like "03:00 – 06:00", "04:00-08:00 IST", etc.
        # Patterns: HH:MM – HH:MM, HH:MM-HH:MM, HH:MM to HH:MM
        time_patterns = [
            r'(\d{1,2}):(\d{2})\s*[–\-to]\s*(\d{1,2}):(\d{2})\s*(ist)?',
            r'(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})',
        ]
        
        for pattern in time_patterns:
            match = re.search(pattern, line)
            if match:
                # Extract the full time window string with IST if present
                full_match = match.group(0)
                return full_match.strip()
    
    return None


_STOPWORDS = {
    "not",
    "available",
    "show",
    "recent",
    "service",
    "context",
    "since",
    "between",
    "last",
    "past",
    "hours",
    "hour",
    "minutes",
    "minute",
}
