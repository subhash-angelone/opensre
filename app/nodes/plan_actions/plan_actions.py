"""Plan investigation actions from available inputs."""

from functools import lru_cache
from pathlib import Path
import re
from typing import Any, cast, get_args

from pydantic import BaseModel

from app.domain import ServiceCatalog, ServiceCatalogError
from app.nodes.investigate.models import InvestigateInput
from app.nodes.investigate.types import ExecutedHypothesis
from app.nodes.plan_actions.build_prompt import (
    plan_actions_with_llm,
    select_actions,
)
from app.nodes.plan_actions.detect_sources import detect_sources
from app.nodes.plan_actions.extract_keywords import extract_keywords
from app.output import debug_print
from app.services import get_llm_for_tools
from app.tools.investigation_registry import get_available_actions
from app.tools.investigation_registry.models import InvestigationAction
from app.tools.investigation_registry.prioritization import (
    DETERMINISTIC_FALLBACK_REASON,
    get_prioritized_actions_with_reasons,
)
from app.types.evidence import EvidenceSource

# Default tool budget if not specified in state
DEFAULT_TOOL_BUDGET = 10
_PRIORITIZATION_SOURCES = frozenset(get_args(EvidenceSource))
SourceConfig = dict[str, object]
AvailableSources = dict[str, SourceConfig]
_DOMAIN_DIRECTORY = Path(__file__).resolve().parents[2] / "domain"


def _hypothesis_actions(hypothesis: ExecutedHypothesis) -> list[str]:
    return hypothesis.get("actions", [])


def _evidence_object_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _get_executed_action_names(executed_hypotheses: list[ExecutedHypothesis]) -> set[str]:
    executed_actions: set[str] = set()
    for hypothesis in executed_hypotheses:
        executed_actions.update(action for action in _hypothesis_actions(hypothesis) if action)
    return executed_actions


def _seed_action_names_for_sources(
    available_sources: AvailableSources,
) -> list[str]:
    seeded: list[str] = []

    if "s3_audit" in available_sources:
        seeded.append("get_s3_object")

    if "openclaw" in available_sources:
        seeded.append("search_openclaw_conversations")
        seeded.append("list_openclaw_tools")

    return seeded


def _fallback_action_names_from_inclusion_reasons(
    inclusion_reasons: list[dict[str, Any]],
) -> list[str]:
    fallback_action_names: list[str] = []
    seen: set[str] = set()

    for reason_entry in inclusion_reasons:
        reasons = reason_entry.get("reasons", [])
        action_name = reason_entry.get("name")
        if not isinstance(reasons, list) or not isinstance(action_name, str):
            continue
        if DETERMINISTIC_FALLBACK_REASON not in reasons or action_name in seen:
            continue
        seen.add(action_name)
        fallback_action_names.append(action_name)

    return fallback_action_names


def _seed_plan_actions(
    planned_actions: list[str],
    available_action_names: list[str],
    available_sources: AvailableSources,
) -> list[str]:
    allowed_seeds = [
        action_name
        for action_name in _seed_action_names_for_sources(available_sources)
        if action_name in available_action_names
    ]

    result: list[str] = []
    seen: set[str] = set()
    for action_name in [*allowed_seeds, *planned_actions]:
        if action_name in seen:
            continue
        seen.add(action_name)
        result.append(action_name)
    return result


@lru_cache(maxsize=1)
def _load_domain_catalog() -> ServiceCatalog | None:
    try:
        return ServiceCatalog.from_directory(_DOMAIN_DIRECTORY)
    except ServiceCatalogError as exc:
        debug_print(f"Domain catalog unavailable: {exc}")
        return None


def _raw_alert_annotations(raw_alert: dict[str, object] | str) -> dict[str, str]:
    if not isinstance(raw_alert, dict):
        return {}
    nested = raw_alert.get("annotations") or raw_alert.get("commonAnnotations") or {}
    if not isinstance(nested, dict):
        nested = {}
    merged = {**nested, **{k: v for k, v in raw_alert.items() if k not in nested}}
    return {str(key): str(value).strip() for key, value in merged.items() if str(value).strip()}


def _time_window_minutes_from_hint(value: str) -> int | None:
    hint = value.strip().lower()
    if not hint:
        return None
    match = re.search(r"(\d{1,2}):(\d{2})\s*(?:–|-|to)\s*(\d{1,2}):(\d{2})", hint)
    if not match:
        return None
    start_hour, start_minute, end_hour, end_minute = (int(part) for part in match.groups())
    start_total = (start_hour * 60) + start_minute
    end_total = (end_hour * 60) + end_minute
    if end_total <= start_total:
        end_total += 24 * 60
    minutes = end_total - start_total
    if minutes <= 0:
        return None
    return min(minutes, 24 * 60)


def _domain_logs_hints(input_data: InvestigateInput) -> dict[str, object]:
    catalog = _load_domain_catalog()
    if catalog is None:
        return {}

    raw_alert = input_data.raw_alert
    annotations = _raw_alert_annotations(raw_alert)
    raw_alert_dict = raw_alert if isinstance(raw_alert, dict) else {}

    query_text = " ".join(
        part
        for part in [
            input_data.problem_md,
            input_data.alert_name,
            str(raw_alert_dict.get("alert_name", "")).strip(),
            str(raw_alert_dict.get("error_message", "")).strip(),
            annotations.get("summary", ""),
            annotations.get("description", ""),
        ]
        if part
    )

    service: object = None
    for candidate in (
        annotations.get("service_id", ""),
        annotations.get("service", ""),
        annotations.get("service_name", ""),
        str(raw_alert_dict.get("service_id", "")).strip(),
        str(raw_alert_dict.get("service", "")).strip(),
        str(raw_alert_dict.get("service_name", "")).strip(),
    ):
        if not candidate:
            continue
        service = catalog.find_service(candidate)
        if service is not None:
            break

    classifier_hints = catalog.build_classifier_context(raw_query=query_text, max_services=3)
    hint_by_service_id = {
        str(entry.get("service_id", "")).strip(): entry for entry in classifier_hints
    }
    if service is None:
        top_hint_service_id = (
            str(classifier_hints[0].get("service_id", "")).strip() if classifier_hints else ""
        )
        service = catalog.find_service(top_hint_service_id) if top_hint_service_id else None
    if service is None:
        return {}

    environment_name = (
        annotations.get("environment")
        or annotations.get("env")
        or str(raw_alert_dict.get("environment", "")).strip()
        or str(raw_alert_dict.get("env", "")).strip()
        or getattr(service, "default_environment", None)
        or ""
    )

    environments = getattr(service, "environments", {})
    environment = environments.get(environment_name)
    if environment is None and environment_name:
        environment = environments.get(environment_name.lower())
    if environment is None and environments:
        first_environment_name = sorted(environments.keys())[0]
        environment = environments[first_environment_name]
        environment_name = first_environment_name
    if environment is None:
        return {}

    service_id = str(getattr(service, "service_id", "")).strip()
    service_hint = hint_by_service_id.get(service_id, {})
    time_window_hint = str(service_hint.get("time_window_hint", "")).strip()
    time_range_minutes = _time_window_minutes_from_hint(time_window_hint)

    result: dict[str, object] = {
        "logs_topic": str(getattr(environment, "logs_topic", "")).strip(),
        "application_name": str(getattr(environment, "logs_application_name", "")).strip(),
        "service_id": service_id,
        "environment": environment_name,
    }
    if time_window_hint:
        result["time_window_hint"] = time_window_hint
    if time_range_minutes is not None:
        result["time_range_minutes"] = time_range_minutes
    return result


def _enrich_logs_api_source_with_domain_hints(
    source: dict[str, object],
    hints: dict[str, object],
) -> None:
    if not hints:
        return
    if not str(source.get("logs_topic", "")).strip() and hints.get("logs_topic"):
        source["logs_topic"] = hints["logs_topic"]
    if not str(source.get("application_name", "")).strip() and hints.get("application_name"):
        source["application_name"] = hints["application_name"]
    hinted_minutes = hints.get("time_range_minutes")
    if isinstance(hinted_minutes, int) and hinted_minutes > 0:
        source["time_range_minutes"] = min(hinted_minutes, 24 * 60)
    if hints.get("service_id"):
        source["domain_service_id"] = hints["service_id"]
    if hints.get("environment"):
        source["domain_environment"] = hints["environment"]
    if hints.get("time_window_hint"):
        source["domain_time_window_hint"] = hints["time_window_hint"]


def _ensure_seed_actions_available(
    available_actions: list[InvestigationAction],
    action_pool: list[InvestigationAction],
    available_sources: AvailableSources,
    tool_budget: int,
    executed_hypotheses: list[ExecutedHypothesis],
    additional_required_action_names: list[str] | None = None,
) -> tuple[list[InvestigationAction], list[str]]:
    selected = list(available_actions)
    selected_names = {action.name for action in selected}
    executed_action_names = _get_executed_action_names(executed_hypotheses)
    pool_by_name = {action.name: action for action in action_pool}
    seed_actions: list[InvestigationAction] = []
    required_action_names = [
        *_seed_action_names_for_sources(available_sources),
        *(additional_required_action_names or []),
    ]
    seen_required: set[str] = set()

    for action_name in required_action_names:
        if action_name in seen_required:
            continue
        seen_required.add(action_name)
        if action_name in executed_action_names:
            continue
        action = pool_by_name.get(action_name)
        if action is None or action_name in selected_names:
            continue
        if not action.is_available(available_sources):
            continue
        seed_actions.append(action)
        selected_names.add(action_name)

    if seed_actions:
        selected = [*seed_actions, *selected]

    selected = selected[:tool_budget]
    return selected, [action.name for action in selected]


def detect_reroute_trigger(
    evidence: dict[str, object],
    available_sources: AvailableSources,
    executed_hypotheses: list[ExecutedHypothesis],
) -> tuple[bool, str]:
    """
    Detect if new evidence requires rerouting to different tools.

    Rerouting is triggered when new evidence changes the likely source family,
    such as discovering an audit_key from S3 metadata that enables tracing
    external vendor interactions.

    Args:
        evidence: Current evidence gathered
        available_sources: Currently available data sources
        executed_hypotheses: History of executed hypotheses

    Returns:
        Tuple of (should_reroute, reroute_reason)
    """
    # Check if s3_audit source was discovered from evidence but not yet utilized
    s3_audit_in_sources = "s3_audit" in available_sources

    # Check if we've already done audit tracing in a previous loop
    s3_audit_already_executed = any(
        "get_s3_object" in _hypothesis_actions(hyp) for hyp in executed_hypotheses
    )

    # Trigger reroute if s3_audit source available but audit not yet executed
    if s3_audit_in_sources and not s3_audit_already_executed:
        return (
            True,
            "s3_audit source discovered from S3 metadata - rerouting to external API tracing",
        )

    # Check for Grafana service name discovery without log fetching
    grafana_service_names = evidence.get("grafana_service_names", [])
    grafana_logs = evidence.get("grafana_logs", [])
    if grafana_service_names and not grafana_logs:
        grafana_logs_already_queried = any(
            "query_grafana_logs" in _hypothesis_actions(hyp) for hyp in executed_hypotheses
        )
        if not grafana_logs_already_queried:
            return True, "grafana service names discovered but logs not yet fetched"

    # Check for vendor audit discovered in Lambda logs
    vendor_audit = evidence.get("vendor_audit_from_logs")
    vendor_audit_already_rerouted = any(
        (hyp.get("audit") or {}).get("reroute_reason")
        == "external vendor audit discovered in Lambda logs"
        for hyp in executed_hypotheses
    )
    if vendor_audit and not vendor_audit_already_rerouted:
        return True, "external vendor audit discovered in Lambda logs"

    return False, ""


def plan_actions(
    input_data: InvestigateInput,
    plan_model: type[BaseModel],
    _pipeline_name: str = "",
    resolved_integrations: dict[str, object] | None = None,
) -> tuple[
    BaseModel | None,
    AvailableSources,
    list[str],
    list[InvestigationAction],
    bool,
    str,
    list[dict[str, Any]],
]:
    """
    Interpret inputs, select actions, and request a plan from the LLM.

    Supports rerouting when new evidence changes the likely source family,
    and enforces per-step tool budgets to cap prompt size and execution breadth.

    Args:
        input_data: InvestigateInput (or compatible) object
        plan_model: Pydantic model for structured LLM output
        _pipeline_name: Unused (was for memory lookup, kept for caller compatibility)
        resolved_integrations: Pre-fetched integration credentials from resolve_integrations node

    Returns:
        Tuple of (plan_or_none, available_sources, available_action_names, available_actions, rerouted, reroute_reason, inclusion_reasons)
    """
    # Get tool budget from input (with default)
    tool_budget = getattr(input_data, "tool_budget", DEFAULT_TOOL_BUDGET)

    available_sources = detect_sources(
        input_data.raw_alert, input_data.context, resolved_integrations=resolved_integrations
    )
    domain_hints = _domain_logs_hints(input_data)
    logs_api_source = available_sources.get("logs_api")
    if isinstance(logs_api_source, dict):
        _enrich_logs_api_source_with_domain_hints(logs_api_source, domain_hints)

    # Enhance sources with dynamically discovered information from evidence (e.g., audit_key from S3 metadata)
    s3_object = _evidence_object_dict(input_data.evidence.get("s3_object", {}))
    s3_metadata = _evidence_object_dict(s3_object.get("metadata", {}))
    audit_key = s3_metadata.get("audit_key")
    bucket = s3_object.get("bucket")
    if s3_object.get("found") and audit_key and bucket and "s3_audit" not in available_sources:
        available_sources["s3_audit"] = {"bucket": bucket, "key": audit_key}
        debug_print(f"Added s3_audit source: s3://{bucket}/{audit_key}")

    # Detect if rerouting is needed based on new evidence
    rerouted, reroute_reason = detect_reroute_trigger(
        evidence=input_data.evidence,
        available_sources=available_sources,
        executed_hypotheses=input_data.executed_hypotheses,
    )
    if rerouted:
        debug_print(f"REROUTE TRIGGERED: {reroute_reason}")

    debug_print(f"Relevant sources: {list(available_sources.keys())}")

    keywords = extract_keywords(input_data.problem_md, input_data.alert_name)
    prioritization_sources = [
        cast(EvidenceSource, source_name)
        for source_name in available_sources
        if source_name in _PRIORITIZATION_SOURCES
    ]
    all_actions = get_available_actions()
    if keywords or prioritization_sources:
        candidate_actions, inclusion_reasons = get_prioritized_actions_with_reasons(
            sources=prioritization_sources,
            keywords=keywords,
        )
    else:
        candidate_actions = all_actions
        inclusion_reasons = []

    # Apply tool budget to cap the selected tool set before prompt construction
    available_actions, available_action_names = select_actions(
        actions=candidate_actions,
        available_sources=available_sources,
        executed_hypotheses=input_data.executed_hypotheses,
        tool_budget=tool_budget,
    )
    available_actions, available_action_names = _ensure_seed_actions_available(
        available_actions=available_actions,
        action_pool=all_actions,
        available_sources=available_sources,
        tool_budget=tool_budget,
        executed_hypotheses=input_data.executed_hypotheses,
        additional_required_action_names=_fallback_action_names_from_inclusion_reasons(
            inclusion_reasons
        ),
    )

    if not available_action_names:
        return (
            None,
            available_sources,
            available_action_names,
            available_actions,
            rerouted,
            reroute_reason,
            inclusion_reasons,
        )

    llm = get_llm_for_tools()

    plan = plan_actions_with_llm(
        llm=llm,
        plan_model=plan_model,
        problem_md=input_data.problem_md,
        executed_hypotheses=input_data.executed_hypotheses,
        available_actions=available_actions,
        available_sources=available_sources,
        memory_context="",
    )

    plan.actions = _seed_plan_actions(
        planned_actions=plan.actions,
        available_action_names=available_action_names,
        available_sources=available_sources,
    )

    debug_print(f"Plan: {plan.actions} | {plan.rationale[:100]}...")
    if len(plan.actions) > tool_budget:
        debug_print(f"WARNING: Plan exceeds tool budget ({len(plan.actions)} > {tool_budget})")
        plan.actions = plan.actions[:tool_budget]

    return (
        plan,
        available_sources,
        available_action_names,
        available_actions,
        rerouted,
        reroute_reason,
        inclusion_reasons,
    )
