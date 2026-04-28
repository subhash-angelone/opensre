"""Terminal renderer for remote agent streaming events.

Reuses spinner and label patterns from app.output so that remote investigation
output looks identical to a local ``opensre investigate`` run.

Handles both ``stream_mode: ["updates"]`` (legacy node-level) and
``stream_mode: ["events"]`` (fine-grained tool/LLM callbacks).
"""

from __future__ import annotations

import re
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from app.output import (
    ProgressTracker,
    _node_label,
    get_output_format,
    is_verbose_output,
    render_investigation_header,
)
from app.remote.reasoning import reasoning_text
from app.remote.stream import StreamEvent

_RESET = "\033[0m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_WHITE = "\033[37m"
_CYAN = "\033[1;36m"

_NODE_START_KINDS = frozenset(
    {
        "on_chain_start",
    }
)

_NODE_END_KINDS = frozenset(
    {
        "on_chain_end",
    }
)

_TRACE_SNIPPET_KEYS = ("message", "text", "error", "content", "body", "summary")


@dataclass
class ToolTraceStep:
    node_name: str
    tool_name: str
    started_at: float
    run_id: str = ""
    ended_at: float | None = None
    input_args: dict[str, Any] = field(default_factory=dict)
    reason: str = ""
    query_text: str | None = None
    normalized_query: str | None = None
    retry_count: int | None = None
    fallback_path: str | None = None
    result_count: int | None = None
    snippets: list[str] = field(default_factory=list)

    @property
    def elapsed_ms(self) -> int:
        if self.ended_at is None:
            return 0
        return max(0, int((self.ended_at - self.started_at) * 1000))


@dataclass
class NodeTraceStep:
    node_name: str
    started_at: float
    ended_at: float | None = None
    reason: str = ""
    tool_steps: list[ToolTraceStep] = field(default_factory=list)

    @property
    def elapsed_ms(self) -> int:
        if self.ended_at is None:
            return 0
        return max(0, int((self.ended_at - self.started_at) * 1000))


class StreamRenderer:
    """Renders a stream of LangGraph SSE events as live terminal progress.

    Wraps ProgressTracker to show the same spinners and resolved-dot lines
    that local investigations produce, driven by remote streaming events.
    When receiving ``events``-mode events, the spinner subtext is updated
    in real time with tool calls, LLM reasoning, and other decisions.
    """

    def __init__(self) -> None:
        self._tracker = ProgressTracker()
        self._active_node: str | None = None
        self._events_received: int = 0
        self._node_names_seen: list[str] = []
        self._final_state: dict[str, Any] = {}
        self._stream_completed = False
        self._stream_started_at: float | None = None
        self._node_trace: list[NodeTraceStep] = []
        self._current_node_step: NodeTraceStep | None = None
        self._tool_trace: list[ToolTraceStep] = []
        self._active_tools: list[ToolTraceStep] = []

    @property
    def events_received(self) -> int:
        return self._events_received

    @property
    def node_names_seen(self) -> list[str]:
        return list(self._node_names_seen)

    @property
    def final_state(self) -> dict[str, Any]:
        return dict(self._final_state)

    @property
    def stream_completed(self) -> bool:
        return self._stream_completed

    @property
    def trace_nodes(self) -> list[NodeTraceStep]:
        return list(self._node_trace)

    @property
    def trace_tools(self) -> list[ToolTraceStep]:
        return list(self._tool_trace)

    def render_stream(self, events: Iterator[StreamEvent]) -> dict[str, Any]:
        """Consume a full event stream and render progress to the terminal.

        Returns the accumulated final state dict.
        """
        _print_connection_banner()

        for event in events:
            self._handle_event(event)

        self._finish_active_node()
        self._print_report()
        return dict(self._final_state)

    def _handle_event(self, event: StreamEvent) -> None:
        self._events_received += 1
        if self._stream_started_at is None:
            self._stream_started_at = event.timestamp

        if event.event_type == "metadata":
            return

        if event.event_type == "end":
            self._stream_completed = True
            self._finish_active_node()
            return

        if event.event_type == "updates":
            self._handle_update(event)
            return

        if event.event_type == "events":
            self._handle_events_mode(event)
            return

    def _handle_update(self, event: StreamEvent) -> None:
        node = event.node_name
        if not node:
            return

        canonical = _canonical_node_name(node)

        if canonical != self._active_node:
            self._finish_active_node(event.timestamp)
            self._active_node = canonical
            if canonical not in self._node_names_seen:
                self._node_names_seen.append(canonical)
            self._tracker.start(canonical)
            self._start_node_trace(canonical, event.timestamp)

        self._merge_state(event.data.get(node, event.data))

    def _handle_events_mode(self, event: StreamEvent) -> None:
        """Process a fine-grained ``events``-mode SSE event.

        Node lifecycle is inferred from ``on_chain_start`` /
        ``on_chain_end`` events whose ``langgraph_node`` matches a
        graph-level node.  Sub-node callbacks (tool calls, LLM
        reasoning) update the active spinner's subtext in real time.
        """
        node = event.node_name
        kind = event.kind

        if not node:
            return

        canonical = _canonical_node_name(node)

        if kind in _NODE_START_KINDS and self._is_graph_node_event(event):
            if canonical != self._active_node:
                self._finish_active_node(event.timestamp)
                self._active_node = canonical
                if canonical not in self._node_names_seen:
                    self._node_names_seen.append(canonical)
                self._tracker.start(canonical)
                self._start_node_trace(canonical, event.timestamp)
            return

        if kind in _NODE_END_KINDS and self._is_graph_node_event(event):
            output = event.data.get("data", {}).get("output", {})
            if isinstance(output, dict):
                self._merge_state(output)
            if canonical == self._active_node:
                self._finish_active_node(event.timestamp)
            return

        if kind == "on_tool_start":
            self._record_tool_start(event, canonical)
        if canonical == self._active_node:
            text = reasoning_text(kind, event.data, canonical)
            if text:
                self._tracker.update_subtext(canonical, text)
                self._update_node_reason(canonical, text)
        if kind == "on_tool_end":
            self._record_tool_end(event, canonical)

    @staticmethod
    def _is_graph_node_event(event: StreamEvent) -> bool:
        """True when the event is a top-level graph node transition.

        LangGraph tags graph-level node chains with ``graph:step:<N>``.
        Sub-chains inside a node (tool executors, LLM calls) lack this tag.
        """
        name = str(event.data.get("name", ""))
        tags = event.tags
        if any(t.startswith("graph:step:") for t in tags):
            return True
        if any(t.startswith("langsmith:") for t in tags):
            return False
        return bool(name == event.node_name)

    def _finish_active_node(self, ended_at: float | None = None) -> None:
        if self._active_node is None:
            return
        self._finish_node_trace(self._active_node, ended_at or time.monotonic())
        message = self._build_node_message(self._active_node)
        self._tracker.complete(self._active_node, message=message)
        self._active_node = None

    def _merge_state(self, update: Any) -> None:
        if isinstance(update, dict):
            self._final_state.update(update)

    def _build_node_message(self, node: str) -> str | None:
        if node == "plan_actions":
            actions = self._final_state.get("planned_actions", [])
            if actions:
                return f"Planned actions: {actions}"
        if node == "resolve_integrations":
            integrations = self._final_state.get("resolved_integrations", {})
            if integrations:
                names = list(integrations.keys())
                return f"Resolved: {names}"
        if node in {"diagnose", "diagnose_root_cause"}:
            score = self._final_state.get("validity_score")
            if score is not None:
                return f"validity:{int(score * 100)}%"
        return None

    def _print_report(self) -> None:
        alert_name = self._final_state.get("alert_name", "Unknown")
        pipeline = self._final_state.get("pipeline_name", "Unknown")
        severity = self._final_state.get("severity", "unknown")

        if alert_name != "Unknown" or pipeline != "Unknown":
            render_investigation_header(alert_name, pipeline, severity)

        root_cause = self._final_state.get("root_cause", "")
        report = self._final_state.get("report", "")

        if root_cause:
            _print_section("Root Cause", root_cause)
        if report:
            _print_section("Report", report)
        elif not root_cause:
            if self._final_state.get("is_noise"):
                _print_info("Alert classified as noise — no investigation needed.")
            elif self._events_received == 0:
                _print_info("No events received from the remote agent.")
        self._print_trace_summary()
        if is_verbose_output():
            self._print_trace_detail()

    def _start_node_trace(self, node_name: str, started_at: float) -> None:
        step = NodeTraceStep(node_name=node_name, started_at=started_at)
        self._node_trace.append(step)
        self._current_node_step = step

    def _finish_node_trace(self, node_name: str, ended_at: float) -> None:
        step = self._current_node_step
        if step is not None and step.node_name == node_name and step.ended_at is None:
            step.ended_at = ended_at
            if not step.reason and (message := self._build_node_message(node_name)):
                step.reason = message
        self._current_node_step = None

    def _update_node_reason(self, node_name: str, reason: str) -> None:
        step = self._current_node_step
        if step is not None and step.node_name == node_name:
            step.reason = reason

    def _record_tool_start(self, event: StreamEvent, canonical: str) -> None:
        name = str(event.data.get("name", "") or "tool")
        payload = event.data.get("data", {})
        input_args = payload.get("input", {}) if isinstance(payload, dict) else {}
        step = ToolTraceStep(
            node_name=canonical,
            tool_name=name,
            started_at=event.timestamp,
            run_id=event.run_id,
            input_args=input_args if isinstance(input_args, dict) else {},
            reason=f"calling {name.replace('_', ' ')}",
            query_text=_extract_query_text(input_args),
        )
        self._active_tools.append(step)

    def _record_tool_end(self, event: StreamEvent, canonical: str) -> None:
        name = str(event.data.get("name", "") or "tool")
        step = self._pop_active_tool(event, canonical, name)
        if step is None:
            step = ToolTraceStep(node_name=canonical, tool_name=name, started_at=event.timestamp)
        step.ended_at = event.timestamp

        payload = event.data.get("data", {})
        output = payload.get("output") if isinstance(payload, dict) else None
        if isinstance(output, dict):
            step.query_text = step.query_text or _extract_query_text(output)
            step.normalized_query = str(output.get("search_query_used") or "").strip() or None
            step.retry_count = _extract_retry_count(output)
            step.fallback_path = _extract_fallback_path(output)
            step.result_count = _extract_result_count(output)
            step.snippets = _extract_snippets(output)
            step.reason = _tool_reason(output, step.result_count, step.fallback_path)
        else:
            text = str(output or "").strip()
            if text:
                step.reason = _bounded_text(text, 120)

        self._tool_trace.append(step)
        node_step = self._current_node_step
        if node_step is not None and node_step.node_name == canonical:
            node_step.tool_steps.append(step)
            if step.reason:
                node_step.reason = step.reason

    def _print_trace_summary(self) -> None:
        if not self._node_trace:
            return
        node_steps = [step for step in self._node_trace if step.ended_at is not None]
        if not node_steps:
            return
        totals: dict[str, int] = {}
        for step in node_steps:
            totals[step.node_name] = totals.get(step.node_name, 0) + step.elapsed_ms
        slowest_name, slowest_elapsed = max(totals.items(), key=lambda item: item[1])
        lines = [f"{_node_label(name)} {_fmt_ms(elapsed)}" for name, elapsed in totals.items()]
        lines.append(f"Slowest step: {_node_label(slowest_name)} {_fmt_ms(slowest_elapsed)}")
        _print_section("Trace Summary", "\n".join(lines))

    def _print_trace_detail(self) -> None:
        if not self._node_trace:
            return
        stream_start = self._stream_started_at or 0.0
        lines: list[str] = []
        for step in self._node_trace:
            if step.ended_at is None:
                continue
            lines.append(
                f"NODE {_node_label(step.node_name)} "
                f"start={_fmt_offset(step.started_at, stream_start)} "
                f"end={_fmt_offset(step.ended_at, stream_start)} "
                f"elapsed={_fmt_ms(step.elapsed_ms)}"
            )
            if step.reason:
                lines.append(f"  why: {_bounded_text(step.reason, 160)}")
            for tool_step in step.tool_steps:
                lines.append(
                    f"  TOOL {tool_step.tool_name} "
                    f"start={_fmt_offset(tool_step.started_at, stream_start)} "
                    f"end={_fmt_offset(tool_step.ended_at or tool_step.started_at, stream_start)} "
                    f"elapsed={_fmt_ms(tool_step.elapsed_ms)}"
                )
                if tool_step.query_text:
                    lines.append(f"    query: {_bounded_text(tool_step.query_text, 160)}")
                if tool_step.normalized_query:
                    lines.append(
                        f"    normalized_query: {_bounded_text(tool_step.normalized_query, 160)}"
                    )
                if tool_step.retry_count is not None:
                    lines.append(f"    retries: {tool_step.retry_count}")
                if tool_step.fallback_path:
                    lines.append(f"    fallback: {tool_step.fallback_path}")
                if tool_step.result_count is not None:
                    lines.append(f"    result_count: {tool_step.result_count}")
                if tool_step.reason:
                    lines.append(f"    why: {_bounded_text(tool_step.reason, 160)}")
                for snippet in tool_step.snippets:
                    lines.append(f"    snippet: {snippet}")
        if lines:
            _print_section("Trace Detail", "\n".join(lines))

    def _pop_active_tool(
        self,
        event: StreamEvent,
        canonical: str,
        tool_name: str,
    ) -> ToolTraceStep | None:
        if event.run_id:
            for index in range(len(self._active_tools) - 1, -1, -1):
                step = self._active_tools[index]
                if (
                    step.node_name == canonical
                    and step.tool_name == tool_name
                    and step.run_id == event.run_id
                ):
                    return self._active_tools.pop(index)
        for index in range(len(self._active_tools) - 1, -1, -1):
            step = self._active_tools[index]
            if step.node_name == canonical and step.tool_name == tool_name:
                return self._active_tools.pop(index)
        return None


def _canonical_node_name(name: str) -> str:
    """Map LangGraph node names to the canonical names used by ProgressTracker."""
    mapping = {
        "diagnose_root_cause": "diagnose_root_cause",
        "diagnose": "diagnose_root_cause",
        "publish_findings": "publish_findings",
        "publish": "publish_findings",
    }
    return mapping.get(name, name)


def _print_connection_banner() -> None:
    if get_output_format() == "rich":
        sys.stdout.write(
            f"\n  {_BOLD}{_CYAN}Remote Investigation{_RESET}"
            f"  {_DIM}streaming from deployed agent{_RESET}\n\n"
        )
    else:
        print("\n  Remote Investigation  streaming from deployed agent\n")
    sys.stdout.flush()


def _print_section(title: str, content: str) -> None:
    if get_output_format() == "rich":
        sys.stdout.write(f"\n  {_BOLD}{_WHITE}{title}{_RESET}\n")
        for line in content.strip().splitlines():
            sys.stdout.write(f"  {_DIM}{line}{_RESET}\n")
    else:
        print(f"\n  {title}")
        for line in content.strip().splitlines():
            print(f"  {line}")
    sys.stdout.flush()


def _print_info(message: str) -> None:
    if get_output_format() == "rich":
        sys.stdout.write(f"\n  {_DIM}{message}{_RESET}\n")
    else:
        print(f"\n  {message}")
    sys.stdout.flush()


def _extract_query_text(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("search_query_used", "query", "search_keyword", "log_query"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_retry_count(output: dict[str, Any]) -> int | None:
    attempt_count = output.get("search_attempt_count")
    if isinstance(attempt_count, int):
        return max(0, attempt_count - 1)
    return None


def _extract_fallback_path(output: dict[str, Any]) -> str | None:
    attempted = output.get("search_queries_attempted")
    if isinstance(attempted, list) and attempted:
        compact = [str(item).strip() for item in attempted if str(item).strip()]
        if len(compact) > 1:
            return " -> ".join(compact)
    fallback_applied = output.get("search_fallback_applied")
    if fallback_applied:
        normalized = output.get("search_query_used")
        if isinstance(normalized, str) and normalized.strip():
            return normalized.strip()
    return None


def _extract_result_count(output: dict[str, Any]) -> int | None:
    if isinstance(output.get("total_returned"), int):
        return int(output["total_returned"])
    for key in ("lines", "logs", "records", "rows", "matches"):
        value = output.get(key)
        if isinstance(value, list):
            return len(value)
    return None


def _extract_snippets(output: dict[str, Any], *, max_snippets: int = 2) -> list[str]:
    snippets: list[str] = []
    for key in ("lines", "logs", "records", "rows", "matches"):
        value = output.get(key)
        if not isinstance(value, list):
            continue
        for item in value:
            snippet = _snippet_from_item(item)
            if snippet:
                snippets.append(snippet)
            if len(snippets) >= max_snippets:
                return snippets
    return snippets


def _snippet_from_item(item: Any) -> str | None:
    if isinstance(item, str):
        return _bounded_text(_redact_sensitive_text(item), 140)
    if isinstance(item, dict):
        for key in _TRACE_SNIPPET_KEYS:
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return _bounded_text(_redact_sensitive_text(value.strip()), 140)
    return None


def _tool_reason(output: dict[str, Any], result_count: int | None, fallback_path: str | None) -> str:
    if error := output.get("error"):
        return f"error: {error}"
    if result_count is not None:
        reason = f"returned {result_count} result(s)"
        if fallback_path:
            return f"{reason}; fallback applied"
        return reason
    if output.get("available") is False:
        return "tool unavailable"
    return "completed"


def _bounded_text(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _redact_sensitive_text(text: str) -> str:
    redacted = text
    redacted = redacted.replace("\n", " ")
    redacted = redacted.replace("\r", " ")
    for pattern in (
        r"(?i)(api[_-]?key|token|password|secret)\s*[:=]\s*([^\s,;]+)",
        r"(?i)(authorization)\s*[:=]\s*([^\s,;]+)",
    ):
        redacted = re.sub(pattern, r"\1=***REDACTED***", redacted)
    return redacted


def _fmt_ms(elapsed_ms: int) -> str:
    return f"{elapsed_ms / 1000:.1f}s" if elapsed_ms >= 1000 else f"{elapsed_ms}ms"


def _fmt_offset(timestamp: float, base: float) -> str:
    return f"+{max(0.0, timestamp - base):.1f}s"
