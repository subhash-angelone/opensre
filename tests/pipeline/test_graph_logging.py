from __future__ import annotations

from app.pipeline.graph import with_logging


def test_with_logging_suppresses_output_when_disabled(capsys, monkeypatch) -> None:
    monkeypatch.setenv("OPENSRE_NODE_DEBUG", "0")

    def _node(state):
        return {"seen": state["value"] + 1}

    wrapped = with_logging("example_node", _node)
    result = wrapped({"value": 41})

    captured = capsys.readouterr()
    assert result == {"seen": 42}
    assert captured.out == ""


def test_with_logging_prints_only_node_names_at_level_one(capsys, monkeypatch) -> None:
    monkeypatch.setenv("OPENSRE_NODE_DEBUG", "1")

    def _node(state):
        return {"seen": state["value"] + 1}

    wrapped = with_logging("example_node", _node)
    result = wrapped({"value": 41})

    captured = capsys.readouterr()
    assert result == {"seen": 42}
    assert "[IN] example_node" in captured.out
    assert "[OUT] example_node" in captured.out
    assert "{'value': 41}" not in captured.out
    assert "{'seen': 42}" not in captured.out


def test_with_logging_prints_node_input_and_output_at_level_two(capsys, monkeypatch) -> None:
    monkeypatch.setenv("OPENSRE_NODE_DEBUG", "2")

    def _node(state):
        return {"seen": state["value"] + 1}

    wrapped = with_logging("example_node", _node)
    result = wrapped({"value": 41})

    captured = capsys.readouterr()
    assert result == {"seen": 42}
    assert "[IN] example_node: {'value': 41}" in captured.out
    assert "[OUT] example_node: {'seen': 42}" in captured.out
