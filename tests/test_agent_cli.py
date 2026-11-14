"""Puntos de entrada de los agentes y del proyector."""

from __future__ import annotations

import pytest

from eda_retail.agents.cli import AGENTS, build_handlers, parse_args
from eda_retail.agents.interpreter import Interpreter
from eda_retail.agents.planner import Planner
from eda_retail.agents.replanner import Replanner
from eda_retail.agents.runtime import AgentRunner
from eda_retail.command.handler import CommandHandler
from eda_retail.llm.fake import FakeLlmClient


@pytest.mark.parametrize(
    ("name", "event_type", "agent_type"),
    [("interpreter", "CarrierNoticeReceived", Interpreter), ("planner", "ShipmentDelayed", Planner),
     ("replanner", "StepFailed", Replanner)],
)
def test_each_agent_consumes_its_event(name, event_type, agent_type):
    """Cada agente consume solo su evento (design.md §3) mediante su unidad de trabajo"""
    handlers = build_handlers(name, FakeLlmClient(), CommandHandler())
    assert list(handlers) == [event_type]
    runner = handlers[event_type]
    assert isinstance(runner, AgentRunner) and isinstance(runner.agent, agent_type)
    assert runner.agent.name == name


def test_unknown_agent_is_rejected():
    """Un nombre de agente desconocido se rechaza al arrancar"""
    assert set(AGENTS) == {"interpreter", "planner", "replanner"}
    with pytest.raises(SystemExit):
        parse_args(["writer"])
