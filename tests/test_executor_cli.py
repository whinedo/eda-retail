"""Punto de entrada de los ejecutores."""

from __future__ import annotations

import pytest

from eda_retail.command.handler import CommandHandler
from eda_retail.config import Settings
from eda_retail.events.catalog import Action
from eda_retail.executors.cli import build_executor, parse_args
from eda_retail.llm.fake import FakeLlmClient


@pytest.mark.parametrize("action", [a.value for a in Action])
def test_each_action_has_its_own_executor(action):
    """Cada acción tiene un ejecutor propio, con su consumer group, que solo atiende su acción"""
    ex = build_executor(action, FakeLlmClient(), CommandHandler(), Settings())
    assert ex.name == f"executor-{action}"
    assert ex.action.action == action
    assert list(ex.handlers) == ["StepRequested"]


def test_unknown_action_is_rejected():
    """Una acción fuera de la lista permitida no arranca"""
    with pytest.raises(SystemExit):
        parse_args(["delete_order"])
