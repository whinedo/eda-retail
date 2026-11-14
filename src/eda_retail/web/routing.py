"""Emisor y destino de cada evento (spec web-front, "Origen de emisor y destino").

El destino sale de los tipos de evento que declara cada componente (design.md §3); los
proyectores leen todos los eventos y figuran siempre como último consumidor.
"""

from __future__ import annotations

from typing import Any

from eda_retail.agents.cli import AGENTS
from eda_retail.orchestrator.runner import EVENT_TYPES as ORCHESTRATOR_EVENT_TYPES

PROJECTORS = "projectors"
ORCHESTRATOR = "orchestrator"
EXECUTOR_PREFIX = "executor-"
STEP_REQUESTED = "StepRequested"


def receivers(event_type: str, payload: dict[str, Any]) -> list[str]:
    """Consumidores del evento, el principal primero y los proyectores al final."""
    found: list[str] = []
    if event_type in ORCHESTRATOR_EVENT_TYPES:
        found.append(ORCHESTRATOR)
    found.extend(name for name, (consumed, _) in AGENTS.items() if consumed == event_type)
    if event_type == STEP_REQUESTED and payload.get("action"):
        found.append(f"{EXECUTOR_PREFIX}{payload['action']}")
    found.append(PROJECTORS)
    return found


def node_condition(node: str) -> tuple[str, list[Any]]:
    """Condición SQL sobre `events` para los eventos que `node` emite o consume."""
    if node == PROJECTORS:
        return "TRUE", []
    consumes: list[str] = []
    params: list[Any] = [node]
    if node == ORCHESTRATOR:
        consumes.append("event_type = ANY(%s)")
        params.append(list(ORCHESTRATOR_EVENT_TYPES))
    if node in AGENTS:
        consumes.append("event_type = %s")
        params.append(AGENTS[node][0])
    if node.startswith(EXECUTOR_PREFIX):
        consumes.append("(event_type = %s AND payload->>'action' = %s)")
        params.extend([STEP_REQUESTED, node.removeprefix(EXECUTOR_PREFIX)])
    return "(" + " OR ".join(["producer = %s", *consumes]) + ")", params
