"""Conversores de las versiones anteriores del catálogo y lectura en la versión vigente.

- `StepCompleted` y `StepFailed` v1 → v2: los v1 no llevan `plan_version`; se leen como
  de la versión 1 del plan, la primera (supuesto A3 de odd/tasks/orchestration.md).
  `StepFailed` v1 se lee con un intento.
- `LlmCallRecorded` v1 → v2: los v1 no llevan la versión ni la huella del prompt; se leen con
  `prompt_version` `"unknown"` y `prompt_hash` nulo (spec agent-harness).
"""

from __future__ import annotations

from typing import Any

from eda_retail.events.catalog import CATALOG
from eda_retail.events.envelope import EventEnvelope
from eda_retail.events.versioning import Upcasters

UPCASTERS = Upcasters()


@UPCASTERS.register("StepCompleted", 1)
def _step_completed_v1(payload: dict[str, Any]) -> dict[str, Any]:
    return {"step_id": payload["step_id"], "plan_version": 1, "result": payload["result"]}


@UPCASTERS.register("StepFailed", 1)
def _step_failed_v1(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "step_id": payload["step_id"], "plan_version": 1, "error": payload["error"],
        "retryable": payload["retryable"], "attempts": 1,
    }


@UPCASTERS.register("LlmCallRecorded", 1)
def _llm_call_recorded_v1(payload: dict[str, Any]) -> dict[str, Any]:
    return {**payload, "prompt_version": "unknown", "prompt_hash": None}


def to_current(event: EventEnvelope, upcasters: Upcasters = UPCASTERS) -> EventEnvelope:
    """El evento en la versión vigente del catálogo; si ya lo está o no es del catálogo, el mismo."""
    spec = CATALOG.get(event.event_type)
    if spec is None or event.event_version >= spec.version:
        return event
    return upcasters.upcast(event, spec.version)
