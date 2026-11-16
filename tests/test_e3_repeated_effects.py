"""Métrica de pasos repetidos de E3 (spec experiments, «E3 Fallo de un paso y replanificación»).

Sin PostgreSQL: la métrica se calcula sobre las filas de `event_log.events`. Un paso se repite cuando su
efecto se ejecuta más de una vez, no cuando se confirma dos veces: un paso mantenido en una revisión se
vuelve a pedir con la misma clave y el ejecutor responde otro `StepCompleted` sin repetir el efecto
(spec idempotency, «Paso mantenido en una revisión»).
"""

from __future__ import annotations

from eda_retail.experiments.e3_replan import repeated_effects


def _event(event_type: str, incident: str, command: str, payload: dict | None = None) -> dict:
    return {"event_type": event_type, "correlation_id": incident, "causation_id": command, "payload": payload or {}}


def _executed(incident: str, command: str, step_id: str, effect: str = "ClaimFiled") -> list[dict]:
    """Lo que publica un ejecutor que ejecuta el efecto: el evento del efecto y `StepCompleted`."""
    return [
        _event("LlmCallRecorded", incident, command),
        _event(effect, incident, command, {"order_id": "P-1"}),
        _event("StepCompleted", incident, command, {"step_id": step_id, "plan_version": 1}),
    ]


def test_step_confirmed_again_after_revision_is_not_repeated():
    """Ningún notify_customer ni file_claim completado se repite: un paso confirmado dos veces sin efecto nuevo"""
    events = [
        *_executed("I-1", "cmd-v1", "S2"),
        # v2 vuelve a pedir el paso mantenido; la clave ya está completada: solo StepCompleted
        _event("StepCompleted", "I-1", "cmd-v2", {"step_id": "S2", "plan_version": 2}),
    ]
    assert repeated_effects(events) == 0


def test_effect_executed_twice_for_the_same_step_is_repeated():
    """Ningún notify_customer ni file_claim completado se repite: el mismo paso ejecuta su efecto dos veces"""
    events = [*_executed("I-1", "cmd-1", "S2"), *_executed("I-1", "cmd-2", "S2")]
    assert repeated_effects(events) == 1


def test_distinct_steps_and_incidents_are_not_repeated():
    """Ningún notify_customer ni file_claim completado se repite: pasos e incidencias distintos"""
    events = [
        *_executed("I-1", "cmd-1", "S1", "CustomerNotified"),
        *_executed("I-1", "cmd-2", "S2"),
        *_executed("I-2", "cmd-3", "S2"),
    ]
    assert repeated_effects(events) == 0
