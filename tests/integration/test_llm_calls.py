"""observability-audit: registro de las llamadas al LLM en el log; presupuesto por incidencia."""

from __future__ import annotations

import uuid
from dataclasses import replace

from eda_retail.command.handler import CommandHandler, incident_id_for_notice
from eda_retail.command.models import EscalateIncident, RecordLlmCall

from .command_helpers import RULES, events_of, handler, open_incident, register_notice, version_of


def _record(conn, incident: str, *, tokens_in=100, tokens_out=50, passed=True, attempt=1, agent="planner"):
    result = handler().handle(
        conn,
        RecordLlmCall(
            command_id=uuid.uuid4(), expected_version=0, producer=agent, incident_id=incident,
            agent=agent, model="fake-model", tokens_in=tokens_in, tokens_out=tokens_out,
            latency_ms=12, validation_passed=passed, attempt=attempt,
        ),
    )
    conn.commit()
    return result


def _counters(conn, incident: str) -> tuple[int, int]:
    row = conn.execute("SELECT tokens_used, llm_calls FROM incidents WHERE incident_id = %s", (incident,)).fetchone()
    conn.commit()
    return row["tokens_used"], row["llm_calls"]


def test_planner_call(conn):
    """Llamada del planificador"""
    incident, _ = open_incident(conn)
    v = version_of(conn, incident)
    result = _record(conn, incident, tokens_in=300, tokens_out=120, passed=False, attempt=2)
    assert result.accepted
    [ev] = result.events
    assert ev.event_type == "LlmCallRecorded"
    assert ev.payload == {
        "incident_id": incident, "agent": "planner", "model": "fake-model", "tokens_in": 300,
        "tokens_out": 120, "latency_ms": 12, "validation_passed": False, "attempt": 2,
    }
    assert _counters(conn, incident) == (420, 1)
    assert version_of(conn, incident) == v  # hecho de solo añadir: no participa en la concurrencia


def test_llm_call_is_recorded_on_closed_incident(conn):
    """Las llamadas se registran aunque la incidencia esté cerrada (el coste ya se produjo)"""
    incident, _ = open_incident(conn, orders=["P9-no-existe"])  # escalada al abrir
    assert _record(conn, incident).accepted


def test_llm_call_requires_incident(conn):
    """RecordLlmCall de una incidencia inexistente se rechaza"""
    assert _record(conn, "I-no-existe").reason == "incident_not_found"


def test_token_budget_exhausted(conn):
    """Presupuesto de tokens agotado (comprobación del command handler)"""
    incident, _ = open_incident(conn)
    tight = CommandHandler(replace(RULES, max_tokens_per_incident=500, max_llm_calls_per_incident=10))
    assert tight.check_budget(conn, incident) is None
    _record(conn, incident, tokens_in=400, tokens_out=100)
    assert tight.check_budget(conn, incident) == "budget_exhausted"


def test_call_budget_exhausted(conn):
    """El presupuesto de llamadas al LLM también se agota"""
    incident, _ = open_incident(conn)
    tight = CommandHandler(replace(RULES, max_tokens_per_incident=10_000, max_llm_calls_per_incident=2))
    _record(conn, incident, tokens_in=1, tokens_out=1)
    assert tight.check_budget(conn, incident) is None
    _record(conn, incident, tokens_in=1, tokens_out=1)
    assert tight.check_budget(conn, incident) == "budget_exhausted"


def test_escalate_before_incident_exists(conn):
    """EscalateIncident de un aviso sin incidencia crea la incidencia escalada"""
    notice, _ = register_notice(conn)
    incident = incident_id_for_notice(notice)
    result = handler().handle(
        conn,
        EscalateIncident(command_id=uuid.uuid4(), expected_version=0, producer="interpreter",
                         incident_id=incident, reason="llm_output_invalid"),
    )
    conn.commit()
    assert result.accepted
    row = conn.execute("SELECT status, cause FROM incidents WHERE incident_id = %s", (incident,)).fetchone()
    conn.commit()
    assert (row["status"], row["cause"]) == ("escalated", "unknown")
    assert [e["event_type"] for e in events_of(conn, incident)][-1] == "IncidentEscalated"


def test_escalate_unknown_incident_without_notice(conn):
    """EscalateIncident sin incidencia ni aviso se rechaza"""
    result = handler().handle(
        conn,
        EscalateIncident(command_id=uuid.uuid4(), expected_version=0, producer="x", incident_id="I-nada", reason="r"),
    )
    conn.commit()
    assert result.reason == "incident_not_found"
