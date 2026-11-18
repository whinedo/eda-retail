"""E20 Dependencias de V2 (spec experiments) y Fallos de las dependencias de V2 (spec synthetic-data): servidor de
correo y agente del transportista con fallos, contra los clientes MCP y A2A reales, métricas del log y H20.

Sin Kafka ni PostgreSQL. La ejecución de punta a punta está en tests/integration/test_e20_v2_dependencies.py.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from eda_retail.config import Settings
from eda_retail.executor_notify_customer.mail_client import McpMailClient, SendEmailInput
from eda_retail.experiments import harness
from eda_retail.experiments.e20_v2_dependencies import VARIANTS, h20_variant, provoked, settings_for, summarize
from eda_retail.mail_server.directory import synthetic_directory
from eda_retail.mail_server.store import MemoryMailStore
from eda_retail.step_executors.errors import BusinessError, Transient
from eda_retail.synthetic.v2_faults import FaultyCarrier, FaultyMail, chosen

TODAY = date(2026, 11, 15)
SETTINGS = replace(Settings(), a2a_timeout_s=0.5)


def _store() -> MemoryMailStore:
    return MemoryMailStore(synthetic_directory())


def _send(server, key: str, store: MemoryMailStore):
    ref = next(iter(store.directory))
    return McpMailClient(server).send_email(SendEmailInput(customer_ref=ref, subject="s", body="b", idempotency_key=key))


def test_reproducible_choice_of_v2_faults():
    """Elección reproducible de los fallos de V2"""
    keys = [f"K{i}:1" for i in range(200)]
    first = [chosen(7, "down", k, 0.5) for k in keys]
    assert first == [chosen(7, "down", k, 0.5) for k in keys]
    assert 0 < sum(first) < len(keys)
    assert first != [chosen(8, "down", k, 0.5) for k in keys]  # otra semilla, otra elección
    incidents = [f"I-{i}" for i in range(50)]
    a = FaultyCarrier(SETTINGS, "unauthorized", probability=0.5, seed=7, today=TODAY)
    b = FaultyCarrier(SETTINGS, "unauthorized", probability=0.5, seed=7, today=TODAY)
    for incident in incidents[:6]:
        assert a.consult("P-1", "delay", incident_id=incident).outcome == b.consult(
            "P-1", "delay", incident_id=incident).outcome
    assert a.injected == b.injected


def test_mail_retry_can_succeed_after_a_chosen_failure():
    """Elección reproducible de los fallos de V2: la elección cuenta las llamadas con la misma clave"""
    seed = next(s for s in range(100) if chosen(s, "down", "K1:1", 0.5) and not chosen(s, "down", "K1:2", 0.5))
    store = _store()
    server = FaultyMail("down", probability=0.5, seed=seed).build(store)
    with pytest.raises(Transient, match="mail_unavailable"):
        _send(server, "K1", store)
    assert _send(server, "K1", store).message_id and len(store.mailbox) == 1


@pytest.mark.parametrize("mode, error, reason", [("down", Transient, "mail_unavailable"),
                                                 ("invalid", BusinessError, "mcp_output_invalid")])
def test_unstable_mail_server(mode, error, reason):
    """Servidor de correo inestable"""
    store = _store()
    mail = FaultyMail(mode, probability=1.0, seed=1)
    server = mail.build(store)
    for key in ("K1", "K2", "K3", "K4"):
        with pytest.raises(error, match=reason):
            _send(server, key, store)
    assert store.mailbox == []
    assert (mail.calls, mail.keys, mail.injected) == (4, 4, 4)


def test_mail_server_without_chosen_calls_sends_as_usual():
    """Servidor de correo inestable: las llamadas no elegidas se atienden sin fallo"""
    store = _store()
    mail = FaultyMail("invalid", probability=0.0, seed=1)
    out = _send(mail.build(store), "K1", store)
    assert out.message_id == store.mailbox[0].message_id and mail.injected == 0


@pytest.mark.parametrize("mode, outcome, tasks", [(None, "answered", 1), ("slow", "timeout", 1),
                                                  ("unauthorized", "failed", 0),
                                                  ("bad_card", "rejected_by_guardrail", 0)])
def test_unstable_carrier_agent_with_injected_faults(mode, outcome, tasks):
    """Agente del transportista inestable con fallos inyectados"""
    carrier = FaultyCarrier(SETTINGS, mode, probability=1.0, seed=1, today=TODAY)
    result = carrier.consult("P-1", "delay", incident_id="I-1")
    assert result.outcome == outcome == carrier.expected_outcome("I-1")
    assert carrier.tasks_received == tasks
    assert carrier.tasks_on_injected == (tasks if mode else 0)
    assert result.latency_ms <= (SETTINGS.a2a_timeout_s + 1.5) * 1000


def test_harness_builds_the_v2_mail_server_with_the_given_builder():
    """El harness monta el servidor de correo de E20 sobre el buzón respaldado por el servicio `mail`"""
    services = harness.Services()
    mail = FaultyMail("down", probability=0.0, seed=1)
    store = MemoryMailStore(synthetic_directory(), new_id=services.mail.call, dedup=False)
    assert harness.mail_server_for(services, 1, mail.build) is None
    server = harness.mail_server_for(services, 2, mail.build)
    out = _send(server, "K1", store)  # el buzón del servidor es otro: lo que cuenta es el servicio `mail`
    assert out.message_id.startswith("M-") and services.mail.executions == {"K1": 1} and mail.calls == 1


def test_settings_for_enables_v2_with_a_short_a2a_deadline():
    s = settings_for(replace(Settings(), poc_version=1, a2a_timeout_s=5))
    assert (s.poc_version, s.a2a_timeout_s) == (2, 1.0)


def _event(kind: str, incident: str = "I-1", producer: str = "orchestrator", **payload) -> dict:
    return {"event_type": kind, "correlation_id": incident, "producer": producer, "payload": payload}


NOTIFY = "executor-notify_customer"
STEP = {"step_id": "S1", "action": "notify_customer", "params": {"order_id": "P-1"}, "depends_on": []}
PLAN = {"I-1": [["S1", "notify_customer", {"order_id": "P-1"}]]}


def test_summarize_counts_outcomes_mismatches_plans_and_addresses():
    events = [
        _event("CarrierConsulted", "I-1", outcome="answered", latency_ms=40),
        _event("CarrierConsulted", "I-2", outcome="answered", latency_ms=1200),
        _event("PlanCreated", "I-1", "planner", plan_version=1, steps=[STEP]),
        _event("PlanCreated", "I-1", "planner", plan_version=1, steps=[]),  # solo cuenta el primero
        _event("StepFailed", "I-1", NOTIFY, error="mail_unavailable", retryable=True),
        _event("StepFailed", "I-2", NOTIFY, error="mail_unavailable", retryable=False),
        _event("StepFailed", "I-2", "executor-reserve_stock", error="out_of_stock", retryable=False),
        _event("CustomerNotified", "I-1", message_id="M-1"),
        _event("Leaked", "I-3", note="escribe a ana@example.test"),
    ]
    m = summarize(events, lambda incident: "timeout" if incident == "I-2" else "answered")
    assert m["carrier_outcomes"] == {"answered": 2}
    assert m["outcome_mismatches"] == [{"incident_id": "I-2", "outcome": "answered", "expected": "timeout"}]
    assert m["max_consult_latency_ms"] == 1200 and m["planned"] == PLAN
    assert m["step_failed_errors"] == {"mail_unavailable": 2} and m["mail_unavailable_not_retryable"] == 1
    assert m["customer_notified"] == 1 and m["events_with_address"] == 1


def _metrics(**over) -> dict:
    base = {"open": 0, "outcome_mismatches": [], "max_consult_latency_ms": 60, "planned": PLAN,
            "max_mail_executions_per_key": 1, "events_with_address": 0, "mail_unavailable_not_retryable": 0,
            "injected": {"mail": 0, "carrier": 0}, "carrier": {"tasks_on_injected": 0}, "mail_calls": 0,
            "mail_keys": 0, "step_failed_errors": {},
            "invariants": {f"I{i}": {"violations": 0} for i in range(1, 6)}}
    return {**base, **over}


def test_provoked_per_variant():
    assert provoked("baseline", _metrics())
    assert not provoked("mail_down", _metrics(injected={"mail": 3, "carrier": 0}, mail_calls=5, mail_keys=5))
    assert provoked("mail_down", _metrics(injected={"mail": 3, "carrier": 0}, mail_calls=8, mail_keys=5))
    invalid = {"injected": {"mail": 2, "carrier": 0}}
    assert provoked("mail_invalid", _metrics(**invalid, step_failed_errors={"mcp_output_invalid": 2}))
    assert not provoked("mail_invalid", _metrics(**invalid, step_failed_errors={"mcp_output_invalid": 1}))
    for variant in ("carrier_slow", "carrier_401", "carrier_bad_card"):
        assert not provoked(variant, _metrics())
        assert provoked(variant, _metrics(injected={"mail": 0, "carrier": 4}))


@pytest.mark.parametrize("over", [
    {"open": 1},
    {"outcome_mismatches": [{"incident_id": "I-1"}]},
    {"max_consult_latency_ms": 2600},
    {"planned": {}},
    {"planned": {"I-1": [["S1", "file_claim", {"order_id": "P-1"}]]}},
    {"max_mail_executions_per_key": 2},
    {"events_with_address": 1},
    {"mail_unavailable_not_retryable": 1},
    {"invariants": {"I1": {"violations": 1}}},
])
def test_h20_fails_on_each_broken_condition(over):
    carrier = {"injected": {"mail": 0, "carrier": 2}}
    assert h20_variant("carrier_slow", _metrics(**carrier), PLAN)
    assert not h20_variant("carrier_slow", _metrics(**{**carrier, **over}), PLAN)


def test_h20_requires_no_tasks_for_401_and_bad_card():
    injected = {"injected": {"mail": 0, "carrier": 2}, "carrier": {"tasks_on_injected": 1}}
    assert h20_variant("carrier_slow", _metrics(**injected), PLAN)
    assert not h20_variant("carrier_401", _metrics(**injected), PLAN)
    assert not h20_variant("carrier_bad_card", _metrics(**injected), PLAN)


def test_every_variant_injects_a_single_fault():
    assert VARIANTS["baseline"] is None
    assert all(len({"mail", "carrier"} & set(spec)) == 1 for spec in VARIANTS.values() if spec)
