"""E23: oráculos de replan, datos, recuentos y H23 (spec experiments, «E23 Duplicados de negocio…»)."""

from __future__ import annotations

import json
from decimal import Decimal

from eda_retail.experiments.e23_business_duplicates import (
    EXERCISED,
    MAX_RESENDS,
    SHARED_EVERY,
    VARIANTS,
    RefundAgainOracle,
    RerequestOracle,
    h23,
    resend_plan,
    shared_order_dataset,
    summarize,
)
from eda_retail.experiments.oracle import Oracle
from eda_retail.llm.generation import LlmRequest
from eda_retail.llm.schemas import PlanProposal
from eda_retail.replanner.handler import check_revision
from eda_retail.rules import BusinessRules
from eda_retail.synthetic.generator import generate

DATA = generate(seed=11, size=12)
LOSS = next(x for x in DATA.notices if x.cause == "loss")


def _replan(oracle: Oracle, prompt: str) -> list[dict]:
    out = oracle.client().complete(LlmRequest(agent="replanner", incident_id="I1", system="s", prompt=prompt,
                                              schema={}))
    return PlanProposal.model_validate(json.loads(out.text)).command_steps()


def _prompt(*lines: str, failed: str) -> str:
    return ("Plan vigente:\n" + "".join(f"- {line}\n" for line in lines)
            + f"\nFallo: el paso {failed} terminó con el error out_of_stock y no se puede reintentar.")


AMOUNTS = {o.order_id: o.amount for o in DATA.orders}


def _current(*steps: tuple[str, str, list[str], str]) -> list[dict]:
    """El plan vigente del replanner: (step_id, acción, depends_on, estado) del pedido LOSS."""
    def params(action: str) -> dict:
        return {"order_id": LOSS.order_id} | ({"amount": float(AMOUNTS[LOSS.order_id])}
                                             if action == "issue_refund" else {})
    return [{"step_id": i, "action": a, "params": params(a), "depends_on": deps, "status": status}
            for i, a, deps, status in steps]


def _prompt_of(current: list[dict], failed: str) -> str:
    o = {"order_id": LOSS.order_id}
    return _prompt(*(f"{s['step_id']}: {s['action']} {o} depends_on {s['depends_on']} -> {s['status']}"
                     for s in current), failed=failed)


LOSS_FAILED = _current(("S1", "notify_customer", [], "completed"), ("S2", "file_claim", [], "completed"),
                       ("S3", "reserve_stock", [], "failed"), ("S4", "reissue_shipment", ["S3"], "pending"))


def _accepted(current: list[dict], plan: list[dict]) -> bool:
    """El replanner del sistema acepta la revisión (spec incident-planning: `check_revision`)."""
    return not isinstance(check_revision(current, plan, BusinessRules(), AMOUNTS), str)


def _loss_failed() -> str:
    return _prompt_of(LOSS_FAILED, "S3")


def test_replan_asks_again_for_a_claim_and_a_notice_already_served():
    """Reclamación solicitada dos veces: el oráculo de la variante (b) vuelve a pedir `file_claim` y
    `notify_customer` ya completados con `step_id` nuevos para el mismo pedido, y el replanner lo acepta"""
    plan = _replan(RerequestOracle(DATA), _loss_failed())
    assert [(s["step_id"], s["action"]) for s in plan] == [
        ("S5", "issue_refund"), ("S6", "notify_customer"), ("S7", "file_claim")]
    assert all(s["params"]["order_id"] == LOSS.order_id for s in plan)
    assert _accepted(LOSS_FAILED, plan)


def test_replan_proposes_the_refund_again_after_a_later_failure():
    """Reembolso único: el oráculo de la variante (c) mantiene, tras el reembolso, la reserva fallida (su
    `step_id`, su clave y por tanto su fallo); cuando vuelve a fallar, el replan siguiente propone `issue_refund`
    otra vez, y el replanner acepta las dos revisiones"""
    first = _replan(RefundAgainOracle(DATA), _loss_failed())
    assert [(s["step_id"], s["action"], s["depends_on"]) for s in first] == [
        ("S5", "issue_refund", []), ("S3", "reserve_stock", ["S5"])]
    assert _accepted(LOSS_FAILED, first)
    again = _current(("S1", "notify_customer", [], "completed"), ("S2", "file_claim", [], "completed"),
                     ("S3", "reserve_stock", ["S5"], "failed"), ("S4", "reissue_shipment", ["S3"], "cancelled"),
                     ("S5", "issue_refund", [], "completed"))
    second = _replan(RefundAgainOracle(DATA), _prompt_of(again, "S3"))
    assert [(s["step_id"], s["action"]) for s in second] == [("S6", "issue_refund")]
    assert _accepted(again, second)


def test_shared_order_dataset_gives_some_orders_a_second_notice():
    """Segundo aviso de un pedido: uno de cada `SHARED_EVERY` pedidos recibe un segundo aviso con otro
    `carrier_notice_id` y las mismas etiquetas; los pedidos no se repiten"""
    data = shared_order_dataset(11, 12)
    twins = data.notices[12:]
    assert len(twins) == len(DATA.notices[::SHARED_EVERY]) and data.orders == DATA.orders
    for original, twin in zip(DATA.notices[::SHARED_EVERY], twins, strict=True):
        assert twin.carrier_notice_id == f"{original.carrier_notice_id}-B"
        assert (twin.order_id, twin.cause, twin.severity, twin.expected_steps) == (
            original.order_id, original.cause, original.severity, original.expected_steps)
        assert json.loads(twin.api_text)["notice_id"] == twin.carrier_notice_id
    assert len({n.carrier_notice_id for n in data.notices}) == len(data.notices)


def test_resend_plan_mixes_ordered_and_concurrent_resends():
    """Avisos reenviados en orden y a la vez: cada aviso se reenvía de 1 a 5 veces, unos en orden y otros a la vez,
    igual con la misma semilla"""
    plan = resend_plan(7, 20)
    assert plan == resend_plan(7, 20)
    assert [i for i, _, _ in plan] == list(range(20))
    assert all(1 <= times <= MAX_RESENDS for _, times, _ in plan)
    assert {concurrent for _, _, concurrent in plan} == {True, False}


def _ev(event_type: str, incident: str, **payload) -> dict:
    return {"event_type": event_type, "correlation_id": incident, "payload": payload}


def _effects(**overrides) -> dict:
    return {"claims": [], "messages": [], "refunds": [], "reservations": []} | overrides


def test_summary_counts_effects_per_incident():
    """Pedido en dos incidencias: cada incidencia mantiene sus efectos y el informe los separa por incidencia"""
    events = [
        _ev("CarrierNoticeReceived", "I1", carrier_notice_id="N1"),
        _ev("CarrierNoticeReceived", "I2", carrier_notice_id="N1-B"),
        _ev("ShipmentDelayed", "I1", order_ids=["P1"]), _ev("ShipmentDelayed", "I2", order_ids=["P1"]),
        _ev("StepRequested", "I1", step_id="S5", action="issue_refund", params={"order_id": "P1"}),
        _ev("StepRequested", "I1", step_id="S7", action="issue_refund", params={"order_id": "P1"}),
    ]
    effects = _effects(
        claims=[{"incident_id": "I1", "order_id": "P1"}, {"incident_id": "I2", "order_id": "P1"}],
        messages=[{"incident_id": i, "order_id": "P1", "plan_version": 1} for i in ("I1", "I2")],
        refunds=[{"incident_id": "I1", "order_id": "P1", "amount": "40.00"}],
        reservations=[{"incident_id": "I2", "order_id": "P1"}])
    m = summarize(events, effects, {"P1": Decimal("40.00")}, discards=3, resends=3)
    assert m == {
        "resends": 3, "duplicate_notice_discards": 3, "incidents_per_notice": 1, "shipment_delayed_per_incident": 1,
        "claims_per_incident_order": 1, "notices_per_incident_order_version": 1, "refunds_per_incident_order": 1,
        "over_refunded": 0, "active_reservations_per_incident_order": 1,
        "requested_again": {"file_claim": 0, "notify_customer": 0, "issue_refund": 1}, "shared_orders": 1}


def test_summary_finds_repeated_effects_and_refunds_over_the_order_amount():
    """Los recuentos ven un efecto repetido: dos reembolsos del pedido en la incidencia superan su importe"""
    refunds = [{"incident_id": "I1", "order_id": "P1", "amount": "40.00"}] * 2
    m = summarize([], _effects(refunds=refunds, messages=[{"incident_id": "I1", "order_id": "P1",
                                                           "plan_version": 2}] * 2),
                  {"P1": Decimal("40.00")}, discards=0, resends=0)
    assert (m["refunds_per_incident_order"], m["over_refunded"], m["notices_per_incident_order_version"]) == (2, 1, 2)


def _clean(**overrides) -> dict:
    inv = {name: {"violations": 0} for name in ("I1", "I2", "I3", "I4", "I5")}
    return {"resends": 4, "duplicate_notice_discards": 4, "incidents_per_notice": 1,
            "shipment_delayed_per_incident": 1, "claims_per_incident_order": 1,
            "notices_per_incident_order_version": 1, "refunds_per_incident_order": 1, "over_refunded": 0,
            "active_reservations_per_incident_order": 1, "shared_orders": 2, "invariants": inv,
            "requested_again": {"file_claim": 1, "notify_customer": 1, "issue_refund": 1}} | overrides


def test_h23_holds_when_every_count_is_at_its_maximum():
    """Cumplimiento de H23: se cumple si todos los recuentos del informe son los máximos de las specs"""
    assert all(h23(v, _clean()) for v in VARIANTS)
    for broken in ({"claims_per_incident_order": 2}, {"notices_per_incident_order_version": 2},
                   {"refunds_per_incident_order": 2}, {"over_refunded": 1}, {"incidents_per_notice": 2},
                   {"shipment_delayed_per_incident": 2}, {"active_reservations_per_incident_order": 2},
                   {"duplicate_notice_discards": 3},
                   {"invariants": {"I1": {"violations": 0}, "I3": {"violations": 1}}}):
        assert not h23("resent_notices", _clean(**broken)), broken


def test_h23_needs_each_variant_to_be_exercised():
    """Una variante que no provocó su duplicado no cuenta como cumplida"""
    idle = _clean(resends=0, duplicate_notice_discards=0, shared_orders=0,
                  requested_again={"file_claim": 0, "notify_customer": 0, "issue_refund": 0})
    assert not any(h23(v, idle) for v in VARIANTS)
    assert set(EXERCISED) == set(VARIANTS)


def test_expexecutor_checks_of_e23():
    from eda_retail.expexecutor.catalog import CATALOG

    variants = {v: _clean(h23=True) for v in VARIANTS}
    assert all(c.ok for c in CATALOG["e23"].checks({"metrics": {"variants": variants}}))
    variants["shared_order"] = _clean(h23=False, claims_per_incident_order=2)
    failed = [c.metric for c in CATALOG["e23"].checks({"metrics": {"variants": variants}}) if not c.ok]
    assert failed == ["metrics.variants.shared_order.h23", "metrics.variants.shared_order.claims_per_incident_order"]
