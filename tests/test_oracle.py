"""LLM oráculo de los experimentos: responde con las etiquetas del generador."""

from __future__ import annotations

import json

from eda_retail.experiments.oracle import Oracle
from eda_retail.llm.generation import LlmRequest
from eda_retail.llm.prompts.interpreter import NoticeInterpretation
from eda_retail.llm.schemas import PlanProposal
from eda_retail.synthetic.generator import generate


def _ask(oracle: Oracle, agent: str, prompt: str) -> dict:
    out = oracle.client().complete(LlmRequest(agent=agent, incident_id="I1", system="s", prompt=prompt, schema={}))
    return json.loads(out.text)


def test_oracle_interprets_and_plans_from_labels():
    """El oráculo interpreta el aviso y propone el plan esperado de su etiqueta"""
    data = generate(seed=11, size=12)
    oracle = Oracle(data)
    for n in data.notices:
        got = NoticeInterpretation.model_validate(_ask(oracle, "interpreter", f"<aviso>\n{n.text}\n</aviso>"))
        assert (got.order_ids, got.cause, got.severity, got.estimated_delay_hours) == (
            [n.order_id], n.cause, n.severity, n.delay_hours)
        plan = PlanProposal.model_validate(_ask(oracle, "planner", f"Pedidos afectados:\n- {n.order_id}"))
        assert plan.command_steps() == list(n.expected_steps)


def test_oracle_replans_with_refund_when_reissue_is_impossible():
    """Tras un fallo de reserve_stock, el oráculo sustituye la reserva y el reenvío por un reembolso
    y no repite los pasos completados (regla del replanner)"""
    data = generate(seed=11, size=12)
    n = next(x for x in data.notices if x.cause == "loss")
    amount = next(o.amount for o in data.orders if o.order_id == n.order_id)
    prompt = (
        "Plan vigente (versión 1):\n"
        f"- S1: notify_customer {{'order_id': '{n.order_id}'}} depends_on [] -> completed\n"
        f"- S2: file_claim {{'order_id': '{n.order_id}'}} depends_on [] -> completed\n"
        f"- S3: reserve_stock {{'order_id': '{n.order_id}'}} depends_on [] -> failed, error out_of_stock\n"
        f"- S4: reissue_shipment {{'order_id': '{n.order_id}'}} depends_on ['S3'] -> pending\n\n"
        "Fallo: el paso S3 terminó con el error out_of_stock y no se puede reintentar."
    )
    plan = PlanProposal.model_validate(_ask(Oracle(data), "replanner", prompt)).command_steps()
    assert [(s["step_id"], s["action"]) for s in plan] == [("S5", "issue_refund")]
    assert plan[0]["params"] == {"order_id": n.order_id, "amount": float(amount)}


def test_oracle_writes_valid_drafts():
    """El oráculo redacta un borrador en español que solo menciona su pedido"""
    from decimal import Decimal

    from eda_retail.llm.prompts.writer import DraftRequest, validate_draft

    data = generate(seed=11, size=3)
    n = data.notices[0]
    text = _ask(Oracle(data), "writer", f"Redacta un aviso.\nPedido: {n.order_id} (1 x SKU-LAMP).")["text"]
    for kind in ("customer_notice", "claim"):
        request = DraftRequest(kind, "I1", n.order_id, "SKU-LAMP", 1, n.cause, n.delay_hours, (Decimal(1),))
        assert validate_draft(text, request) is None
