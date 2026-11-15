"""Validación determinista de los planes que propone el LLM (specs incident-planning y llm-boundaries).

- solo acciones permitidas; `release_stock` y los `step_id` `-comp` son compensaciones y las deriva el
  código, nunca el plan (`compensation_not_allowed_in_plan`);
- cada paso con `step_id`, `action`, `params` y `depends_on`; `step_id` único; las dependencias existen
  y no forman ciclos;
- `issue_refund` con `amount` no superior al máximo configurado ni, si se dan, al importe de su pedido
  (`refund_limit`);
- `issue_refund` y `reissue_shipment` del mismo pedido son excluyentes, también frente a los pasos ya
  completados (`refund_and_reissue_conflict`).

`ctx` es cualquier objeto con `rules` (`BusinessRules`). La usan el planner, el replanner, el handler
compartido (hasta T10b), E5 y la base síncrona.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import ValidationError

from eda_retail.events.catalog import Action, PlanStep


class PlanRejected(Exception):
    """El plan incumple una regla: `reason` es el motivo y `detail` su explicación."""

    def __init__(self, reason: str, detail: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


COMPENSATION_SUFFIX = "-comp"
PLANNABLE_ACTIONS = {a.value for a in Action} - {Action.RELEASE_STOCK.value}


def validate_plan(
    ctx: Any,
    raw_steps: list[dict[str, Any]],
    *,
    external_ids: set[str],
    completed: list[dict[str, Any]] | None = None,
    order_amounts: Mapping[str, Decimal] | None = None,
) -> list[PlanStep]:
    """Valida los pasos; `external_ids` son pasos ya completados de los que se puede depender
    y `completed` sus filas (acción y params), para las reglas que dependen de lo ya hecho.
    `order_amounts` son los importes de los pedidos en `view_orders`: con ellos, un reembolso
    tampoco puede superar el importe de su pedido (el planificador, design.md §4)."""
    if not raw_steps:
        raise PlanRejected("invalid_plan", "el plan no tiene pasos")
    steps: list[PlanStep] = []
    for i, raw in enumerate(raw_steps):
        try:
            steps.append(PlanStep.model_validate(raw))
        except ValidationError as exc:
            fields = ", ".join(".".join(str(p) for p in e["loc"]) for e in exc.errors())
            raise PlanRejected("invalid_plan", f"paso {i}: campos ausentes o inválidos: {fields}") from exc

    for s in steps:
        if s.action == Action.RELEASE_STOCK or s.step_id.endswith(COMPENSATION_SUFFIX):
            raise PlanRejected("compensation_not_allowed_in_plan", f"paso {s.step_id}")
    for s in steps:
        if s.action not in PLANNABLE_ACTIONS:
            raise PlanRejected("action_not_allowed", f"acción {s.action}")

    ids = [s.step_id for s in steps]
    if len(set(ids)) != len(ids):
        raise PlanRejected("invalid_plan", "step_id repetido")
    known = set(ids) | external_ids
    for s in steps:
        missing = [d for d in s.depends_on if d not in known]
        if missing:
            raise PlanRejected("invalid_plan", f"{s.step_id} depende de pasos inexistentes: {missing}")
    _reject_cycles(steps)

    for s in steps:
        if s.action == Action.ISSUE_REFUND:
            _check_refund(ctx, s, order_amounts)
    _reject_refund_and_reissue(steps, completed or [])
    return steps


_EXCLUSIVE = {Action.ISSUE_REFUND.value, Action.REISSUE_SHIPMENT.value}


def _reject_refund_and_reissue(steps: list[PlanStep], completed: list[dict[str, Any]]) -> None:
    """El cliente no puede recibir a la vez el producto reenviado y su importe."""
    actions: dict[str, set[str]] = {}
    for action, params in [(s.action, s.params) for s in steps] + [(r["action"], r["params"]) for r in completed]:
        if action in _EXCLUSIVE:
            actions.setdefault(str(params.get("order_id")), set()).add(action)
    for order_id, found in actions.items():
        if found == _EXCLUSIVE:
            raise PlanRejected("refund_and_reissue_conflict", f"pedido {order_id}: reembolso y reenvío a la vez")


def _reject_cycles(steps: list[PlanStep]) -> None:
    graph = {s.step_id: [d for d in s.depends_on if d in {x.step_id for x in steps}] for s in steps}
    state: dict[str, int] = {}  # 1 = visitando, 2 = hecho

    def visit(node: str) -> None:
        if state.get(node) == 2:
            return
        if state.get(node) == 1:
            raise PlanRejected("invalid_plan", f"dependencia circular en {node}")
        state[node] = 1
        for dep in graph[node]:
            visit(dep)
        state[node] = 2

    for node in graph:
        visit(node)


def _check_refund(ctx: Any, s: PlanStep, order_amounts: Mapping[str, Decimal] | None) -> None:
    try:
        amount = Decimal(str(s.params["amount"]))
    except (KeyError, InvalidOperation) as exc:
        raise PlanRejected("invalid_plan", f"{s.step_id}: issue_refund sin amount válido") from exc
    if not amount.is_finite() or amount <= 0:
        raise PlanRejected("invalid_plan", f"{s.step_id}: amount debe ser un número positivo")
    if amount > ctx.rules.max_refund_amount:
        raise PlanRejected("refund_limit", f"{s.step_id}: {amount} > {ctx.rules.max_refund_amount}")
    if order_amounts is None:
        return
    order_id = str(s.params.get("order_id"))
    if order_id not in order_amounts:
        raise PlanRejected("refund_limit", f"{s.step_id}: el pedido {order_id} no tiene importe en view_orders")
    if amount > order_amounts[order_id]:
        raise PlanRejected("refund_limit", f"{s.step_id}: {amount} > importe del pedido {order_amounts[order_id]}")
