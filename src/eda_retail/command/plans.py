"""CreatePlan y RevisePlan: validación determinista de los planes propuestos por el LLM.

Reglas (specs incident-planning y llm-boundaries):
- solo acciones permitidas; `release_stock` y los `step_id` `-comp` son compensaciones y
  las deriva el código, nunca el plan (`compensation_not_allowed_in_plan`);
- cada paso con `step_id`, `action`, `params` y `depends_on`; `step_id` único; las
  dependencias existen y no forman ciclos;
- `issue_refund` con `amount` no superior al máximo configurado (`refund_limit`);
- `issue_refund` y `reissue_shipment` del mismo pedido son excluyentes, también frente a
  los pasos ya completados (`refund_and_reissue_conflict`);
- una revisión mantiene el `step_id` y la versión de origen de los pasos que conserva y
  no vuelve a incluir pasos completados.

La cancelación de pasos no mantenidos y las compensaciones las hace la orquestación.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from psycopg.types.json import Jsonb
from pydantic import ValidationError

from eda_retail.command.handler import Context, Rejected, executes
from eda_retail.command.models import CreatePlan, RevisePlan
from eda_retail.events.catalog import Action, PlanStep

COMPENSATION_SUFFIX = "-comp"
PLANNABLE_ACTIONS = {a.value for a in Action} - {Action.RELEASE_STOCK.value}


def validate_plan(
    ctx: Context,
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
        raise Rejected("invalid_plan", "el plan no tiene pasos")
    steps: list[PlanStep] = []
    for i, raw in enumerate(raw_steps):
        try:
            steps.append(PlanStep.model_validate(raw))
        except ValidationError as exc:
            fields = ", ".join(".".join(str(p) for p in e["loc"]) for e in exc.errors())
            raise Rejected("invalid_plan", f"paso {i}: campos ausentes o inválidos: {fields}") from exc

    for s in steps:
        if s.action == Action.RELEASE_STOCK or s.step_id.endswith(COMPENSATION_SUFFIX):
            raise Rejected("compensation_not_allowed_in_plan", f"paso {s.step_id}")
    for s in steps:
        if s.action not in PLANNABLE_ACTIONS:
            raise Rejected("action_not_allowed", f"acción {s.action}")

    ids = [s.step_id for s in steps]
    if len(set(ids)) != len(ids):
        raise Rejected("invalid_plan", "step_id repetido")
    known = set(ids) | external_ids
    for s in steps:
        missing = [d for d in s.depends_on if d not in known]
        if missing:
            raise Rejected("invalid_plan", f"{s.step_id} depende de pasos inexistentes: {missing}")
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
            raise Rejected("refund_and_reissue_conflict", f"pedido {order_id}: reembolso y reenvío a la vez")


def _reject_cycles(steps: list[PlanStep]) -> None:
    graph = {s.step_id: [d for d in s.depends_on if d in {x.step_id for x in steps}] for s in steps}
    state: dict[str, int] = {}  # 1 = visitando, 2 = hecho

    def visit(node: str) -> None:
        if state.get(node) == 2:
            return
        if state.get(node) == 1:
            raise Rejected("invalid_plan", f"dependencia circular en {node}")
        state[node] = 1
        for dep in graph[node]:
            visit(dep)
        state[node] = 2

    for node in graph:
        visit(node)


def _check_refund(ctx: Context, s: PlanStep, order_amounts: Mapping[str, Decimal] | None) -> None:
    try:
        amount = Decimal(str(s.params["amount"]))
    except (KeyError, InvalidOperation) as exc:
        raise Rejected("invalid_plan", f"{s.step_id}: issue_refund sin amount válido") from exc
    if not amount.is_finite() or amount <= 0:
        raise Rejected("invalid_plan", f"{s.step_id}: amount debe ser un número positivo")
    if amount > ctx.rules.max_refund_amount:
        raise Rejected("refund_limit", f"{s.step_id}: {amount} > {ctx.rules.max_refund_amount}")
    if order_amounts is None:
        return
    order_id = str(s.params.get("order_id"))
    if order_id not in order_amounts:
        raise Rejected("refund_limit", f"{s.step_id}: el pedido {order_id} no tiene importe en view_orders")
    if amount > order_amounts[order_id]:
        raise Rejected("refund_limit", f"{s.step_id}: {amount} > importe del pedido {order_amounts[order_id]}")


def _insert_step(ctx: Context, s: PlanStep, origin_version: int) -> None:
    ctx.conn.execute(
        """
        INSERT INTO plan_steps (incident_id, step_id, origin_plan_version, action, params, depends_on, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'pending')
        """,
        (ctx.command.incident_id, s.step_id, origin_version, s.action, Jsonb(s.params), s.depends_on),  # type: ignore[attr-defined]
    )


def _steps_payload(steps: list[PlanStep]) -> list[dict[str, Any]]:
    return [s.model_dump(mode="json") for s in steps]


@executes(CreatePlan)
def _create_plan(ctx: Context) -> None:
    cmd: CreatePlan = ctx.command  # type: ignore[assignment]
    if ctx.incident["plan_version"] != 0:  # type: ignore[index]
        raise Rejected("plan_exists")
    steps = validate_plan(ctx, cmd.steps, external_ids=set())
    ctx.bump_version()
    for s in steps:
        _insert_step(ctx, s, origin_version=1)
    ctx.conn.execute("UPDATE incidents SET plan_version = 1 WHERE incident_id = %s", (cmd.incident_id,))
    ctx.emit("PlanCreated", {"incident_id": cmd.incident_id, "plan_version": 1, "steps": _steps_payload(steps)})


@executes(RevisePlan)
def _revise_plan(ctx: Context) -> None:
    cmd: RevisePlan = ctx.command  # type: ignore[assignment]
    current = ctx.incident["plan_version"]  # type: ignore[index]
    if current == 0:
        raise Rejected("no_plan")
    existing = {
        r["step_id"]: r
        for r in ctx.conn.execute(
            "SELECT step_id, action, params, status FROM plan_steps WHERE incident_id = %s", (cmd.incident_id,)
        ).fetchall()
    }
    completed = {sid for sid, r in existing.items() if r["status"] == "completed"}
    steps = validate_plan(
        ctx, cmd.steps, external_ids=completed, completed=[existing[sid] for sid in completed]
    )
    for s in steps:
        if s.step_id in completed:
            raise Rejected("completed_step_in_plan", f"paso {s.step_id}")
        old = existing.get(s.step_id)
        if old is not None and old["action"] != s.action:
            raise Rejected("invalid_plan", f"{s.step_id} cambia de acción ({old['action']} -> {s.action})")

    new_version = current + 1
    ctx.bump_version()
    for s in steps:
        if s.step_id in existing:
            # Paso mantenido: conserva step_id y versión de origen (y por tanto su idempotency_key).
            ctx.conn.execute(
                """
                UPDATE plan_steps
                SET params = %s, depends_on = %s,
                    status = CASE WHEN status IN ('failed', 'cancelled') THEN 'pending' ELSE status END
                WHERE incident_id = %s AND step_id = %s
                """,
                (Jsonb(s.params), s.depends_on, cmd.incident_id, s.step_id),
            )
        else:
            _insert_step(ctx, s, origin_version=new_version)
    ctx.conn.execute(
        "UPDATE incidents SET plan_version = %s, revisions = revisions + 1 WHERE incident_id = %s",
        (new_version, cmd.incident_id),
    )
    ctx.emit(
        "PlanRevised",
        {"incident_id": cmd.incident_id, "plan_version": new_version, "steps": _steps_payload(steps), "reason": cmd.reason},
    )
