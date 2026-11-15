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

from typing import Any

from psycopg.types.json import Jsonb

from eda_retail.command.handler import Context, Rejected, executes
from eda_retail.command.models import CreatePlan, RevisePlan
from eda_retail.events.catalog import PlanStep
from eda_retail.plan_validation import (  # noqa: F401
    COMPENSATION_SUFFIX,
    PLANNABLE_ACTIONS,
    validate_plan,
)


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
