"""RequestStep y RecordStepResult.

- Un paso solo se pide con sus dependencias completadas (spec incident-planning).
- `issue_refund` es el paso pivote: no se pide mientras haya compensaciones sin terminar.
- La `idempotency_key` se calcula con la versión del plan en la que se creó el paso.
- Al pedirse una compensación (`-comp`), el paso original pasa a `compensating`; al
  completarse, a `compensated`.
- Un resultado de una `plan_version` anterior a la vigente se añade al log (auditoría)
  pero no cambia el estado del paso (supuesto A2). Si el paso estaba `cancelled` y era
  compensable, se deriva su compensación (supuesto A4).
"""

from __future__ import annotations

from typing import Any

from psycopg.types.json import Jsonb

from eda_retail.command.handler import Context, Rejected, executes
from eda_retail.command.models import RecordStepResult, RequestStep
from eda_retail.events.catalog import EFFECT_EVENT, Action
from eda_retail.infra.effect_keys import compute_idempotency_key

COMPENSATION_SUFFIX = "-comp"
# Acción compensable -> su compensación (tabla de la saga, spec incident-planning).
COMPENSATIONS = {Action.RESERVE_STOCK.value: Action.RELEASE_STOCK.value}


def load_step(ctx: Context, step_id: str) -> dict[str, Any]:
    row = ctx.conn.execute(
        "SELECT * FROM plan_steps WHERE incident_id = %s AND step_id = %s",
        (ctx.command.incident_id, step_id),  # type: ignore[attr-defined]
    ).fetchone()
    if row is None:
        raise Rejected("step_not_found", f"paso {step_id}")
    return row


def blocking_reason(ctx: Context, step: dict[str, Any]) -> Rejected | None:
    """Por qué no puede pedirse ahora un paso `pending`, o None si puede."""
    incident_id = ctx.command.incident_id  # type: ignore[attr-defined]
    if step["depends_on"]:
        done = ctx.conn.execute(
            "SELECT count(*) AS n FROM plan_steps WHERE incident_id = %s AND step_id = ANY(%s) AND status = 'completed'",
            (incident_id, step["depends_on"]),
        ).fetchone()["n"]
        if done < len(step["depends_on"]):
            return Rejected("dependencies_pending", f"{step['step_id']} depende de {step['depends_on']}")
    if step["action"] == Action.ISSUE_REFUND:
        pending = ctx.conn.execute(
            """
            SELECT count(*) AS n FROM plan_steps
            WHERE incident_id = %s AND (status = 'compensating' OR (compensates IS NOT NULL AND status <> 'completed'))
            """,
            (incident_id,),
        ).fetchone()["n"]
        if pending:
            return Rejected("compensation_pending", f"{pending} compensaciones sin terminar")
    return None


def emit_request(ctx: Context, step: dict[str, Any]) -> None:
    """Marca el paso `requested` y emite su StepRequested (sin comprobar ni subir la versión)."""
    incident_id = ctx.command.incident_id  # type: ignore[attr-defined]
    ctx.conn.execute(
        "UPDATE plan_steps SET status = 'requested' WHERE incident_id = %s AND step_id = %s",
        (incident_id, step["step_id"]),
    )
    if step["compensates"]:
        ctx.conn.execute(
            "UPDATE plan_steps SET status = 'compensating' WHERE incident_id = %s AND step_id = %s",
            (incident_id, step["compensates"]),
        )
    ctx.emit(
        "StepRequested",
        {
            "incident_id": incident_id,
            "plan_version": ctx.incident["plan_version"],  # type: ignore[index]
            "step_id": step["step_id"],
            "action": step["action"],
            "params": step["params"],
            "idempotency_key": compute_idempotency_key(
                incident_id, step["step_id"], origin_plan_version=step["origin_plan_version"]
            ),
        },
    )


def derive_compensations(ctx: Context, originals: list[dict[str, Any]]) -> None:
    """Inserta los pasos `-comp` de `originals` (completados y compensables), encadenados
    en orden inverso: la compensación del último paso va primero (spec incident-planning)."""
    incident_id = ctx.command.incident_id  # type: ignore[attr-defined]
    ordered = sorted(originals, key=lambda s: (s["origin_plan_version"], _natural(s["step_id"])), reverse=True)
    previous: str | None = None
    for original in ordered:
        comp_id = f"{original['step_id']}{COMPENSATION_SUFFIX}"
        result = original["result"] or {}
        params = {"order_id": original["params"].get("order_id"), "reservation_id": result.get("reservation_id")}
        ctx.conn.execute(
            """
            INSERT INTO plan_steps
                (incident_id, step_id, origin_plan_version, action, params, depends_on, compensates, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
            ON CONFLICT DO NOTHING
            """,
            (
                incident_id, comp_id, ctx.incident["plan_version"],  # type: ignore[index]
                COMPENSATIONS[original["action"]], Jsonb(params), [previous] if previous else [],
                original["step_id"],
            ),
        )
        previous = comp_id


def _natural(step_id: str) -> tuple[int, str]:
    digits = "".join(ch for ch in step_id if ch.isdigit())
    return (int(digits) if digits else 0, step_id)


@executes(RequestStep)
def _request_step(ctx: Context) -> None:
    cmd: RequestStep = ctx.command  # type: ignore[assignment]
    step = load_step(ctx, cmd.step_id)
    if step["status"] != "pending":
        raise Rejected("step_not_requestable", f"estado {step['status']}")
    if (reason := blocking_reason(ctx, step)) is not None:
        raise reason
    ctx.bump_version()
    emit_request(ctx, step)


@executes(RecordStepResult)
def _record_step_result(ctx: Context) -> None:
    cmd: RecordStepResult = ctx.command  # type: ignore[assignment]
    step = load_step(ctx, cmd.step_id)
    current = ctx.incident["plan_version"]  # type: ignore[index]
    if cmd.plan_version > current:
        raise Rejected("invalid_command", f"plan_version {cmd.plan_version} posterior a la vigente {current}")
    stale = cmd.plan_version < current
    if not stale and step["status"] != "requested":
        raise Rejected("step_not_requested", f"estado {step['status']}")
    if cmd.outcome == "completed":
        if cmd.result is None:
            raise Rejected("invalid_command", "un paso completado lleva result")
    elif cmd.error is None or cmd.retryable is None:
        raise Rejected("invalid_command", "un paso fallido lleva error y retryable")

    ctx.bump_version()
    if cmd.outcome == "failed":
        if not stale:
            ctx.conn.execute(
                "UPDATE plan_steps SET status = 'failed', attempts = %s WHERE incident_id = %s AND step_id = %s",
                (cmd.attempts, cmd.incident_id, cmd.step_id),
            )
        ctx.emit(
            "StepFailed",
            {"step_id": cmd.step_id, "plan_version": cmd.plan_version, "error": cmd.error,
             "retryable": cmd.retryable, "attempts": cmd.attempts},
        )
        return

    ctx.emit("StepCompleted", {"step_id": cmd.step_id, "plan_version": cmd.plan_version, "result": cmd.result})
    if cmd.effect is not None:
        ctx.emit(EFFECT_EVENT[Action(step["action"])], cmd.effect)
    if stale and not (step["status"] == "cancelled" and step["action"] in COMPENSATIONS):
        return  # obsoleto: queda en el log, no decide
    ctx.conn.execute(
        "UPDATE plan_steps SET status = 'completed', attempts = %s, result = %s WHERE incident_id = %s AND step_id = %s",
        (cmd.attempts, Jsonb(cmd.result), cmd.incident_id, cmd.step_id),
    )
    if stale:  # paso compensable cancelado que sí tuvo efecto (supuesto A4)
        derive_compensations(ctx, [{**step, "result": cmd.result}])
        return
    if step["compensates"]:
        ctx.conn.execute(
            "UPDATE plan_steps SET status = 'compensated' WHERE incident_id = %s AND step_id = %s AND status = 'compensating'",
            (cmd.incident_id, step["compensates"]),
        )
