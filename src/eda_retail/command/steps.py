"""RequestStep y RecordStepResult.

- Un paso solo se pide con sus dependencias completadas (spec incident-planning).
- `issue_refund` es el paso pivote: no se pide mientras haya compensaciones pendientes.
- La `idempotency_key` se calcula con la versión del plan en la que se creó el paso.
- Al completarse una compensación (`-comp`), el paso original pasa a `compensated`.
"""

from __future__ import annotations

from eda_retail.command.handler import Context, Rejected, executes
from eda_retail.command.models import RecordStepResult, RequestStep
from eda_retail.events.catalog import EFFECT_EVENT, Action
from eda_retail.infra.effect_keys import compute_idempotency_key


def _load_step(ctx: Context, step_id: str) -> dict:
    row = ctx.conn.execute(
        "SELECT * FROM plan_steps WHERE incident_id = %s AND step_id = %s",
        (ctx.command.incident_id, step_id),  # type: ignore[attr-defined]
    ).fetchone()
    if row is None:
        raise Rejected("step_not_found", f"paso {step_id}")
    return row


@executes(RequestStep)
def _request_step(ctx: Context) -> None:
    cmd: RequestStep = ctx.command  # type: ignore[assignment]
    step = _load_step(ctx, cmd.step_id)
    if step["status"] != "pending":
        raise Rejected("step_not_requestable", f"estado {step['status']}")
    if step["depends_on"]:
        done = ctx.conn.execute(
            "SELECT count(*) AS n FROM plan_steps WHERE incident_id = %s AND step_id = ANY(%s) AND status = 'completed'",
            (cmd.incident_id, step["depends_on"]),
        ).fetchone()["n"]
        if done < len(step["depends_on"]):
            raise Rejected("dependencies_pending", f"{cmd.step_id} depende de {step['depends_on']}")
    if step["action"] == Action.ISSUE_REFUND:
        compensating = ctx.conn.execute(
            "SELECT count(*) AS n FROM plan_steps WHERE incident_id = %s AND status = 'compensating'",
            (cmd.incident_id,),
        ).fetchone()["n"]
        if compensating:
            raise Rejected("compensation_pending", f"{compensating} compensaciones sin terminar")

    ctx.bump_version()
    ctx.conn.execute(
        "UPDATE plan_steps SET status = 'requested' WHERE incident_id = %s AND step_id = %s",
        (cmd.incident_id, cmd.step_id),
    )
    ctx.emit(
        "StepRequested",
        {
            "incident_id": cmd.incident_id,
            "plan_version": ctx.incident["plan_version"],  # type: ignore[index]
            "step_id": cmd.step_id,
            "action": step["action"],
            "params": step["params"],
            "idempotency_key": compute_idempotency_key(
                cmd.incident_id, cmd.step_id, origin_plan_version=step["origin_plan_version"]
            ),
        },
    )


@executes(RecordStepResult)
def _record_step_result(ctx: Context) -> None:
    cmd: RecordStepResult = ctx.command  # type: ignore[assignment]
    step = _load_step(ctx, cmd.step_id)
    if step["status"] != "requested":
        raise Rejected("step_not_requested", f"estado {step['status']}")
    if cmd.outcome == "completed":
        if cmd.result is None or cmd.effect is None:
            raise Rejected("invalid_command", "un paso completado lleva result y el payload de su evento de efecto")
    elif cmd.error is None or cmd.retryable is None:
        raise Rejected("invalid_command", "un paso fallido lleva error y retryable")

    plan_version = ctx.incident["plan_version"]  # type: ignore[index]
    ctx.bump_version()
    ctx.conn.execute(
        "UPDATE plan_steps SET status = %s WHERE incident_id = %s AND step_id = %s",
        (cmd.outcome, cmd.incident_id, cmd.step_id),
    )
    if cmd.outcome == "failed":
        ctx.emit(
            "StepFailed",
            {"step_id": cmd.step_id, "plan_version": plan_version, "error": cmd.error,
             "retryable": cmd.retryable, "attempts": 1},
        )
        return
    ctx.emit("StepCompleted", {"step_id": cmd.step_id, "plan_version": plan_version, "result": cmd.result})
    ctx.emit(EFFECT_EVENT[Action(step["action"])], cmd.effect)  # type: ignore[arg-type]
    if step["compensates"]:
        ctx.conn.execute(
            "UPDATE plan_steps SET status = 'compensated' WHERE incident_id = %s AND step_id = %s AND status = 'compensating'",
            (cmd.incident_id, step["compensates"]),
        )
