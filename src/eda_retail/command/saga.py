"""ApplyRevision y AdvancePlan: las decisiones de la orquestación (supuesto A1).

El orquestador es un consumidor sin estado: traduce eventos a estos comandos y el command
handler decide con `plan_steps`, en la misma transacción que los eventos.

- `ApplyRevision` (tras un `PlanRevised`): los pasos que la revisión no mantiene y no
  llegaron a tener efecto (`pending`, `requested`, `failed`) pasan a `cancelled` con
  `StepCancelled`; un paso mantenido que seguía `requested` vuelve a `pending` para pedirse
  en la versión nueva (misma `idempotency_key`). Un paso completado y compensable del que
  ya no depende ningún paso vivo y del que dependía uno cancelado recibe su `-comp`.
- `AdvancePlan`: pide a la vez todos los pasos listos (pasos en paralelo) y, si todos los
  pasos están en un estado final, cierra la incidencia con `IncidentResolved`.
"""

from __future__ import annotations

from eda_retail.command.handler import FINAL_STEP_STATUSES, Context, Rejected, executes
from eda_retail.command.models import AdvancePlan, ApplyRevision
from eda_retail.command.steps import COMPENSATIONS, blocking_reason, derive_compensations, emit_request
from eda_retail.events.catalog import Action

_CANCELLABLE = ("pending", "requested", "failed")


@executes(ApplyRevision)
def _apply_revision(ctx: Context) -> None:
    cmd: ApplyRevision = ctx.command  # type: ignore[assignment]
    current = ctx.incident["plan_version"]  # type: ignore[index]
    if cmd.plan_version != current or current < 2:
        raise Rejected("stale_revision", f"versión vigente {current}")
    steps = {
        r["step_id"]: r
        for r in ctx.conn.execute("SELECT * FROM plan_steps WHERE incident_id = %s", (cmd.incident_id,)).fetchall()
    }
    unknown = [s for s in cmd.step_ids if s not in steps]
    if unknown:
        raise Rejected("step_not_found", f"pasos {unknown}")
    kept = set(cmd.step_ids)
    cancelled = [
        r for sid, r in steps.items()
        if sid not in kept and r["compensates"] is None and r["status"] in _CANCELLABLE
    ]
    reset = [sid for sid in kept if steps[sid]["status"] == "requested"]
    cancelled_ids = {r["step_id"] for r in cancelled}
    alive = kept | {sid for sid, r in steps.items() if r["status"] not in ("cancelled", "failed") and sid not in cancelled_ids}
    to_compensate = [
        r for r in steps.values()
        if r["status"] == "completed" and r["action"] in COMPENSATIONS
        and any(r["step_id"] in steps[c]["depends_on"] for c in cancelled_ids)
        and not any(r["step_id"] in steps[a]["depends_on"] for a in alive)
    ]
    if not (cancelled or reset or to_compensate):
        raise Rejected("revision_applied", "la revisión no cambia ningún paso")

    ctx.bump_version()
    for r in cancelled:
        ctx.conn.execute(
            "UPDATE plan_steps SET status = 'cancelled' WHERE incident_id = %s AND step_id = %s",
            (cmd.incident_id, r["step_id"]),
        )
        ctx.emit("StepCancelled", {"incident_id": cmd.incident_id, "plan_version": current, "step_id": r["step_id"]})
    if reset:
        ctx.conn.execute(
            "UPDATE plan_steps SET status = 'pending' WHERE incident_id = %s AND step_id = ANY(%s)",
            (cmd.incident_id, reset),
        )
    derive_compensations(ctx, to_compensate)


@executes(AdvancePlan)
def _advance_plan(ctx: Context) -> None:
    cmd: AdvancePlan = ctx.command  # type: ignore[assignment]
    if ctx.incident["plan_version"] == 0:  # type: ignore[index]
        raise Rejected("no_plan")
    steps = ctx.conn.execute(
        "SELECT * FROM plan_steps WHERE incident_id = %s ORDER BY origin_plan_version, step_id", (cmd.incident_id,)
    ).fetchall()
    ready = [s for s in steps if s["status"] == "pending" and blocking_reason(ctx, s) is None]
    if ready:
        ctx.bump_version()
        for s in ready:
            emit_request(ctx, s)
        return
    if steps and all(s["status"] in FINAL_STEP_STATUSES for s in steps):
        refunded = any(s["action"] == Action.ISSUE_REFUND and s["status"] == "completed" for s in steps)
        ctx.bump_version()
        ctx.conn.execute(
            "UPDATE incidents SET status = 'resolved', closed_at = now() WHERE incident_id = %s", (cmd.incident_id,)
        )
        ctx.emit("IncidentResolved", {"incident_id": cmd.incident_id, "outcome": "refunded" if refunded else "resolved"})
