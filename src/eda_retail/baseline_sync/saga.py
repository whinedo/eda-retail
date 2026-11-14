"""Reglas de la saga de la base síncrona (supuesto A8): las mismas que `command/saga.py` y
`command/steps.py` aplican con SQL sobre `plan_steps`, aquí sobre los pasos en memoria.

- Un paso está listo cuando está `pending` y todas sus dependencias están completadas; los
  listos se ejecutan en orden (versión de origen, `step_id`).
- `issue_refund` es el paso pivote: no se pide mientras quede una compensación pendiente.
- Al revisar el plan se cancelan los pasos no mantenidos que no habían terminado, los
  mantenidos conservan su versión de origen (y su `idempotency_key`) y cada `reserve_stock`
  completado del que dependía un paso cancelado, y del que ya no depende ningún paso vivo,
  se compensa con un `release_stock` (`<step_id>-comp`), en orden inverso y encadenados.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from eda_retail.command import handler  # noqa: F401 — carga command.steps sin import circular
from eda_retail.command.steps import COMPENSATIONS

FINAL = ("completed", "cancelled", "compensated")
_CANCELLABLE = ("pending", "requested", "failed")


def _natural(step_id: str) -> tuple[int, str]:
    digits = "".join(c for c in step_id if c.isdigit())
    return (int(digits) if digits else 0, step_id)



@dataclass
class Step:
    step_id: str
    action: str
    params: dict[str, Any]
    depends_on: list[str]
    origin: int  # versión del plan en la que se creó (idempotency_key)
    status: str = "pending"
    error: str | None = None
    result: dict[str, Any] = field(default_factory=dict)
    compensates: str | None = None

    def view(self) -> dict[str, Any]:
        return {"step_id": self.step_id, "action": self.action, "params": self.params,
                "depends_on": self.depends_on, "status": self.status, "error": self.error}


def ready_steps(steps: dict[str, Step]) -> list[Step]:
    def unblocked(s: Step) -> bool:
        if any(steps[d].status != "completed" for d in s.depends_on):
            return False
        if s.action == "issue_refund":  # paso pivote: no mientras queden compensaciones
            return not any(o.status == "compensating" or (o.compensates and o.status != "completed")
                           for o in steps.values())
        return True
    ready = [s for s in steps.values() if s.status == "pending" and unblocked(s)]
    return sorted(ready, key=lambda s: (s.origin, _natural(s.step_id)))


def apply_revision(steps: dict[str, Step], proposed: list[dict[str, Any]], version: int) -> None:
    """Aplica un plan revisado (ya validado) con la versión nueva `version`."""
    kept = {p["step_id"] for p in proposed}
    cancelled = {sid for sid, s in steps.items()
                 if sid not in kept and s.compensates is None and s.status in _CANCELLABLE}
    alive = kept | {sid for sid, s in steps.items() if s.status not in ("cancelled", "failed") and sid not in cancelled}
    to_compensate = [s for s in steps.values()
                     if s.status == "completed" and s.action in COMPENSATIONS
                     and any(s.step_id in steps[c].depends_on for c in cancelled)
                     and not any(s.step_id in steps[a].depends_on for a in alive if a in steps)]
    for sid in cancelled:
        steps[sid].status = "cancelled"
    for p in proposed:
        old = steps.get(p["step_id"])
        if old is not None:  # paso mantenido: conserva su versión de origen y su clave
            old.params, old.depends_on = p["params"], list(p["depends_on"])
            if old.status in ("failed", "cancelled"):
                old.status, old.error = "pending", None
        else:
            steps[p["step_id"]] = Step(p["step_id"], p["action"], p["params"], list(p["depends_on"]), version)
    previous = None
    for original in sorted(to_compensate, key=lambda s: (s.origin, _natural(s.step_id)), reverse=True):
        comp_id = f"{original.step_id}-comp"
        steps[comp_id] = Step(
            comp_id, COMPENSATIONS[original.action],
            {"order_id": original.params.get("order_id"), "reservation_id": original.result.get("reservation_id")},
            [previous] if previous else [], version, compensates=original.step_id)
        previous = comp_id
