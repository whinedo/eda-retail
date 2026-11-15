"""Reglas de la saga del orquestador (spec incident-planning), sin base de datos.

Cada paso es un dict con las columnas de `orchestrator.plan_steps`: `step_id`, `action`, `params`,
`depends_on`, `compensates`, `status`, `origin_plan_version` y `result`. Son las mismas reglas que
aplicaba el handler compartido (`command/steps.py` y `command/saga.py`), ahora en el único dueño
del estado de la saga:

- un paso se pide cuando sus dependencias están completadas; `issue_refund` (pivote) espera a que no
  quede ninguna compensación sin terminar; una incidencia escalada solo pide compensaciones;
- solo `reserve_stock` es compensable (`release_stock`, `<paso>-comp`), y las compensaciones se
  encadenan en orden inverso;
- una revisión mantiene el `step_id` y la versión de origen de los pasos que conserva, cancela los
  que no conserva y compensa las reservas completadas cuyo dependiente se cancela;
- el escalado cancela los pasos sin efecto y compensa las reservas cuyo dependiente no terminó.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from eda_retail.events.catalog import Action
from eda_retail.infra.effect_keys import compute_idempotency_key

Step = dict[str, Any]

COMPENSATION_SUFFIX = "-comp"
COMPENSATIONS = {Action.RESERVE_STOCK.value: Action.RELEASE_STOCK.value}
FINAL = ("completed", "cancelled", "compensated")
_WITHOUT_EFFECT = ("pending", "requested", "failed")

COMPENSATION_FAILED = "compensation_failed"
RETRIES_EXHAUSTED = "retries_exhausted"
MAX_REVISIONS = "max_revisions"


def _natural(step_id: str) -> tuple[int, str]:
    digits = "".join(ch for ch in step_id if ch.isdigit())
    return (int(digits) if digits else 0, step_id)


def _compensation_pending(steps: list[Step]) -> bool:
    return any(s["status"] == "compensating" or (s["compensates"] and s["status"] != "completed") for s in steps)


def ready_steps(steps: list[Step], *, escalated: bool = False) -> list[Step]:
    """Los pasos `pending` que pueden pedirse ya, en el orden en que se piden."""
    done = {s["step_id"] for s in steps if s["status"] == "completed"}
    ready = []
    for s in sorted(steps, key=lambda s: (s["origin_plan_version"], s["step_id"])):
        if s["status"] != "pending" or (escalated and not s["compensates"]):
            continue
        if not set(s["depends_on"]) <= done:
            continue
        if s["action"] == Action.ISSUE_REFUND.value and _compensation_pending(steps):
            continue
        ready.append(s)
    return ready


def outcome(steps: list[Step]) -> str | None:
    """`resolved` o `refunded` si todos los pasos están en un estado final; None si no."""
    if not steps or any(s["status"] not in FINAL for s in steps):
        return None
    refunded = any(s["action"] == Action.ISSUE_REFUND.value and s["status"] == "completed" for s in steps)
    return "refunded" if refunded else "resolved"


def compensations(originals: list[Step], *, plan_version: int) -> list[Step]:
    """Los pasos `-comp` de `originals` (compensables y completados), en orden inverso y encadenados."""
    ordered = sorted(originals, key=lambda s: (s["origin_plan_version"], _natural(s["step_id"])), reverse=True)
    comps: list[Step] = []
    for original in ordered:
        result = original.get("result") or {}
        comps.append({
            "step_id": f"{original['step_id']}{COMPENSATION_SUFFIX}",
            "action": COMPENSATIONS[original["action"]],
            "params": {"order_id": original["params"].get("order_id"),
                       "reservation_id": result.get("reservation_id")},
            "depends_on": [comps[-1]["step_id"]] if comps else [],
            "status": "pending",
            "origin_plan_version": plan_version,
            "compensates": original["step_id"],
            "result": None,
        })
    return comps


def step_request(incident_id: str, plan_version: int, step: Step) -> dict[str, Any]:
    """Payload del comando `ExecuteStep` y de su `StepRequested`."""
    return {
        "incident_id": incident_id,
        "plan_version": plan_version,
        "step_id": step["step_id"],
        "action": step["action"],
        "params": step["params"],
        "idempotency_key": compute_idempotency_key(incident_id, step["step_id"],
                                                   origin_plan_version=step["origin_plan_version"]),
    }


# Acciones cuyo texto redacta el LLM: su ejecutor necesita la causa y el retraso de la incidencia.
DRAFTED_ACTIONS = frozenset({"notify_customer", "file_claim"})


def execute_step_command(request: dict[str, Any], incident: dict[str, Any]) -> dict[str, Any]:
    """Payload del comando `ExecuteStep`: el de `StepRequested` y, para las acciones redactadas, la causa y
    el retraso de la incidencia, para que el ejecutor no lea el estado del orquestador (design.md §3)."""
    if request["action"] not in DRAFTED_ACTIONS:
        return request
    return {**request, "cause": incident["cause"], "estimated_delay_hours": incident["estimated_delay_hours"]}


def on_failure(step: Step, *, retryable: bool, revisions: int, max_revisions: int) -> tuple[str, str | None]:
    """Qué hace la saga con un paso fallido: ("escalate", motivo) o ("revise", None)."""
    if step["compensates"]:
        return ("escalate", COMPENSATION_FAILED)
    if retryable:  # el ejecutor ya agotó sus reintentos
        return ("escalate", RETRIES_EXHAUSTED)
    if revisions >= max_revisions:
        return ("escalate", MAX_REVISIONS)
    return ("revise", None)


def _reservations_to_compensate(steps: list[Step], cancelled: set[str], alive: set[str]) -> list[Step]:
    by_id = {s["step_id"]: s for s in steps}
    compensated = {s["compensates"] for s in steps if s["compensates"]}
    return [
        s for s in steps
        if s["status"] == "completed" and s["action"] in COMPENSATIONS and s["step_id"] not in compensated
        and any(s["step_id"] in by_id[c]["depends_on"] for c in cancelled)
        and not any(s["step_id"] in by_id[a]["depends_on"] for a in alive)
    ]


@dataclass(frozen=True)
class Revision:
    steps: list[Step]  # todos los pasos tras la revisión (sin las compensaciones nuevas)
    cancelled: list[str]
    compensations: list[Step]


def revise(steps: list[Step], revised: list[dict[str, Any]], *, new_version: int) -> Revision:
    """Aplica un `PlanRevised`: mantiene, añade, cancela y compensa."""
    current = {s["step_id"]: dict(s) for s in steps}
    kept = set()
    for r in revised:
        kept.add(r["step_id"])
        old = current.get(r["step_id"])
        if old is not None:  # paso mantenido: conserva step_id y versión de origen
            status = "pending" if old["status"] in ("failed", "cancelled", "requested") else old["status"]
            current[r["step_id"]] = old | {"params": r["params"], "depends_on": list(r["depends_on"]),
                                           "status": status}
        else:
            current[r["step_id"]] = {"step_id": r["step_id"], "action": r["action"], "params": r["params"],
                                     "depends_on": list(r["depends_on"]), "status": "pending",
                                     "origin_plan_version": new_version, "compensates": None, "result": None}
    cancelled = sorted(
        (sid for sid, s in current.items()
         if sid not in kept and not s["compensates"] and s["status"] in _WITHOUT_EFFECT),
        key=_natural,
    )
    for sid in cancelled:
        current[sid]["status"] = "cancelled"
    after = list(current.values())
    alive = {s["step_id"] for s in after if s["status"] not in ("cancelled", "failed")}
    comps = compensations(_reservations_to_compensate(after, set(cancelled), alive), plan_version=new_version)
    return Revision(after, cancelled, comps)


def escalation(steps: list[Step], reason: str, *, plan_version: int) -> tuple[list[str], list[Step]]:
    """Pasos que el escalado cancela y compensaciones que deriva (nunca tras `compensation_failed`)."""
    cancelled = sorted((s["step_id"] for s in steps if not s["compensates"] and s["status"] in _WITHOUT_EFFECT),
                       key=_natural)
    if reason == COMPENSATION_FAILED:
        return cancelled, []
    gone = set(cancelled)
    after = [s | {"status": "cancelled"} if s["step_id"] in gone else s for s in steps]
    unfinished = {s["step_id"] for s in after if s["status"] != "completed" and not s["compensates"]}
    alive = {s["step_id"] for s in after if s["status"] == "completed"}
    to_compensate = [
        s for s in _reservations_to_compensate(after, unfinished, set())
        if not any(s["step_id"] in d["depends_on"] for d in after if d["step_id"] in alive)
    ]
    return cancelled, compensations(to_compensate, plan_version=plan_version)
