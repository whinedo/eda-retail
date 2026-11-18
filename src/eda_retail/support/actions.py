"""Diagnóstico y acciones de recuperación de `eda-support` (spec support), sobre un `System` que lee y escribe el sistema
real (`support.system.LiveSystem`) o uno falso en los tests.

Cada acción que escribe se planifica primero: `plan_*` comprueba sus condiciones previas, describe lo que haría
(`changes`) y guarda lo observado (`facts`). El token de confirmación es el hash de la acción, sus argumentos y lo
observado: si el estado cambia entre la simulación y la ejecución, el token de la simulación ya no vale.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from eda_retail.infra.positions import positioned_lags
from eda_retail.infra.schemas import COMPONENTS
from eda_retail.projectors.registry import PROJECTORS

OK, FAILED, REFUSED = 0, 1, 2
MODES = ("truncated", "recreated")
GROUPS = ("interpreter", "orchestrator")  # los grupos de Kafka con miembros; el lado de lectura asigna particiones
PUBLISHERS = tuple(schema for schema, component in COMPONENTS.items() if component.publishes)
READ_SIDE_CONSUMERS = ("event_log", *PROJECTORS)
PROJECTORS_NODE = "projectors"
STALE_TOKEN = "el estado cambió desde la simulación: vuelve a simularla y usa el token nuevo"


class System(Protocol):
    def processes(self) -> dict[str, bool]: ...
    def watermarks(self) -> dict[int, tuple[int, int]]: ...
    def group_members(self) -> dict[str, int]: ...
    def group_lags(self, highs: dict[int, int]) -> dict[str, int]: ...
    def outbox(self) -> dict[str, dict[str, Any]]: ...
    def outbox_positions(self) -> dict[int, tuple[int, int]]: ...
    def dead_letters(self) -> dict[str, dict[str, int]]: ...
    def failed_jobs(self) -> dict[str, int]: ...
    def parked_notices(self) -> int: ...
    def read_side_positions(self) -> list[dict[str, Any]]: ...
    def outbox_range(self, schema: str, from_id: int) -> dict[str, Any]: ...
    def view_rows(self, view: str) -> dict[str, int]: ...
    def reset_for_recreated(self, partitions: list[int]) -> dict[str, Any]: ...
    def republish(self) -> dict[str, int]: ...
    def replay_from(self, schema: str, outbox_id: int) -> int: ...
    def rebuild(self, view: str) -> dict[str, Any]: ...
    def start_run(self, action: str, args: dict, token: str, origin: str) -> int: ...
    def finish_run(self, run_id: int, outcome: str, summary: dict) -> None: ...
    def close(self) -> None: ...


class DatabaseUnavailable(Exception):
    """PostgreSQL no responde: la acción no se ejecuta."""


def _db(read: Callable[..., Any], *args: Any) -> Any:
    try:
        return read(*args)
    except Exception as exc:  # cualquier fallo de la base para la acción
        raise DatabaseUnavailable(f"PostgreSQL no responde: {exc}") from exc


@dataclass
class Plan:
    action: str
    args: dict[str, Any]
    facts: dict[str, Any] = field(default_factory=dict)
    changes: dict[str, Any] = field(default_factory=dict)
    refusal: str | None = None

    @property
    def token(self) -> str:
        raw = json.dumps({"action": self.action, "args": self.args, "facts": self.facts}, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def as_dict(self) -> dict[str, Any]:
        return {"action": self.action, "args": self.args, "changes": self.changes, "refusal": self.refusal,
                "token": None if self.refusal else self.token}


# -- diagnóstico ----------------------------------------------------------------------------------------------------
def topic_lost(marks: dict[int, tuple[int, int]], positions: dict[int, tuple[int, int]]) -> bool:
    """El topic tiene menos mensajes de los que recuerdan los outbox: en alguna partición, el offset más alto guardado
    no es menor que la marca alta, o el más bajo es menor que la marca baja (o la partición ya no existe)."""
    for partition, (lowest, highest) in positions.items():
        if partition not in marks:
            return True
        low, high = marks[partition]
        if highest >= high or lowest < low:
            return True
    return False


def status(system: System) -> dict[str, Any]:
    """Diagnóstico de solo lectura; si Kafka o PostgreSQL no responden, lo dice en `alerts` y sigue con el resto."""
    report: dict[str, Any] = {"processes": system.processes(), "topic": None, "groups": None, "read_side": None,
                              "outbox": None, "dead_letters": None, "failed_jobs": None, "parked_notices": None,
                              "alerts": [], "errors": {}}
    alerts: list[str] = report["alerts"]
    marks: dict[int, tuple[int, int]] | None = None
    try:
        marks = system.watermarks()
        highs = {p: high for p, (_, high) in marks.items()}
        members = system.group_members()
        lags = system.group_lags(highs) if highs else dict.fromkeys(GROUPS, 0)  # sin topic, nada que leer
        report["topic"] = {str(p): {"low": low, "high": high} for p, (low, high) in sorted(marks.items())}
        report["groups"] = {g: {"members": members.get(g, 0), "lag": lags.get(g)} for g in GROUPS}
    except Exception as exc:  # noqa: BLE001
        marks = None
        alerts.append("kafka_down")
        report["errors"]["kafka"] = str(exc)
    try:
        outbox, positions = system.outbox(), system.outbox_positions()
        report["outbox"] = outbox
        report["dead_letters"] = system.dead_letters()
        report["failed_jobs"] = system.failed_jobs()
        report["parked_notices"] = system.parked_notices()
        rows = system.read_side_positions()
    except Exception as exc:  # noqa: BLE001
        alerts.append("postgres_down")
        report["errors"]["postgres"] = str(exc)
        return report
    if marks is not None:
        highs = {p: high for p, (_, high) in marks.items()}
        report["read_side"] = positioned_lags(rows, highs, READ_SIDE_CONSUMERS)
        if topic_lost(marks, positions):
            alerts.insert(0, "topic_lost")
    if any(o["pending"] for o in outbox.values()):
        alerts.append("outbox_pending")
    if any(d["rows"] for d in report["dead_letters"].values()):
        alerts.append("dead_letters")
    if any(report["failed_jobs"].values()):
        alerts.append("jobs_failed")
    if report["parked_notices"]:
        alerts.append("parked_notices")
    return report


# -- planes ---------------------------------------------------------------------------------------------------------
def _stopped(system: System) -> tuple[dict[str, Any], str | None]:
    """Sistema parado (spec support): ningún proceso vigilado vivo y ningún miembro en los grupos de Kafka."""
    alive = sorted(node for node, up in system.processes().items() if up)
    facts: dict[str, Any] = {"alive": alive}
    try:
        members = system.group_members()
    except Exception:  # noqa: BLE001
        return facts, "Kafka no responde: no se puede comprobar que el sistema esté parado"
    facts["members"] = members
    busy = sorted(g for g in GROUPS if members.get(g))
    if alive:
        return facts, f"el sistema no está parado: procesos vivos: {', '.join(alive)}"
    if busy:
        return facts, f"el sistema no está parado: el grupo {', '.join(busy)} tiene miembros activos"
    return facts, None


def _topic_refusal(marks: dict[int, tuple[int, int]], mode: str) -> str | None:
    if not marks:
        return "el topic no existe: créalo con sus particiones antes de republicar"
    if any(low < high for low, high in marks.values()):
        return "el topic tiene mensajes: republicar los duplicaría en otro orden"
    recreated = all(high == 0 for _, high in marks.values())
    if mode == "recreated" and not recreated:
        return "el topic parece vaciado con delete_records (sus offsets siguen), no recreado: usa --mode truncated"
    if mode == "truncated" and recreated:
        return "el topic parece recreado (sus offsets empiezan en 0), no vaciado: usa --mode recreated"
    return None


def plan_republish(system: System, mode: str) -> Plan:
    plan = Plan("republish", {"mode": mode})
    plan.facts, refusal = _stopped(system)
    try:
        marks = system.watermarks()
    except Exception:  # noqa: BLE001
        plan.refusal = refusal or "Kafka no responde"
        return plan
    outbox = _db(system.outbox)
    plan.facts |= {"watermarks": sorted(marks.items()),
                   "outbox": {s: [o["rows"], o["max_id"]] for s, o in sorted(outbox.items())}}
    rows = {s: outbox[s]["rows"] for s in PUBLISHERS if s in outbox}
    plan.changes = {"rows": rows, "total": sum(rows.values())}
    if mode == "recreated":
        plan.changes |= {"reset_groups": list(GROUPS), "reset_positions": ["event_log.offsets", "views.offsets"],
                         "partitions": sorted(marks)}
    plan.refusal = refusal or _topic_refusal(marks, mode)
    if plan.refusal is None and not plan.changes["total"]:
        plan.refusal = "los outbox no tienen filas que republicar"
    return plan


def plan_replay_from(system: System, component: str, outbox_id: int) -> Plan:
    plan = Plan("replay-from", {"component": component, "outbox_id": outbox_id})
    if component not in PUBLISHERS:
        plan.refusal = f"el componente {component} no tiene outbox; los que tienen: {', '.join(PUBLISHERS)}"
        return plan
    found = _db(system.outbox_range, component, outbox_id)
    plan.facts = {"range": found}
    if not found["exists"]:
        plan.refusal = f"la fila {outbox_id} no existe en {component}.outbox"
        return plan
    plan.changes = {"rows": found["rows"], "first_id": found["first_id"], "last_id": found["last_id"]}
    return plan


def plan_rebuild(system: System, view: str) -> Plan:
    plan = Plan("rebuild", {"view": view})
    alive = bool(system.processes().get(PROJECTORS_NODE))
    rows = _db(system.view_rows, view)
    plan.facts = {"projectors_alive": alive, "rows": rows}
    plan.changes = {"tables": rows}
    if alive:
        plan.refusal = ("el proceso de los proyectores (eda-projector) está vivo: otro proceso escribiría en la vista "
                        "mientras se reconstruye")
    return plan


def plan(system: System, action: str, args: dict[str, Any]) -> Plan:
    if action == "republish":
        return plan_republish(system, args["mode"])
    if action == "replay-from":
        return plan_replay_from(system, args["component"], args["outbox_id"])
    if action == "rebuild":
        return plan_rebuild(system, args["view"])
    raise ValueError(f"acción desconocida: {action}")


# -- ejecución ------------------------------------------------------------------------------------------------------
def execute(system: System, plan: Plan) -> dict[str, Any]:
    if plan.action == "republish":
        reset = None
        if plan.args["mode"] == "recreated":
            reset = system.reset_for_recreated(plan.changes["partitions"])
        republished = system.republish()
        return {"republished": republished, "total": sum(republished.values()), "reset": reset}
    if plan.action == "replay-from":
        return {"rows": system.replay_from(plan.args["component"], plan.args["outbox_id"])}
    return system.rebuild(plan.args["view"])


def confirmed(system: System, action: str, args: dict[str, Any], token: str, origin: str
              ) -> tuple[int, str, dict[str, Any]]:
    """Ejecución confirmada: vuelve a planificar, comprueba el token y registra la ejecución en `support.runs`.
    Devuelve el código de salida, el resultado y el resumen."""
    current = plan(system, action, args)
    run_id = _db(system.start_run, action, args, token, origin)
    refusal = current.refusal or (STALE_TOKEN if current.token != token else None)
    if refusal is not None:
        summary = {"reason": refusal}
        _db(system.finish_run, run_id, "refused", summary)
        return REFUSED, "refused", summary
    try:
        summary = execute(system, current)
    except Exception as exc:  # noqa: BLE001 - la ejecución queda como fallida, con su error
        summary = {"error": f"{type(exc).__name__}: {exc}"}
        _db(system.finish_run, run_id, "failed", summary)
        return FAILED, "failed", summary
    _db(system.finish_run, run_id, "succeeded", summary)
    return OK, "succeeded", summary
