"""E16: consumidor zombie y rebalanceo (spec experiments, «E16 Consumidor zombie y rebalanceo (H16)»; spec
synthetic-data, «Congelación de un proceso»).

En modo multiproceso: dos instancias de `eda-orchestrator` en el grupo `orchestrator`, con `session.timeout.ms`
acortado (`SESSION_TIMEOUT_MS`), y el resto de componentes en el harness. Con todas las incidencias en curso (los
resultados de los pasos llegan con `STEP_DELAYS` de retraso para que no terminen antes), se congela con `SIGSTOP` la
instancia que posee más particiones durante `FREEZE_S`, más que su sesión, y se reanuda con `SIGCONT`.

Lo que mide sale del log de cada instancia (`processes.read_log`), de la base de datos y de un muestreo de
`orchestrator.plan_steps` e `orchestrator.incidents` en cada turno del harness:

- el rebalanceo: las particiones de la congelada, cuándo las recibe otra instancia (respecto a la congelación) y si
  fue antes de reanudar;
- la zombie: el mensaje que tenía a medio procesar al congelarse, lo que procesó después de reanudar (con su
  resultado: `duplicate` es un descarte del inbox) y si su proceso sigue vivo o con qué código salió (si la
  confirmación del offset falla tras perder el grupo, el proceso termina);
- los mensajes que procesaron las dos instancias (el inbox deja un solo `processed`);
- por incidencia, si dos instancias procesaron mensajes de ella solapados en el tiempo y las transacciones que
  esperaron al bloqueo de la incidencia más de `LOCK_WAIT_MIN_S`;
- H16: ningún paso con dos `ExecuteStep` de `command_id` distinto en la misma versión del plan, ninguna transición
  inválida (un paso `completed` solo pasa a `compensating` y este a `compensated`; una incidencia cerrada no cambia)
  e I1 a I5 sin violaciones.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Mapping
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.processes import Message, OrchestratorProcess, ProcessLog, wait_for_group
from eda_retail.experiments.report import Report
from eda_retail.synthetic.faults import FaultPlan

SESSION_TIMEOUT_MS = 6000  # el mínimo por defecto del broker (group.min.session.timeout.ms)
FREEZE_S = 2 * SESSION_TIMEOUT_MS / 1000  # más que la sesión, para que Kafka reasigne sus particiones
STEP_DELAYS = {"reserve_stock": 3.0, "file_claim": 3.0, "reissue_shipment": 3.0}  # incidencias en curso al congelar
GROUP_TIMEOUT_S = 60.0  # espera a que las dos instancias tengan particiones
IN_COURSE_TIMEOUT_S = 60.0  # espera a que el orquestador tenga todas las incidencias
LOCK_WAIT_MIN_S = 0.05  # una espera al bloqueo menor no se informa
RUN_TIMEOUT_S = 600.0
IDLE_S = 60.0  # mayor que la congelación: mientras dura, puede que nada avance
ALLOWED_AFTER = {"completed": {"completed", "compensating", "compensated"}, "compensating": {"compensating", "compensated"},
                 "compensated": {"compensated"}}
CLOSED = ("resolved", "escalated")


class SagaSampler:
    """Muestrea el estado de los pasos y de las incidencias y anota las transiciones inválidas que observa."""

    def __init__(self, h: Harness) -> None:
        self._h = h
        self._steps: dict[tuple[str, str], str] = {}
        self._incidents: dict[str, str] = {}
        self.invalid: list[dict[str, Any]] = []
        self.samples = 0

    def __call__(self) -> None:
        c = self._h.conn
        steps = c.execute("SELECT incident_id, step_id, status FROM orchestrator.plan_steps").fetchall()
        incidents = c.execute("SELECT incident_id, status FROM orchestrator.incidents").fetchall()
        c.commit()
        self.samples += 1
        for r in steps:
            key, status = (r["incident_id"], r["step_id"]), r["status"]
            before = self._steps.get(key)
            if before in ALLOWED_AFTER and status not in ALLOWED_AFTER[before]:
                self.invalid.append({"incident": key[0], "step": key[1], "from": before, "to": status})
            self._steps[key] = status
        for r in incidents:
            before = self._incidents.get(r["incident_id"])
            if before in CLOSED and r["status"] != before:
                self.invalid.append({"incident": r["incident_id"], "step": None, "from": before, "to": r["status"]})
            self._incidents[r["incident_id"]] = r["status"]


def duplicate_commands(h: Harness) -> list[dict[str, Any]]:
    """Pasos con más de un `ExecuteStep` (`command_id` distinto) en la misma versión del plan."""
    rows = h.conn.execute(
        """
        SELECT incident_id, payload ->> 'step_id' AS step_id, (payload ->> 'plan_version')::int AS plan_version,
               count(*) AS commands
        FROM orchestrator.command_outbox WHERE command_type = 'ExecuteStep'
        GROUP BY 1, 2, 3 HAVING count(*) > 1 ORDER BY 1, 2, 3
        """).fetchall()
    h.conn.commit()
    return [dict(r) for r in rows]


def in_course(h: Harness) -> int:
    """Incidencias que el orquestador ya tiene."""
    row = h.conn.execute("SELECT count(*) AS n FROM orchestrator.incidents").fetchone()
    h.conn.commit()
    return row["n"]


def _brief(m: Message) -> dict[str, Any]:
    return {"event_type": m.event_type, "incident": m.incident, "partition": m.partition, "offset": m.offset,
            "outcome": m.outcome}


def rebalance(logs: Mapping[str, ProcessLog], victim: str, frozen: float, resumed: float) -> dict[str, Any]:
    """Particiones de la congelada y cuándo (segundos desde la congelación) otra instancia las tenía todas."""
    partitions = logs[victim].assignment(before=frozen)
    taken_at = None
    for name, log in logs.items():
        if name == victim:
            continue
        for t, kind, parts in log.rebalances:
            if t > frozen and kind == "assigned" and set(partitions) <= set(parts):
                taken_at = t if taken_at is None else min(taken_at, t)
                break
    return {"victim": victim, "victim_partitions": partitions,
            "reassigned_after_s": None if taken_at is None else round(taken_at - frozen, 3),
            "resumed_after_s": round(resumed - frozen, 3),
            "reassigned_before_resume": taken_at is not None and taken_at < resumed}


def zombie(log: ProcessLog, frozen: float, resumed: float) -> dict[str, Any]:
    """Lo que la congelada tenía a medio procesar y lo que procesó tras reanudar."""
    in_flight = [m for m in log.messages if m.start <= frozen and (m.end is None or m.end > frozen)]
    after = [m for m in log.messages if m.start >= resumed]
    return {"in_flight_at_freeze": [_brief(m) for m in in_flight],
            "processed_after_resume": [_brief(m) for m in after],
            "outcomes_after_resume": dict(Counter(m.outcome or "unfinished" for m in after)),
            "rebalances_after_resume": [(round(t - resumed, 3), kind, parts) for t, kind, parts in log.rebalances
                                        if t >= resumed]}


def taken_over(logs: Mapping[str, ProcessLog], victim: str, partitions: list[int], frozen: float,
               resumed: float) -> dict[str, Any]:
    """Mensajes de las particiones de la congelada que otra instancia procesó mientras estaba congelada."""
    out = Counter()
    for name, log in logs.items():
        if name != victim:
            out.update(m.outcome or "unfinished" for m in log.messages
                       if m.partition in partitions and frozen < m.start < resumed)
    return {"messages": sum(out.values()), "outcomes": dict(out)}


def overlaps(logs: Mapping[str, ProcessLog], end_of_run: float) -> dict[str, dict[str, Any]]:
    """Por incidencia: las instancias que procesaron mensajes de ella y si dos lo hicieron solapadas en el tiempo
    (un mensaje sin terminar dura hasta `end_of_run`)."""
    spans: dict[str, list[tuple[str, float, float]]] = {}
    for name, log in logs.items():
        for m in log.messages:
            spans.setdefault(m.incident, []).append((name, m.start, m.end if m.end is not None else end_of_run))
    out = {}
    for incident, items in sorted(spans.items()):
        overlap = any(a[0] != b[0] and a[1] < b[2] and b[1] < a[2]
                      for i, a in enumerate(items) for b in items[i + 1:])
        out[incident] = {"instances": sorted({n for n, _, _ in items}), "overlap": overlap}
    return out


def double_processed(logs: Mapping[str, ProcessLog]) -> list[dict[str, Any]]:
    """Mensajes (partición, offset) que más de una instancia empezó a procesar, con el resultado de cada una. El inbox
    deja como mucho un `processed` por mensaje; los demás son `duplicate` o no terminaron (`unfinished`)."""
    seen: dict[tuple[int, int], dict[str, str]] = {}
    for name, log in logs.items():
        for m in log.messages:
            seen.setdefault((m.partition, m.offset), {})[name] = m.outcome or "unfinished"
    return [{"partition": p, "offset": o, "outcomes": outcomes} for (p, o), outcomes in sorted(seen.items())
            if len(outcomes) > 1]


def lock_waits(logs: Mapping[str, ProcessLog], frozen: float) -> list[dict[str, Any]]:
    """Transacciones que esperaron al bloqueo de su incidencia más de `LOCK_WAIT_MIN_S` (`at_s`: cuándo acabó la
    espera, respecto a la congelación)."""
    return [{"instance": name, "incident": incident, "wait_s": wait, "thread": thread, "at_s": round(t - frozen, 3)}
            for name, log in logs.items() for t, incident, wait, thread in log.lock_waits if wait >= LOCK_WAIT_MIN_S]


def h16(metrics: Mapping[str, Any]) -> bool:
    return (not metrics["duplicate_commands"] and not metrics["invalid_transitions"]
            and all(inv["violations"] == 0 for inv in metrics["invariants"].values()))


def _victim(processes: list[OrchestratorProcess]) -> OrchestratorProcess:
    """La instancia que posee más particiones ahora (la primera, si empatan)."""
    return max(processes, key=lambda p: len(p.log().assignment()))


def run_e16(base: Settings, *, seed: int = 42, incidents: int = 50) -> Report:
    config = RunConfig(seed=seed, incidents=incidents, faults=FaultPlan(seed=seed, step_delay_seconds=STEP_DELAYS),
                       orchestrator_processes=2, session_timeout_ms=SESSION_TIMEOUT_MS, idle_seconds=IDLE_S,
                       timeout_seconds=RUN_TIMEOUT_S)
    with Harness(base, config) as h:
        sampler = SagaSampler(h)
        assignment = wait_for_group(h.processes, h.partitions(), GROUP_TIMEOUT_S, step=h.step)
        start = time.monotonic()
        h.send_notices()
        h.run_while(lambda: in_course(h) < incidents, IN_COURSE_TIMEOUT_S, on_turn=sampler)
        victim = _victim(h.processes)
        closed_at_freeze = h.closed()
        frozen = victim.freeze()
        h.run_while(lambda: True, FREEZE_S, on_turn=sampler)
        resumed = victim.resume()
        h.run_until_closed(on_turn=sampler)
        sampler()
        result = h.result(time.monotonic() - start)
        logs = {p.name: p.log() for p in h.processes}
        end_of_run = time.time()
        moved = rebalance(logs, victim.name, frozen, resumed)
        by_incident = overlaps(logs, end_of_run)
        metrics = {
            "closed": result.closed, "open": result.open, "escalated": result.escalated,
            "escalation_reasons": result.escalation_reasons, "elapsed_s": round(result.elapsed_seconds, 1),
            "group_before_freeze": assignment, "in_course_at_freeze": in_course(h),
            "closed_at_freeze": closed_at_freeze,
            "rebalance": moved,
            "taken_over": taken_over(logs, victim.name, moved["victim_partitions"], frozen, resumed),
            "zombie": zombie(logs[victim.name], frozen, resumed)
            | {"alive": victim.alive(), "exit_code": victim.popen.poll()},
            "double_processed": double_processed(logs),
            "inbox_duplicates": {n: sum(m.outcome == "duplicate" for m in log.messages) for n, log in logs.items()},
            "overlaps": {"incidents": sum(v["overlap"] for v in by_incident.values()), "by_incident": by_incident},
            "lock_waits": lock_waits(logs, frozen),
            "duplicate_commands": duplicate_commands(h),
            "invalid_transitions": sampler.invalid, "samples": sampler.samples,
        }
        metrics["invariants"] = invariants.check(h)
    return Report(
        experiment="E16", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "mode": "multiprocess", "orchestrator_processes": 2,
                "session_timeout_ms": SESSION_TIMEOUT_MS, "freeze_s": FREEZE_S, "step_delays_s": STEP_DELAYS,
                "lock_wait_min_s": LOCK_WAIT_MIN_S},
        metrics=metrics, hypothesis={"H16": h16(metrics)},
    )
