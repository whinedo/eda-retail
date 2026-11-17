"""E12: ejecutor caído y ejecutor colgado (spec experiments, «E12 Ejecutor caído y ejecutor colgado (H12)»; spec
synthetic-data, «Ejecutor caído o colgado»).

En proceso (decisión del usuario; el modo multiproceso llega con E16): `InProcessCommands` simula el ejecutor caído o
colgado y el dispatcher va en su propio hilo, como en el orquestador, para que un ejecutor colgado lo bloquee a él y
no a los relays ni al lado de lectura. Una ejecución por variante, con plazos cortos (`STEP_TIMEOUT_S`,
`COMMAND_TIMEOUT_S`):

- `baseline`: sin fallo; la referencia de H12a («terminan como siempre») y de H12b (latencia p95);
- `down`: el ejecutor de `file_claim` rechaza la conexión toda la ejecución;
- `hang`: el ejecutor de `file_claim` acepta la petición y no responde nunca; cada intento ocupa al dispatcher
  `COMMAND_TIMEOUT_S` (bloqueo de cabeza de línea, H12b exploratoria);
- `hang_recovery`: lo mismo, pero se recupera al cabo de `RECOVER_AFTER_S`, con plazos ya vencidos, y atiende una vez
  lo que tenía aceptado;
- `compensation`: `inventory` cae con la primera reserva durante `COMP_DOWN_S`, más que un plazo: la reserva vence,
  la saga deriva su compensación (`release_stock`, `-comp`) y esta se completa cuando `inventory` vuelve, antes de que
  venza su propio plazo.

H12a: ninguna incidencia abierta, toda incidencia con `file_claim` cerrada dentro de `STEP_TIMEOUT_S` más un margen
(la latencia máxima de `baseline` más `MARGIN_S`) y las demás con el mismo estado final que en `baseline`; en
`compensation`, cada compensación completada una vez tras la recuperación. H12b: la latencia p95 de las incidencias
sin `file_claim` no supera el doble de la de `baseline`; un resultado negativo es un hallazgo.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from eda_retail.config import Settings
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report
from eda_retail.synthetic.faults import ExecutorFault, FaultPlan
from eda_retail.synthetic.generator import Dataset, generate

TARGET = "executor_file_claim"
STEP_TIMEOUT_S = 6.0  # SAGA_STEP_TIMEOUT_S de la ejecución
COMMAND_TIMEOUT_S = 1.0  # COMMAND_HTTP_TIMEOUT_S: lo que un intento a un ejecutor colgado ocupa al dispatcher
RECOVER_AFTER_S = 1.5 * STEP_TIMEOUT_S  # hang_recovery: con algunos plazos ya vencidos
COMP_DOWN_S = 1.5 * STEP_TIMEOUT_S  # compensation: vence la reserva, no la compensación (empieza al vencer aquella)
MARGIN_S = 10.0
RUN_TIMEOUT_S = 600.0
VARIANTS: dict[str, dict[str, Any]] = {
    "baseline": {},
    "down": {"executor_faults": (ExecutorFault(TARGET, "down"),)},
    "hang": {"executor_faults": (ExecutorFault(TARGET, "hang"),)},
    "hang_recovery": {"executor_faults": (ExecutorFault(TARGET, "hang", duration_s=RECOVER_AFTER_S),)},
    "compensation": {"executor_faults": (ExecutorFault("inventory", "down", duration_s=COMP_DOWN_S),)},
}
FAULTED = ("down", "hang", "hang_recovery")


def uses_file_claim(data: Dataset) -> dict[str, bool]:
    """Si cada incidencia del conjunto usa `file_claim` (su plan esperado)."""
    return {incident_id_for_notice(n.carrier_notice_id): any(s["action"] == "file_claim" for s in n.expected_steps)
            for n in data.notices}


# -- recuentos -----------------------------------------------------------------------------------------
def p95(values: Iterable[float]) -> float | None:
    """Percentil 95 por el método del rango más cercano; None sin valores."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)] if ordered else None


def summarize_incidents(latency_s: Mapping[str, float], status: Mapping[str, str],
                        uses: Mapping[str, bool]) -> dict[str, Any]:
    """Latencias y estados de las incidencias con y sin `file_claim`. `latency_s`: de `CarrierNoticeReceived` al
    cierre, de las cerradas; `status`: estado final de cada incidencia."""
    groups = {"affected": [i for i, u in uses.items() if u], "unaffected": [i for i, u in uses.items() if not u]}
    out: dict[str, Any] = {"file_claim_share": len(groups["affected"]) / len(uses) if uses else 0.0}
    for name, ids in groups.items():
        closed = [latency_s[i] for i in ids if i in latency_s]
        out[name] = {"incidents": len(ids), "closed": len(closed), "max_latency_s": max(closed, default=None),
                     "p95_latency_s": p95(closed), "status": {i: status.get(i, "open") for i in ids}}
    return out


def summarize_commands(rows: Iterable[Mapping[str, Any]], *, hang_wait_s: float) -> dict[str, Any]:
    """Los `ExecuteStep` al ejecutor afectado: intentos del dispatcher por comando, estados y cuándo empezó su plazo
    respecto al encolado (`start_after_enqueue_s`: el plazo empieza en el primer envío, nunca antes)."""
    rows = list(rows)
    starts = [r["start_after_enqueue_s"] for r in rows if r["start_after_enqueue_s"] is not None]
    return {
        "commands": len(rows),
        "attempts_max": max((r["attempts"] for r in rows), default=0),
        "attempts_mean": sum(r["attempts"] for r in rows) / len(rows) if rows else 0.0,
        "status": dict(Counter(r["status"] for r in rows)),
        "dispatcher_wait_s": hang_wait_s,
        "deadline_start_after_enqueue_s": {"min": min(starts, default=None), "max": max(starts, default=None)},
    }


def summarize_compensations(steps: Iterable[Mapping[str, Any]], releases: Iterable[Mapping[str, Any]],
                            release_attempts: Iterable[int]) -> dict[str, Any]:
    """Las compensaciones: cuántas, con sufijo `-comp`, en qué estado, los intentos de su comando (más de uno: esperó
    al ejecutor caído) y las liberaciones por reserva (`StockReleased` por `reservation_id`; una reserva que nunca se
    hizo porque `inventory` estaba caído no se libera: su compensación se completa sin efecto)."""
    steps, attempts = list(steps), list(release_attempts)
    per_reservation = Counter(r["reservation_id"] for r in releases)
    return {
        "compensations": len(steps),
        "with_comp_suffix": sum(1 for s in steps if s["step_id"].endswith("-comp")),
        "status": dict(Counter(s["status"] for s in steps)),
        "release_attempts_max": max(attempts, default=0),
        "releases_per_reservation": max(per_reservation.values(), default=0),
    }


# -- hipótesis ---------------------------------------------------------------------------------------------
def _invariants_ok(m: Mapping[str, Any]) -> bool:
    return all(inv["violations"] == 0 for inv in m["invariants"].values())


def h12a(m: Mapping[str, Any], baseline: Mapping[str, Any]) -> bool:
    """Vivacidad de una variante con el ejecutor afectado: nada abierto, las incidencias con `file_claim` cerradas en
    el plazo más el margen y las demás con el mismo estado que en `baseline`."""
    bound = STEP_TIMEOUT_S + (baseline["incidents"]["affected"]["max_latency_s"] or 0.0) + MARGIN_S
    affected = m["incidents"]["affected"]
    return (m["open"] == 0 and affected["closed"] == affected["incidents"]
            and (affected["max_latency_s"] or 0.0) <= bound
            and m["incidents"]["unaffected"]["status"] == baseline["incidents"]["unaffected"]["status"]
            and _invariants_ok(m))


def h12b(m: Mapping[str, Any], baseline: Mapping[str, Any]) -> bool:
    """Aislamiento: la p95 de las incidencias sin `file_claim` no supera el doble de la de `baseline`."""
    now, before = m["incidents"]["unaffected"]["p95_latency_s"], baseline["incidents"]["unaffected"]["p95_latency_s"]
    return now is not None and before is not None and now <= 2 * before


def compensation_ok(m: Mapping[str, Any]) -> bool:
    """Compensación de un paso con plazo vencido: alguna compensación, todas `-comp` y completadas, alguna esperó al
    ejecutor caído (más de un intento), ninguna reserva liberada más de una vez y nada abierto."""
    c = m["compensation"]
    return (c["compensations"] > 0 and c["with_comp_suffix"] == c["compensations"]
            and c["status"] == {"completed": c["compensations"]} and c["release_attempts_max"] > 1
            and c["releases_per_reservation"] <= 1 and m["open"] == 0 and _invariants_ok(m))


# -- ejecución -----------------------------------------------------------------------------------------
def _rows(h: Harness, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    rows = [dict(r) for r in h.conn.execute(sql, params).fetchall()]
    h.conn.commit()
    return rows


def _measure(h: Harness) -> dict[str, Any]:
    latency = {r["incident_id"]: float(r["s"]) for r in _rows(h, """
        SELECT correlation_id AS incident_id,
               extract(epoch FROM max(occurred_at) FILTER (WHERE event_type IN ('IncidentResolved', 'IncidentEscalated'))
                                 - min(occurred_at) FILTER (WHERE event_type = 'CarrierNoticeReceived')) AS s
        FROM event_log.events GROUP BY correlation_id
        HAVING count(*) FILTER (WHERE event_type IN ('IncidentResolved', 'IncidentEscalated')) > 0""")}
    status = {r["incident_id"]: r["status"] for r in _rows(h, "SELECT incident_id, status FROM"
                                                               " views.view_incident_status")}
    commands = _rows(h, """
        SELECT c.attempts, c.status,
               extract(epoch FROM (d.expires_at - make_interval(secs => d.timeout_s)) - c.created_at)
                   AS start_after_enqueue_s
        FROM orchestrator.command_outbox c LEFT JOIN orchestrator.deadlines d USING (command_id)
        WHERE c.target = %s AND c.command_type = 'ExecuteStep'""", (TARGET,))
    claim_steps = _rows(h, "SELECT status, error FROM orchestrator.plan_steps WHERE action = 'file_claim'")
    steps = _rows(h, "SELECT step_id, status FROM orchestrator.plan_steps WHERE compensates IS NOT NULL")
    releases = _rows(h, "SELECT payload->>'reservation_id' AS reservation_id FROM event_log.events"
                        " WHERE event_type = 'StockReleased'")
    attempts = [r["attempts"] for r in _rows(h, "SELECT attempts FROM orchestrator.command_outbox"
                                                "  WHERE command_type = 'ExecuteStep'"
                                                "  AND payload->>'action' = 'release_stock'")]
    return {"incidents": summarize_incidents(latency, status, uses_file_claim(h.data)),
            "file_claim_commands": summarize_commands(commands, hang_wait_s=h.commands.hang_wait_s[TARGET]),
            "file_claim_steps": dict(Counter(f"{r['status']}:{r['error'] or ''}" for r in claim_steps)),
            "recovered_results": dict(h.recovered),
            "compensation": summarize_compensations(steps, releases, attempts)}


def run_variant(base: Settings, variant: str, *, seed: int, incidents: int) -> dict[str, Any]:
    config = RunConfig(seed=seed, incidents=incidents, faults=FaultPlan(seed=seed, **VARIANTS[variant]),
                       step_timeout_seconds=STEP_TIMEOUT_S, command_timeout_seconds=COMMAND_TIMEOUT_S,
                       dispatcher_thread=True, timeout_seconds=RUN_TIMEOUT_S)
    with Harness(base, config) as h:
        h.send_notices()
        result = h.result(h.run_until_closed())
        metrics = {"closed": result.closed, "open": result.open, "escalated": result.escalated,
                   "escalation_reasons": result.escalation_reasons, **_measure(h)}
        metrics["invariants"] = invariants.check(h)
    return metrics


def evaluate(results: Mapping[str, dict[str, Any]]) -> dict[str, Any]:
    """H12a por variante (y la compensación), H12b exploratoria y la nota que pide la spec si no se cumple."""
    baseline = results["baseline"]
    for v in FAULTED:
        results[v]["h12a"] = h12a(results[v], baseline)
        results[v]["h12b"] = h12b(results[v], baseline)
    results["compensation"]["h12a"] = compensation_ok(results["compensation"])
    h12b_all = all(results[v]["h12b"] for v in FAULTED)
    return {
        "H12a": all(results[v]["h12a"] for v in (*FAULTED, "compensation")) and _invariants_ok(baseline),
        "H12b": h12b_all,
        "h12b_note": None if h12b_all else (
            "H12b no se cumple: hallazgo, no fallo del experimento. La latencia p95 de las incidencias sin "
            "file_claim supera el doble de la de baseline en " + ", ".join(v for v in FAULTED if not results[v]["h12b"])
            + "; es coherente con el bloqueo de cabeza de línea del dispatcher, que invoca un comando por turno."),
    }


def run_e12(base: Settings, *, seed: int = 42, incidents: int = 50) -> Report:
    results = {v: run_variant(base, v, seed=seed, incidents=incidents) for v in VARIANTS}
    verdict = evaluate(results)
    return Report(
        experiment="E12", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "mode": "in_process", "target": TARGET, "step_timeout_s": STEP_TIMEOUT_S,
                "command_timeout_s": COMMAND_TIMEOUT_S, "recover_after_s": RECOVER_AFTER_S,
                "compensation_down_s": COMP_DOWN_S, "margin_s": MARGIN_S, "variants": list(VARIANTS),
                "file_claim_share": generate_share(seed, incidents)},
        metrics={"variants": results, "h12b_note": verdict["h12b_note"]},
        hypothesis={"H12a": verdict["H12a"], "H12b": verdict["H12b"]},
    )


def generate_share(seed: int, incidents: int) -> float:
    """Proporción de incidencias del conjunto que usan `file_claim` (la spec pide aproximadamente la mitad)."""
    uses = uses_file_claim(generate(seed, incidents))
    return sum(uses.values()) / len(uses) if uses else 0.0
