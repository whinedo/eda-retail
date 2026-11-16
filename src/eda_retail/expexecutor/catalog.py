"""Catálogo de los experimentos (spec expexecutor, «Formato del informe de resultados»).

El propósito y el resultado esperado de cada experimento salen de su requisito en `specs/experiments`
(hipótesis y umbrales). Las comprobaciones leen `report.json` y dicen qué métrica respalda el resultado
o en cuál difiere; no deciden la hipótesis, que es la que escribió el experimento.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

MISSING = object()


@dataclass(frozen=True)
class Check:
    metric: str  # ruta en report.json, p. ej. `metrics.open`
    expected: str
    obtained: Any
    ok: bool


@dataclass(frozen=True)
class Entry:
    title: str
    purpose: str
    expected: str
    checks: Callable[[dict[str, Any]], list[Check]]


def lookup(data: Any, path: str) -> Any:
    """Valor de `report.json` por su ruta con puntos (`metrics.views.orders.identical`)."""
    for key in path.split("."):
        if not isinstance(data, dict) or key not in data:
            return MISSING
        data = data[key]
    return data


def _check(report: dict[str, Any], path: str, expected: str, ok: Callable[[Any], bool]) -> Check:
    value = lookup(report, path)
    return Check(path, expected, value, value is not MISSING and ok(value))


def _e1(report: dict[str, Any]) -> list[Check]:
    runs = lookup(report, "metrics.runs")
    checks = []
    for i, row in enumerate(runs if isinstance(runs, list) else []):
        mode = row.get("idempotency")
        if mode not in ("full", "no_effect_keys"):
            continue  # `none` se mide, pero H1 no lo compara
        expected, ok = ("0", lambda v: v == 0) if mode == "full" else ("> 0", lambda v: v > 0)
        checks.append(Check(
            f"metrics.runs[{i}].duplicates_total ({mode}, {row.get('deliveries')} entregas)", expected,
            row.get("duplicates_total", MISSING),
            "duplicates_total" in row and ok(row["duplicates_total"])))
    return checks


def _e2(report: dict[str, Any]) -> list[Check]:
    return [_check(report, "metrics.open", "0", lambda v: v == 0),
            _check(report, "metrics.repeated_steps", "0", lambda v: v == 0)]


def _e3(report: dict[str, Any]) -> list[Check]:
    incidents = lookup(report, "config.incidents")
    refunded, escalated = lookup(report, "metrics.refunded"), lookup(report, "metrics.escalated")
    total = MISSING if MISSING in (refunded, escalated) else refunded + escalated
    return [
        _check(report, "metrics.open", "0", lambda v: v == 0),
        Check("metrics.refunded + metrics.escalated", f"{incidents} (config.incidents)", total,
              total is not MISSING and total == incidents),
        _check(report, "metrics.repeated_steps", "0", lambda v: v == 0),
        _check(report, "metrics.refunds_over_limit", "0", lambda v: v == 0),
    ]


def _e4(report: dict[str, Any]) -> list[Check]:
    views = lookup(report, "metrics.views")
    return [_check(report, f"metrics.views.{name}.identical", "true", lambda v: v is True)
            for name in (sorted(views) if isinstance(views, dict) else [])]


def _e5(report: dict[str, Any]) -> list[Check]:
    threshold = lookup(report, "metrics.threshold")
    return [_check(report, "metrics.accuracy_mean", f">= {threshold} (metrics.threshold)",
                   lambda v: v is not None and threshold is not MISSING and v >= threshold)]


def _e6(report: dict[str, Any]) -> list[Check]:
    return []  # H6 no fija umbral: describe las diferencias


def _e7(report: dict[str, Any]) -> list[Check]:
    decisions = lookup(report, "metrics.decisions")
    incidents = lookup(report, "metrics.incidents")
    explained = (MISSING if not isinstance(incidents, list)
                 else sum(1 for inc in incidents if inc.get("decisions")))
    return [
        _check(report, "metrics.complete", f"{decisions} (metrics.decisions)",
               lambda v: decisions is not MISSING and v == decisions),
        Check("metrics.incidents (con decisiones explicadas)",
              f"{len(incidents) if isinstance(incidents, list) else '?'} (todas las seleccionadas)", explained,
              isinstance(incidents, list) and bool(incidents) and explained == len(incidents)),
    ]


def _e21(report: dict[str, Any]) -> list[Check]:
    return [
        _check(report, "metrics.replay.changed", "false", lambda v: v is False),
        _check(report, "metrics.replay.lag", "0", lambda v: v == 0),
        _check(report, "metrics.replay.inbox_matches", "true", lambda v: v is True),
        _check(report, "metrics.old_commands.not_replayed", "0", lambda v: v == 0),
        _check(report, "metrics.controls.orchestrator.changed", "true", lambda v: v is True),
    ]


CATALOG: dict[str, Entry] = {
    "e1": Entry(
        "E1", "Mide si los efectos se duplican cuando cada evento y cada comando llegan 2 y 3 veces, con "
        "y sin Idempotency key (inbox de comandos, `effect_keys` y la clave de los servicios simulados).",
        "H1: con idempotencia, 0 efectos duplicados en todas las ejecuciones; sin `effect_keys`, más de 0.",
        _e1),
    "e2": Entry(
        "E2", "Mide si los planes se completan cuando un ejecutor cae después de confirmar su transacción y "
        "antes de responder, y la saga lo vuelve a invocar con el mismo `command_id`.",
        "H2: el 100 % de los planes se completa sin repetir ningún paso ya completado.", _e2),
    "e3": Entry(
        "E3", "Mide la replanificación cuando un paso falla: incidencias de pérdida con los almacenes sin "
        "existencias, de modo que la reserva falla y el replanner revisa el plan.",
        "H3: el 100 % de las incidencias termina con un reembolso o un escalado, sin repetir el efecto de "
        "ningún paso completado y sin superar el límite de reembolso.", _e3),
    "e4": Entry(
        "E4", "Mide la reconstrucción de las vistas: se borra cada vista y se reconstruye volviendo a "
        "consumir `eda.events` desde el principio.",
        "H4: la huella de cada vista reconstruida coincide con la original, y el tiempo de reconstrucción "
        "se registra.", _e4),
    "e5": Entry(
        "E5", "Mide el acierto del planificador con LLM sobre un conjunto etiquetado de incidencias, "
        "comparando el plan generado con el esperado, con al menos tres repeticiones.",
        "H5: el acierto en la selección de acciones es al menos del 80 % (valor inicial).", _e5),
    "e6": Entry(
        "E6", "Compara la versión de eventos con la base síncrona en latencia por incidencia, tokens, coste, "
        "comportamiento ante fallos e incidencias resueltas y escaladas, con al menos tres repeticiones.",
        "H6: no fija umbral; el objetivo es medir y describir las diferencias (media, mediana y p95).", _e6),
    "e7": Entry(
        "E7", "Mide la auditabilidad: se eligen incidencias al azar y se reconstruye su explicación solo con "
        "el log y la traza.",
        "H7: para el 100 % de las seleccionadas se puede indicar qué agente propuso cada paso, con qué "
        "versión del plan y por qué.", _e7),
    "e21": Entry(
        "E21", "Mide la reproducción total del log: con las incidencias cerradas y el sistema parado, se llevan "
        "los offsets de todos los grupos al principio de `eda.events` y se vuelve a consumir hasta que el lag "
        "sea 0; se entregan otra vez los `ExecuteStep` antiguos y, como control, se repite sin el inbox del "
        "orquestador y del intérprete.",
        "H21: reproducir el log no produce ningún efecto, evento ni cambio de estado nuevos, y la variante de "
        "control sin inbox sí produce alguna diferencia.", _e21),
}
