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


def _invariants(report: dict[str, Any], prefix: str = "metrics") -> list[Check]:
    """I1 e I2 sin violaciones (spec experiments, «Invariantes comunes de resiliencia»)."""
    return [_check(report, f"{prefix}.invariants.{name}.violations", "0", lambda v: v == 0) for name in ("I1", "I2")]


def _e11(report: dict[str, Any]) -> list[Check]:
    inv = lookup(report, "metrics.invariants")
    broken = (MISSING if not isinstance(inv, dict)
              else sorted(name for name, v in inv.items() if not isinstance(v, dict) or v.get("violations") != 0))
    return [
        Check("metrics.invariants (I1 a I5)", "[] (sin violaciones)", broken, isinstance(inv, dict) and broken == []),
        _check(report, "metrics.stale_state_changes", "0", lambda v: v == 0),
        _check(report, "metrics.compensations.requested_more_than_once", "0", lambda v: v == 0),
    ]


def _e13(report: dict[str, Any]) -> list[Check]:
    inv = lookup(report, "metrics.invariants")
    broken = (MISSING if not isinstance(inv, dict)
              else sorted(name for name, v in inv.items() if not isinstance(v, dict) or v.get("violations") != 0))
    total = lookup(report, "metrics.failed_incidents.total")
    return [
        Check("metrics.invariants (I1 a I5)", "[] (sin violaciones)", broken, isinstance(inv, dict) and broken == []),
        _check(report, "metrics.compensations.failed", "> 0", lambda v: isinstance(v, int) and v > 0),
        _check(report, "metrics.failed_incidents.escalated_compensation_failed",
               f"{total} (metrics.failed_incidents.total)", lambda v: v == total),
        _check(report, "metrics.failed_incidents.refunds_requested", "0", lambda v: v == 0),
        _check(report, "metrics.failed_incidents.compensations_after_escalation", "0", lambda v: v == 0),
    ]


def _e16(report: dict[str, Any]) -> list[Check]:
    inv = lookup(report, "metrics.invariants")
    broken = (MISSING if not isinstance(inv, dict)
              else sorted(name for name, v in inv.items() if not isinstance(v, dict) or v.get("violations") != 0))
    return [
        _check(report, "hypothesis.H16", "true", lambda v: v is True),
        _check(report, "metrics.rebalance.reassigned_before_resume", "true", lambda v: v is True),
        _check(report, "metrics.duplicate_commands", "[]", lambda v: v == []),
        _check(report, "metrics.invalid_transitions", "[]", lambda v: v == []),
        Check("metrics.invariants (I1 a I5)", "[] (sin violaciones)", broken, isinstance(inv, dict) and broken == []),
    ]


def _e17(report: dict[str, Any]) -> list[Check]:
    variants = ("order_registered", "expired", "projected")
    return [*(_check(report, f"metrics.holds.{variant}", "true", lambda v: v is True) for variant in variants),
            *(c for variant in variants for c in _invariants(report, f"metrics.variants.{variant}"))]


def _e18(report: dict[str, Any]) -> list[Check]:
    poisons = lookup(report, "metrics.poisons")
    failed = (MISSING if not isinstance(poisons, dict)
              else sorted(kind for kind, p in poisons.items() if not p.get("ok")))
    lags = lookup(report, "metrics.lag")
    return [
        Check("metrics.lag (todos los grupos)", "0", lags,
              isinstance(lags, dict) and bool(lags) and all(v == 0 for v in lags.values())),
        Check("metrics.poisons (en la DLQ de quien atiende su tipo)", "[] (ninguno falla)", failed,
              isinstance(poisons, dict) and bool(poisons) and failed == []),
        _check(report, "metrics.unexpected_dead_letters", "0", lambda v: v == 0),
        _check(report, "metrics.open", "0", lambda v: v == 0),
        *_invariants(report),
    ]


def _e21(report: dict[str, Any]) -> list[Check]:
    return [
        _check(report, "metrics.replay.changed", "false", lambda v: v is False),
        _check(report, "metrics.replay.lag", "0", lambda v: v == 0),
        _check(report, "metrics.replay.inbox_matches", "true", lambda v: v is True),
        _check(report, "metrics.old_commands.not_replayed", "0", lambda v: v == 0),
        _check(report, "metrics.control.new_executions", "> 0", lambda v: isinstance(v, int) and v > 0),
        *_invariants(report),
    ]


E12_VARIANTS = ("down", "hang", "hang_recovery", "compensation")


def _e12(report: dict[str, Any]) -> list[Check]:
    """H12a (obligatoria) por variante e I1 e I2; H12b es exploratoria y no se comprueba."""
    checks = [_check(report, "hypothesis.H12a", "true", lambda v: v is True)]
    for variant in ("baseline", *E12_VARIANTS):
        prefix = f"metrics.variants.{variant}"
        if variant != "baseline":
            checks.append(_check(report, f"{prefix}.h12a", "true", lambda v: v is True))
        checks += _invariants(report, prefix)
    return checks


def _e14(report: dict[str, Any]) -> list[Check]:
    """H14 sobre `backpressure`: al menos un 503, sin superar la capacidad, colas vacías y sin comandos duplicados; I1 e
    I2 en las dos variantes."""
    prefix = "metrics.variants.backpressure"
    checks = [
        _check(report, "hypothesis.H14", "true", lambda v: v is True),
        _check(report, f"{prefix}.unavailable_503", "> 0 en total",
               lambda v: isinstance(v, dict) and sum(v.values()) > 0),
        _check(report, f"{prefix}.over_capacity", "{} (ningún ejecutor)", lambda v: v == {}),
        _check(report, f"{prefix}.final_queues.pending_commands", "0", lambda v: v == 0),
        _check(report, f"{prefix}.duplicate_commands", "[]", lambda v: v == []),
    ]
    for variant in ("baseline", "backpressure"):
        checks += _invariants(report, f"metrics.variants.{variant}")
    return checks


E22_VARIANTS = ("execute_step_2", "execute_step_4", "execute_step_8", "other_commands", "same_key",
                "effect_record_crash")
E22_SERVICE_ACTIONS = ("notify_customer", "file_claim", "reserve_stock", "reissue_shipment", "issue_refund")


def _e22(report: dict[str, Any]) -> list[Check]:
    checks = []
    for variant in E22_VARIANTS:
        prefix = f"metrics.variants.{variant}"
        checks += [_check(report, f"{prefix}.h22", "true", lambda v: v is True),
                   *(_check(report, f"{prefix}.executions_per_key.{action}", "<= 1",
                            lambda v: isinstance(v, int) and v <= 1) for action in E22_SERVICE_ACTIONS),
                   _check(report, f"{prefix}.distinct_responses_per_command_id", "<= 1",
                          lambda v: isinstance(v, int) and v <= 1),
                   *_invariants(report, prefix)]
    return checks


E23_VARIANTS = ("resent_notices", "rerequested_steps", "refund_again", "shared_order")
E23_AT_MOST_ONE = ("incidents_per_notice", "shipment_delayed_per_incident", "claims_per_incident_order",
                   "notices_per_incident_order_version", "refunds_per_incident_order",
                   "active_reservations_per_incident_order")


def _e23(report: dict[str, Any]) -> list[Check]:
    checks = []
    for variant in E23_VARIANTS:
        prefix = f"metrics.variants.{variant}"
        checks += [_check(report, f"{prefix}.h23", "true", lambda v: v is True),
                   *(_check(report, f"{prefix}.{count}", "<= 1", lambda v: isinstance(v, int) and v <= 1)
                     for count in E23_AT_MOST_ONE),
                   _check(report, f"{prefix}.over_refunded", "0", lambda v: v == 0),
                   *_invariants(report, prefix)]
    return checks


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
    "e11": Entry(
        "E11", "Mide los resultados que llegan tarde: un 50 % de fallos en `reserve_stock`, resultados retrasados "
        "en `reserve_stock`, `reissue_shipment` y `file_claim` y un plazo de `ExecuteStep` corto, para que haya "
        "resultados de una versión anterior del plan tras un `PlanRevised` y resultados después del plazo.",
        "H11: ningún resultado obsoleto cambia el estado de la saga (salvo la compensación de una reserva cancelada "
        "que se completa tarde, supuesto A4, que se pide una sola vez) e I1 a I5 no tienen violaciones.", _e11),
    "e12": Entry(
        "E12", "Mide la vivacidad y el aislamiento con un ejecutor fuera, en proceso y con el dispatcher en su "
        "propio hilo: 50 incidencias con `file_claim` en `down`, en `hang` y en `hang` con recuperación, y "
        "`inventory` caído más que el plazo de `reserve_stock`, cuya compensación se completa al volver.",
        "H12a: ninguna incidencia queda abierta, las que usan `file_claim` cierran dentro de `SAGA_STEP_TIMEOUT_S` "
        "más un margen, las demás terminan como sin fallo, cada compensación se completa una vez tras la "
        "recuperación e I1 a I5 sin violaciones. H12b (exploratoria): la p95 de las incidencias sin `file_claim` no "
        "supera el doble de la de una ejecución sin fallo; si no se cumple es un hallazgo.", _e12),
    "e13": Entry(
        "E13", "Mide el fallo de una compensación: 50 incidencias con todos los `reissue_shipment` fallando, de "
        "modo que la saga deriva el `release_stock` de cada reserva completada y retiene el reembolso, y un 50 % "
        "de fallos en `release_stock`.",
        "H13: hay al menos una compensación fallida y cada incidencia con una compensación fallida escala con "
        "`compensation_failed`, sin `issue_refund` pedido y sin compensaciones pedidas tras el escalado; I1 a I5 "
        "sin violaciones.", _e13),
    "e14": Entry(
        "E14", "Mide una ráfaga con contrapresión, en proceso: 200 avisos de golpe, el `prepare` del planner a 0,05 s "
        "y el de los ejecutores a 0,1 s, sin límite (`baseline`) y con capacidad 3 por ejecutor (`backpressure`), "
        "que responde 503 a un `ExecuteStep` nuevo con 3 trabajos abiertos.",
        "H14: en `backpressure` hay al menos un 503, ningún ejecutor supera su capacidad, las colas terminan "
        "vacías, ningún paso tiene `ExecuteStep` con `command_id` distinto, todas las incidencias se cierran e I1 a "
        "I5 no tienen violaciones.", _e14),
    "e16": Entry(
        "E16", "Mide un consumidor zombie en modo multiproceso: dos instancias del orquestador en el mismo grupo, "
        "50 incidencias en curso y un `SIGSTOP` a la que posee más particiones durante más tiempo que "
        "`session.timeout.ms`; después `SIGCONT`.",
        "H16: Kafka reasigna las particiones de la congelada antes de que reanude, ningún paso tiene dos "
        "`ExecuteStep` con `command_id` distinto en la misma versión del plan, no hay transiciones de saga "
        "inválidas e I1 a I5 no tienen violaciones.", _e16),
    "e17": Entry(
        "E17", "Mide los avisos aparcados con la proyección atrasada: con los proyectores parados y el plazo de "
        "aparcamiento acortado a 30 s, tres variantes: pedidos registrados después de aparcar los avisos, pedidos "
        "registrados antes con los proyectores parados más que el plazo, y pedidos registrados antes con los "
        "proyectores reanudados antes del plazo.",
        "H17: los avisos se reanudan por `OrderRegistered` en la primera variante, escalan con `no_valid_orders` en "
        "la segunda y se reanudan por el plazo en la tercera; ninguno queda aparcado, el intérprete confirma el "
        "offset al aparcar y un aviso reenviado se descarta.", _e17),
    "e18": Entry(
        "E18", "Mide los mensajes veneno: con las incidencias en curso se publica en mitad de las particiones un "
        "mensaje de cada tipo (JSON inválido, campo obligatorio ausente, `event_type` desconocido, versión sin "
        "upcaster, schema incumplido, datos personales y payload de 900 KB) y se ejecuta hasta lag 0.",
        "H18: ningún grupo se detiene (lag 0), cada veneno llega a la DLQ de cada consumidor que atiende su tipo y "
        "a ninguna más, el `event_type` desconocido queda en el log sin dead letter y todas las incidencias "
        "terminan.", _e18),
    "e21": Entry(
        "E21", "Mide la reproducción total del log: con las incidencias cerradas y el sistema parado, se llevan "
        "los offsets de todos los grupos al principio de `eda.events` y se vuelve a consumir hasta que el lag "
        "sea 0; se entregan otra vez los `ExecuteStep` antiguos y, como control, se reentregan a los ejecutores "
        "sin inbox de comandos ni `effect_keys`.",
        "H21: reproducir el log no produce ningún efecto, evento ni cambio de estado nuevos, y la variante de "
        "control sin idempotencia en los efectos sí repite algún efecto.", _e21),
    "e22": Entry(
        "E22", "Mide las carreras de comandos y la ventana entre el efecto y el registro de su clave: 50 incidencias "
        "con un 50 % de fallos en `reserve_stock` y cuatro variantes: el mismo `ExecuteStep` desde 2, 4 y 8 hilos; "
        "`CreatePlan`, `RevisePlan`, `RegisterCarrierNotice` y `SeedSampleData` concurrentes con el mismo "
        "`command_id`; dos `command_id` con la misma `idempotency_key`; y una caída entre el efecto y el registro en "
        "`notify_customer` e `issue_refund`, con reintento.",
        "H22: como mucho una ejecución del servicio por clave en cada variante, la misma respuesta para todas las "
        "entregas de un `command_id` e I1 a I5 sin violaciones.", _e22),
    "e23": Entry(
        "E23", "Mide los duplicados que nacen del negocio: 50 incidencias con un 50 % de fallos en `reserve_stock` "
        "y cuatro variantes: avisos reenviados de 1 a 5 veces (en orden y a la vez), replanes que vuelven a pedir "
        "`file_claim` y `notify_customer` ya atendidos, un reembolso pedido otra vez tras un replan posterior y el "
        "mismo pedido en dos incidencias.",
        "H23: ningún efecto se repite: un `incident_id` por aviso, cada repetición descartada con "
        "`duplicate_notice`, como mucho una reclamación, un reembolso y una reserva activa por (incidencia, "
        "pedido), un aviso por (incidencia, pedido, versión del plan), ningún reembolso por encima del importe e "
        "I1 a I5 sin violaciones.", _e23),
}
