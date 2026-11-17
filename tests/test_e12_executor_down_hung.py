"""E12: variantes, recuentos, H12a y H12b (spec experiments, «E12 Ejecutor caído y ejecutor colgado (H12)»)."""

from __future__ import annotations

import copy

from eda_retail.experiments.e12_executor_down_hung import (
    COMMAND_TIMEOUT_S,
    STEP_TIMEOUT_S,
    TARGET,
    VARIANTS,
    compensation_ok,
    evaluate,
    p95,
    summarize_commands,
    summarize_compensations,
    summarize_incidents,
    uses_file_claim,
)
from eda_retail.expexecutor.catalog import CATALOG
from eda_retail.synthetic.faults import FaultPlan
from eda_retail.synthetic.generator import generate

CLEAN = {f"I{i}": {"violations": 0} for i in range(1, 6)}


def test_variants_cover_down_hang_recovery_and_compensation():
    """El escenario: `file_claim` en `down`, en `hang` y en `hang` con recuperación tras vencer plazos, la
    compensación con `inventory` caído y una ejecución sin fallo de referencia; cada una es un FaultPlan válido"""
    assert set(VARIANTS) == {"baseline", "down", "hang", "hang_recovery", "compensation"}
    modes = {v: [(f.target, f.mode, f.duration_s) for f in spec.get("executor_faults", ())]
             for v, spec in VARIANTS.items()}
    assert modes["down"] == [(TARGET, "down", None)] and modes["hang"] == [(TARGET, "hang", None)]
    [(_, mode, recover)] = modes["hang_recovery"]
    assert mode == "hang" and recover > STEP_TIMEOUT_S  # con plazos ya vencidos
    [fault] = VARIANTS["compensation"]["executor_faults"]
    assert (fault.target, fault.mode, fault.action) == ("inventory", "down", None)
    # vence la reserva (un plazo) y no la compensación, cuyo plazo empieza cuando vence la reserva
    assert STEP_TIMEOUT_S < fault.duration_s < 2 * STEP_TIMEOUT_S
    assert COMMAND_TIMEOUT_S < STEP_TIMEOUT_S
    for spec in VARIANTS.values():
        FaultPlan(**spec)


def test_incidents_using_file_claim_come_from_the_expected_plan():
    data = generate(42, 50)
    uses = uses_file_claim(data)
    assert len(uses) == 50 and sum(uses.values()) == 18


def test_p95_nearest_rank():
    assert p95([]) is None
    assert p95([3.0]) == 3.0
    assert p95([float(i) for i in range(1, 21)]) == 19.0


def test_incidents_summary_splits_affected_and_unaffected():
    m = summarize_incidents({"A": 4.0, "B": 2.0}, {"A": "resolved", "B": "escalated"},
                            {"A": True, "B": False, "C": True})
    assert m["file_claim_share"] == 2 / 3
    assert (m["affected"]["incidents"], m["affected"]["closed"], m["affected"]["max_latency_s"]) == (2, 1, 4.0)
    assert m["affected"]["status"] == {"A": "resolved", "C": "open"}
    assert m["unaffected"]["p95_latency_s"] == 2.0


def test_hung_executor_report():
    """Ejecutor colgado: el informe da los intentos del dispatcher por comando, el tiempo que esperó a este ejecutor
    y cuándo empezó cada plazo respecto al encolado (en el primer envío, nunca antes)"""
    rows = [{"attempts": 6, "status": "expired", "start_after_enqueue_s": 2.5},
            {"attempts": 2, "status": "delivered", "start_after_enqueue_s": 0.1}]
    m = summarize_commands(rows, hang_wait_s=7.0)
    assert (m["commands"], m["attempts_max"], m["attempts_mean"]) == (2, 6, 4.0)
    assert m["status"] == {"expired": 1, "delivered": 1} and m["dispatcher_wait_s"] == 7.0
    assert m["deadline_start_after_enqueue_s"] == {"min": 0.1, "max": 2.5}


def _compensation(**over) -> dict:
    c = summarize_compensations([{"step_id": "S3-comp", "status": "completed"}],
                                [{"reservation_id": "R1"}], [4])
    return {"compensation": c | over, "open": 0, "invariants": CLEAN}


def test_compensation_of_a_step_with_an_expired_deadline():
    """Compensación de un paso con plazo vencido: la compensación se invoca con el sufijo `-comp` y se completa cuando
    el ejecutor vuelve (más de un intento), sin liberar dos veces"""
    assert compensation_ok(_compensation())
    assert compensation_ok(_compensation(releases_per_reservation=0))  # la reserva nunca se hizo: nada que liberar
    assert not compensation_ok(_compensation(releases_per_reservation=2))
    assert not compensation_ok(_compensation(release_attempts_max=1))  # no esperó al ejecutor caído
    assert not compensation_ok(_compensation(with_comp_suffix=0))
    assert not compensation_ok(_compensation(status={"failed": 1}))
    assert not compensation_ok(_compensation(compensations=0, with_comp_suffix=0, status={}))


def _variant(affected_max=3.0, unaffected_p95=2.0, statuses=None, open_=0, invariants=CLEAN) -> dict:
    return {"open": open_, "invariants": invariants, "incidents": {
        "affected": {"incidents": 1, "closed": 1 - open_, "max_latency_s": affected_max},
        "unaffected": {"p95_latency_s": unaffected_p95, "status": statuses or {"B": "resolved"}}}}


def _results(**variants) -> dict:
    base = {"baseline": _variant(), "compensation": _compensation()}
    base |= {v: _variant() for v in ("down", "hang", "hang_recovery")}
    return base | variants


def test_executor_down_forever():
    """Ejecutor caído para siempre: las incidencias con `file_claim` terminan dentro del plazo más el margen y las que
    no lo usan terminan como siempre (mismo estado que sin fallo)"""
    assert evaluate(_results())["H12a"]
    assert not evaluate(_results(down=_variant(statuses={"B": "escalated"})))["H12a"]
    assert not evaluate(_results(down=_variant(open_=1)))["H12a"]
    assert not evaluate(_results(down=_variant(affected_max=STEP_TIMEOUT_S + 3.0 + 10.1)))["H12a"]


def test_executor_recovery():
    """Recuperación del ejecutor: ningún efecto se ejecuta dos veces (I2); una violación de I2 rompe H12a"""
    broken = copy.deepcopy(CLEAN) | {"I2": {"violations": 1}}
    assert not evaluate(_results(hang_recovery=_variant(invariants=broken)))["H12a"]


def test_h12b_is_exploratory_and_a_negative_result_is_reported_as_a_finding():
    """H12b: p95 de las no afectadas como mucho el doble que sin fallo; si no se cumple, el informe lo dice como un
    hallazgo y H12a no cambia"""
    ok = evaluate(_results())
    assert ok["H12b"] and ok["h12b_note"] is None
    slow = evaluate(_results(hang=_variant(unaffected_p95=4.1)))
    assert slow["H12a"] and not slow["H12b"]
    assert "hallazgo" in slow["h12b_note"] and "hang" in slow["h12b_note"]


def test_expexecutor_checks_h12a_only():
    """expexecutor: E12 está en el catálogo y comprueba H12a por variante e I1 e I2, no H12b"""
    results = _results()
    evaluate(results)
    report = {"hypothesis": {"H12a": True, "H12b": False}, "metrics": {"variants": results}}
    checks = CATALOG["e12"].checks(report)
    assert checks and all(c.ok for c in checks)
    results["hang"]["h12a"] = False
    assert not all(c.ok for c in CATALOG["e12"].checks(report))
