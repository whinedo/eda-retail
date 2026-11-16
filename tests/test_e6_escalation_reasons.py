"""E6: the report counts escalations by reason, per run and per version."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from eda_retail.experiments.e6_compare import _aggregate, _row

PRICES = SimpleNamespace(llm_price_in_per_1k=Decimal(0), llm_price_out_per_1k=Decimal(0))


def _result(reasons: dict[str, int]) -> SimpleNamespace:
    return SimpleNamespace(closed=3, resolved=3 - sum(reasons.values()), escalated=sum(reasons.values()),
                           duplicates={}, latencies_ms=[1.0], llm_calls=1, tokens_in=1, tokens_out=1,
                           elapsed_seconds=1.0, escalation_reasons=reasons)


def test_run_row_keeps_the_escalation_reasons():
    """Comparación sin fallos: cada ejecución guarda sus escaladas por motivo"""
    row = _row("events", _result({"max_revisions": 2}), 0, PRICES)
    assert row["escalation_reasons"] == {"max_revisions": 2}


def test_aggregate_adds_the_reasons_of_every_run():
    """Comparación sin fallos: el informe suma las escaladas por motivo de cada versión"""
    rows = [_row("events", _result(r), 0, PRICES) for r in ({"max_revisions": 2}, {"max_revisions": 1, "budget": 1}, {})]
    assert _aggregate(rows)["escalation_reasons"] == {"budget": 1, "max_revisions": 3}


def test_e6_run_config_gives_the_real_llm_time_for_every_incident():
    """E6 con el LLM real: el tope total del harness crece con las incidencias (300 s cortaba 10 incidencias)"""
    from eda_retail.experiments.e6_compare import run_config

    assert run_config("no_faults", 42, 10).timeout_seconds == 1200
    assert run_config("faults", 42, 50).timeout_seconds == 6000
    assert run_config("faults", 42, 1).timeout_seconds == 300  # nunca por debajo del valor por defecto
    assert run_config("faults", 42, 10).faults.duplicates == 2


def test_e6_scales_the_saga_deadlines_with_the_incidents():
    """E6 con el LLM real: los plazos de la saga crecen con las incidencias (la cola del LLM es compartida)"""
    from eda_retail.experiments.e6_compare import DEADLINE_PER_INCIDENT_S, TIMEOUT_PER_INCIDENT_S, run_config

    config = run_config("faults", 42, 50)
    assert (config.plan_timeout_seconds, config.step_timeout_seconds) == (1500, 1500)
    small = run_config("no_faults", 42, 1)
    assert (small.plan_timeout_seconds, small.step_timeout_seconds) == (300, 60)  # nunca por debajo del defecto
    assert config.timeout_seconds > config.plan_timeout_seconds  # el tope del harness queda por encima
    assert TIMEOUT_PER_INCIDENT_S > DEADLINE_PER_INCIDENT_S


def test_e6_minimum_deadlines_are_the_production_defaults(monkeypatch):
    """Los plazos por defecto de producción no cambian: E6 parte de ellos"""
    from eda_retail.config import Settings
    from eda_retail.experiments import e6_compare

    for name in ("SAGA_PLAN_TIMEOUT_S", "SAGA_STEP_TIMEOUT_S"):
        monkeypatch.delenv(name, raising=False)
    settings = Settings()
    assert (settings.saga_plan_timeout_s, settings.saga_step_timeout_s) == (300.0, 60.0)
    assert (e6_compare.DEFAULT_PLAN_TIMEOUT_S, e6_compare.DEFAULT_STEP_TIMEOUT_S) == (300.0, 60.0)


def test_the_harness_applies_the_run_deadlines_to_its_settings():
    """El harness aplica a la saga los plazos de la ejecución; sin ellos, deja los de la configuración"""
    from dataclasses import replace

    from eda_retail.config import Settings
    from eda_retail.experiments.harness import RunConfig, saga_deadlines

    base = Settings()
    custom = replace(base, **saga_deadlines(RunConfig(seed=1, incidents=1, plan_timeout_seconds=900,
                                                       step_timeout_seconds=450)))
    assert (custom.saga_plan_timeout_s, custom.saga_step_timeout_s) == (900, 450)
    assert saga_deadlines(RunConfig(seed=1, incidents=1)) == {}


def test_the_e6_report_says_which_deadlines_were_used(monkeypatch):
    """Ejecución de un experimento: el informe de E6 dice qué plazos de la saga se usaron"""
    from eda_retail.config import Settings
    from eda_retail.experiments import e6_compare

    def fake_run(version):
        return lambda base, config, data, llm: _row(version, _result({}), 0, PRICES)

    monkeypatch.setattr(e6_compare, "_run_events", fake_run("events"))
    monkeypatch.setattr(e6_compare, "_run_sync", fake_run("sync"))
    report = e6_compare.run_e6(Settings(), incidents=20, repetitions=1, scenarios=("no_faults",),
                               llm=lambda data, n: SimpleNamespace(model="fake"))
    assert report.config["saga_deadlines"] == {"plan_timeout_seconds": 600, "step_timeout_seconds": 600}
