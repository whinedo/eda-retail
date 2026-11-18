"""Todos los componentes en procesos propios (spec experiments, «Todos los componentes en procesos propios»): los
procesos de `scripts/run-agents.sh` contra PostgreSQL y Kafka reales, con el oráculo como LLM (`LLM_CLIENT=oracle`) y
el harness solo sembrando datos, enviando avisos y midiendo. Una ejecución pequeña por el tiempo del CI."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from eda_retail.experiments import invariants
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.processes import COMPONENTS

pytestmark = pytest.mark.slow

INCIDENTS = 4


@pytest.fixture(scope="module")
def run(base_settings):
    config = RunConfig(seed=7, incidents=INCIDENTS, component_processes=True, idle_seconds=60.0,
                       timeout_seconds=240.0)
    with Harness(replace(base_settings, poc_version=1), config) as h:
        h.send_notices()
        h.run_until_closed()
        out = {"closed": h.closed(), "alive": {p.name: p.alive() for p in h.components},
               "deaths": h.process_deaths(), "log_caught_up": h.catch_up_log()}
        out["exit_codes"] = h.stop_components()
        out["invariants"] = invariants.check(h)
        out["executions"] = {f"{service}:{key}": n for (service, key), n in h.service_executions().items()}
        out["logs"] = {p.name: p.log_text().splitlines()[-15:] for p in h.components}
    return out


def _show(run: dict) -> str:
    return json.dumps(run, default=str)[-6000:]


def test_every_component_runs_in_its_own_process_until_all_incidents_close(run):
    """Ejecución de punta a punta con todos los componentes en procesos: con los procesos de `scripts/run-agents.sh`
    y el oráculo como LLM, todas las incidencias se cierran, ningún proceso muere, todos salen limpios al pararlos,
    I1 a I5 no tienen violaciones y cada `idempotency_key` se ejecuta una vez en su servicio simulado"""
    assert run["closed"] == INCIDENTS, _show(run)
    assert set(run["alive"]) == {c.name for c in COMPONENTS} and all(run["alive"].values()), _show(run)
    assert run["deaths"] == {}, _show(run)
    assert run["exit_codes"] == {c.name: 0 for c in COMPONENTS}, _show(run)
    assert run["log_caught_up"] is True, _show(run)
    assert {name: inv["violations"] for name, inv in run["invariants"].items()} == {
        "I1": 0, "I2": 0, "I3": 0, "I4": 0, "I5": 0}, _show(run)
    assert run["executions"] and set(run["executions"].values()) == {1}, _show(run)
