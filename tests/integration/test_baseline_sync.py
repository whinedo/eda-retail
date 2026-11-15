"""baseline_sync: la base síncrona de referencia (spec experiments, «Base síncrona de referencia»)."""

from __future__ import annotations

import pytest

from eda_retail.baseline_sync.runner import run_sync, sync_started
from eda_retail.experiments.e3_replan import only_cause
from eda_retail.experiments.harness import RunConfig, started
from eda_retail.synthetic.faults import FaultPlan

pytestmark = pytest.mark.slow


def _events_inputs(base_settings, config):
    with started(base_settings, config) as h:
        received = [r["id"] for r in h.conn.execute(
            "SELECT payload->>'carrier_notice_id' AS id FROM event_log.events"
            " WHERE event_type = 'CarrierNoticeReceived'")]
        failed = {(r["correlation_id"], r["step_id"]) for r in h.conn.execute(
            "SELECT correlation_id, payload->>'step_id' AS step_id FROM event_log.events "
            "WHERE event_type = 'StepFailed' AND payload->>'error' = 'injected_failure'")}
        h.conn.commit()
    return sorted(received), failed


def test_same_input_in_both_versions(base_settings):
    """Misma entrada en ambas versiones"""
    config = RunConfig(seed=3, incidents=6, faults=FaultPlan(seed=3, fail_probability={"reserve_stock": 0.5}))
    received, failed = _events_inputs(base_settings, config)
    sync = run_sync(base_settings, config)
    assert sorted(sync.received) == received and len(received) == 6
    assert sync.injected_failures == failed and failed


def test_sync_baseline_writes_no_log(base_settings):
    """La base síncrona resuelve las incidencias con llamadas directas, sin events, outbox ni inbox"""
    result = run_sync(base_settings, RunConfig(seed=4, incidents=6))
    assert result.closed == 6 and result.open == 0
    assert result.log_rows == 0
    assert result.llm_calls > 0 and result.tokens_in > 0
    assert result.duplicates == {} and len(result.latencies_ms) == 6


def test_sync_baseline_compensates_before_refund(base_settings):
    """Con el reenvío fallido, la reserva se compensa antes del reembolso y el stock vuelve a su valor"""
    data = only_cause(5, "loss", 3)
    config = RunConfig(seed=5, incidents=3, faults=FaultPlan(seed=5, fail_probability={"reissue_shipment": 1.0}))
    with sync_started(base_settings, config, data=data) as run:
        result = run.result()
        stock = run.conn.execute("SELECT sum(available) AS n FROM baseline_sync.stock").fetchone()["n"]
        statuses = {r["status"] for r in run.conn.execute("SELECT status FROM baseline_sync.reservations")}
        run.conn.commit()
    assert result.closed == 3 and result.refunded == 3
    assert stock == sum(s.available for s in data.stock)
    assert statuses == {"released"}


def test_duplicated_notice_is_handled_once(base_settings):
    """Un aviso repetido no repite la incidencia ni sus efectos en la base síncrona"""
    result = run_sync(base_settings, RunConfig(seed=6, incidents=4, faults=FaultPlan(seed=6, duplicates=3)))
    assert result.closed == 4 and len(result.received) == 12
    assert result.duplicates == {}
