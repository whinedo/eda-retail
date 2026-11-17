"""E18 Mensajes veneno en mitad de la partición (spec experiments) y mensajes veneno inyectados (spec
synthetic-data), de punta a punta con el LLM oráculo. Una sola ejecución de E18 para todos los escenarios."""

from __future__ import annotations

import pytest

from eda_retail.experiments.e18_poison import run_e18
from eda_retail.synthetic import poison

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e18(base_settings, seed=18, incidents=12)


def _attending(report, kind: str) -> dict[str, dict]:
    return {n: c for n, c in report.metrics["poisons"][kind]["consumers"].items() if c["expected"]}


def test_poison_in_the_middle_of_a_partition(report):
    """Veneno en mitad de la partición: el informe guarda su partición y su offset"""
    for p in report.metrics["poisons"].values():
        assert p["partition"] >= 0 and p["offset"] > 0


def test_invalid_json(report):
    """JSON inválido: cada consumidor lo guarda en su dead_letters con el motivo invalid_schema y confirma el offset"""
    attending = _attending(report, poison.INVALID_JSON)
    assert {"interpreter", "orchestrator", "event_log", "incident_status"} <= set(attending)
    assert all(c["dead_letters"] == 1 and c["reasons"] == ["invalid_schema"] and c["published"]
               for c in attending.values())
    assert all(n == 0 for n in report.metrics["lag"].values())


def test_unknown_event_type(report):
    """Evento de un tipo desconocido: ninguno lo lleva a la dead letter queue y event_log lo guarda"""
    unknown = report.metrics["poisons"][poison.UNKNOWN_TYPE]
    assert all(c["dead_letters"] == 0 for c in unknown["consumers"].values())
    assert unknown["in_log"] and unknown["ok"]


def test_unknown_event_version(report):
    """Evento de una versión desconocida: va a la dead letter queue de cada consumidor que atiende su tipo"""
    attending = _attending(report, poison.UNKNOWN_VERSION)
    assert {"orchestrator", "event_log", "incident_status"} <= set(attending) and "interpreter" not in attending
    assert report.metrics["poisons"][poison.UNKNOWN_VERSION]["ok"]


def test_personal_data(report):
    """Evento con datos personales: va a la dead letter queue de cada uno y no se escribe en las vistas ni en el log"""
    pii = report.metrics["poisons"][poison.PERSONAL_DATA]
    assert pii["ok"] and not pii["in_log"]


def test_large_payload(report):
    """Payload muy grande: se procesa o se rechaza por schema sin retrasar a las demás incidencias de la partición"""
    assert report.metrics["poisons"][poison.LARGE_PAYLOAD]["ok"]
    assert report.metrics["open"] == 0


def test_dlq_reconciliation(report):
    """Reconciliación de la DLQ: cada veneno aparece exactamente una vez por cada consumidor que atiende su tipo"""
    assert report.metrics["unexpected_dead_letters"] == 0
    assert all(p["ok"] for p in report.metrics["poisons"].values())
    assert report.metrics["dlq_messages"] >= report.metrics["dead_letter_rows"]


def test_h18(report):
    """Cumplimiento de H18"""
    assert report.hypothesis == {"H18": True}
