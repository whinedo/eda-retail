"""E23 Duplicados de negocio (spec experiments), reenvío de avisos y pedido compartido (spec synthetic-data), de
punta a punta con los oráculos de replan. Una sola ejecución de E23 (las cuatro variantes) para todos los
escenarios; la semilla 6 con 10 incidencias provoca los cuatro duplicados (fallos deterministas por clave)."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e23_business_duplicates import run_e23

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e23(base_settings, seed=6, incidents=10)


def _variant(report, name: str) -> dict:
    return report.metrics["variants"][name]


def _show(metrics: dict) -> str:
    return json.dumps(metrics, default=str)


def test_repeated_notice(report):
    """Aviso repetido: un solo `incident_id` y un solo `ShipmentDelayed` por aviso, y cada repetición (en orden y a
    la vez) descartada con `duplicate_notice`"""
    m = _variant(report, "resent_notices")
    assert m["resends"] > 0, _show(m)
    assert m["duplicate_notice_discards"] == m["resends"], _show(m)
    assert (m["incidents_per_notice"], m["shipment_delayed_per_incident"]) == (1, 1), _show(m)


def test_claim_requested_twice(report):
    """Reclamación solicitada dos veces: un replan vuelve a pedir `file_claim` y no se crea otra reclamación"""
    m = _variant(report, "rerequested_steps")
    assert m["requested_again"]["file_claim"] > 0, _show(m)
    assert m["claims_per_incident_order"] == 1, _show(m)


def test_customer_notice_after_a_revision(report):
    """Aviso al cliente tras una revisión: como mucho un aviso por incidencia, pedido y versión del plan"""
    m = _variant(report, "rerequested_steps")
    assert m["requested_again"]["notify_customer"] > 0, _show(m)
    assert m["notices_per_incident_order_version"] == 1, _show(m)


def test_single_refund(report):
    """Reembolso único: un plan posterior vuelve a proponer `issue_refund` y no se emite un segundo reembolso (I3)
    ni se reembolsa más que el importe del pedido"""
    m = _variant(report, "refund_again")
    assert m["requested_again"]["issue_refund"] > 0, _show(m)
    assert (m["refunds_per_incident_order"], m["over_refunded"]) == (1, 0), _show(m)
    assert m["invariants"]["I3"]["violations"] == 0, _show(m)


def test_order_in_two_incidents(report):
    """Pedido en dos incidencias: cada incidencia mantiene sus propios efectos, contados por (incidencia, pedido)"""
    m = _variant(report, "shared_order")
    assert m["shared_orders"] > 0, _show(m)
    assert m["claims_per_incident_order"] <= 1 and m["refunds_per_incident_order"] <= 1, _show(m)
    assert m["active_reservations_per_incident_order"] <= 1, _show(m)


def test_h23(report):
    """Cumplimiento de H23: todos los recuentos en los máximos de las specs en las cuatro variantes"""
    assert report.hypothesis == {"H23": True}, _show(report.metrics)
