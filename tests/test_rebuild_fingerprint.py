"""cqrs-views «Reconstrucción de vistas»: la huella no depende del orden en que PostgreSQL devuelve las filas."""

from __future__ import annotations

from contextlib import nullcontext

from eda_retail.projectors.rebuild import fingerprint


class _Conn:
    """Conexión falsa: devuelve las filas de cada tabla en el orden dado."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def transaction(self):
        return nullcontext()

    def execute(self, sql: str, params=None):
        return iter(self._rows)


def test_fingerprint_ignores_row_order_when_the_key_has_several_columns():
    """Reconstrucción de order_status: dos filas con el mismo pedido y distinta incidencia dan la misma huella en cualquier orden"""
    rows = [
        {"order_id": "P1", "incident_id": "I-a", "status": "delayed"},
        {"order_id": "P1", "incident_id": "I-b", "status": "refunded"},
    ]
    assert fingerprint(_Conn(rows), "order_status") == fingerprint(_Conn(list(reversed(rows))), "order_status")


def test_fingerprint_changes_when_a_row_changes():
    """La huella cambia si la vista cambia: detecta una vista reconstruida distinta"""
    rows = [{"order_id": "P1", "incident_id": "I-a", "status": "delayed"}]
    changed = [{"order_id": "P1", "incident_id": "I-a", "status": "refunded"}]
    assert fingerprint(_Conn(rows), "order_status") != fingerprint(_Conn(changed), "order_status")


def test_parked_notices_fingerprint_is_independent_of_row_order():
    """Reconstrucción de parked_notices: la huella de los avisos aparcados no depende del orden de las filas"""
    rows = [
        {"incident_id": "I-a", "order_ids": ["P-1001"], "deadline_at": "2026-11-15 20:13:36+00:00"},
        {"incident_id": "I-b", "order_ids": ["P-1004", "P-1005"], "deadline_at": "2026-11-15 20:14:02+00:00"},
    ]
    assert fingerprint(_Conn(rows), "parked_notices") == fingerprint(_Conn(list(reversed(rows))), "parked_notices")
