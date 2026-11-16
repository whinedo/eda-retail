"""Escenarios de la spec web-front sin test propio (T3, grupo C), en el backend del panel."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from eda_retail.web.events import events_after


class _Rows:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class _Conn:
    """Conexión mínima: devuelve las filas de `event_log.events` dadas, como haría psycopg con dict_row."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def execute(self, query: str, params: Any = None) -> _Rows:
        assert "FROM event_log.events" in query
        return _Rows(self._rows)


def test_executor_result():
    """Resultado de un ejecutor"""
    # GIVEN un `StepCompleted` publicado por `executor-notify_customer`
    row = {
        "seq": 7, "event_id": uuid.uuid4(), "event_type": "StepCompleted",
        "occurred_at": datetime(2026, 11, 16, 10, 0, tzinfo=UTC), "correlation_id": "INC-1",
        "causation_id": uuid.uuid4(), "producer": "executor-notify_customer",
        "payload": {"incident_id": "INC-1", "step_id": "S1", "plan_version": 1, "action": "notify_customer"},
        "trace_context": None, "kafka_partition": 0, "kafka_offset": 3,
    }

    # WHEN se muestra en el panel
    [shown] = events_after(_Conn([row]), after_seq=6)

    # THEN su emisor es `executor-notify_customer` y su destino `orchestrator`
    assert shown["sender"] == "executor-notify_customer"
    assert shown["receiver"] == "orchestrator"
