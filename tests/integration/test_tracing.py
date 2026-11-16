"""Observabilidad: trazas de OpenTelemetry síncronas (SQL, funciones) y asíncronas (outbox → Kafka)."""

from __future__ import annotations

from dataclasses import replace

import pytest
from opentelemetry.trace import SpanKind, StatusCode

from eda_retail.infra import tracing

PING = "TestPing"


















def test_sync_sql_statements_are_traced(conn, spans):
    """Operaciones síncronas: cada sentencia SQL dentro de una operación genera un span"""
    with tracing.tracer().start_as_current_span("test.sync") as parent:
        conn.execute("SELECT 1").fetchone()
    conn.rollback()
    db = [s for s in spans.get_finished_spans() if s.attributes.get("db.system") == "postgresql"]
    assert db and all(s.parent.span_id == parent.get_span_context().span_id for s in db)
    assert db[0].kind is SpanKind.CLIENT


def test_orphan_sql_statements_are_not_traced(conn, spans):
    """Las sentencias sin operación en curso (p. ej. el polling del relay) no crean trazas sueltas"""
    conn.execute("SELECT 1").fetchone()
    conn.rollback()
    assert [s for s in spans.get_finished_spans() if s.attributes.get("db.system") == "postgresql"] == []


def test_traced_function_records_span_and_error(spans):
    """Operaciones síncronas en proceso: el decorador traced crea un span y registra errores"""

    @tracing.traced("test.compute")
    def compute(x: int) -> int:
        if x < 0:
            raise ValueError("negativo")
        return x * 2

    assert compute(2) == 4
    with pytest.raises(ValueError):
        compute(-1)
    done = [s for s in spans.get_finished_spans() if s.name == "test.compute"]
    assert [s.status.status_code for s in done] == [StatusCode.UNSET, StatusCode.ERROR]




def test_setup_tracing_respects_switch(base_settings):
    """TRACING_ENABLED=false: setup_tracing no instala proveedor"""
    assert tracing.setup_tracing("test", replace(base_settings, tracing_enabled=False)) is None
