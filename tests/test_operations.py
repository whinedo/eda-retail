"""incident-metrics y operations-view sin PostgreSQL ni Kafka: agregados de las consultas, retraso de las vistas y
endpoints `/api/operations/*` con las consultas sustituidas. Las vistas reales están en
tests/integration/test_operations_view.py."""

from __future__ import annotations

import contextlib
import re

import pytest
from fastapi.testclient import TestClient

from eda_retail.queries.operations import lag_of, llm_stats, p95
from eda_retail.web import app as web_app


def _calls(agent: str, latencies: list[int], validated: int) -> list[dict]:
    return [{"agent": agent, "tokens_in": 10, "tokens_out": 2, "latency_ms": ms, "validated": i < validated}
            for i, ms in enumerate(latencies)]


def _agent(stats, name: str):
    return next(s for s in stats if s.agent == name)


def test_latency_p95():
    """Percentil 95 de la latencia: con 20 llamadas de 100 a 2000 ms, el p95 es 1900 ms (la posición 19 de 20) y la
    media 1050 ms"""
    planner = _agent(llm_stats(_calls("planner", list(range(100, 2001, 100)), 20)), "planner")
    assert (planner.calls, planner.latency_p95_ms, planner.latency_mean_ms) == (20, 1900, 1050)
    assert p95([]) is None and p95([7]) == 7


def test_validation_percentage():
    """Porcentaje de validaciones superadas: 4 llamadas del replanificador, 3 superadas, dan 75 %"""
    assert _agent(llm_stats(_calls("replanner", [10, 20, 30, 40], 3)), "replanner").validated_pct == 75


def test_agent_without_calls():
    """Agente sin llamadas: su fila aparece con 0 llamadas y sin latencia ni porcentaje"""
    writer = _agent(llm_stats(_calls("planner", [10], 1)), "writer")
    assert (writer.calls, writer.latency_mean_ms, writer.latency_p95_ms, writer.validated_pct) == (0, None, None, None)
    assert [s.agent for s in llm_stats([])] == ["interpreter", "planner", "replanner", "writer"]


def test_projector_up_to_date():
    """Proyector al día: end offset 130 y offset aplicado 129 dan lag 0"""
    lag = lag_of({1: 129}, {1: 130})
    assert [(p.partition, p.lag) for p in lag.partitions] == [(1, 0)] and lag.lag == 0


def test_projector_behind():
    """Proyector atrasado: end offset 130 y offset aplicado 126 dan lag 3 (los offsets 127, 128 y 129)"""
    assert lag_of({1: 126}, {1: 130}).partitions[0].lag == 3


def test_partition_without_applied_messages():
    """Partición sin mensajes aplicados: sin posición y con end offset 40, el lag es 40"""
    lag = lag_of({0: 5}, {0: 6, 2: 40})
    assert {p.partition: (p.applied, p.lag) for p in lag.partitions} == {0: (5, 0), 2: (None, 40)}
    assert lag.lag == 40


def test_kafka_does_not_answer():
    """Kafka no responde: la respuesta trae las posiciones aplicadas con lag nulo y no falla"""
    lag = lag_of({0: 5, 1: 9}, None)
    assert [(p.applied, p.end_offset, p.lag) for p in lag.partitions] == [(5, None, None), (9, None, None)]
    assert lag.lag is None


class _RecordingConn:
    """Conexión que anota cada SQL y no devuelve filas."""

    def __init__(self) -> None:
        self.sql: list[str] = []

    @contextlib.contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=None):
        self.sql.append(" ".join(sql.split()))
        return self

    def fetchone(self):
        return None

    def fetchall(self):
        return []


ROUTES = ("summary", "incidents", "llm", "carriers", "freshness")
_TABLE = re.compile(r"\b(?:FROM|JOIN)\s+([a-z_]{2,}\.[a-z_]+)", re.IGNORECASE)  # esquema.tabla, no alias.columna
_WRITE = re.compile(r"\b(INSERT|UPDATE|DELETE|TRUNCATE|CREATE|DROP|ALTER)\b", re.IGNORECASE)


@pytest.fixture
def api(monkeypatch):
    conn = _RecordingConn()
    monkeypatch.setattr(web_app, "kafka_end_offsets", lambda *_a, **_k: None)  # Kafka sin respuesta
    app = web_app.create_app("postgresql://u:p@127.0.0.1:1/x")
    app.dependency_overrides[web_app._read_only_conn] = lambda: conn
    return TestClient(app, base_url="http://localhost"), conn


def test_operations_endpoints_only_read_views(api):
    """Acceso de solo lectura y local: los endpoints de operaciones solo leen las vistas y las posiciones"""
    client, conn = api
    replies = [client.get(f"/api/operations/{route}") for route in ROUTES]
    assert [r.status_code for r in replies] == [200] * len(ROUTES)
    tables = {t for sql in conn.sql for t in _TABLE.findall(sql)}
    assert {"views.view_incident_metrics", "views.view_llm_calls", "views.view_carrier_consults"} <= tables
    assert all(t.startswith("views.view_") for t in tables - {"views.offsets", "event_log.offsets"}), tables
    assert not [sql for sql in conn.sql if _WRITE.search(sql)]


def test_empty_operations(api):
    """Primer arranque: contadores a 0, desglose de escaladas vacío y tabla vacía, con applied"""
    client, _ = api
    summary = client.get("/api/operations/summary").json()
    assert summary["by_state"] == {"open": 0, "resolved": 0, "refunded": 0, "escalated": 0}
    assert summary["escalations"] == [] and summary["applied"] == {}
    assert client.get("/api/operations/incidents").json() == {"incidents": [], "applied": {}, "next_before": None}
    freshness = client.get("/api/operations/freshness").json()["consumers"]
    assert {c["consumer"] for c in freshness} >= {"operations", "event_log"}
    assert all(c["lag"] is None for c in freshness)  # Kafka sin respuesta: lag nulo, sin error


@pytest.mark.parametrize("route", ROUTES)
def test_write_rejected(api, route):
    """Escritura rechazada: un POST a cualquier ruta /api/operations/* responde 405 y no cambia nada"""
    client, conn = api
    assert client.post(f"/api/operations/{route}").status_code == 405
    assert conn.sql == []


def test_incidents_validates_state_and_cursor(api):
    client, _ = api
    assert client.get("/api/operations/incidents", params={"state": "lost"}).status_code == 422
    assert client.get("/api/operations/incidents", params={"before": "no-es-fecha|I-1"}).status_code == 422
    assert client.get("/api/operations/incidents",
                      params={"before": "2026-11-17T10:00:00+00:00|I-1", "state": "escalated"}).status_code == 200
