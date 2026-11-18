"""`eda-support` contra PostgreSQL y Kafka reales (spec support). La republicación usa una base de datos propia: lee
el outbox de todos los componentes y la base compartida tiene los de otros tests."""

from __future__ import annotations

import json
import uuid
from dataclasses import replace

import psycopg
import pytest

from eda_retail.events.envelope import new_event
from eda_retail.experiments.harness import isolated_database
from eda_retail.infra.db import connect
from eda_retail.infra.outbox import append
from eda_retail.support import cli
from eda_retail.support.actions import OK
from eda_retail.support.system import LiveSystem

from .kafka_helpers import read_all

pytestmark = pytest.mark.integration


def _run(settings, capsys, *argv: str) -> tuple[int, dict]:
    code = cli.run(["--json", *argv], lambda: LiveSystem(settings))
    return code, json.loads(capsys.readouterr().out)


def _runs(conn: psycopg.Connection) -> int:
    with conn.transaction():
        return conn.execute("SELECT count(*) AS n FROM support.runs").fetchone()["n"]


def _events(n: int, producer: str = "planner", incident: str | None = None) -> list:
    """Eventos de una incidencia: con la misma clave, el relay los publica en la misma partición."""
    incident = incident or f"I-{uuid.uuid4().hex[:8]}"
    return [new_event(event_type="PlanCreated", correlation_id=incident, causation_id=None, producer=producer,
                      payload={"incident_id": incident, "n": i}) for i in range(n)]


def test_status_reads_the_real_system_without_writing(settings, conn, capsys):
    """Diagnóstico de un sistema sano"""
    before = _runs(conn)
    code, report = _run(settings, capsys, "status")
    assert code == OK
    assert {"processes", "topic", "groups", "read_side", "outbox", "dead_letters", "failed_jobs",
            "parked_notices", "alerts"} <= set(report)
    assert set(report["topic"]) == {"0", "1", "2"} and "kafka_down" not in report["alerts"]
    assert set(report["groups"]) == {"interpreter", "orchestrator"}
    assert "planner" in report["outbox"] and "views" in report["dead_letters"]
    assert _runs(conn) == before


def test_status_when_kafka_does_not_answer(settings, capsys):
    """Kafka no responde"""
    code, report = _run(replace(settings, kafka_bootstrap_servers="127.0.0.1:1"), capsys, "status")
    assert code == OK and "kafka_down" in report["alerts"]
    assert report["outbox"] and report["failed_jobs"] is not None and report["parked_notices"] is not None


def test_replay_from_marks_the_rows_and_records_the_run(settings, conn, capsys):
    """Filas marcadas para publicarse otra vez; Ejecución registrada"""
    with conn.transaction():
        ids = append(conn, "planner", _events(3))
        conn.execute("UPDATE planner.outbox SET published_at = now() WHERE id = ANY(%s)", (ids,))
    before = _runs(conn)
    try:
        code, dry = _run(settings, capsys, "replay-from", "planner", str(ids[1]))
        assert code == OK and dry["plan"]["changes"]["first_id"] == ids[1]
        assert _runs(conn) == before  # Simulación sin registro
        code, done = _run(settings, capsys, "replay-from", "planner", str(ids[1]), "--confirm", dry["plan"]["token"])
        assert code == OK and done["outcome"] == "succeeded" and done["result"]["rows"] >= 2
        with conn.transaction():
            rows = conn.execute("SELECT id, published_at FROM planner.outbox WHERE id = ANY(%s) ORDER BY id",
                                (ids,)).fetchall()
            run = conn.execute("SELECT action, args, origin, outcome, finished_at FROM support.runs"
                               " ORDER BY id DESC LIMIT 1").fetchone()
        assert rows[0]["published_at"] is not None and [r["published_at"] for r in rows[1:]] == [None, None]
        assert run["action"] == "replay-from" and run["args"] == {"component": "planner", "outbox_id": ids[1]}
        assert run["origin"] == "cli" and run["outcome"] == "succeeded" and run["finished_at"] is not None
    finally:
        with conn.transaction():  # que el relay de otro test no las publique en su topic
            conn.execute("UPDATE planner.outbox SET published_at = now() WHERE published_at IS NULL")


def test_rebuild_dry_run_lists_the_tables_of_the_view(settings, conn, capsys):
    """Reconstrucción de una vista: la simulación da las tablas y sus filas sin escribir"""
    before = _runs(conn)
    code, dry = _run(settings, capsys, "rebuild", "orders")
    assert code == OK and set(dry["plan"]["changes"]["tables"]) == {"views.view_orders"}
    assert _runs(conn) == before


def test_republish_a_recreated_topic(base_settings, topic, capsys):
    """Republicación de un topic recreado"""
    with isolated_database(base_settings, f"eda_support_{uuid.uuid4().hex[:8]}") as url:
        settings = replace(base_settings, database_url=url, events_topic=topic)
        db = connect(url)
        try:
            with db.transaction():
                incident = f"I-{uuid.uuid4().hex[:8]}"
                planner = append(db, "planner", _events(2, "planner", incident))
                intake = append(db, "intake", _events(1, "intake", incident))
                # Su posición en el topic perdido: intake iba antes que planner en la partición 0.
                for offset, (schema, row_id) in enumerate([("intake", intake[0]), ("planner", planner[0]),
                                                           ("planner", planner[1])]):
                    db.execute(f"UPDATE {schema}.outbox SET published_at = now(), kafka_partition = 0,"
                               " kafka_offset = %s WHERE id = %s", (offset + 40, row_id))
                db.execute("INSERT INTO views.offsets (consumer, kafka_topic, kafka_partition, kafka_offset)"
                           " VALUES ('orders', %s, 0, 42)", (topic,))
            code, dry = _run(settings, capsys, "republish", "--mode", "recreated")
            assert code == OK, dry
            assert dry["plan"]["changes"]["total"] == 3 and dry["plan"]["changes"]["reset_groups"]
            code, done = _run(settings, capsys, "republish", "--mode", "recreated", "--confirm", dry["plan"]["token"])
            assert code == OK and done["result"]["total"] == 3, done
            messages = read_all(settings.kafka_bootstrap_servers, topic, 3)
            assert [m.value["producer"] for m in messages] == ["intake", "planner", "planner"]
            with db.transaction():
                position = db.execute("SELECT kafka_offset FROM views.offsets WHERE kafka_topic = %s",
                                      (topic,)).fetchone()["kafka_offset"]
                outcome = db.execute("SELECT outcome FROM support.runs").fetchone()["outcome"]
            assert position == -1 and outcome == "succeeded"
            code, _ = _run(settings, capsys, "republish", "--mode", "recreated")
            assert code == 2  # Topic que no está vacío: ya tiene los mensajes republicados
        finally:
            db.close()
