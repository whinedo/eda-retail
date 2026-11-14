"""event-backbone: outbox, orden por incidencia y Kafka como bus de entrega."""

from __future__ import annotations

import contextlib
import uuid
from collections import defaultdict
from dataclasses import replace

from confluent_kafka.admin import AdminClient, NewTopic

from eda_retail.events.envelope import new_event
from eda_retail.infra.event_store import append_events
from eda_retail.infra.relay import OutboxRelay

from .kafka_helpers import read_all


def _incident() -> str:
    return f"I-{uuid.uuid4().hex[:8]}"


def _ev(incident: str, event_type: str):
    return new_event(
        event_type=event_type,
        correlation_id=incident,
        causation_id=None,
        producer="test",
        payload={"incident_id": incident},
    )


def _drain(relay: OutboxRelay) -> int:
    total = 0
    while (n := relay.run_once()) > 0:
        total += n
    return total


def test_failure_between_change_and_publication(connect_db, settings):
    """Fallo entre el cambio y la publicación"""
    incident = _incident()
    writer = connect_db()
    with writer.transaction():
        [seq] = append_events(writer, [_ev(incident, "ShipmentDelayed")])
    writer.close()  # el proceso cae antes de enviar el evento al bus

    relay = OutboxRelay.from_settings(settings)  # reinicio: relay nuevo
    try:
        published = _drain(relay)
    finally:
        relay.close()

    msgs = [m for m in read_all(settings.kafka_bootstrap_servers, settings.events_topic, published) if m.key == incident]
    assert len(msgs) == 1
    assert msgs[0].value["seq"] == seq

    check = connect_db()
    row = check.execute(
        "SELECT count(*) AS n FROM events WHERE correlation_id = %s", (incident,)
    ).fetchone()
    assert row["n"] == 1
    published = check.execute("SELECT published_at FROM outbox WHERE seq = %s", (seq,)).fetchone()
    assert published["published_at"] is not None


def test_step_order(connect_db, settings):
    """Orden de pasos"""
    incident = _incident()
    conn = connect_db()
    with conn.transaction():
        append_events(conn, [_ev(incident, "PlanCreated")])
    with conn.transaction():
        append_events(conn, [_ev(incident, "StepRequested")])

    relay = OutboxRelay.from_settings(settings)
    try:
        published = _drain(relay)
    finally:
        relay.close()

    msgs = [m for m in read_all(settings.kafka_bootstrap_servers, settings.events_topic, published) if m.key == incident]
    assert [m.event_type for m in msgs] == ["PlanCreated", "StepRequested"]
    assert len({m.partition for m in msgs}) == 1  # misma clave -> misma partición
    assert msgs[0].offset < msgs[1].offset


def test_topic_loss(connect_db, settings):
    """Pérdida del topic"""
    conn = connect_db()
    incidents = [_incident() for _ in range(200)]
    first_seq = None
    with conn.transaction():
        for incident in incidents:
            seqs = append_events(
                conn, [_ev(incident, "ShipmentDelayed"), _ev(incident, "PlanCreated")]
            )
            first_seq = first_seq if first_seq is not None else seqs[0]

    relay = OutboxRelay.from_settings(settings)
    try:
        _drain(relay)
    finally:
        relay.close()

    # El topic se pierde: se sustituye por uno vacío y se republica desde el log.
    empty_topic = f"test.{uuid.uuid4().hex[:12]}"
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for fut in admin.create_topics([NewTopic(empty_topic, num_partitions=3, replication_factor=1)]).values():
        fut.result(timeout=30)
    try:
        replay = OutboxRelay.from_settings(replace(settings, events_topic=empty_topic))
        try:
            marked = replay.replay_from(first_seq)
            assert marked >= 400
            _drain(replay)
        finally:
            replay.close()

        wanted = set(incidents)
        msgs = [
            m
            for m in read_all(settings.kafka_bootstrap_servers, empty_topic, marked, timeout=60)
            if m.key in wanted
        ]
        assert len(msgs) == 400
        by_partition: dict[int, list[int]] = defaultdict(list)
        for m in msgs:
            by_partition[m.partition].append(m.value["seq"])
        for seqs in by_partition.values():
            assert seqs == sorted(seqs)  # orden de seq dentro de cada partición
        per_incident: dict[str, list[str]] = defaultdict(list)
        for m in sorted(msgs, key=lambda m: m.value["seq"]):
            per_incident[m.key].append(m.event_type)
        assert all(v == ["ShipmentDelayed", "PlanCreated"] for v in per_incident.values())
    finally:
        for fut in admin.delete_topics([empty_topic]).values():
            with contextlib.suppress(Exception):  # limpieza best effort
                fut.result(timeout=30)
