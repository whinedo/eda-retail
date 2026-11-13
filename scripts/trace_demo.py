"""Demo de trazas en Jaeger: aviso → intérprete → planificador, cada uno en su proceso.

    uv run python scripts/trace_demo.py

Usa una base `eda_demo` y un topic `eda.trace-demo` propios para no ensuciar el log de
desarrollo (`events` es solo de añadir). Al terminar imprime la URL de la traza en Jaeger.
Los handlers son simulados: solo escriben el evento siguiente, sin LLM ni efectos.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid
from dataclasses import replace

import psycopg
from confluent_kafka.admin import AdminClient, NewTopic

from eda_retail.config import Settings, load_settings
from eda_retail.events.envelope import EventEnvelope, new_event
from eda_retail.infra.consumer import EventConsumer, Outcome
from eda_retail.infra.db import connect
from eda_retail.infra.event_store import append_events
from eda_retail.infra.migrations import upgrade
from eda_retail.infra.relay import OutboxRelay
from eda_retail.infra.tracing import setup_tracing, traced

DEMO_DB = "eda_demo"
DEMO_TOPIC = "eda.trace-demo"
JAEGER_UI = "http://localhost:16686"


def demo_settings() -> Settings:
    s = load_settings()
    base, _, _ = s.database_url.rpartition("/")
    return replace(s, database_url=f"{base}/{DEMO_DB}", events_topic=DEMO_TOPIC)


def prepare(settings: Settings) -> None:
    base, _, _ = settings.database_url.rpartition("/")
    with psycopg.connect(f"{base}/postgres", autocommit=True) as admin:
        if admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DEMO_DB,)).fetchone() is None:
            admin.execute(f"CREATE DATABASE {DEMO_DB}")
    upgrade(settings.database_url)
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    if DEMO_TOPIC not in admin.list_topics(timeout=10).topics:
        for fut in admin.create_topics([NewTopic(DEMO_TOPIC, num_partitions=3, replication_factor=1)]).values():
            fut.result(timeout=30)


def run_component(component: str, arg: str) -> None:
    settings = demo_settings()
    provider = setup_tracing(component, settings)
    try:
        if component == "command-handler":
            register_notice(settings, arg)
        elif component == "relay":
            relay = OutboxRelay.from_settings(settings)
            try:
                while relay.run_once() > 0:
                    pass
            finally:
                relay.close()
        else:
            consume_one(settings, component, arg)
    finally:
        if provider is not None:
            provider.shutdown()  # vacía el BatchSpanProcessor antes de salir


@traced("RegisterCarrierNotice")
def register_notice(settings: Settings, incident: str) -> None:
    conn = connect(settings.database_url)
    try:
        with conn.transaction():
            append_events(
                conn,
                [
                    new_event(
                        event_type="CarrierNoticeReceived",
                        correlation_id=incident,
                        causation_id=None,
                        producer="command-handler",
                        payload={"carrier_notice_id": f"N-{incident}", "carrier": "demo", "raw_text": "retraso"},
                    )
                ],
            )
    finally:
        conn.close()


NEXT = {"interpreter": ("CarrierNoticeReceived", "ShipmentDelayed"), "planner": ("ShipmentDelayed", "PlanCreated")}


def consume_one(settings: Settings, component: str, incident: str) -> None:
    consumes, produces = NEXT[component]

    def handler(conn: psycopg.Connection, event: EventEnvelope) -> None:
        time.sleep(0.05)  # simula el trabajo del agente
        append_events(
            conn,
            [
                new_event(
                    event_type=produces,
                    correlation_id=event.correlation_id,
                    causation_id=event.event_id,
                    producer=component,
                    payload={"incident_id": event.correlation_id},
                )
            ],
        )

    consumer = EventConsumer.from_settings(settings, name=f"demo-{component}", handlers={consumes: handler})
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            outcome = consumer.handle_message(msg)
            consumer.commit(msg)
            if outcome is Outcome.PROCESSED and json.loads(msg.value())["correlation_id"] == incident:
                return
    finally:
        consumer.close()
    raise SystemExit(f"{component}: no llegó {consumes} de {incident}")


def find_trace(incident: str) -> str | None:
    tags = urllib.parse.quote(json.dumps({"eda.incident_id": incident}))
    url = f"{JAEGER_UI}/api/traces?service=command-handler&tags={tags}&lookback=1h"
    for _ in range(20):
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.load(resp)["data"]
        if data:
            return data[0]["traceID"]
        time.sleep(1)
    return None


def main() -> None:
    if len(sys.argv) == 3:
        run_component(sys.argv[1], sys.argv[2])
        return
    settings = demo_settings()
    prepare(settings)
    incident = f"I-{uuid.uuid4().hex[:8]}"
    steps = ["command-handler", "relay", "interpreter", "relay", "planner", "relay"]
    for component in steps:
        subprocess.run([sys.executable, __file__, component, incident], check=True)
    trace_id = find_trace(incident)
    if trace_id is None:
        raise SystemExit("Jaeger no devolvió la traza: ¿está arrancado (docker compose up -d jaeger)?")
    print(f"incidencia {incident}: {JAEGER_UI}/trace/{trace_id}")


if __name__ == "__main__":
    main()
