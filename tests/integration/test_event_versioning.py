"""event-backbone: versionado de eventos con conversores al leer."""

from __future__ import annotations

import uuid

from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.events.versioning import Upcasters
from eda_retail.infra.event_store import append_events


def test_event_of_previous_version(conn):
    """Evento de versión anterior"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    v1 = build_event(
        "ShipmentDelayed",
        {"incident_id": incident, "order_ids": ["P1"], "cause": "delay", "estimated_delay_hours": 24, "severity": "low"},
        correlation_id=incident, causation_id=None, producer="interpreter",
    )
    with conn.transaction():
        [seq] = append_events(conn, [v1])

    # Un proyector que espera la versión 2 (hipotética: el retraso en minutos).
    projector_upcasters = Upcasters()

    @projector_upcasters.register("ShipmentDelayed", 1)
    def _v1_to_v2(payload: dict) -> dict:
        p = dict(payload)
        p["estimated_delay_minutes"] = p.pop("estimated_delay_hours") * 60
        return p

    row = conn.execute(
        "SELECT event_id, event_type, event_version, occurred_at, correlation_id, causation_id, producer, payload "
        "FROM events WHERE seq = %s", (seq,)
    ).fetchone()
    read = projector_upcasters.upcast(EventEnvelope.model_validate(row), 2)
    assert read.event_version == 2
    assert read.payload["estimated_delay_minutes"] == 1440

    after = conn.execute("SELECT event_version, payload FROM events WHERE seq = %s", (seq,)).fetchone()
    assert after["event_version"] == 1
    assert after["payload"]["estimated_delay_hours"] == 24
