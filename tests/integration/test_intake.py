"""`intake` contra PostgreSQL real (spec cqrs-views, "Aviso registrado" y "Aviso repetido")."""

from __future__ import annotations

import uuid

from eda_retail.events.ids import incident_id_for_notice
from eda_retail.intake.handler import specs

from .component_helpers import body, client, outbox


def _register(api, notice: str, command_id: uuid.UUID | None = None):
    payload = {"carrier_notice_id": notice, "carrier": "demo-express", "raw_text": "Retraso de 24 horas en P-1001"}
    return api.post("/commands/RegisterCarrierNotice",
                    json=body(incident_id_for_notice(notice), payload, command_id))


def test_registered_notice(database_url, conn):
    """Aviso registrado: intake.carrier_notices y CarrierNoticeReceived con causation_id C1 en intake.outbox"""
    notice = f"N-{uuid.uuid4().hex[:8]}"
    command_id = uuid.uuid4()
    with client(database_url, "intake", specs()) as api:
        response = _register(api, notice, command_id)

    assert response.status_code == 202
    assert response.json()["incident_id"] == incident_id_for_notice(notice)
    with conn.transaction():
        row = conn.execute("SELECT incident_id, carrier FROM intake.carrier_notices WHERE carrier_notice_id = %s",
                           (notice,)).fetchone()
    assert row == {"incident_id": incident_id_for_notice(notice), "carrier": "demo-express"}
    [event] = outbox(conn, "intake", incident_id_for_notice(notice))
    assert event["event_type"] == "CarrierNoticeReceived"
    assert event["causation_id"] == command_id
    assert event["payload"]["carrier_notice_id"] == notice


def test_repeated_notice(database_url, conn):
    """Aviso repetido: otro command_id para N-1 se rechaza con duplicate_notice y sin otro evento"""
    notice = f"N-{uuid.uuid4().hex[:8]}"
    with client(database_url, "intake", specs()) as api:
        assert _register(api, notice).status_code == 202
        second = _register(api, notice)

    assert second.status_code == 422
    assert second.json()["reason"] == "duplicate_notice"
    assert len(outbox(conn, "intake", incident_id_for_notice(notice))) == 1
