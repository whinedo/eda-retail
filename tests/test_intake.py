"""Command handler de `intake` (spec cqrs-views, "Aviso registrado" y "Aviso repetido")."""

from __future__ import annotations

import uuid

from eda_retail.events.ids import incident_id_for_notice
from eda_retail.infra.commands import Accepted, CommandEnvelope, Rejected
from eda_retail.intake import handler


class _Result:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class _Conn:
    def __init__(self, rowcount: int = 1) -> None:
        self.rowcount = rowcount
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))
        return _Result(self.rowcount)


def _command(notice: str, correlation_id: str | None = None) -> CommandEnvelope:
    return CommandEnvelope(
        command_id=uuid.uuid4(), command_type="RegisterCarrierNotice",
        correlation_id=correlation_id or incident_id_for_notice(notice), causation_id=None,
        payload={"carrier_notice_id": notice, "carrier": "demo-express", "raw_text": "Retraso de 24 horas en P-1001"},
    )


def _handle(conn, command):
    spec = handler.specs()["RegisterCarrierNotice"]
    return spec.handle(conn, command, spec.payload_model.model_validate(command.payload), None)


def test_intake_only_attends_register_carrier_notice():
    assert set(handler.specs()) == {"RegisterCarrierNotice"}


def test_registered_notice_is_written_and_emits_carrier_notice_received():
    """Aviso registrado: el aviso va a intake.carrier_notices y CarrierNoticeReceived lleva causation_id C1"""
    conn = _Conn(rowcount=1)
    command = _command("N-1")

    outcome = _handle(conn, command)

    assert isinstance(outcome, Accepted)
    [event] = outcome.events
    assert event.event_type == "CarrierNoticeReceived"
    assert event.causation_id == command.command_id
    assert event.correlation_id == incident_id_for_notice("N-1")
    assert event.producer == "intake"
    assert event.payload == {"carrier_notice_id": "N-1", "carrier": "demo-express",
                             "raw_text": "Retraso de 24 horas en P-1001"}
    assert outcome.body == {"carrier_notice_id": "N-1", "incident_id": incident_id_for_notice("N-1")}
    [(sql, params)] = conn.statements
    assert sql.startswith("INSERT INTO intake.carrier_notices")
    assert params[:2] == ("N-1", incident_id_for_notice("N-1"))


def test_repeated_notice_is_rejected_with_duplicate_notice():
    """Aviso repetido: otro command_id para un aviso ya registrado se rechaza con duplicate_notice"""
    outcome = _handle(_Conn(rowcount=0), _command("N-1"))
    assert outcome == Rejected("duplicate_notice")


def test_command_must_be_correlated_with_the_incident_of_the_notice():
    conn = _Conn()
    outcome = _handle(conn, _command("N-1", correlation_id="I-other"))
    assert outcome == Rejected("correlation_mismatch")
    assert conn.statements == []
