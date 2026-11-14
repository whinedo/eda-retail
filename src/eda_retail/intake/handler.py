"""Command handler de `intake`: la entrada de avisos del transportista (design.md §3).

Atiende `RegisterCarrierNotice` (de `eda-notice` y del front): guarda el aviso en
`intake.carrier_notices` y publica `CarrierNoticeReceived` desde su outbox. El texto del aviso es
contenido no confiable: se guarda en el evento tal cual y nadie lo ejecuta.
"""

from __future__ import annotations

from typing import Any

import psycopg
from pydantic import BaseModel, ConfigDict, Field

from eda_retail.events.catalog import build_event
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandSpec, Rejected

SCHEMA = "intake"


class RegisterCarrierNotice(BaseModel):
    model_config = ConfigDict(extra="forbid")

    carrier_notice_id: str = Field(min_length=1)
    carrier: str = Field(min_length=1)
    raw_text: str = Field(min_length=1)


def register_carrier_notice(
    conn: psycopg.Connection, command: CommandEnvelope, payload: RegisterCarrierNotice, prepared: Any
) -> Accepted | Rejected:
    incident_id = incident_id_for_notice(payload.carrier_notice_id)
    if command.correlation_id != incident_id:
        return Rejected("correlation_mismatch")
    inserted = conn.execute(
        "INSERT INTO intake.carrier_notices (carrier_notice_id, incident_id, carrier) VALUES (%s, %s, %s)"
        " ON CONFLICT DO NOTHING",
        (payload.carrier_notice_id, incident_id, payload.carrier),
    ).rowcount
    if inserted == 0:
        return Rejected("duplicate_notice")
    event = build_event(
        "CarrierNoticeReceived",
        payload.model_dump(),
        correlation_id=incident_id,
        causation_id=command.command_id,
        producer=SCHEMA,
    )
    return Accepted([event], {"carrier_notice_id": payload.carrier_notice_id, "incident_id": incident_id})


def specs() -> dict[str, CommandSpec]:
    return {"RegisterCarrierNotice": CommandSpec(RegisterCarrierNotice, register_carrier_notice)}
