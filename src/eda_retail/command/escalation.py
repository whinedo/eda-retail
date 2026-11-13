"""Escalado de una incidencia desde la infraestructura (dead letter queue)."""

from __future__ import annotations

import uuid

import psycopg

from eda_retail.command.handler import CommandHandler
from eda_retail.command.models import EscalateIncident
from eda_retail.infra.consumer import DeadLetterHook
from eda_retail.infra.dead_letters import INVALID_SCHEMA


def escalation_hook(handler: CommandHandler, *, producer: str, reason: str = INVALID_SCHEMA) -> DeadLetterHook:
    """Escala la incidencia si existe y está abierta (spec resilience, «Evento con esquema
    inválido»); si no, el comando se rechaza (`incident_closed`, `incident_not_found`) y
    solo queda la dead letter."""

    def escalate(conn: psycopg.Connection, correlation_id: str, event_id: uuid.UUID) -> None:
        handler.handle(
            conn,
            EscalateIncident(
                command_id=event_id, expected_version=handler.current_version(conn, correlation_id) or 0,
                producer=producer, incident_id=correlation_id, reason=reason,
            ),
        )

    return escalate
