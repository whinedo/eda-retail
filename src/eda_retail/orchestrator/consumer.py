"""Consumidor del orquestador (grupo e inbox `orchestrator`): cada evento es una transición de la saga
en la transacción de su inbox (design.md §4.2)."""

from __future__ import annotations

import uuid

import psycopg
from confluent_kafka import Consumer

from eda_retail.command.rules import BusinessRules
from eda_retail.config import Settings
from eda_retail.infra.consumer import EventConsumer
from eda_retail.infra.dead_letters import INVALID_SCHEMA
from eda_retail.orchestrator.saga import SCHEMA, Saga, lock

EVENT_TYPES = ("ShipmentDelayed", "PlanCreated", "PlanRevised", "StepCompleted", "StepFailed", "IncidentEscalated")


def dead_letter_hook(settings: Settings, rules: BusinessRules):
    """Un evento de una incidencia que no cumple el esquema la escala (spec resilience)."""

    def escalate(conn: psycopg.Connection, correlation_id: str, event_id: uuid.UUID) -> None:
        t = lock(conn, settings, rules, correlation_id, event_id)
        if t is None:
            return
        t.escalate(INVALID_SCHEMA, publish=True)
        t.flush()

    return escalate


def build_consumer(conn: psycopg.Connection, kafka: Consumer, settings: Settings,
                   rules: BusinessRules) -> EventConsumer:
    saga = Saga(settings, rules)
    return EventConsumer(
        SCHEMA, conn, kafka, settings.events_topic, saga.handlers(),
        inbox_enabled=settings.inbox_enabled, on_dead_letter=dead_letter_hook(settings, rules), schema=SCHEMA,
    )
