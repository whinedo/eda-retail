"""Consumidor del intérprete: avisos, reanudación de avisos aparcados y sus plazos (design.md §4.3).

- `CarrierNoticeReceived`: ejecuta su `OpenIncident` con `execute_command`. El `command_id` es el
  `event_id` del aviso, así que el inbox de comandos también descarta el evento reentregado.
- `OrderRegistered`: reanuda los avisos aparcados que nombran ese pedido, con los datos del evento.
- `expire_parked`: al vencer el plazo de un aviso aparcado consulta `view_orders` una sola vez (el
  `OrderRegistered` pudo consumirse antes de aparcar el aviso) y publica `ShipmentDelayed` con los
  pedidos que ya están o `IncidentEscalated` con `no_valid_orders`.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import psycopg
from confluent_kafka import Consumer

from eda_retail.config import Settings
from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.infra import outbox
from eda_retail.infra.commands import CommandEnvelope, CommandSpec, execute_command
from eda_retail.infra.consumer import EventConsumer, Outcome
from eda_retail.infra.dead_letters import INVALID_SCHEMA
from eda_retail.interpreter.handler import (
    NO_VALID_ORDERS,
    SCHEMA,
    NoticePreparer,
    OpenIncident,
    open_incident,
    shipment_delayed,
)
from eda_retail.llm.generation import LlmClient
from eda_retail.queries.cost_per_incident import Prices, budget_from_cost, get_incident_cost
from eda_retail.queries.orders import get_orders
from eda_retail.rules import BusinessRules

_PARKED = """
    SELECT p.incident_id, p.order_ids, i.command_id, i.cause, i.estimated_delay_hours, i.severity
    FROM interpreter.pending_notices p JOIN interpreter.interpretations i USING (incident_id)
"""

# Eventos que consume el intérprete de Kafka (también los usa el front para el destino de cada evento).
EVENT_TYPES = ("CarrierNoticeReceived", "OrderRegistered")


def _known_orders(conn: psycopg.Connection, order_ids: list[str]) -> list[str]:
    found = get_orders(conn, order_ids).found
    return [o for o in order_ids if o in found]


def _interpreted(conn: psycopg.Connection, carrier_notice_id: str) -> bool:
    with conn.transaction():
        return conn.execute(
            "SELECT 1 FROM interpreter.interpretations WHERE carrier_notice_id = %s", (carrier_notice_id,)
        ).fetchone() is not None


class InterpretNotice:
    """Unidad de trabajo de un `CarrierNoticeReceived`: el LLM va antes de la transacción.

    `after_prepare` (el arnés, E8: caída `before_tx`) se llama tras `prepare` y antes de abrir la transacción."""

    def __init__(self, settings: Settings, llm: LlmClient, rules: BusinessRules, *,
                 after_prepare: Callable[[], None] | None = None) -> None:
        self._settings = settings
        self._llm = llm
        self._rules = rules
        self._prices = Prices.from_settings(settings)
        self._after_prepare = after_prepare

    def process(self, conn: psycopg.Connection, event: EventEnvelope) -> Outcome:
        incident_id = incident_id_for_notice(event.payload["carrier_notice_id"])
        command = CommandEnvelope(command_id=event.event_id, command_type="OpenIncident",
                                  correlation_id=incident_id, causation_id=event.event_id, payload=event.payload)
        prepare = NoticePreparer(
            self._llm, self._rules,
            interpreted=lambda notice: _interpreted(conn, notice),
            budget=lambda incident: budget_from_cost(get_incident_cost(conn, incident, self._prices), self._rules),
            known_orders=lambda order_ids: _known_orders(conn, order_ids),
        )
        if self._after_prepare is not None:
            prepare = _then(prepare, self._after_prepare)
        spec = CommandSpec(OpenIncident, open_incident(self._settings), prepare=prepare)
        response = execute_command(conn, SCHEMA, command, spec)
        return Outcome.DUPLICATE if response.replayed else Outcome.PROCESSED


def _then(prepare: Callable[[CommandEnvelope, Any], Any], after: Callable[[], None]) -> Callable[..., Any]:
    def prepared(command: CommandEnvelope, payload: Any) -> Any:
        result = prepare(command, payload)
        after()
        return result

    return prepared


def _resolve(conn: psycopg.Connection, incident_id: str, status: str, order_ids: list[str],
             reason: str | None = None) -> None:
    conn.execute("DELETE FROM interpreter.pending_notices WHERE incident_id = %s", (incident_id,))
    conn.execute(
        "UPDATE interpreter.interpretations SET status = %s, order_ids = %s, reason = %s, resolved_at = now()"
        " WHERE incident_id = %s",
        (status, order_ids, reason, incident_id),
    )


def _emit(event_type: str, payload: dict[str, Any], incident_id: str, cause: uuid.UUID) -> EventEnvelope:
    return build_event(event_type, payload, correlation_id=incident_id, causation_id=cause, producer=SCHEMA)


def resume_on_order(conn: psycopg.Connection, event: EventEnvelope) -> None:
    """Reanuda los avisos aparcados por el pedido de este `OrderRegistered` (en la transacción del inbox)."""
    order_id = event.payload["order_id"]
    rows = conn.execute(f"{_PARKED} WHERE %s = ANY(p.order_ids) FOR UPDATE OF p", (order_id,)).fetchall()
    events = []
    for row in rows:
        incident_id = row["incident_id"]
        events.append(_emit("ShipmentDelayed", shipment_delayed(incident_id, [order_id], row), incident_id,
                            event.event_id))
        _resolve(conn, incident_id, "interpreted", [order_id])
    outbox.append(conn, SCHEMA, events)


def expire_parked(conn: psycopg.Connection) -> int:
    """Resuelve los avisos aparcados cuyo plazo ha vencido. Devuelve cuántos ha resuelto."""
    with conn.transaction():
        due = conn.execute(
            "SELECT incident_id, order_ids FROM interpreter.pending_notices WHERE deadline_at <= now()"
        ).fetchall()
    resolved = 0
    for item in due:
        known = _known_orders(conn, item["order_ids"])  # una sola consulta, fuera de la transacción
        with conn.transaction():
            row = conn.execute(f"{_PARKED} WHERE p.incident_id = %s FOR UPDATE OF p SKIP LOCKED",
                               (item["incident_id"],)).fetchone()
            if row is None:  # ya lo reanudó un OrderRegistered
                continue
            incident_id = row["incident_id"]
            if known:
                emitted = _emit("ShipmentDelayed", shipment_delayed(incident_id, known, row), incident_id,
                                row["command_id"])
                _resolve(conn, incident_id, "interpreted", known)
            else:
                emitted = _emit("IncidentEscalated", {"incident_id": incident_id, "reason": NO_VALID_ORDERS},
                                incident_id, row["command_id"])
                _resolve(conn, incident_id, "escalated", row["order_ids"], NO_VALID_ORDERS)
            outbox.append(conn, SCHEMA, [emitted])
        resolved += 1
    return resolved


def escalate_dead_letter(conn: psycopg.Connection, correlation_id: str, event_id: uuid.UUID) -> None:
    """Un aviso que no cumple el esquema escala su incidencia (spec resilience, "Evento con esquema inválido")."""
    if not correlation_id.startswith("I-"):  # p. ej. un OrderRegistered: no es de ninguna incidencia
        return
    outbox.append(conn, SCHEMA, [_emit("IncidentEscalated", {"incident_id": correlation_id,
                                                             "reason": INVALID_SCHEMA}, correlation_id, event_id)])


def build_consumer(conn: psycopg.Connection, kafka: Consumer, settings: Settings, llm: LlmClient,
                   rules: BusinessRules, *, after_prepare: Callable[[], None] | None = None) -> EventConsumer:
    notice = InterpretNotice(settings, llm, rules, after_prepare=after_prepare)
    return EventConsumer(
        SCHEMA, conn, kafka, settings.events_topic,
        dict(zip(EVENT_TYPES, (notice, resume_on_order), strict=True)),
        inbox_enabled=settings.inbox_enabled, on_dead_letter=escalate_dead_letter, schema=SCHEMA,
    )
