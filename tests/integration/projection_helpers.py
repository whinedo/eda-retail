"""Alimentar los proyectores por Kafka en los tests de integración.

`publish` publica en el topic del test eventos del catálogo (`History` construye los de una
incidencia), como hacen los relays de los componentes. `catch_up` pone al día un proyector
consumiendo ese topic con su consumidor posicionado.
"""

from __future__ import annotations

import uuid
from collections import defaultdict

from eda_retail.config import Settings
from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.effect_keys import compute_idempotency_key
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.outbox import encode_event
from eda_retail.infra.positions import Position
from eda_retail.projectors.base import consumer_for
from eda_retail.projectors.registry import PROJECTORS

_published: dict[str, set[uuid.UUID]] = defaultdict(set)


def publish(settings: Settings, events: list[EventEnvelope]) -> list[Position]:
    """Publica los eventos (clave `correlation_id`) y devuelve la posición de cada uno, en orden."""
    producer = make_producer(settings.kafka_bootstrap_servers)
    positions: list[Position | None] = [None] * len(events)

    def on_delivery_for(i: int):
        def on_delivery(err, msg) -> None:
            assert err is None, err
            positions[i] = Position(msg.topic(), msg.partition(), msg.offset())
        return on_delivery

    for i, event in enumerate(events):
        producer.produce(settings.events_topic, key=event.correlation_id.encode(), value=encode_event(event),
                         headers=[("event_type", event.event_type.encode())], on_delivery=on_delivery_for(i))
    assert producer.flush(30) == 0
    _published[settings.events_topic].update(e.event_id for e in events)
    return [p for p in positions if p is not None]


def new_incident() -> str:
    return f"I-{uuid.uuid4().hex[:8]}"


def plan_step(step_id: str, action: str, depends_on=(), *, order_id: str = "P1") -> dict:
    return {"step_id": step_id, "action": action, "params": {"order_id": order_id}, "depends_on": list(depends_on)}


LOSS_PLAN = [
    plan_step("S1", "notify_customer"),
    plan_step("S2", "file_claim"),
    plan_step("S3", "reserve_stock"),
    plan_step("S4", "reissue_shipment", ["S3"]),
]


class History:
    """Eventos del catálogo de una incidencia, como los publicarían sus productores.

    Cada evento lleva `correlation_id` = la incidencia y `causation_id` = el anterior, y su
    `producer` es el componente que lo publica en el modelo con dueños (intérprete, planificador,
    orquestador, ejecutores, inventory).
    """

    def __init__(self, incident: str | None = None) -> None:
        self.incident = incident or new_incident()
        self.events: list[EventEnvelope] = []  # los que aún no se publicaron
        self._last: EventEnvelope | None = None

    def _add(self, event_type: str, payload: dict, producer: str) -> EventEnvelope:
        cause = self._last.event_id if self._last else None
        event = build_event(event_type, payload, correlation_id=self.incident, causation_id=cause,
                            producer=producer)
        self.events.append(event)
        self._last = event
        return event

    def take(self) -> list[EventEnvelope]:
        """Los eventos pendientes de publicar; la historia sigue encadenando causas."""
        events, self.events = self.events, []
        return events

    def delayed(self, order_ids=("P1",), cause: str = "loss", severity: str = "high") -> EventEnvelope:
        return self._add("ShipmentDelayed", {"incident_id": self.incident, "order_ids": list(order_ids),
                                             "cause": cause, "estimated_delay_hours": 24, "severity": severity},
                         "interpreter")

    def plan_created(self, steps: list[dict]) -> EventEnvelope:
        return self._add("PlanCreated", {"incident_id": self.incident, "plan_version": 1, "steps": steps},
                         "planner")

    def plan_revised(self, steps: list[dict], plan_version: int = 2) -> EventEnvelope:
        return self._add("PlanRevised", {"incident_id": self.incident, "plan_version": plan_version,
                                         "reason": "revisión", "steps": steps}, "replanner")

    def requested(self, step: dict, plan_version: int = 1, origin_plan_version: int = 1) -> EventEnvelope:
        key = compute_idempotency_key(self.incident, step["step_id"], origin_plan_version=origin_plan_version)
        return self._add("StepRequested", {"incident_id": self.incident, "plan_version": plan_version,
                                           "step_id": step["step_id"], "action": step["action"],
                                           "params": step["params"], "idempotency_key": key}, "orchestrator")

    def completed(self, step_id: str, result: dict, plan_version: int = 1,
                  producer: str = "executor-notify_customer") -> EventEnvelope:
        return self._add("StepCompleted", {"step_id": step_id, "plan_version": plan_version, "result": result},
                         producer)

    def failed(self, step_id: str, error: str, plan_version: int = 1, retryable: bool = False,
               producer: str = "inventory") -> EventEnvelope:
        return self._add("StepFailed", {"step_id": step_id, "plan_version": plan_version, "error": error,
                                        "retryable": retryable, "attempts": 1}, producer)

    def customer_notified(self, order_id: str = "P1", message_id: str = "M1") -> EventEnvelope:
        return self._add("CustomerNotified", {"order_id": order_id, "message_id": message_id},
                         "executor-notify_customer")

    def escalated(self, reason: str, producer: str = "orchestrator") -> EventEnvelope:
        return self._add("IncidentEscalated", {"incident_id": self.incident, "reason": reason}, producer)

    def resolved(self, outcome: str = "resolved") -> EventEnvelope:
        return self._add("IncidentResolved", {"incident_id": self.incident, "outcome": outcome}, "orchestrator")




def catch_up(settings: Settings, conn, view: str) -> int:
    """Consume el topic del test hasta el final con el proyector de `view`."""
    consumer = consumer_for(PROJECTORS[view].build(), settings, conn)
    try:
        return consumer.catch_up()
    finally:
        consumer.close()
