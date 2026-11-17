"""Mensajes veneno inyectados (spec synthetic-data, «Mensajes veneno inyectados»; experimento E18).

Se publican directamente en el topic de eventos, sin pasar por ningún outbox, en particiones con
carga. Cada veneno de un tipo legible es un `ShipmentDelayed` (lo atienden el orquestador,
`event_log` y los proyectores de incidencias) con el `correlation_id` de una incidencia: si llega a
la dead letter de un consumidor con escalado, esa incidencia se escala. El JSON inválido no lleva
`event_type` ni `correlation_id` legibles, así que lo atienden todos y no escala nada.

`unknown_type` no es un veneno para nadie: un `event_type` fuera del catálogo con la estructura
común válida se ignora sin dead letter (tolerant reader) y `event_log` lo guarda.

Todo es determinista con la semilla: el `event_id` de cada veneno es un UUID derivado de la semilla
y del tipo de veneno, y la partición de cada uno se reparte en orden.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from eda_retail.events.catalog import CATALOG

INVALID_JSON = "invalid_json"
MISSING_FIELD = "missing_field"
UNKNOWN_TYPE = "unknown_type"
UNKNOWN_VERSION = "unknown_version"
INVALID_PAYLOAD = "invalid_payload"
PERSONAL_DATA = "personal_data"
LARGE_PAYLOAD = "large_payload"
KINDS = (INVALID_JSON, MISSING_FIELD, UNKNOWN_TYPE, UNKNOWN_VERSION, INVALID_PAYLOAD, PERSONAL_DATA, LARGE_PAYLOAD)

POISON_TYPE = "ShipmentDelayed"
UNKNOWN_EVENT_TYPE = "CarrierRenamed"  # no está en el catálogo
LARGE_PAYLOAD_BYTES = 900_000  # por debajo del `message.max.bytes` por defecto (1 MB)
_NAMESPACE = uuid.UUID("6f1d0c1e-2b7a-4d2e-9a51-e18e18e18e18")


@dataclass(frozen=True)
class Poison:
    kind: str
    value: bytes
    key: str
    event_id: uuid.UUID | None
    correlation_id: str | None
    event_type: str | None  # el tipo que se puede leer del mensaje; None: lo atienden todos


@dataclass(frozen=True)
class InjectedPoison:
    poison: Poison
    partition: int
    offset: int


def _body(kind: str, event_id: uuid.UUID, incident_id: str) -> dict[str, Any]:
    return {
        "event_id": str(event_id), "event_type": POISON_TYPE, "event_version": CATALOG[POISON_TYPE].version,
        "occurred_at": datetime(2026, 11, 17, tzinfo=UTC).isoformat(), "correlation_id": incident_id,
        "causation_id": None, "producer": "poison",
        "payload": {"incident_id": incident_id, "order_ids": ["P-POISON"], "cause": "delay",
                    "estimated_delay_hours": 24, "severity": "low"},
    }


def _mutate(kind: str, body: dict[str, Any]) -> None:
    if kind == MISSING_FIELD:
        del body["producer"]  # campo obligatorio de la estructura común
    elif kind == UNKNOWN_TYPE:
        body["event_type"] = UNKNOWN_EVENT_TYPE
    elif kind == UNKNOWN_VERSION:
        body["event_version"] = CATALOG[POISON_TYPE].version + 1  # sin upcaster
    elif kind == INVALID_PAYLOAD:
        body["payload"]["estimated_delay_hours"] = -5
    elif kind == PERSONAL_DATA:
        body["payload"]["order_ids"] = ["ana.garcia@example.com"]
    elif kind == LARGE_PAYLOAD:
        body["payload"]["notes"] = "x" * LARGE_PAYLOAD_BYTES  # campo fuera del schema: se rechaza
    else:
        raise ValueError(f"tipo de veneno desconocido: {kind!r}")


def poison_message(kind: str, *, seed: int, incident_id: str) -> Poison:
    """El mensaje veneno `kind`; los de tipo legible llevan el `correlation_id` `incident_id`."""
    if kind == INVALID_JSON:
        return Poison(kind, f"{{veneno {seed}: no es JSON".encode(), f"poison-{seed}", None, None, None)
    event_id = uuid.uuid5(_NAMESPACE, f"{seed}:{kind}")
    body = _body(kind, event_id, incident_id)
    _mutate(kind, body)
    return Poison(kind, json.dumps(body).encode(), incident_id, event_id, incident_id, body["event_type"])


def poison_set(seed: int, incident_ids: Sequence[str]) -> list[Poison]:
    """Un veneno de cada tipo; los de tipo legible, cada uno con una incidencia de `incident_ids` en orden."""
    targets = iter(incident_ids)
    poisons = []
    for kind in KINDS:
        incident = "" if kind == INVALID_JSON else next(targets)
        poisons.append(poison_message(kind, seed=seed, incident_id=incident))
    return poisons


def raw_dead_letter(poison: Poison) -> Any:
    """Lo que guarda una dead letter del veneno en `raw_message` (para reconocerlo sin `event_id`)."""
    return {"raw": poison.value.decode()} if poison.kind == INVALID_JSON else json.loads(poison.value)



def inject(producer: Any, topic: str, poisons: Sequence[Poison], partitions: Sequence[int],
           timeout: float = 30.0) -> list[InjectedPoison]:
    """Publica cada veneno en una partición de `partitions` (en orden, repartidos) y anota su offset."""
    positions: dict[int, tuple[int, int]] = {}
    errors: list[Any] = []

    def on_delivery_for(i: int):
        def on_delivery(err, msg) -> None:
            if err is not None:
                errors.append(err)
            else:
                positions[i] = (msg.partition(), msg.offset())
        return on_delivery

    for i, poison in enumerate(poisons):
        producer.produce(topic, key=poison.key.encode(), value=poison.value,
                         partition=partitions[i % len(partitions)], on_delivery=on_delivery_for(i))
    pending = producer.flush(timeout)
    if pending or errors or len(positions) != len(poisons):
        raise RuntimeError(f"venenos sin confirmar: pendientes={pending}, errores={errors}")
    return [InjectedPoison(p, *positions[i]) for i, p in enumerate(poisons)]
