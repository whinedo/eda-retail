"""Catálogo de eventos (docs/README.md): un esquema de payload por tipo y versión.

`build_event` es la única forma de crear un evento del dominio: valida el payload con su
esquema, comprueba que no lleva datos personales y rellena la estructura común.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from eda_retail.events.envelope import EventEnvelope, new_event


class Cause(StrEnum):
    DELAY = "delay"
    LOSS = "loss"
    DAMAGE = "damage"
    WRONG_ADDRESS = "wrong_address"
    STRIKE = "strike"
    UNKNOWN = "unknown"


class Severity(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

    @property
    def rank(self) -> int:
        return list(Severity).index(self)


class Action(StrEnum):
    NOTIFY_CUSTOMER = "notify_customer"
    FILE_CLAIM = "file_claim"
    RESERVE_STOCK = "reserve_stock"
    REISSUE_SHIPMENT = "reissue_shipment"
    RELEASE_STOCK = "release_stock"
    ISSUE_REFUND = "issue_refund"


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PlanStep(_Payload):
    step_id: str = Field(min_length=1)
    # str y no Action: el command handler distingue acción no permitida de compensación.
    action: str = Field(min_length=1)
    params: dict[str, Any]
    depends_on: list[str]


class CarrierNoticeReceived(_Payload):
    carrier_notice_id: str = Field(min_length=1)
    carrier: str = Field(min_length=1)
    raw_text: str


class ShipmentDelayed(_Payload):
    incident_id: str
    order_ids: list[str] = Field(min_length=1)
    cause: Cause
    estimated_delay_hours: int = Field(ge=0)
    severity: Severity


class PlanCreated(_Payload):
    incident_id: str
    plan_version: int = Field(ge=1)
    steps: list[PlanStep] = Field(min_length=1)


class PlanRevised(PlanCreated):
    reason: str


class StepRequested(_Payload):
    incident_id: str
    plan_version: int = Field(ge=1)
    step_id: str
    action: Action
    params: dict[str, Any]
    idempotency_key: str


class StepCompleted(_Payload):
    """v2: `plan_version` del `StepRequested` que lo pidió (detecta resultados obsoletos)."""

    step_id: str
    plan_version: int = Field(ge=1)
    result: dict[str, Any]


class StepFailed(_Payload):
    """v2: `plan_version` del `StepRequested` y `attempts` que hizo el ejecutor."""

    step_id: str
    plan_version: int = Field(ge=1)
    error: str = Field(min_length=1)
    retryable: bool
    attempts: int = Field(ge=1)


class StepCancelled(_Payload):
    """Paso que una revisión del plan no mantiene y que no llegó a tener efecto."""

    incident_id: str
    plan_version: int = Field(ge=1)
    step_id: str


class CustomerNotified(_Payload):
    order_id: str
    message_id: str


class ClaimFiled(_Payload):
    order_id: str
    claim_id: str


class StockReserved(_Payload):
    order_id: str
    warehouse: str
    reservation_id: str


class StockReleased(_Payload):
    reservation_id: str


class ShipmentReissued(_Payload):
    order_id: str
    new_shipment_id: str


class RefundIssued(_Payload):
    order_id: str
    amount: Decimal = Field(gt=0)
    refund_id: str


class OrderRegistered(_Payload):
    """Pedido registrado por `inventory`; alimenta la proyección `view_orders` (spec cqrs-views)."""

    order_id: str
    customer_ref: str
    amount: Decimal = Field(gt=0)


class IncidentResolved(_Payload):
    incident_id: str
    outcome: Literal["resolved", "refunded"]


class IncidentEscalated(_Payload):
    incident_id: str
    reason: str = Field(min_length=1)


class LlmCallRecorded(_Payload):
    """Una llamada al LLM, aceptada o no (spec observability-audit). Sin el prompt ni la salida."""

    incident_id: str
    agent: str = Field(min_length=1)
    model: str = Field(min_length=1)
    tokens_in: int = Field(ge=0)
    tokens_out: int = Field(ge=0)
    latency_ms: int = Field(ge=0)
    validation_passed: bool
    attempt: int = Field(ge=1)


@dataclass(frozen=True)
class EventSpec:
    version: int
    model: type[_Payload]


_VERSIONS = {"StepCompleted": 2, "StepFailed": 2}

CATALOG: dict[str, EventSpec] = {
    m.__name__: EventSpec(_VERSIONS.get(m.__name__, 1), m)
    for m in (
        CarrierNoticeReceived, ShipmentDelayed, PlanCreated, PlanRevised, StepRequested,
        StepCompleted, StepFailed, StepCancelled, CustomerNotified, ClaimFiled, StockReserved,
        StockReleased, ShipmentReissued, RefundIssued, IncidentResolved, IncidentEscalated,
        LlmCallRecorded, OrderRegistered,
    )
}

# Efecto que publica cada acción junto a StepCompleted (spec step-execution).
EFFECT_EVENT: dict[Action, str] = {
    Action.NOTIFY_CUSTOMER: "CustomerNotified",
    Action.FILE_CLAIM: "ClaimFiled",
    Action.RESERVE_STOCK: "StockReserved",
    Action.RELEASE_STOCK: "StockReleased",
    Action.REISSUE_SHIPMENT: "ShipmentReissued",
    Action.ISSUE_REFUND: "RefundIssued",
}


class PayloadError(ValueError):
    """El payload no cumple el esquema del catálogo."""


class PersonalDataError(ValueError):
    """El payload contiene datos personales en claro (spec event-backbone)."""


_PERSONAL_KEYS = {"email", "name", "full_name", "first_name", "last_name", "address", "phone", "customer_name"}
_EMAIL = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
# raw_text es contenido del transportista que el catálogo guarda tal cual.
_FREE_TEXT_FIELDS = {("CarrierNoticeReceived", "raw_text")}


def _personal_data(value: Any, path: str) -> str | None:
    if isinstance(value, dict):
        for k, v in value.items():
            if str(k).lower() in _PERSONAL_KEYS:
                return f"{path}.{k}"
            if found := _personal_data(v, f"{path}.{k}"):
                return found
    elif isinstance(value, list):
        for i, v in enumerate(value):
            if found := _personal_data(v, f"{path}[{i}]"):
                return found
    elif isinstance(value, str) and _EMAIL.search(value):
        return path
    return None


def validate_payload(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Valida el payload y lo devuelve normalizado a JSON."""
    spec = CATALOG.get(event_type)
    if spec is None:
        raise PayloadError(f"tipo de evento fuera del catálogo: {event_type}")
    try:
        model = spec.model.model_validate(payload)
    except ValidationError as exc:
        fields = ", ".join(".".join(str(p) for p in e["loc"]) for e in exc.errors())
        raise PayloadError(f"{event_type}: campos ausentes o inválidos: {fields}") from exc
    data = model.model_dump(mode="json")
    checked = {k: v for k, v in data.items() if (event_type, k) not in _FREE_TEXT_FIELDS}
    if found := _personal_data(checked, "payload"):
        raise PersonalDataError(f"{event_type}: dato personal en {found}")
    return data


def build_event(
    event_type: str,
    payload: dict[str, Any],
    *,
    correlation_id: str,
    causation_id: uuid.UUID | None,
    producer: str,
) -> EventEnvelope:
    data = validate_payload(event_type, payload)
    return new_event(
        event_type=event_type,
        event_version=CATALOG[event_type].version,
        correlation_id=correlation_id,
        causation_id=causation_id,
        producer=producer,
        payload=data,
    )
