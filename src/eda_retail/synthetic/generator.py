"""Generador de avisos sintéticos con etiquetas (spec synthetic-data).

`generate(seed, size)` produce, siempre igual para la misma semilla, un conjunto con:
- pedidos simulados (solo referencias: `customer_ref`, nunca datos personales) y sus
  existencias por almacén;
- un aviso por pedido, con su variante (`cause`), gravedad y horas de retraso, en texto
  libre y en formato de API (JSON), y el plan esperado como etiqueta.

Las variantes se reparten por turnos en un orden barajado con la semilla, así un conjunto
de 50 tiene 8 o 9 de cada una (supuesto A2 de odd/tasks/experiments.md). El plan esperado
sigue incident-planning y, donde la spec no lo fija, el supuesto A1.
"""

from __future__ import annotations

import json
import random
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any

VARIANTS = ("delay", "loss", "damage", "wrong_address", "strike", "unknown")
CARRIERS = ("demo-express", "rapid-cargo", "north-parcel")
WAREHOUSES = ("W1", "W2")
SKUS = ("SKU-LAMP", "SKU-CHAIR", "SKU-MUG", "SKU-DESK", "SKU-RUG", "SKU-KETTLE")
STOCK_PER_SKU = 100

_API_CODES = {
    "delay": "DELAYED", "loss": "LOST", "damage": "DAMAGED", "wrong_address": "BAD_ADDRESS",
    "strike": "STRIKE", "unknown": "EXCEPTION",
}
_TEXTS = {
    "delay": (
        ("Aviso de incidencia: el envío del pedido {order} llegará con {hours} horas de retraso por saturación "
         "en el centro de distribución."),
        "Retraso estimado de {hours} horas en la entrega del pedido {order} por condiciones meteorológicas.",
    ),
    "loss": (
        ("Aviso de incidencia: el paquete del pedido {order} se ha extraviado en tránsito y no ha podido "
         "localizarse."),
        "No encontramos el bulto del pedido {order}: se da por perdido tras la búsqueda en el almacén de tránsito.",
    ),
    "damage": (
        "El paquete del pedido {order} ha llegado dañado al centro de reparto y no puede entregarse.",
        "Incidencia en el pedido {order}: la mercancía presenta roturas tras un golpe en el transporte.",
    ),
    "wrong_address": (
        "No se ha podido entregar el pedido {order}: la dirección de entrega es incorrecta o incompleta.",
        "Devolución en curso del pedido {order} por dirección errónea.",
    ),
    "strike": (
        "Huelga del personal de reparto: el pedido {order} sufrirá un retraso de {hours} horas.",
        "Por un paro en la red de transporte, el pedido {order} queda retenido unas {hours} horas.",
    ),
    "unknown": (
        "Incidencia con el envío del pedido {order}. Contacte con el transportista para más información.",
        "Excepción registrada en el seguimiento del pedido {order}, sin más detalle.",
    ),
}


@dataclass(frozen=True)
class SyntheticOrder:
    order_id: str
    customer_ref: str
    sku: str
    quantity: int
    amount: Decimal
    carrier: str


@dataclass(frozen=True)
class SyntheticStock:
    warehouse: str
    sku: str
    available: int


@dataclass(frozen=True)
class SyntheticNotice:
    carrier_notice_id: str
    carrier: str
    order_id: str
    cause: str
    severity: str
    delay_hours: int
    text: str
    api_text: str
    expected_steps: tuple[dict[str, Any], ...]

    def raw_text(self, fmt: str) -> str:
        """El aviso en el formato pedido: `text` (texto libre) o `api` (JSON)."""
        return {"text": self.text, "api": self.api_text}[fmt]

    @property
    def expected_actions(self) -> tuple[str, ...]:
        return tuple(s["action"] for s in self.expected_steps)


@dataclass(frozen=True)
class Dataset:
    seed: int
    orders: tuple[SyntheticOrder, ...]
    stock: tuple[SyntheticStock, ...]
    notices: tuple[SyntheticNotice, ...]

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, default=str, ensure_ascii=False)


def severity_for(cause: str, delay_hours: int) -> str:
    """Supuesto A1: gravedad por variante y horas de retraso."""
    if cause == "delay":
        return "low" if delay_hours <= 24 else "medium" if delay_hours <= 72 else "high"
    if cause in ("loss", "damage"):
        return "high"
    return "medium"


def expected_plan(cause: str, severity: str, order_id: str) -> list[dict[str, Any]]:
    """Plan esperado (incident-planning y supuesto A1), con el formato de `PlanStep`."""
    if severity == "low":
        actions = ["notify_customer"]
    elif cause == "delay":
        actions = ["notify_customer", "file_claim"] if severity == "high" else ["notify_customer"]
    elif cause in ("loss", "damage"):
        actions = ["notify_customer", "file_claim", "reserve_stock", "reissue_shipment"]
    elif cause == "wrong_address":
        actions = ["notify_customer", "reserve_stock", "reissue_shipment"]
    else:  # strike, unknown
        actions = ["notify_customer"]
    steps = []
    for i, action in enumerate(actions, start=1):
        depends = [f"S{actions.index('reserve_stock') + 1}"] if action == "reissue_shipment" else []
        steps.append({"step_id": f"S{i}", "action": action, "params": {"order_id": order_id}, "depends_on": depends})
    return steps


def _delay_hours(rng: random.Random, cause: str) -> int:
    if cause == "delay":
        return rng.choice((12, 24, 48, 72, 96))
    if cause == "strike":
        return rng.choice((24, 48))
    return 0


def generate(seed: int, size: int) -> Dataset:
    rng = random.Random(seed)
    variants = [VARIANTS[i % len(VARIANTS)] for i in range(size)]
    rng.shuffle(variants)
    orders, notices = [], []
    for i, cause in enumerate(variants, start=1):
        order_id = f"P-S{seed}N{i:04d}"  # sin guiones internos: el redactor lo reconoce como un solo pedido
        carrier = rng.choice(CARRIERS)
        order = SyntheticOrder(
            order_id=order_id, customer_ref=f"CUST-{rng.randrange(10_000):04d}", sku=rng.choice(SKUS),
            quantity=rng.randint(1, 3), amount=Decimal(rng.randrange(1_000, 50_000)) / 100, carrier=carrier,
        )
        hours = _delay_hours(rng, cause)
        severity = severity_for(cause, hours)
        notice_id = f"N-{seed}-{i:04d}"
        text = rng.choice(_TEXTS[cause]).format(order=order_id, hours=hours)
        api = {"notice_id": notice_id, "carrier": carrier, "order_id": order_id,
               "event_code": _API_CODES[cause], "delay_hours": hours}
        orders.append(order)
        notices.append(SyntheticNotice(
            carrier_notice_id=notice_id, carrier=carrier, order_id=order_id, cause=cause, severity=severity,
            delay_hours=hours, text=text, api_text=json.dumps(api, sort_keys=True),
            expected_steps=tuple(expected_plan(cause, severity, order_id)),
        ))
    stock = tuple(SyntheticStock(w, sku, STOCK_PER_SKU) for w in WAREHOUSES for sku in SKUS)
    return Dataset(seed, tuple(orders), stock, tuple(notices))
