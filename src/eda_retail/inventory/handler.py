"""Command handler de `inventory`: pedidos, stock y reservas (design.md §3).

- `SeedSampleData` carga pedidos y stock de ejemplo y publica un `OrderRegistered` por cada pedido
  nuevo (alimenta `view_orders`). Repetirlo no publica nada.
- `ExecuteStep` de `reserve_stock` y `release_stock`, los pasos de la saga sobre el stock. El
  resultado de negocio es un evento: un fallo como `out_of_stock` es un comando aceptado cuyo
  resultado es `StepFailed`. `inventory.effect_keys` evita repetir un efecto cuando llega otro
  `ExecuteStep` con la misma `idempotency_key`, en la misma transacción que el efecto.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import psycopg
from pydantic import BaseModel, ConfigDict, Field

from eda_retail.config import Settings
from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import effect_keys
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandSpec, Rejected

SCHEMA = "inventory"
# Los datos de ejemplo no pertenecen a ninguna incidencia.
SAMPLE_DATA_CORRELATION = "sample-data"
RESERVE_STOCK = "reserve_stock"
RELEASE_STOCK = "release_stock"
OUT_OF_STOCK = "out_of_stock"
ORDER_NOT_FOUND = "order_not_found"
COMPENSATION_SUFFIX = "-comp"


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1)
    customer_ref: str = Field(min_length=1)
    sku: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    amount: Decimal = Field(gt=0)
    carrier: str = Field(min_length=1)


class StockLevel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    warehouse: str = Field(min_length=1)
    sku: str = Field(min_length=1)
    available: int = Field(ge=0)


class SeedSampleData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    orders: list[Order]
    stock: list[StockLevel]


class ExecuteStep(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: str = Field(min_length=1)
    plan_version: int = Field(ge=1)
    step_id: str = Field(min_length=1)
    action: str = Field(min_length=1)
    params: dict[str, Any]
    idempotency_key: str = Field(min_length=1)


class BusinessError(Exception):
    """El paso no puede hacerse por el estado de `inventory` (no se reintenta)."""


def original_step(step_id: str) -> str:
    """El paso que deshace una compensación (`<paso>-comp`)."""
    return step_id.removesuffix(COMPENSATION_SUFFIX)


def _event(command: CommandEnvelope, event_type: str, payload: dict[str, Any]) -> EventEnvelope:
    return build_event(event_type, payload, correlation_id=command.correlation_id,
                       causation_id=command.command_id, producer=SCHEMA)


def seed_sample_data(
    conn: psycopg.Connection, command: CommandEnvelope, payload: SeedSampleData, prepared: Any
) -> Accepted:
    events = []
    for order in payload.orders:
        inserted = conn.execute(
            "INSERT INTO inventory.orders (order_id, customer_ref, sku, quantity, amount, carrier)"
            " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (order.order_id, order.customer_ref, order.sku, order.quantity, order.amount, order.carrier),
        ).rowcount
        if inserted:
            events.append(_event(command, "OrderRegistered", {
                "order_id": order.order_id, "customer_ref": order.customer_ref, "amount": str(order.amount),
            }))
    stock = sum(
        conn.execute(
            "INSERT INTO inventory.stock (warehouse, sku, available) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (level.warehouse, level.sku, level.available),
        ).rowcount
        for level in payload.stock
    )
    return Accepted(events, {"orders": len(events), "stock": stock})


def _reserve(conn: psycopg.Connection, step: ExecuteStep) -> tuple[dict[str, Any], tuple[str, dict[str, Any]]]:
    order = conn.execute(
        "SELECT order_id, sku, quantity FROM inventory.orders WHERE order_id = %s", (step.params.get("order_id"),)
    ).fetchone()
    if order is None:
        raise BusinessError(ORDER_NOT_FOUND)
    row = conn.execute(
        """
        UPDATE inventory.stock SET available = available - %s
        WHERE (warehouse, sku) = (
            SELECT warehouse, sku FROM inventory.stock WHERE sku = %s AND available >= %s
            ORDER BY warehouse LIMIT 1 FOR UPDATE
        )
        RETURNING warehouse
        """,
        (order["quantity"], order["sku"], order["quantity"]),
    ).fetchone()
    if row is None:
        raise BusinessError(OUT_OF_STOCK)
    reservation_id = f"R-{uuid.uuid4().hex[:8]}"
    conn.execute(
        "INSERT INTO inventory.reservations"
        " (reservation_id, order_id, warehouse, sku, quantity, status, incident_id, step_id)"
        " VALUES (%s, %s, %s, %s, %s, 'active', %s, %s)",
        (reservation_id, order["order_id"], row["warehouse"], order["sku"], order["quantity"],
         step.incident_id, step.step_id),
    )
    effect = {"order_id": order["order_id"], "warehouse": row["warehouse"], "reservation_id": reservation_id}
    return {"reservation_id": reservation_id, "warehouse": row["warehouse"]}, ("StockReserved", effect)


def _release(conn: psycopg.Connection, step: ExecuteStep) -> tuple[dict[str, Any], tuple[str, dict[str, Any]] | None]:
    """Libera la reserva indicada o, sin ella, la del paso que compensa. Sin reserva no hace nada."""
    if reservation_id := step.params.get("reservation_id"):
        row = conn.execute(
            "SELECT * FROM inventory.reservations WHERE reservation_id = %s FOR UPDATE", (reservation_id,)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM inventory.reservations WHERE incident_id = %s AND step_id = %s"
            " ORDER BY status = 'active' DESC, created_at DESC LIMIT 1 FOR UPDATE",
            (step.incident_id, original_step(step.step_id)),
        ).fetchone()
    if row is None:
        return {"released": False}, None
    if row["status"] != "active":
        return {"released": False, "reservation_id": row["reservation_id"]}, None
    conn.execute("UPDATE inventory.reservations SET status = 'released' WHERE reservation_id = %s",
                 (row["reservation_id"],))
    conn.execute(
        "UPDATE inventory.stock SET available = available + %s WHERE warehouse = %s AND sku = %s",
        (row["quantity"], row["warehouse"], row["sku"]),
    )
    result = {"released": True, "reservation_id": row["reservation_id"]}
    return result, ("StockReleased", {"reservation_id": row["reservation_id"]})


_ACTIONS = {RESERVE_STOCK: _reserve, RELEASE_STOCK: _release}


def _execute_step(settings: Settings):
    def handle(
        conn: psycopg.Connection, command: CommandEnvelope, step: ExecuteStep, prepared: Any
    ) -> Accepted | Rejected:
        action = _ACTIONS.get(step.action)
        if action is None:
            return Rejected("action_not_handled")
        if command.correlation_id != step.incident_id:
            return Rejected("incident_mismatch")
        events: list[EventEnvelope] = []
        try:
            # Punto de guardado: un fallo de negocio deshace la clave y el efecto a medias, y otro
            # ExecuteStep con la misma clave puede volver a intentarlo.
            with conn.transaction():
                claim = effect_keys.acquire(
                    conn, key=step.idempotency_key, incident_id=step.incident_id, step_id=step.step_id,
                    action=step.action, lock_seconds=settings.effect_lock_seconds,
                    enabled=settings.effect_keys_enabled, schema=SCHEMA,
                )
                if claim.status is effect_keys.ClaimStatus.COMPLETED:
                    result = claim.result or {}
                elif claim.status is effect_keys.ClaimStatus.BUSY:
                    # Solo con una clave en curso de otra transacción ya confirmada: no debería
                    # ocurrir, porque clave y efecto se confirman juntos. El cliente reintenta.
                    raise RuntimeError(f"efecto {step.idempotency_key} en curso")
                else:
                    result, effect = action(conn, step)
                    effect_keys.complete(conn, step.idempotency_key, result,
                                         enabled=settings.effect_keys_enabled, schema=SCHEMA)
                    if effect is not None:
                        events.append(_event(command, *effect))
        except BusinessError as exc:
            return Accepted([_event(command, "StepFailed", {
                "step_id": step.step_id, "plan_version": step.plan_version, "error": str(exc),
                "retryable": False, "attempts": 1,
            })])
        events.append(_event(command, "StepCompleted", {
            "step_id": step.step_id, "plan_version": step.plan_version, "result": result,
        }))
        return Accepted(events)

    return handle


def specs(settings: Settings) -> dict[str, CommandSpec]:
    return {
        "SeedSampleData": CommandSpec(SeedSampleData, seed_sample_data),
        "ExecuteStep": CommandSpec(ExecuteStep, _execute_step(settings)),
    }
