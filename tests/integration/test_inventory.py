"""`inventory` contra PostgreSQL real: datos de ejemplo, reservas y liberaciones de stock (T4)."""

from __future__ import annotations

import uuid
from decimal import Decimal

from eda_retail.config import Settings
from eda_retail.inventory.handler import SAMPLE_DATA_CORRELATION, specs

from .component_helpers import body, client, outbox


def _api(database_url):
    return client(database_url, "inventory", specs(Settings()))


def _seed(api, *, order: str, sku: str, quantity: int, available: int, command_id: uuid.UUID | None = None):
    payload = {
        "orders": [{"order_id": order, "customer_ref": "CUST-1", "sku": sku, "quantity": quantity,
                    "amount": "39.90", "carrier": "demo-express"}],
        "stock": [{"warehouse": "W1", "sku": sku, "available": available}],
    }
    return api.post("/commands/SeedSampleData", json=body(SAMPLE_DATA_CORRELATION, payload, command_id))


def _step(api, incident: str, step_id: str, action: str, params: dict, *, key: str | None = None,
          command_id: uuid.UUID | None = None):
    payload = {"incident_id": incident, "plan_version": 1, "step_id": step_id, "action": action,
               "params": params, "idempotency_key": key or f"k-{uuid.uuid4().hex}"}
    return api.post("/commands/ExecuteStep", json=body(incident, payload, command_id))


def _available(conn, sku: str) -> int:
    with conn.transaction():
        return conn.execute("SELECT available FROM inventory.stock WHERE sku = %s", (sku,)).fetchone()["available"]


def _reservations(conn, incident: str) -> list[dict]:
    with conn.transaction():
        return conn.execute("SELECT * FROM inventory.reservations WHERE incident_id = %s", (incident,)).fetchall()


def _fresh() -> tuple[str, str, str]:
    tag = uuid.uuid4().hex[:8]
    return f"P-{tag}", f"SKU-{tag}", f"I-{tag}"


def _order_events(conn, order: str) -> list[dict]:
    return [e for e in outbox(conn, "inventory", SAMPLE_DATA_CORRELATION)
            if e["event_type"] == "OrderRegistered" and e["payload"]["order_id"] == order]


def test_sample_data_by_command(database_url, conn):
    """Datos de ejemplo por comando: el pedido va a inventory.orders y OrderRegistered a su outbox, una sola vez"""
    order, sku, _ = _fresh()
    with _api(database_url) as api:
        assert _seed(api, order=order, sku=sku, quantity=1, available=5).status_code == 202
        assert _seed(api, order=order, sku=sku, quantity=1, available=5).status_code == 202

    with conn.transaction():
        row = conn.execute("SELECT customer_ref, amount FROM inventory.orders WHERE order_id = %s",
                           (order,)).fetchone()
    assert row == {"customer_ref": "CUST-1", "amount": Decimal("39.90")}
    [event] = _order_events(conn, order)
    assert event["payload"] == {"order_id": order, "customer_ref": "CUST-1", "amount": "39.90"}
    assert _available(conn, sku) == 5


def test_valid_command_reserves_stock(database_url, conn):
    """Comando válido: la reserva en inventory.reservations y StockReserved y StepCompleted en su outbox"""
    order, sku, incident = _fresh()
    command_id = uuid.uuid4()
    with _api(database_url) as api:
        _seed(api, order=order, sku=sku, quantity=2, available=5)
        response = _step(api, incident, "s3", "reserve_stock", {"order_id": order}, command_id=command_id)

    assert response.status_code == 202
    [reservation] = _reservations(conn, incident)
    assert (reservation["order_id"], reservation["step_id"], reservation["status"]) == (order, "s3", "active")
    assert _available(conn, sku) == 3
    events = outbox(conn, "inventory", incident)
    assert [e["event_type"] for e in events] == ["StockReserved", "StepCompleted"]
    assert all(e["causation_id"] == command_id for e in events)
    assert events[0]["payload"] == {"order_id": order, "warehouse": "W1",
                                    "reservation_id": reservation["reservation_id"]}
    assert events[1]["payload"]["step_id"] == "s3"
    assert events[1]["payload"]["result"]["reservation_id"] == reservation["reservation_id"]


def test_reservation_without_enough_stock_fails_with_out_of_stock(database_url, conn):
    """Reserva de stock simulada / Error de negocio: 3 unidades y se piden 4, StepFailed out_of_stock"""
    order, sku, incident = _fresh()
    with _api(database_url) as api:
        _seed(api, order=order, sku=sku, quantity=4, available=3)
        response = _step(api, incident, "s3", "reserve_stock", {"order_id": order})

    assert response.status_code == 202
    [event] = outbox(conn, "inventory", incident)
    assert event["event_type"] == "StepFailed"
    assert event["payload"]["error"] == "out_of_stock"
    assert event["payload"]["retryable"] is False
    assert _reservations(conn, incident) == []
    assert _available(conn, sku) == 3


def test_unknown_order_fails_with_order_not_found(database_url, conn):
    _, _, incident = _fresh()
    with _api(database_url) as api:
        _step(api, incident, "s3", "reserve_stock", {"order_id": "P-unknown"})
    [event] = outbox(conn, "inventory", incident)
    assert (event["event_type"], event["payload"]["error"]) == ("StepFailed", "order_not_found")


def test_new_command_for_an_effect_already_done_returns_the_stored_result(database_url, conn):
    """Evento nuevo que pide un efecto ya ejecutado: inbox nuevo, effect_keys no repite la reserva"""
    order, sku, incident = _fresh()
    key = f"k-{uuid.uuid4().hex}"
    with _api(database_url) as api:
        _seed(api, order=order, sku=sku, quantity=1, available=5)
        first = _step(api, incident, "s3", "reserve_stock", {"order_id": order}, key=key)
        second = _step(api, incident, "s3", "reserve_stock", {"order_id": order}, key=key)

    assert first.status_code == second.status_code == 202
    assert "Idempotent-Replayed" not in second.headers
    assert len(_reservations(conn, incident)) == 1
    assert _available(conn, sku) == 4
    events = outbox(conn, "inventory", incident)
    assert [e["event_type"] for e in events] == ["StockReserved", "StepCompleted", "StepCompleted"]
    assert events[1]["payload"]["result"] == events[2]["payload"]["result"]


def test_command_invoked_twice_reserves_once(database_url, conn):
    """Comando invocado dos veces: la misma respuesta y una sola reserva"""
    order, sku, incident = _fresh()
    command_id = uuid.uuid4()
    with _api(database_url) as api:
        _seed(api, order=order, sku=sku, quantity=1, available=5)
        first = _step(api, incident, "s3", "reserve_stock", {"order_id": order}, command_id=command_id)
        second = _step(api, incident, "s3", "reserve_stock", {"order_id": order}, command_id=command_id)

    assert first.json() == second.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert len(_reservations(conn, incident)) == 1
    assert len(outbox(conn, "inventory", incident)) == 2


def test_compensation_releases_the_reservation_of_its_step(database_url, conn):
    """La compensación reserve_stock-comp libera la reserva del paso original y repone el stock"""
    order, sku, incident = _fresh()
    with _api(database_url) as api:
        _seed(api, order=order, sku=sku, quantity=2, available=5)
        _step(api, incident, "reserve_stock", "reserve_stock", {"order_id": order})
        response = _step(api, incident, "reserve_stock-comp", "release_stock", {})

    assert response.status_code == 202
    [reservation] = _reservations(conn, incident)
    assert reservation["status"] == "released"
    assert _available(conn, sku) == 5
    events = outbox(conn, "inventory", incident)
    assert [e["event_type"] for e in events] == ["StockReserved", "StepCompleted", "StockReleased", "StepCompleted"]
    assert events[2]["payload"] == {"reservation_id": reservation["reservation_id"]}
    assert events[3]["payload"]["result"] == {"released": True, "reservation_id": reservation["reservation_id"]}


def test_release_without_reservation(database_url, conn):
    """Liberación sin reserva: StepCompleted con released falso, sin StockReleased ni cambio de stock"""
    order, sku, incident = _fresh()
    with _api(database_url) as api:
        _seed(api, order=order, sku=sku, quantity=4, available=3)
        _step(api, incident, "reserve_stock", "reserve_stock", {"order_id": order})  # out_of_stock
        response = _step(api, incident, "reserve_stock-comp", "release_stock", {})

    assert response.status_code == 202
    events = outbox(conn, "inventory", incident)
    assert [e["event_type"] for e in events] == ["StepFailed", "StepCompleted"]
    assert events[1]["payload"]["result"] == {"released": False}
    assert _available(conn, sku) == 3


def test_execute_step_of_another_action_is_rejected_without_events(database_url, conn):
    """Comando rechazado: un ExecuteStep que inventory no atiende responde 422 sin escribir eventos"""
    _, _, incident = _fresh()
    with _api(database_url) as api:
        response = _step(api, incident, "s1", "file_claim", {"order_id": "P-1001"})
    assert response.status_code == 422
    assert response.json()["reason"] == "action_not_handled"
    assert outbox(conn, "inventory", incident) == []


def test_repeated_compensation_releases_nothing(database_url, conn):
    """Compensación repetida: un release_stock ya completado para la reserva R1 que llega de nuevo no libera nada
    y devuelve el resultado guardado"""
    order, sku, incident = _fresh()
    key, command_id = f"k-{uuid.uuid4().hex}", uuid.uuid4()
    with _api(database_url) as api:
        _seed(api, order=order, sku=sku, quantity=2, available=5)
        _step(api, incident, "S3", "reserve_stock", {"order_id": order})
        first = _step(api, incident, "S3-comp", "release_stock", {"order_id": order}, key=key, command_id=command_id)
        redelivered = _step(api, incident, "S3-comp", "release_stock", {"order_id": order}, key=key,
                            command_id=command_id)
        again = _step(api, incident, "S3-comp", "release_stock", {"order_id": order}, key=key)

    assert first.status_code == redelivered.status_code == again.status_code == 202
    assert redelivered.json() == first.json() and redelivered.headers["Idempotent-Replayed"] == "true"
    assert _available(conn, sku) == 5
    [reservation] = _reservations(conn, incident)
    assert reservation["status"] == "released"
    events = outbox(conn, "inventory", incident)
    assert [e["event_type"] for e in events] == ["StockReserved", "StepCompleted", "StockReleased", "StepCompleted",
                                                 "StepCompleted"]
    assert events[4]["payload"]["result"] == events[3]["payload"]["result"] == {
        "released": True, "reservation_id": reservation["reservation_id"]}
