"""Buzón y directorio del servidor de correo: en PostgreSQL (`mail_server`) o en memoria (tests y arnés).

`send` resuelve la dirección del cliente y guarda el correo; devuelve `None` si el cliente no está en el
directorio. Con la misma `idempotency_key` devuelve el correo ya guardado (spec mcp-mail, «Envío repetido
con la misma clave»), en su propia transacción.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

import psycopg


@dataclass(frozen=True)
class Mail:
    message_id: str
    idempotency_key: str
    customer_ref: str
    to_address: str
    subject: str
    body: str
    accepted_at: datetime


class MailStore(Protocol):
    def send(self, *, customer_ref: str, subject: str, body: str, idempotency_key: str) -> Mail | None: ...


def new_message_id(_key: str = "") -> str:
    return f"M-{uuid.uuid4().hex[:12]}"


@dataclass
class MemoryMailStore:
    """Buzón en memoria. `new_id` genera el `message_id` (el arnés pasa el servicio simulado, que cuenta las
    ejecuciones); con `dedup=False` no reconoce la clave y la deduplicación queda en manos de `new_id`."""

    directory: dict[str, str]
    new_id: Callable[[str], str] = new_message_id
    dedup: bool = True
    mailbox: list[Mail] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def send(self, *, customer_ref: str, subject: str, body: str, idempotency_key: str) -> Mail | None:
        with self._lock:
            address = self.directory.get(customer_ref)
            if address is None:
                return None
            if self.dedup:
                for mail in self.mailbox:
                    if mail.idempotency_key == idempotency_key:
                        return mail
            mail = Mail(self.new_id(idempotency_key), idempotency_key, customer_ref, address, subject, body,
                        datetime.now(UTC))
            self.mailbox.append(mail)
            return mail


class PgMailStore:
    """Buzón en `mail_server.mailbox` y directorio en `mail_server.customers`; una conexión por envío."""

    def __init__(self, connect: Callable[[], psycopg.Connection]) -> None:
        self._connect = connect

    def send(self, *, customer_ref: str, subject: str, body: str, idempotency_key: str) -> Mail | None:
        with self._connect() as conn, conn.transaction():
            row = conn.execute("SELECT email FROM mail_server.customers WHERE customer_ref = %s",
                               (customer_ref,)).fetchone()
            if row is None:
                return None
            conn.execute(
                "INSERT INTO mail_server.mailbox (message_id, idempotency_key, customer_ref, to_address, subject,"
                " body) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (idempotency_key) DO NOTHING",
                (new_message_id(), idempotency_key, customer_ref, row["email"], subject, body),
            )
            sent = conn.execute(
                "SELECT message_id, idempotency_key, customer_ref, to_address, subject, body, accepted_at"
                " FROM mail_server.mailbox WHERE idempotency_key = %s", (idempotency_key,)).fetchone()
        return Mail(**sent)


def seed_customers(conn: psycopg.Connection, directory: Iterable[tuple[str, str]]) -> int:
    """Siembra el directorio (idempotente). Devuelve cuántos clientes nuevos añadió."""
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany("INSERT INTO mail_server.customers (customer_ref, email) VALUES (%s, %s)"
                        " ON CONFLICT (customer_ref) DO NOTHING", list(directory))
        return max(cur.rowcount, 0)
