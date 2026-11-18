"""Caída de la base de datos (spec synthetic-data, «Caída de la base de datos»; E10).

`PostgresOutage` corta PostgreSQL sobre su contenedor de Docker (`POSTGRES_CONTAINER`, por defecto `eda-postgres`, el
`container_name` de docker-compose.yml) con la misma mecánica que `BrokerOutage` (`broker_faults.ContainerOutage`):
`stopped` lo para y lo arranca (las conexiones se cortan y las transacciones abiertas se deshacen) y `paused` lo congela
y lo descongela (las conexiones quedan abiertas sin respuesta). Con `duration_s`, un temporizador lo devuelve a su
estado a la hora prevista; al salir, también por una excepción, lo devuelve si aún no lo estaba y espera a que acepte
una conexión nueva y responda a `SELECT 1` (`postgres_ready`).
"""

from __future__ import annotations

import os
from collections.abc import Callable

import psycopg

from eda_retail.synthetic.broker_faults import ContainerOutage

DEFAULT_CONTAINER = "eda-postgres"
READY_CONNECT_TIMEOUT_S = 2  # el mínimo de libpq: una base congelada no responde y el sondeo no debe esperar más


def postgres_container() -> str:
    return os.environ.get("POSTGRES_CONTAINER", DEFAULT_CONTAINER)


def postgres_ready(database_url: str, connect_timeout: int = READY_CONNECT_TIMEOUT_S) -> Callable[[], bool]:
    """True cuando la base acepta una conexión nueva y responde a `SELECT 1` (cada sondeo, con su conexión)."""

    def ready() -> bool:
        try:
            with psycopg.connect(database_url, connect_timeout=connect_timeout, autocommit=True) as conn:
                conn.execute("SELECT 1").fetchone()
        except Exception:  # noqa: BLE001 — base aún sin responder
            return False
        return True

    return ready


class PostgresOutage(ContainerOutage):
    """Corte de PostgreSQL durante el bloque `with` (`POSTGRES_CONTAINER`, por defecto `eda-postgres`)."""

    container_env = "POSTGRES_CONTAINER"
    default_container = DEFAULT_CONTAINER
    cut_label = "de la base de datos"
    subject = "la base de datos"
