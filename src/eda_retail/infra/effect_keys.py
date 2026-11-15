"""Almacén de claves de idempotencia de los efectos (design.md §7, spec idempotency).

Distinto del inbox: el inbox deduplica eventos por consumidor; `effect_keys` deduplica
efectos por `idempotency_key`, aunque los pida un evento nuevo.

Uso dentro de la transacción del ejecutor:

    claim = acquire(conn, key=..., ...)
    COMPLETED -> reutilizar claim.result, sin repetir el efecto
    BUSY      -> otra instancia lo tiene en curso: reintentar más tarde (no confirmar offset)
    ACQUIRED  -> ejecutar el efecto y llamar a complete(...) en la misma transacción

Las claves no se borran: se conservan mientras la incidencia esté abierta y después
(spec «Conservación de las claves»). La purga tras el plazo de seguridad no existe aún.
"""

from __future__ import annotations

import enum
import hashlib
import json
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from eda_retail.infra.schemas import qualified


class ClaimStatus(enum.Enum):
    ACQUIRED = "acquired"
    COMPLETED = "completed"
    BUSY = "busy"


@dataclass(frozen=True)
class Claim:
    status: ClaimStatus
    result: dict[str, Any] | None = None


def _table(schema: str | None) -> str:
    """`<schema>.effect_keys` del ejecutor; sin esquema, la tabla compartida (hasta T10)."""
    return qualified(schema, "effect_keys") if schema is not None else "effect_keys"


def compute_idempotency_key(incident_id: str, step_id: str, *, origin_plan_version: int) -> str:
    """hash(incident_id, step_id, versión del plan en la que se creó el paso). Determinista."""
    material = json.dumps([incident_id, step_id, origin_plan_version], separators=(",", ":"))
    return hashlib.sha256(material.encode()).hexdigest()


def acquire(
    conn: psycopg.Connection,
    *,
    key: str,
    incident_id: str,
    step_id: str,
    action: str,
    lock_seconds: int,
    enabled: bool = True,
    schema: str | None = None,
) -> Claim:
    """Registra la clave con INSERT … ON CONFLICT DO NOTHING o informa de su estado.

    Con `enabled=False` (EFFECT_KEYS_ENABLED=false, experimento E1) no escribe nada y
    siempre devuelve ACQUIRED.
    """
    if not enabled:
        return Claim(ClaimStatus.ACQUIRED)
    table = _table(schema)
    inserted = conn.execute(
        f"""
        INSERT INTO {table} (idempotency_key, incident_id, step_id, action, status, locked_until)
        VALUES (%s, %s, %s, %s, 'in_progress', now() + make_interval(secs => %s))
        ON CONFLICT DO NOTHING
        """,
        (key, incident_id, step_id, action, lock_seconds),
    ).rowcount
    if inserted == 1:
        return Claim(ClaimStatus.ACQUIRED)

    row = conn.execute(
        f"""
        SELECT status, result, locked_until > now() AS locked
        FROM {table} WHERE idempotency_key = %s
        FOR UPDATE
        """,
        (key,),
    ).fetchone()
    if row["status"] == "completed":
        return Claim(ClaimStatus.COMPLETED, row["result"])
    if row["locked"]:
        return Claim(ClaimStatus.BUSY)
    # in_progress con el plazo vencido: esta instancia toma el relevo.
    conn.execute(
        f"UPDATE {table} SET locked_until = now() + make_interval(secs => %s) WHERE idempotency_key = %s",
        (lock_seconds, key),
    )
    return Claim(ClaimStatus.ACQUIRED)


def complete(
    conn: psycopg.Connection, key: str, result: dict[str, Any], *, enabled: bool = True, schema: str | None = None
) -> None:
    """Marca la clave como completada con su resultado (misma transacción que el efecto)."""
    if not enabled:
        return
    conn.execute(
        f"""
        UPDATE {_table(schema)}
        SET status = 'completed', result = %s, completed_at = now(), locked_until = NULL
        WHERE idempotency_key = %s
        """,
        (Jsonb(result), key),
    )


def stored_result(conn: psycopg.Connection, key: str, *, schema: str | None = None) -> dict[str, Any] | None:
    """El resultado de una clave ya completada, o None (sin bloquear; para saltarse trabajo previo)."""
    with conn.transaction():
        row = conn.execute(
            f"SELECT result FROM {_table(schema)} WHERE idempotency_key = %s AND status = 'completed'", (key,)
        ).fetchone()
    return (row["result"] or {}) if row is not None else None
