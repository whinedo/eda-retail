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


class ClaimStatus(enum.Enum):
    ACQUIRED = "acquired"
    COMPLETED = "completed"
    BUSY = "busy"


@dataclass(frozen=True)
class Claim:
    status: ClaimStatus
    result: dict[str, Any] | None = None


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
) -> Claim:
    """Registra la clave con INSERT … ON CONFLICT DO NOTHING o informa de su estado.

    Con `enabled=False` (EFFECT_KEYS_ENABLED=false, experimento E1) no escribe nada y
    siempre devuelve ACQUIRED.
    """
    if not enabled:
        return Claim(ClaimStatus.ACQUIRED)
    inserted = conn.execute(
        """
        INSERT INTO effect_keys (idempotency_key, incident_id, step_id, action, status, locked_until)
        VALUES (%s, %s, %s, %s, 'in_progress', now() + make_interval(secs => %s))
        ON CONFLICT DO NOTHING
        """,
        (key, incident_id, step_id, action, lock_seconds),
    ).rowcount
    if inserted == 1:
        return Claim(ClaimStatus.ACQUIRED)

    row = conn.execute(
        """
        SELECT status, result, locked_until > now() AS locked
        FROM effect_keys WHERE idempotency_key = %s
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
        "UPDATE effect_keys SET locked_until = now() + make_interval(secs => %s) WHERE idempotency_key = %s",
        (lock_seconds, key),
    )
    return Claim(ClaimStatus.ACQUIRED)


def complete(
    conn: psycopg.Connection, key: str, result: dict[str, Any], *, enabled: bool = True
) -> None:
    """Marca la clave como completada con su resultado (misma transacción que el efecto)."""
    if not enabled:
        return
    conn.execute(
        """
        UPDATE effect_keys
        SET status = 'completed', result = %s, completed_at = now(), locked_until = NULL
        WHERE idempotency_key = %s
        """,
        (Jsonb(result), key),
    )
