"""Reglas deterministas del agente del transportista simulado (spec a2a-carrier, supuesto A1).

El agente no usa un LLM: decide con esta tabla, la causa del retraso y una semilla (`CARRIER_AGENT_SEED`),
así los experimentos son repetibles. Para un `order_id` y una causa responde la skill `delivery_options`:

| Causa | `new_delivery_date` | `can_reship` | `accepts_claim` | Pide `tracking_ref` |
| --- | --- | --- | --- | --- |
| `delay` | hoy + 1 a 3 días | no | no | no |
| `strike` | hoy + 3 a 7 días | no | no | no |
| `loss` | nula | sí | sí | sí |
| `damage` | nula | sí | sí | sí |
| `wrong_address` | hoy + 2 a 4 días (devolución y nueva entrega) | sí | no | no |
| `unknown` | nula | no | no | no |

Los días dentro de cada rango salen de un hash de (semilla, pedido, causa), no del `hash` de Python
(que cambia en cada proceso): la misma semilla da la misma fecha relativa. `loss` y `damage` exigen el
número de seguimiento sintético del pedido (`synthetic.tracking`): sin él, o con uno que no coincide,
la tarea queda en `input-required` pidiendo `tracking_ref`. `notes` es un texto fijo por causa (≤ 500
caracteres). Sin base de datos ni datos personales.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

AGENT_NAME = "carrier-agent"
SKILL_ID = "delivery_options"
NOTES_MAX = 500


@dataclass(frozen=True)
class Rule:
    days: tuple[int, int] | None  # rango de días hasta la nueva fecha; None: sin fecha
    can_reship: bool
    accepts_claim: bool
    requires_tracking: bool
    notes: str


RULES: dict[str, Rule] = {
    "delay": Rule((1, 3), False, False, False, "El envío sigue en tránsito; se entregará en la nueva fecha."),
    "strike": Rule((3, 7), False, False, False, "Huelga en la red de reparto: no hay reenvíos hasta que termine."),
    "loss": Rule(None, True, True, True, "Envío dado por perdido: se puede reenviar y se acepta la reclamación."),
    "damage": Rule(None, True, True, True, "Mercancía dañada: se puede reenviar y se acepta la reclamación."),
    "wrong_address": Rule((2, 4), True, False, False, "Dirección incorrecta: el paquete vuelve y se puede reenviar."),
    "unknown": Rule(None, False, False, False, "Incidencia sin detalle: el transportista la está investigando."),
}


def requires_tracking(cause: str) -> bool:
    return RULES[cause].requires_tracking


def _days(seed: int, order_id: str, cause: str, low: int, high: int) -> int:
    digest = hashlib.sha256(f"{seed}:{order_id}:{cause}".encode()).digest()
    return low + int.from_bytes(digest[:4], "big") % (high - low + 1)


def delivery_options(order_id: str, cause: str, *, seed: int, today: date) -> dict[str, Any]:
    """La respuesta de `delivery_options` para un pedido y una causa conocida (KeyError si no lo es)."""
    rule = RULES[cause]
    new_date = None
    if rule.days is not None:
        new_date = (today + timedelta(days=_days(seed, order_id, cause, *rule.days))).isoformat()
    return {"new_delivery_date": new_date, "can_reship": rule.can_reship, "accepts_claim": rule.accepts_claim,
            "notes": rule.notes[:NOTES_MAX]}
