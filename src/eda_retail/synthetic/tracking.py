"""Número de seguimiento sintético de un pedido (supuesto A4 de odd/tasks/v2-mcp-a2a.md).

`tracking_ref(order_id)` es siempre el mismo para el mismo pedido y no lleva datos personales. Lo
conocen el agente del transportista simulado (que lo exige para `loss` y `damage`) y el planificador,
que lo envía cuando el agente lo pide en `input-required`. En un sistema real saldría del envío.
"""

from __future__ import annotations

import hashlib


def tracking_ref(order_id: str) -> str:
    return "TRK-" + hashlib.sha256(order_id.encode()).hexdigest()[:10].upper()
