"""Reglas de negocio configurables que el código comprueba (spec llm-boundaries)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(frozen=True)
class BusinessRules:
    max_refund_amount: Decimal = field(
        default_factory=lambda: Decimal(os.environ.get("MAX_REFUND_AMOUNT", "500.00"))
    )
