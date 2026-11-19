"""Reglas de negocio configurables que el código comprueba (spec llm-boundaries)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal


def _int_env(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


@dataclass(frozen=True)
class BusinessRules:
    max_refund_amount: Decimal = field(
        default_factory=lambda: Decimal(os.environ.get("MAX_REFUND_AMOUNT", "500.00"))
    )
    # Presupuesto por incidencia (spec llm-boundaries, "Presupuesto por incidencia").
    max_tokens_per_incident: int = field(default_factory=lambda: _int_env("LLM_MAX_TOKENS_PER_INCIDENT", 50_000))
    max_llm_calls_per_incident: int = field(default_factory=lambda: _int_env("LLM_MAX_CALLS_PER_INCIDENT", 10))
    # Reintentos de generación acotados (spec llm-boundaries).
    max_generations: int = field(default_factory=lambda: _int_env("LLM_MAX_GENERATIONS", 3))
    # Entrada no confiable acotada (spec agent-harness): un aviso más largo se escala sin llamar al LLM.
    max_untrusted_input_chars: int = field(default_factory=lambda: _int_env("MAX_UNTRUSTED_INPUT_CHARS", 8_000))
    # Revisiones del plan (spec incident-planning, "Revisiones agotadas").
    max_plan_revisions: int = field(default_factory=lambda: _int_env("MAX_PLAN_REVISIONS", 2))
    # Reembolso en lugar de un reenvío caro (spec incident-planning): reenviar cuesta el importe del pedido más su
    # transporte (SHIPPING_COST_PCT %) más EFFECT_COST_REISSUE; si supera el REFUND_OVER_REISSUE_PCT % del importe,
    # el código pone un reembolso.
    shipping_cost_pct: Decimal = field(default_factory=lambda: Decimal(os.environ.get("SHIPPING_COST_PCT") or "10"))
    effect_cost_reissue: Decimal = field(
        default_factory=lambda: Decimal(os.environ.get("EFFECT_COST_REISSUE") or "0")
    )
    refund_over_reissue_pct: Decimal = field(
        default_factory=lambda: Decimal(os.environ.get("REFUND_OVER_REISSUE_PCT") or "120")
    )
