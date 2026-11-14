"""Consulta de `cost_per_incident` (spec observability-audit): solo lee `view_*` y
`views.offsets`, nunca escribe y responde con `applied` (offset aplicado por partición).

El coste en dinero se calcula aquí con los precios de la configuración; la vista solo guarda
hechos del log.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import psycopg

from eda_retail.command.rules import BusinessRules
from eda_retail.config import Settings
from eda_retail.llm.generation import Budget
from eda_retail.projectors.cost_per_incident import PROJECTOR_NAME
from eda_retail.queries.positions import applied


@dataclass(frozen=True)
class Prices:
    llm_in_per_1k: Decimal
    llm_out_per_1k: Decimal
    claim: Decimal
    reissue: Decimal

    @classmethod
    def from_settings(cls, settings: Settings) -> Prices:
        return cls(
            llm_in_per_1k=settings.llm_price_in_per_1k, llm_out_per_1k=settings.llm_price_out_per_1k,
            claim=settings.effect_cost_claim, reissue=settings.effect_cost_reissue,
        )


@dataclass(frozen=True)
class IncidentCost:
    incident_id: str
    llm_calls: int
    tokens_in: int
    tokens_out: int
    claims: int
    reissues: int
    refunds_amount: Decimal
    llm_cost: Decimal
    effects_cost: Decimal
    applied: dict[int, int]


def get_incident_cost(
    conn: psycopg.Connection, incident_id: str, prices: Prices, *, topic: str | None = None
) -> IncidentCost | None:
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        row = conn.execute(
            "SELECT incident_id, llm_calls, tokens_in, tokens_out, claims, reissues, refunds_amount "
            "FROM views.view_cost_per_incident WHERE incident_id = %s",
            (incident_id,),
        ).fetchone()
    if row is None:
        return None
    llm_cost = (Decimal(row["tokens_in"]) / 1000 * prices.llm_in_per_1k
                + Decimal(row["tokens_out"]) / 1000 * prices.llm_out_per_1k)
    effects_cost = row["claims"] * prices.claim + row["reissues"] * prices.reissue + row["refunds_amount"]
    return IncidentCost(**row, llm_cost=llm_cost, effects_cost=effects_cost, applied=position)


def budget_from_cost(cost: IncidentCost | None, rules: BusinessRules) -> Budget:
    """Presupuesto de LLM gastado según esta proyección (sin fila: nada gastado). Lo usan los
    componentes con LLM, que no leen el estado de otros (spec llm-boundaries, "Presupuesto por incidencia")."""
    used, calls = (cost.tokens_in + cost.tokens_out, cost.llm_calls) if cost is not None else (0, 0)
    return Budget(used, calls, rules.max_tokens_per_incident, rules.max_llm_calls_per_incident)
