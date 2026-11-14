"""Consulta de `cost_per_incident` (spec observability-audit): solo lee `view_*` y
`views.offsets`, nunca escribe y responde con `applied` (offset aplicado por partición).

El coste en dinero se calcula aquí con los precios de la configuración; la vista solo guarda
hechos del log.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import psycopg

from eda_retail.config import Settings
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
