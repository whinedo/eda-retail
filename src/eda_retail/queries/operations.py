"""Consultas de operaciones (spec incident-metrics, «Consultas de operaciones» y «Retraso de las vistas»).

Solo leen las vistas del proyector `operations`, `view_cost_per_incident`, `view_carrier_dashboard`, `view_orders` (el
beneficio) y las posiciones
aplicadas (`views.offsets`, `event_log.offsets`); nunca escriben y cada respuesta lleva `applied` (la posición del
proyector `operations`). Los agregados (medias, p95, costes, duraciones) se calculan aquí: las vistas solo guardan
hechos. La única lectura que no es una vista son las marcas de agua de Kafka del retraso: metadatos del bus.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, get_args

import psycopg
from confluent_kafka import Consumer, TopicPartition

from eda_retail.event_log.store import CONSUMER as EVENT_LOG_CONSUMER
from eda_retail.event_log.store import SCHEMA as EVENT_LOG_SCHEMA
from eda_retail.projectors.operations import PROJECTOR_NAME
from eda_retail.projectors.registry import PROJECTORS
from eda_retail.queries.carrier_dashboard import CarrierRow
from eda_retail.queries.cost_per_incident import Prices
from eda_retail.queries.positions import applied, events_topic

IncidentState = Literal["open", "resolved", "refunded", "escalated"]
STATES: tuple[str, ...] = get_args(IncidentState)
AGENTS = ("interpreter", "planner", "replanner", "writer")
MAX_INCIDENTS = 100  # filas por página de la tabla de incidencias
KAFKA_TIMEOUT_S = 2.0  # sin respuesta de Kafka en este plazo, el retraso es nulo


def p95(values: Iterable[float]) -> float | None:
    """Percentil 95 por rango más próximo: el valor en la posición ⌈0,95·N⌉ de los N valores ordenados."""
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[math.ceil(0.95 * len(ordered)) - 1]


def _money(tokens_in: int, tokens_out: int, claims: int, reissues: int, prices: Prices) -> tuple[Decimal, Decimal]:
    llm = Decimal(tokens_in) / 1000 * prices.llm_in_per_1k + Decimal(tokens_out) / 1000 * prices.llm_out_per_1k
    return llm, claims * prices.claim + reissues * prices.reissue


# -- resumen ------------------------------------------------------------------------------------
@dataclass(frozen=True)
class OperationsSummary:
    by_state: dict[str, int]
    escalations: list[dict[str, Any]]  # {reason, count}, de más a menos
    by_plan_version: dict[int, int]
    step_failures: list[dict[str, Any]]  # {action, error_code, count}
    cost_by_scenario: list[dict[str, Any]]  # {cause, severity, incidents, mean_tokens, llm_cost, effects_cost, refunded}
    prices_configured: bool
    applied: dict[int, int]


def get_operations_summary(conn: psycopg.Connection, prices: Prices, *, incident_id: str | None = None,
                           topic: str | None = None) -> OperationsSummary:
    where, params = ("WHERE m.incident_id = %s", [incident_id]) if incident_id else ("", [])
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        states = conn.execute(f"SELECT state, count(*) AS n FROM views.view_incident_metrics m {where} GROUP BY 1",
                              params).fetchall()
        reasons = conn.execute(
            f"SELECT escalation_reason AS reason, count(*) AS count FROM views.view_incident_metrics m {where}"
            f" {'AND' if where else 'WHERE'} state = 'escalated' GROUP BY 1 ORDER BY 2 DESC, 1", params).fetchall()
        versions = conn.execute(
            f"SELECT plan_version, count(*) AS n FROM views.view_incident_metrics m {where} GROUP BY 1 ORDER BY 1",
            params).fetchall()
        failures = conn.execute(
            "SELECT coalesce(action, 'unknown') AS action, error_code, count(*) AS count FROM views.view_step_failures"
            + (" WHERE incident_id = %s" if incident_id else "") + " GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2",
            params).fetchall()
        scenarios = conn.execute(
            f"""
            SELECT m.cause, m.severity, count(*) AS incidents,
                   coalesce(sum(c.tokens_in), 0) AS tokens_in, coalesce(sum(c.tokens_out), 0) AS tokens_out,
                   coalesce(sum(c.claims), 0) AS claims, coalesce(sum(c.reissues), 0) AS reissues,
                   coalesce(sum(c.refunds_amount), 0) AS refunded
            FROM views.view_incident_metrics m
            LEFT JOIN views.view_cost_per_incident c USING (incident_id)
            {where} GROUP BY 1, 2 ORDER BY 1 NULLS LAST, 2 NULLS LAST
            """, params).fetchall()
    cost = []
    for r in scenarios:
        llm, effects = _money(r["tokens_in"], r["tokens_out"], r["claims"], r["reissues"], prices)
        cost.append({"cause": r["cause"], "severity": r["severity"], "incidents": r["incidents"],
                     "mean_tokens": (r["tokens_in"] + r["tokens_out"]) / r["incidents"],
                     "llm_cost": llm, "effects_cost": effects, "refunded": Decimal(r["refunded"])})
    return OperationsSummary(
        by_state=dict.fromkeys(STATES, 0) | {r["state"]: r["n"] for r in states},
        escalations=[dict(r) for r in reasons],
        by_plan_version={r["plan_version"]: r["n"] for r in versions},
        step_failures=[dict(r) for r in failures],
        cost_by_scenario=cost,
        prices_configured=any((prices.llm_in_per_1k, prices.llm_out_per_1k, prices.claim, prices.reissue)),
        applied=position,
    )


# -- incidencias --------------------------------------------------------------------------------
@dataclass(frozen=True)
class IncidentMetricsRow:
    incident_id: str
    carrier: str
    cause: str | None
    severity: str | None
    order_count: int
    state: str
    escalation_reason: str | None
    plan_version: int
    steps_failed: int
    compensations: int
    opened_at: datetime
    closed_at: datetime | None
    duration_s: float | None
    tokens: int
    llm_cost: Decimal
    effects_cost: Decimal
    refunded: Decimal
    last_event_at: datetime
    last_event_type: str
    last_event_id: str
    kafka_partition: int
    kafka_offset: int


@dataclass(frozen=True)
class IncidentMetricsPage:
    incidents: list[IncidentMetricsRow]
    has_more: bool
    applied: dict[int, int]


def get_incident_metrics(conn: psycopg.Connection, prices: Prices, *, state: str | None = None,
                         escalation_reason: str | None = None, incident_id: str | None = None,
                         limit: int = MAX_INCIDENTS, before: tuple[datetime, str] | None = None,
                         topic: str | None = None) -> IncidentMetricsPage:
    """Filas de `view_incident_metrics`, las cambiadas más recientemente primero. `before` (`last_event_at`,
    `incident_id`) de la última fila de la página anterior pide la siguiente."""
    where, params = [], []
    for column, value in (("m.state", state), ("m.escalation_reason", escalation_reason),
                          ("m.incident_id", incident_id)):
        if value:
            where.append(f"{column} = %s")
            params.append(value)
    if before is not None:
        where.append("(m.last_event_at, m.incident_id) < (%s, %s)")
        params += list(before)
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        rows = conn.execute(
            """
            SELECT m.*, m.last_event_id::text AS last_event_id,
                   extract(epoch FROM m.closed_at - m.opened_at)::float AS duration_s,
                   coalesce(c.tokens_in, 0) AS tokens_in, coalesce(c.tokens_out, 0) AS tokens_out,
                   coalesce(c.claims, 0) AS claims, coalesce(c.reissues, 0) AS reissues,
                   coalesce(c.refunds_amount, 0) AS refunded
            FROM views.view_incident_metrics m LEFT JOIN views.view_cost_per_incident c USING (incident_id)
            """ + (f" WHERE {' AND '.join(where)}" if where else "")
            + " ORDER BY m.last_event_at DESC, m.incident_id DESC LIMIT %s",
            [*params, limit + 1]).fetchall()
    out = []
    for r in rows[:limit]:
        llm, effects = _money(r["tokens_in"], r["tokens_out"], r["claims"], r["reissues"], prices)
        fields = {k: r[k] for k in IncidentMetricsRow.__dataclass_fields__ if k in r}
        out.append(IncidentMetricsRow(**fields | {"tokens": r["tokens_in"] + r["tokens_out"], "llm_cost": llm,
                                                   "effects_cost": effects, "refunded": Decimal(r["refunded"])}))
    return IncidentMetricsPage(out, len(rows) > limit, position)


# -- LLM ----------------------------------------------------------------------------------------
@dataclass(frozen=True)
class LlmAgentStats:
    agent: str
    calls: int
    tokens_in: int
    tokens_out: int
    latency_mean_ms: float | None
    latency_p95_ms: float | None
    validated_pct: float | None


@dataclass(frozen=True)
class LlmStats:
    agents: list[LlmAgentStats]
    applied: dict[int, int]


def llm_stats(rows: Iterable[Mapping[str, Any]]) -> list[LlmAgentStats]:
    """Por agente (los cuatro con LLM siempre, aunque no tengan llamadas, y los demás que aparezcan)."""
    by_agent: dict[str, list[Mapping[str, Any]]] = {a: [] for a in AGENTS}
    for r in rows:
        by_agent.setdefault(r["agent"], []).append(r)
    out = []
    for agent, calls in by_agent.items():
        latencies = [c["latency_ms"] for c in calls]
        out.append(LlmAgentStats(
            agent=agent, calls=len(calls), tokens_in=sum(c["tokens_in"] for c in calls),
            tokens_out=sum(c["tokens_out"] for c in calls),
            latency_mean_ms=sum(latencies) / len(latencies) if latencies else None,
            latency_p95_ms=p95(latencies),
            validated_pct=100 * sum(bool(c["validated"]) for c in calls) / len(calls) if calls else None))
    return out


def get_llm_stats(conn: psycopg.Connection, *, topic: str | None = None) -> LlmStats:
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        rows = conn.execute(
            "SELECT agent, tokens_in, tokens_out, latency_ms, validated FROM views.view_llm_calls").fetchall()
    return LlmStats(llm_stats(rows), position)


# -- transportistas -----------------------------------------------------------------------------
@dataclass(frozen=True)
class CarrierOperations:
    carriers: list[CarrierRow]
    consults: list[dict[str, Any]]  # {outcome, consults, mean_rounds, mean_latency_ms}; vacía en la versión 1
    applied: dict[int, int]


def get_carrier_operations(conn: psycopg.Connection, *, topic: str | None = None) -> CarrierOperations:
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        carriers = conn.execute("SELECT carrier, incidents, delays, claims_filed, refunds_issued "
                                "FROM views.view_carrier_dashboard ORDER BY carrier").fetchall()
        consults = conn.execute(
            "SELECT outcome, count(*) AS consults, avg(rounds)::float AS mean_rounds,"
            " avg(latency_ms)::float AS mean_latency_ms FROM views.view_carrier_consults GROUP BY 1 ORDER BY 1"
        ).fetchall()
    return CarrierOperations([CarrierRow(**r) for r in carriers], [dict(r) for r in consults], position)


# -- beneficio ----------------------------------------------------------------------------------
CENT = Decimal("0.01")


@dataclass(frozen=True)
class ProfitTotals:
    """Hechos que suma la consulta de beneficio: ingresos, importe de los pedidos reenviados y los de cost_per_incident."""

    revenue: Decimal  # suma de `amount` de todos los pedidos de view_orders
    reissued_amount: Decimal  # suma del importe del pedido de cada reenvío (view_reissued_shipments)
    reissues: int
    refunds: Decimal
    claims: int
    tokens_in: int
    tokens_out: int


@dataclass(frozen=True)
class Profit:
    revenue: Decimal
    shipping: Decimal  # `shipping_pct` % del importe de cada pedido
    reissue_shipping: Decimal  # el mismo porcentaje del pedido de cada reenvío: un reenvío paga el transporte otra vez
    reissue_fixed: Decimal  # `EFFECT_COST_REISSUE` por reenvío
    refunds: Decimal
    claims: Decimal
    llm: Decimal
    profit: Decimal  # ingresos menos la suma de los gastos
    shipping_pct: Decimal
    applied: dict[int, int]


def profit_lines(totals: ProfitTotals, prices: Prices, applied_offsets: dict[int, int] | None = None) -> Profit:
    """El beneficio y cada gasto por separado, redondeados a céntimos (spec incident-metrics, «Beneficio»)."""
    pct = prices.shipping_pct / 100
    llm, _ = _money(totals.tokens_in, totals.tokens_out, 0, 0, prices)
    costs = {
        "shipping": totals.revenue * pct, "reissue_shipping": totals.reissued_amount * pct,
        "reissue_fixed": totals.reissues * prices.reissue, "refunds": Decimal(totals.refunds),
        "claims": totals.claims * prices.claim, "llm": llm,
    }
    costs = {k: Decimal(v).quantize(CENT) for k, v in costs.items()}
    revenue = Decimal(totals.revenue).quantize(CENT)
    return Profit(revenue=revenue, **costs, profit=revenue - sum(costs.values()), shipping_pct=prices.shipping_pct,
                  applied=applied_offsets or {})


def get_profit(conn: psycopg.Connection, prices: Prices, *, topic: str | None = None) -> Profit:
    """Beneficio total: `view_orders`, `view_reissued_shipments` y `view_cost_per_incident` (todas las incidencias)."""
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        orders = conn.execute("SELECT coalesce(sum(amount), 0) AS revenue FROM views.view_orders").fetchone() or {}
        reissued = conn.execute(
            "SELECT coalesce(sum(o.amount), 0) AS amount FROM views.view_reissued_shipments r"
            " LEFT JOIN views.view_orders o USING (order_id)").fetchone() or {}
        cost = conn.execute(
            "SELECT coalesce(sum(reissues), 0) AS reissues, coalesce(sum(refunds_amount), 0) AS refunds,"
            " coalesce(sum(claims), 0) AS claims, coalesce(sum(tokens_in), 0) AS tokens_in,"
            " coalesce(sum(tokens_out), 0) AS tokens_out FROM views.view_cost_per_incident").fetchone() or {}
    totals = ProfitTotals(
        revenue=Decimal(orders.get("revenue", 0)), reissued_amount=Decimal(reissued.get("amount", 0)),
        reissues=int(cost.get("reissues", 0)), refunds=Decimal(cost.get("refunds", 0)),
        claims=int(cost.get("claims", 0)), tokens_in=int(cost.get("tokens_in", 0)),
        tokens_out=int(cost.get("tokens_out", 0)),
    )
    return profit_lines(totals, prices, position)


# -- retraso de las vistas ----------------------------------------------------------------------
@dataclass(frozen=True)
class PartitionLag:
    partition: int
    applied: int | None  # último offset aplicado; None si no aplicó nada
    end_offset: int | None  # None si Kafka no respondió
    lag: int | None


@dataclass(frozen=True)
class ConsumerLag:
    consumer: str
    partitions: list[PartitionLag]
    lag: int | None


def lag_of(applied_offsets: Mapping[int, int], end_offsets: Mapping[int, int] | None) -> ConsumerLag:
    """Por partición, `end offset` − (aplicado + 1): los mensajes aún sin aplicar; sin nada aplicado, el `end offset`.
    Sin `end_offsets` (Kafka no respondió), el `lag` es nulo. El nombre se pone después."""
    partitions = sorted(set(applied_offsets) | set(end_offsets or {}))
    rows = []
    for p in partitions:
        done = applied_offsets.get(p)
        end = None if end_offsets is None else end_offsets.get(p, 0)
        lag = None if end is None else max(end - (done + 1 if done is not None else 0), 0)
        rows.append(PartitionLag(p, done, end, lag))
    total = None if end_offsets is None else sum(r.lag or 0 for r in rows)
    return ConsumerLag("", rows, total)


def kafka_end_offsets(bootstrap_servers: str, topic: str, timeout: float = KAFKA_TIMEOUT_S) -> dict[int, int] | None:
    """Marca de agua alta de cada partición, o None si Kafka no responde en `timeout`."""
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": "web.freshness",
                         "enable.auto.commit": False, "socket.timeout.ms": int(timeout * 1000)})
    try:
        partitions = consumer.list_topics(topic, timeout=timeout).topics[topic].partitions
        return {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout)[1] for p in partitions}
    except Exception:  # noqa: BLE001 — Kafka sin respuesta: el retraso es nulo, no un error
        return None
    finally:
        consumer.close()


@dataclass(frozen=True)
class Freshness:
    consumers: list[ConsumerLag]


def get_freshness(conn: psycopg.Connection, end_offsets: Callable[[str], dict[int, int] | None], *,
                  topic: str | None = None) -> Freshness:
    """El retraso de cada proyector y de `event_log` respecto a las marcas de agua de `end_offsets(topic)`."""
    topic = events_topic(topic)
    with conn.transaction():
        positions = {name: applied(conn, name, topic) for name in PROJECTORS}
        rows = conn.execute(
            f"SELECT kafka_partition, kafka_offset FROM {EVENT_LOG_SCHEMA}.offsets"
            " WHERE consumer = %s AND kafka_topic = %s AND kafka_offset >= 0", (EVENT_LOG_CONSUMER, topic)).fetchall()
    positions[EVENT_LOG_CONSUMER] = {r["kafka_partition"]: r["kafka_offset"] for r in rows}
    ends = end_offsets(topic)
    return Freshness([ConsumerLag(name, lag_of(done, ends).partitions, lag_of(done, ends).lag)
                      for name, done in sorted(positions.items())])
