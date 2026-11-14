"""Detalle de un nodo del mapa (spec web-front, "Detalle de un nodo"). Solo lectura."""

from __future__ import annotations

import psycopg

from eda_retail.web.health import HealthChecker
from eda_retail.web.routing import consumed_types
from eda_retail.web.topology import Node


def llm_metrics(conn: psycopg.Connection, agent: str) -> dict[str, int]:
    """Métricas de las llamadas al LLM de un agente, a partir de sus eventos LlmCallRecorded."""
    row = conn.execute(
        """
        SELECT count(*) AS calls,
               coalesce(sum((payload->>'tokens_in')::bigint), 0) AS tokens_in,
               coalesce(sum((payload->>'tokens_out')::bigint), 0) AS tokens_out,
               coalesce(round(avg((payload->>'latency_ms')::numeric)), 0) AS avg_latency_ms,
               count(*) FILTER (WHERE (payload->>'validation_passed')::boolean) AS validations_passed
          FROM events
         WHERE event_type = 'LlmCallRecorded' AND payload->>'agent' = %s
        """,
        (agent,),
    ).fetchone()
    return {key: int(value) for key, value in row.items()}


def emitted_types(conn: psycopg.Connection, producer: str) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT event_type FROM events WHERE producer = %s ORDER BY event_type", (producer,)
    ).fetchall()
    return [r["event_type"] for r in rows]


def node_detail(conn: psycopg.Connection, node: Node, health: HealthChecker) -> dict:
    return {
        **node.as_dict(),
        "up": health.process_up(node),
        "consumes": consumed_types(node.id),
        "emits": emitted_types(conn, node.id),
        "llm": llm_metrics(conn, node.llm_agent) if node.llm_agent else None,
    }
