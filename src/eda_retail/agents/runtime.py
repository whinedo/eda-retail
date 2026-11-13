"""Unidad de trabajo de los agentes con LLM (regla 6 de CLAUDE.md, spec llm-boundaries).

1. Antes de pagar una llamada al LLM, comprueba en el inbox si el evento ya se procesó.
2. El agente decide: lee lo que necesita en transacciones cortas y llama al LLM sin ninguna
   transacción abierta. Devuelve comandos, nunca escribe.
3. Una sola transacción: fila de inbox, comandos y un `RecordLlmCall` por llamada.

Si el comando principal choca con otro (`concurrency_conflict`), se deshace todo y el
agente vuelve a decidir con el estado nuevo, como mucho `max_conflicts` veces. El coste de
las llamadas de los intentos perdidos también se registra (ya se ha pagado).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

import psycopg

from eda_retail.command.handler import CommandHandler, CommandResult
from eda_retail.command.models import Command, RecordLlmCall
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import inbox, tracing
from eda_retail.infra.consumer import Outcome
from eda_retail.llm.generation import LlmCall


@dataclass(frozen=True)
class Decision:
    incident_id: str
    commands: list[Command] = field(default_factory=list)
    calls: list[LlmCall] = field(default_factory=list)


class Agent(Protocol):
    name: str

    def decide(self, conn: psycopg.Connection, event: EventEnvelope) -> Decision: ...


class ConcurrencyRetriesExhausted(RuntimeError):
    """El comando siguió en conflicto tras los reintentos; el offset no se confirma."""


class _Conflict(Exception):
    pass


class AgentRunner:
    def __init__(
        self, agent: Agent, handler: CommandHandler, *, inbox_enabled: bool = True, max_conflicts: int = 3
    ) -> None:
        self.agent = agent
        self._handler = handler
        self._inbox_enabled = inbox_enabled
        self._max_conflicts = max_conflicts
        self.results: list[CommandResult] = []  # resultados del último evento (para trazas y tests)

    def process(self, conn: psycopg.Connection, event: EventEnvelope) -> Outcome:
        with conn.transaction():
            if inbox.seen(conn, self.agent.name, event.event_id, enabled=self._inbox_enabled):
                return Outcome.DUPLICATE
        spent: list[LlmCall] = []
        for _ in range(self._max_conflicts):
            decision = self.agent.decide(conn, event)
            spent.extend(decision.calls)
            try:
                return self._apply(conn, event, decision, spent)
            except _Conflict:
                continue
        raise ConcurrencyRetriesExhausted(f"{self.agent.name}: conflicto persistente en {event.event_id}")

    def _apply(
        self, conn: psycopg.Connection, event: EventEnvelope, decision: Decision, spent: list[LlmCall]
    ) -> Outcome:
        with tracing.tracer().start_as_current_span(f"agent {self.agent.name} apply"), conn.transaction():
            if not inbox.register(conn, self.agent.name, event.event_id, enabled=self._inbox_enabled):
                # Otra instancia lo procesó mientras se llamaba al LLM: no se escribe el comando,
                # pero el coste de estas llamadas sí se registra.
                self._record(conn, event, decision.incident_id, spent)
                return Outcome.DUPLICATE
            results = [self._handler.handle(conn, c) for c in decision.commands]
            if any(r.reason == "concurrency_conflict" for r in results):
                raise _Conflict()
            self._record(conn, event, decision.incident_id, spent)
            self.results = results
        return Outcome.PROCESSED

    def _record(self, conn: psycopg.Connection, event: EventEnvelope, incident_id: str, calls: list[LlmCall]) -> None:
        for call in calls:
            self._handler.handle(
                conn,
                RecordLlmCall(
                    command_id=event.event_id, expected_version=0, producer=self.agent.name,
                    incident_id=incident_id, agent=call.agent, model=call.model, tokens_in=call.tokens_in,
                    tokens_out=call.tokens_out, latency_ms=call.latency_ms,
                    validation_passed=call.validation_passed, attempt=call.attempt,
                ),
            )
