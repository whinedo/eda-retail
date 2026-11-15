"""Agente intérprete (spec incident-interpretation): aviso en texto libre -> `OpenIncident`.

El texto del aviso es contenido no confiable: va delimitado como datos, el agente no tiene
herramientas y la salida solo puede tener los campos del esquema (`extra="forbid"`). Las
reglas que no dependen del texto las comprueba el código, no el LLM.
"""

from __future__ import annotations

import psycopg

from eda_retail.agents.runtime import Decision
from eda_retail.command.handler import CommandHandler, incident_id_for_notice
from eda_retail.command.models import EscalateIncident, OpenIncident
from eda_retail.command.rules import BusinessRules
from eda_retail.events.envelope import EventEnvelope
from eda_retail.llm.generation import LlmClient, generate
from eda_retail.llm.prompts.interpreter import (  # noqa: F401
    SYSTEM,
    NoticeInterpretation,
    check_interpretation,
    notice_request,
)


class Interpreter:
    name = "interpreter"
    TOOLS: tuple = ()  # sin herramientas con efectos (spec llm-boundaries)

    def __init__(self, llm: LlmClient, handler: CommandHandler, rules: BusinessRules | None = None) -> None:
        self._llm = llm
        self._handler = handler
        self._rules = rules or handler.rules

    def decide(self, conn: psycopg.Connection, event: EventEnvelope) -> Decision:
        p = event.payload
        incident_id = incident_id_for_notice(p["carrier_notice_id"])
        with conn.transaction():
            budget = self._handler.budget(conn, incident_id)
        request = notice_request(incident_id, p["carrier"], p["raw_text"])
        gen = generate(self._llm, request, NoticeInterpretation, max_generations=self._rules.max_generations,
                       budget=budget, check=check_interpretation)
        common = {"command_id": event.event_id, "expected_version": 0, "producer": self.name,
                  "incident_id": incident_id}
        if gen.value is None:
            # Supuesto A5: la incidencia se crea ya escalada a partir del aviso.
            return Decision(incident_id, [EscalateIncident(**common, reason=gen.reason)], gen.calls)
        v = gen.value
        command = OpenIncident(
            **common, carrier_notice_id=p["carrier_notice_id"], order_ids=v.order_ids, cause=v.cause.value,
            estimated_delay_hours=v.estimated_delay_hours, severity=v.severity.value,
        )
        return Decision(incident_id, [command], gen.calls)
