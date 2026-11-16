"""Escenarios de la spec idempotency que se prueban sin PostgreSQL ni Kafka.

«Caída tras enviar el correo»: el trabajo del ejecutor de `notify_customer` (prepare fuera de la
transacción y handle) con el servicio de correo simulado, que registra la clave de cada llamada.
"""

from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import dataclass, field, replace

from eda_retail.config import Settings
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.commands import Accepted, CommandEnvelope
from eda_retail.infra.effect_keys import compute_idempotency_key
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.queries.cost_per_incident import budget_from_cost
from eda_retail.rules import BusinessRules
from eda_retail.step_executors import runtime
from eda_retail.step_executors.actions import build_action
from eda_retail.step_executors.services import FakeService

RULES = BusinessRules()
NOTICE = ("Hemos sentido mucho el retraso de tu pedido. El transportista nos ha avisado y "
          "te escribiremos en cuanto tengamos noticias de la entrega.")


@dataclass
class _MailService(FakeService):
    """Servicio de correo simulado que además guarda la clave de cada llamada."""

    keys: list[str] = field(default_factory=list)

    def call(self, key: str) -> str:
        self.keys.append(key)
        return super().call(key)


class _FakeConn:
    """Conexión falsa para `handle`: acepta los INSERT del efecto y de las llamadas al LLM."""

    def execute(self, sql: str, params: tuple = ()) -> None:
        pass


def test_crash_after_sending_the_mail():
    """Caída tras enviar el correo"""
    mail = _MailService("mail", "M")
    action = build_action("notify_customer", llm=FakeLlmClient([{"text": NOTICE}] * 2), rules=RULES, service=mail,
                          order_known=lambda order_id: True,
                          budget=lambda incident_id: budget_from_cost(None, RULES))
    key = compute_idempotency_key("I-1", "S1", origin_plan_version=1)
    step = runtime.ExecuteStep(incident_id="I-1", plan_version=1, step_id="S1", action="notify_customer",
                               params={"order_id": "P-1"}, idempotency_key=key, cause="delay",
                               estimated_delay_hours=24)
    command = CommandEnvelope(command_id=uuid.uuid4(), command_type="ExecuteStep", correlation_id="I-1",
                              causation_id=uuid.uuid4(), payload=step.model_dump())
    registered: dict[str, dict] = {}  # effect_keys: la caída no llega a registrar la clave
    prepare = runtime.StepPreparer(action, completed=registered.get, max_attempts=3,
                                   backoff=Backoff(0.5, 10, seed=1), sleep=[].append)

    first = prepare(command, step)  # envía el aviso y el ejecutor cae antes de handle
    again = prepare(command, step)  # reentrega del mismo ExecuteStep
    outcome = runtime.execute_step(action, replace(Settings(), effect_keys_enabled=False))(
        _FakeConn(), command, step, again)

    # El reintento usa la misma clave hacia el servicio de correo.
    assert mail.keys == [key, key]
    # El cliente recibe un solo aviso: el servicio reconoce la clave y devuelve el envío anterior.
    assert mail.executions == Counter({key: 1}) and mail.executed == 1
    assert again.external_id == first.external_id
    assert isinstance(outcome, Accepted)
    notified = [e.payload for e in outcome.events if e.event_type == "CustomerNotified"]
    assert notified == [{"order_id": "P-1", "message_id": first.external_id}]
