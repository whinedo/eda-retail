"""Las acciones de los ejecutores REST, una por esquema `executor_<acción>` (reserve y release son de inventory)."""

from __future__ import annotations

from collections.abc import Callable

from eda_retail.executor_file_claim.handler import FileClaim
from eda_retail.executor_issue_refund.handler import IssueRefund
from eda_retail.executor_notify_customer.handler import NotifyCustomer
from eda_retail.executor_reissue_shipment.handler import ReissueShipment
from eda_retail.llm.generation import Budget, LlmClient
from eda_retail.llm.prompts.writer import Writer
from eda_retail.rules import BusinessRules
from eda_retail.step_executors.runtime import RecordingAction
from eda_retail.step_executors.services import FakeService, Services

ACTIONS = ("notify_customer", "file_claim", "reissue_shipment", "issue_refund")


def build_action(action: str, *, llm: LlmClient, rules: BusinessRules, service: FakeService | None,
                 order_known: Callable[[str], bool], budget: Callable[[str], Budget]) -> RecordingAction:
    """La acción con su servicio simulado (o el que se pase, para los tests). KeyError si no es de un ejecutor."""
    services = Services()
    writer = Writer(llm, max_generations=rules.max_generations)
    factories: dict[str, Callable[[], RecordingAction]] = {
        "notify_customer": lambda: NotifyCustomer(writer, service or services.mail, order_known=order_known,
                                                  budget=budget),
        "file_claim": lambda: FileClaim(writer, service or services.claims, order_known=order_known, budget=budget),
        "reissue_shipment": lambda: ReissueShipment(service or services.carrier, order_known=order_known),
        "issue_refund": lambda: IssueRefund(service or services.payments, rules, order_known=order_known),
    }
    return factories[action]()
