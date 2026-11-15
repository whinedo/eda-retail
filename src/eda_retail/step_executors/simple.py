"""Acciones sin redactor: `reissue_shipment` e `issue_refund` (el pedido se comprueba en `view_orders`)."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from eda_retail.command.rules import BusinessRules
from eda_retail.executors.actions import REFUND_LIMIT, Escalating
from eda_retail.executors.base import BusinessError
from eda_retail.executors.services import FakeService
from eda_retail.step_executors.drafted import ORDER_NOT_FOUND
from eda_retail.step_executors.runtime import ExecuteStep, Preparation


class Simple:
    action: str
    needs_context = False

    def __init__(self, service: FakeService, *, order_known: Callable[[str], bool]) -> None:
        self._service = service
        self._order_known = order_known

    def check(self, step: ExecuteStep) -> None:
        """Reglas propias de la acción antes del efecto (BusinessError si no puede hacerse)."""

    def prepare(self, step: ExecuteStep) -> Preparation:
        try:
            self.check(step)
            if not self._order_known(str(step.params.get("order_id"))):
                raise BusinessError(ORDER_NOT_FOUND)
        except BusinessError as exc:
            return Preparation(error=exc)
        return Preparation()

    def call(self, step: ExecuteStep, preparation: Preparation) -> str:
        return self._service.call(step.idempotency_key)


class Refund(Simple):
    """El reembolso por encima del máximo no se ejecuta y escala la incidencia (spec step-execution)."""

    def __init__(self, service: FakeService, rules: BusinessRules, *, order_known: Callable[[str], bool]) -> None:
        super().__init__(service, order_known=order_known)
        self._rules = rules

    def check(self, step: ExecuteStep) -> None:
        if Decimal(str(step.params["amount"])) > self._rules.max_refund_amount:
            raise Escalating(REFUND_LIMIT)
