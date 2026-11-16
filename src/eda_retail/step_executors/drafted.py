"""Acciones cuyo texto redacta el agente redactor (`notify_customer` y `file_claim`).

El borrador se pide fuera de la transacción (supuesto A9) con referencias, nunca datos personales: el
pedido (de `view_orders`, que no tiene SKU ni cantidad) y la causa y el retraso que trae el comando.
El presupuesto de LLM de la incidencia sale de `cost_per_incident`. Un borrador inválido es un fallo
reintentable (`invalid_draft`); sin presupuesto, no reintentable.
"""

from __future__ import annotations

from collections.abc import Callable

from eda_retail.llm.generation import BUDGET_EXHAUSTED, Budget
from eda_retail.llm.prompts.writer import INVALID_DRAFT, DraftKind, DraftRequest, Writer
from eda_retail.step_executors.errors import BusinessError, EffectError
from eda_retail.step_executors.runtime import ExecuteStep, Preparation
from eda_retail.step_executors.services import FakeService

ORDER_NOT_FOUND = "order_not_found"


class Drafted:
    action: str
    kind: DraftKind
    needs_context = True

    def __init__(self, writer: Writer, service: FakeService | None, *, order_known: Callable[[str], bool],
                 budget: Callable[[str], Budget]) -> None:
        self._writer = writer
        self._service = service
        self._order_known = order_known
        self._budget = budget

    def prepare(self, step: ExecuteStep) -> Preparation:
        order_id = str(step.params.get("order_id"))
        if not self._order_known(order_id):
            return Preparation(error=BusinessError(ORDER_NOT_FOUND))
        draft = self._writer.draft(
            DraftRequest(kind=self.kind, incident_id=step.incident_id, order_id=order_id, sku=None, quantity=None,
                         cause=str(step.cause), estimated_delay_hours=int(step.estimated_delay_hours or 0)),
            self._budget(step.incident_id),
        )
        if draft.text is None:
            retryable = draft.error != BUDGET_EXHAUSTED
            return Preparation(calls=draft.calls, error=EffectError(draft.error or INVALID_DRAFT, retryable=retryable))
        return Preparation(draft.text, draft.calls)

    def call(self, step: ExecuteStep, preparation: Preparation) -> str:
        assert self._service is not None, "una acción redactada sin servicio define su propio call"
        return self._service.call(step.idempotency_key)
