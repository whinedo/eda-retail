"""LLM oráculo para los experimentos deterministas (E1–E4, E7; decisión D2).

Responde como lo haría un modelo perfecto, con las etiquetas del generador: el intérprete
devuelve la causa, la gravedad y el retraso del aviso; el planificador, el plan esperado;
el replanner quita el paso fallido (y lo que depende de él) y los ya completados, que no
se repiten, y, si el reenvío ya no es
posible, lo sustituye por un reembolso del importe del pedido; el redactor escribe un
borrador válido. Así lo que se mide es la arquitectura, no la calidad del modelo (E5).
"""

from __future__ import annotations

import re
from typing import Any

from eda_retail.llm.fake import FakeLlmClient
from eda_retail.llm.generation import LlmRequest
from eda_retail.synthetic.generator import Dataset

_ORDER = re.compile(r"P-S\d+N\d{4}")
_PLAN_LINE = re.compile(r"^- (?P<id>[\w-]+): (?P<action>\w+) .* depends_on (?P<deps>\[.*?\]) -> (?P<status>\w+)")
_FAILED = re.compile(r"el paso (?P<id>[\w-]+) terminó con el error")
_REISSUE = {"reserve_stock", "reissue_shipment"}


class Oracle:
    model = "oracle"

    def __init__(self, data: Dataset) -> None:
        self._notices = {n.order_id: n for n in data.notices}
        self._amounts = {o.order_id: o.amount for o in data.orders}

    def client(self) -> FakeLlmClient:
        client = FakeLlmClient(responder=self)
        client.model = self.model
        return client

    def __call__(self, request: LlmRequest) -> dict[str, Any]:
        match = _ORDER.search(request.prompt)
        if match is None:
            raise ValueError(f"oráculo: la petición de {request.agent} no menciona ningún pedido")
        order = match.group(0)
        if request.agent == "interpreter":
            n = self._notices[order]
            return {"order_ids": [order], "cause": n.cause, "estimated_delay_hours": n.delay_hours,
                    "severity": n.severity}
        if request.agent == "planner":
            return {"steps": [dict(s) for s in self._notices[order].expected_steps]}
        if request.agent == "replanner":
            return {"steps": self._replan(request.prompt, order)}
        if request.agent == "writer":
            return {"text": f"Le informamos de que su pedido {order} ha sufrido una incidencia en el transporte. "
                            "Estamos gestionando la solución y le mantendremos informado de cualquier novedad."}
        raise ValueError(f"oráculo: agente desconocido {request.agent}")

    def _replan(self, prompt: str, order: str) -> list[dict[str, Any]]:
        steps = []
        for line in prompt.splitlines():
            m = _PLAN_LINE.match(line)
            if m and not m["id"].endswith("-comp"):  # las compensaciones las deriva el código
                steps.append({"step_id": m["id"], "action": m["action"], "deps": eval_list(m["deps"]),
                              "status": m["status"]})
        failed_id = _FAILED.search(prompt)["id"]
        failed = next(s for s in steps if s["step_id"] == failed_id)
        dropped = {failed_id} | ({s["step_id"] for s in steps if s["action"] in _REISSUE}
                                 if failed["action"] in _REISSUE else set())
        dropped |= {s["step_id"] for s in steps if set(s["deps"]) & dropped}
        dropped |= {s["step_id"] for s in steps if s["status"] == "completed"}  # el replanner no los repite
        kept = [{"step_id": s["step_id"], "action": s["action"], "params": {"order_id": order},
                 "depends_on": s["deps"]} for s in steps if s["step_id"] not in dropped]
        if failed["action"] in _REISSUE:
            next_id = max(int(s["step_id"][1:]) for s in steps) + 1
            kept.append({"step_id": f"S{next_id}", "action": "issue_refund",
                         "params": {"order_id": order, "amount": float(self._amounts[order])}, "depends_on": []})
        return kept


def eval_list(raw: str) -> list[str]:
    """`['S3']` (repr de una lista de step_id) a lista, sin `eval`."""
    return re.findall(r"'([\w-]+)'", raw)
