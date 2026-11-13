"""Esquemas de las salidas del LLM y su versión estricta para `output_config.format`.

Las salidas se validan aquí con pydantic (forma) y después en código (reglas de negocio y
command handler). El esquema que se envía al modelo se cierra (`additionalProperties:
false`) y pierde las restricciones numéricas y de longitud, que comprueba pydantic.
"""

from __future__ import annotations

import copy
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LlmOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StepParams(LlmOutput):
    """Supuesto A7: los pasos solo llevan el pedido y, para reembolsos, el importe."""

    order_id: str = Field(min_length=1)
    amount: float | None = None


class ProposedStep(LlmOutput):
    step_id: str = Field(min_length=1)
    action: str = Field(min_length=1)
    params: StepParams
    depends_on: list[str]


class PlanProposal(LlmOutput):
    steps: list[ProposedStep] = Field(min_length=1)

    def command_steps(self) -> list[dict[str, Any]]:
        """Pasos en la forma que espera el command handler (sin `amount` vacío)."""
        return [
            {
                "step_id": s.step_id, "action": s.action, "depends_on": list(s.depends_on),
                "params": s.params.model_dump(exclude_none=True),
            }
            for s in self.steps
        ]


_DROPPED = {"title", "default", "minLength", "maxLength", "pattern", "minimum", "maximum",
            "exclusiveMinimum", "exclusiveMaximum", "maxItems"}


def strict_schema(model: type[BaseModel]) -> dict[str, Any]:
    schema = copy.deepcopy(model.model_json_schema())
    return _close(schema)


def _close(node: Any) -> Any:
    if isinstance(node, dict):
        out = {k: _close(v) for k, v in node.items() if k not in _DROPPED}
        if out.get("minItems", 0) > 1:
            del out["minItems"]
        if out.get("type") == "object" and "properties" in out:
            out["additionalProperties"] = False
            out["required"] = sorted(out["properties"])
        return out
    if isinstance(node, list):
        return [_close(v) for v in node]
    return node
