"""Agente redactor (spec step-execution, "Contenido redactado por el LLM").

Lo invoca el ejecutor de `notify_customer` y de `file_claim`; no consume eventos ni
escribe. Redacta el aviso al cliente o la reclamación a partir de referencias (pedido,
producto, causa), nunca de datos personales, y el código valida el borrador antes de
enviarlo:
- longitud dentro de los límites de su tipo;
- en español (heurística determinista de palabras funcionales);
- sin referencias a otros pedidos;
- sin importes salvo los autorizados (p. ej. el reclamado);
- sin correos electrónicos ni teléfonos.
Si no hay borrador válido, el error es `invalid_draft` y el ejecutor publicará
`StepFailed` con `retryable` verdadero.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import Field

from eda_retail.llm.generation import BUDGET_EXHAUSTED, Budget, LlmCall, LlmClient
from eda_retail.llm.harness import AgentHarness, AgentSpec
from eda_retail.llm.schemas import LlmOutput

INVALID_DRAFT = "invalid_draft"
DraftKind = Literal["customer_notice", "claim"]

LENGTH_LIMITS: dict[str, tuple[int, int]] = {"customer_notice": (40, 800), "claim": (40, 2000)}
_ORDER_REF = re.compile(r"\bP-?(?=[0-9A-Za-z]*\d)[0-9A-Za-z]+\b")
_AMOUNT = re.compile(
    r"(?:€\s*(?P<pre>\d+(?:[.,]\d{1,2})?))|(?:(?P<post>\d+(?:[.,]\d{1,2})?)\s*(?:€|EUR\b|euros?\b))", re.IGNORECASE
)
_EMAIL = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
_PHONE = re.compile(r"(?:\+\d{1,3}[\s-]?)?(?:\d[\s-]?){9,}")
_WORDS = re.compile(r"[a-záéíóúüñ]+", re.IGNORECASE)
_SPANISH = {"de", "la", "el", "que", "y", "en", "los", "las", "por", "con", "su", "tu", "del", "se", "un",
            "una", "nos", "te", "para", "al", "es", "lo", "no", "le", "hemos", "sentimos"}
_ENGLISH = {"the", "and", "of", "to", "we", "your", "is", "are", "will", "that", "for", "with", "you", "our"}

SYSTEM = """Eres el redactor de una tienda online. Escribe en español, con tono cordial y breve.
Usa solo los datos que se te dan: no inventes pedidos, importes, plazos ni compensaciones.
No incluyas datos personales (nombres, correos, teléfonos, direcciones).
Responde solo con el JSON del esquema, con el texto en el campo text."""


@dataclass(frozen=True)
class DraftRequest:
    kind: DraftKind
    incident_id: str
    order_id: str
    sku: str | None  # None cuando el ejecutor solo conoce view_orders (sin SKU ni cantidad)
    quantity: int | None
    cause: str
    estimated_delay_hours: int
    authorized_amounts: tuple[Decimal, ...] = ()


@dataclass(frozen=True)
class DraftResult:
    text: str | None
    error: str | None
    calls: list[LlmCall] = field(default_factory=list)


class Draft(LlmOutput):
    text: str = Field(min_length=1)


def _amount(raw: str) -> Decimal | None:
    try:
        return Decimal(raw.replace(",", "."))
    except InvalidOperation:
        return None


def validate_draft(text: str, request: DraftRequest) -> str | None:
    """Devuelve el motivo del rechazo, o None si el borrador puede enviarse."""
    low, high = LENGTH_LIMITS[request.kind]
    if not low <= len(text) <= high:
        return f"longitud {len(text)} fuera de [{low}, {high}]"
    words = [w.lower() for w in _WORDS.findall(text)]
    spanish = sum(w in _SPANISH for w in words)
    if spanish < 3 or spanish <= sum(w in _ENGLISH for w in words):
        return "idioma: el borrador no está en español"
    others = sorted({ref for ref in _ORDER_REF.findall(text) if ref != request.order_id})
    if others:
        return f"menciona otros pedidos: {', '.join(others)}"
    allowed = set(request.authorized_amounts)
    for m in _AMOUNT.finditer(text):
        value = _amount(m.group("pre") or m.group("post"))
        if value not in allowed:
            return f"importe no autorizado: {m.group(0).strip()}"
    if _EMAIL.search(text) or _PHONE.search(text):
        return "dato personal en el borrador"
    return None


TEMPLATE = ("Redacta {what}.\nPedido: {order_id}{item}.\n"
            "Causa: {cause}. Retraso estimado: {delay} h.\n"
            "Importes que puedes mencionar: {amounts}.")
WHAT = {"customer_notice": "un aviso al cliente sobre su pedido", "claim": "una reclamación al transportista"}
ITEM_TEMPLATE = " ({quantity} x {sku})"
AMOUNT_TEMPLATE = "{amount} EUR"
NO_AMOUNTS = "ninguno"

SPEC = AgentSpec(agent="writer", prompt_version="1", system=SYSTEM,
                 templates=(TEMPLATE, WHAT["customer_notice"], WHAT["claim"], ITEM_TEMPLATE, AMOUNT_TEMPLATE,
                            NO_AMOUNTS), output=Draft, max_tokens=2_000)


def _prompt(r: DraftRequest) -> str:
    what = WHAT["customer_notice"] if r.kind == "customer_notice" else WHAT["claim"]
    amounts = ", ".join(AMOUNT_TEMPLATE.format(amount=a) for a in r.authorized_amounts) or NO_AMOUNTS
    item = ITEM_TEMPLATE.format(quantity=r.quantity, sku=r.sku) if r.sku is not None else ""
    return TEMPLATE.format(what=what, order_id=r.order_id, item=item, cause=r.cause,
                           delay=r.estimated_delay_hours, amounts=amounts)


class Writer:
    name = "writer"
    TOOLS: tuple = ()  # sin herramientas con efectos (spec llm-boundaries)

    def __init__(self, llm: LlmClient, *, max_generations: int) -> None:
        self._harness = AgentHarness(llm, max_generations=max_generations)

    def draft(self, request: DraftRequest, budget: Budget) -> DraftResult:
        gen = self._harness.run(SPEC.request(request.incident_id, _prompt(request)), budget=budget,
                                check=lambda d: validate_draft(d.text, request))
        if gen.value is not None:
            return DraftResult(gen.value.text, None, gen.calls)
        return DraftResult(None, BUDGET_EXHAUSTED if gen.reason == BUDGET_EXHAUSTED else INVALID_DRAFT, gen.calls)
