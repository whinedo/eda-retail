"""step-execution: contenido redactado por el LLM y su validación determinista."""

from __future__ import annotations

from decimal import Decimal

from eda_retail.llm.fake import FakeLlmClient
from eda_retail.llm.generation import Budget
from eda_retail.llm.prompts.writer import DraftRequest, Writer, validate_draft

ORDER = "P-1a2b3c4d"
GOOD = (f"Hola. Te escribimos por tu pedido {ORDER}. El transportista nos ha avisado de que la entrega "
        "llegará con un día de retraso. Sentimos las molestias y te mantendremos informado.")


def _request(kind: str = "customer_notice", **kw) -> DraftRequest:
    return DraftRequest(**{"kind": kind, "incident_id": "I1", "order_id": ORDER, "sku": "SKU1", "quantity": 1,
                           "cause": "delay", "estimated_delay_hours": 24, **kw})


def _budget() -> Budget:
    return Budget(tokens_used=0, llm_calls=0, max_tokens=10_000, max_calls=10)


def test_draft_with_another_order():
    """Borrador con datos de otro pedido"""
    draft = GOOD + " También revisamos el pedido P-9f9f9f9f de otro cliente."
    assert "P-9f9f9f9f" in validate_draft(draft, _request())
    result = Writer(FakeLlmClient([{"text": draft}]), max_generations=1).draft(_request(), _budget())
    assert result.text is None
    assert result.error == "invalid_draft"  # el ejecutor publicará StepFailed invalid_draft, retryable


def test_valid_draft_is_returned():
    """Un borrador correcto pasa la validación y se devuelve con su llamada registrable"""
    result = Writer(FakeLlmClient([{"text": GOOD}]), max_generations=1).draft(_request(), _budget())
    assert (result.text, result.error) == (GOOD, None)
    assert [c.validation_passed for c in result.calls] == [True]


def test_draft_length_is_bounded():
    """Un borrador demasiado corto o demasiado largo se rechaza"""
    assert "longitud" in validate_draft(f"Pedido {ORDER} con retraso.", _request())
    assert "longitud" in validate_draft(GOOD + " Sentimos las molestias." * 80, _request())


def test_draft_must_be_spanish():
    """Un borrador que no está en español se rechaza"""
    english = (f"Hello. We are writing about your order {ORDER}. The carrier told us that the delivery "
               "will arrive one day late. We are sorry for the trouble and we will keep you posted.")
    assert "idioma" in validate_draft(english, _request())


def test_unauthorized_amounts_are_rejected():
    """Un importe no autorizado se rechaza; el autorizado de la reclamación se admite"""
    assert "importe" in validate_draft(GOOD + " Te devolveremos 30,00 €.", _request())
    claim = (f"Reclamamos al transportista la pérdida del pedido {ORDER}, de una unidad de SKU1, "
             "por un importe de 50.00 EUR, que no se ha entregado en el plazo acordado.")
    assert validate_draft(claim, _request("claim", authorized_amounts=(Decimal("50.00"),))) is None
    assert "importe" in validate_draft(claim, _request("claim", authorized_amounts=(Decimal("20.00"),)))


def test_personal_data_is_rejected():
    """Un borrador con un correo electrónico se rechaza (sin datos personales)"""
    assert "dato personal" in validate_draft(GOOD + " Escríbenos a ana@example.com.", _request())


def test_writer_has_no_tools_and_no_personal_data_in_prompt():
    """El redactor no tiene herramientas y su prompt solo lleva referencias"""
    fake = FakeLlmClient([{"text": GOOD}])
    writer = Writer(fake, max_generations=1)
    writer.draft(_request(), _budget())
    assert writer.TOOLS == ()
    assert ORDER in fake.requests[0].prompt and "@" not in fake.requests[0].prompt
