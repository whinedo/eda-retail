"""Errores de un efecto (specs step-execution y resilience): transitorio (se reintenta) o de negocio.

Los usan los ejecutores REST y, hasta T10b, los ejecutores anteriores.
"""

from __future__ import annotations

TIMEOUT = "timeout"
REFUND_LIMIT = "refund_limit"


class EffectError(Exception):
    """El efecto no se pudo ejecutar. `retryable` distingue transitorio de negocio."""

    escalate: str | None = None  # motivo con el que, además, se escala la incidencia

    def __init__(self, error: str, *, retryable: bool) -> None:
        super().__init__(error)
        self.error = error
        self.retryable = retryable


class Transient(EffectError):
    def __init__(self, error: str = TIMEOUT) -> None:
        super().__init__(error, retryable=True)


class BusinessError(EffectError):
    def __init__(self, error: str) -> None:
        super().__init__(error, retryable=False)


class Escalating(BusinessError):
    """Error de negocio que, además del `StepFailed`, escala la incidencia con su motivo."""

    def __init__(self, error: str) -> None:
        super().__init__(error)
        self.escalate = error
