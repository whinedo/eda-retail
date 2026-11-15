"""Reexporta el redactor desde `eda_retail.llm.prompts.writer` (los agentes anteriores se borran en T10b)."""

from eda_retail.llm.prompts.writer import (  # noqa: F401
    _ORDER_REF,
    INVALID_DRAFT,
    LENGTH_LIMITS,
    SYSTEM,
    Draft,
    DraftKind,
    DraftRequest,
    DraftResult,
    Writer,
    _prompt,
    validate_draft,
)
