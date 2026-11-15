"""Reexporta `BusinessRules` desde `eda_retail.rules` (el handler compartido se borra en T10b)."""

from eda_retail.rules import BusinessRules, _int_env

__all__ = ["BusinessRules", "_int_env"]
