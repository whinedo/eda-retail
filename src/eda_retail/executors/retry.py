"""Reexporta `Backoff` desde `eda_retail.infra.backoff` (los ejecutores anteriores se borran en T10b)."""

from eda_retail.infra.backoff import Backoff

__all__ = ["Backoff"]
