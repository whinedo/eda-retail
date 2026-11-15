"""Reexporta los servicios simulados desde `eda_retail.step_executors.services` (hasta T10b)."""

from eda_retail.step_executors.services import FakeService, Services

__all__ = ["FakeService", "Services"]
