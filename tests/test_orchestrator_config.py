"""Configuración y destinos de los comandos de la saga (T7)."""

from __future__ import annotations

from eda_retail.config import Settings
from eda_retail.infra import ownership
from eda_retail.orchestrator.saga import step_target, target_url


def test_saga_settings_have_defaults_and_read_the_environment(monkeypatch):
    for name in ("SAGA_STEP_TIMEOUT_S", "SAGA_PLAN_TIMEOUT_S", "REPLANNER_URL"):
        monkeypatch.delenv(name, raising=False)
    s = Settings()
    assert (s.saga_step_timeout_s, s.saga_plan_timeout_s, s.replanner_url) == (60.0, 300.0, "http://127.0.0.1:8104")
    monkeypatch.setenv("SAGA_STEP_TIMEOUT_S", "2")
    assert Settings().saga_step_timeout_s == 2.0


def test_each_command_goes_to_the_system_that_handles_it():
    """Enrutado por acción: reserve_stock y release_stock a inventory; el resto, a su ejecutor"""
    s = Settings()
    assert target_url(s, step_target("reserve_stock")) == s.inventory_url
    assert target_url(s, step_target("release_stock")) == s.inventory_url
    assert target_url(s, step_target("issue_refund")) == s.executor_issue_refund_url
    assert target_url(s, step_target("notify_customer")) == s.executor_notify_customer_url
    assert target_url(s, "planner") == s.planner_url
    assert target_url(s, "replanner") == s.replanner_url


def test_orchestrator_is_a_checked_component():
    assert ownership.COMPONENT_PACKAGES["eda_retail/orchestrator"] == "orchestrator"
