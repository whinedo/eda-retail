"""Proceso de un componente: su API de comandos REST y su relay (design.md §4.4, T4)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from eda_retail.config import Settings
from eda_retail.infra.component import build_app, listen_address


class _Relay:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self, timeout=None) -> None:
        self.stopped = True


def test_settings_have_the_url_of_each_system_and_the_command_timeout(monkeypatch):
    """Cada sistema tiene su URL (<SISTEMA>_URL) y hay un tiempo de espera para los comandos"""
    for name in ("INTAKE_URL", "INVENTORY_URL", "COMMAND_HTTP_TIMEOUT_S"):
        monkeypatch.delenv(name, raising=False)
    defaults = Settings()
    assert defaults.intake_url == "http://127.0.0.1:8101"
    assert defaults.inventory_url == "http://127.0.0.1:8102"
    assert defaults.command_http_timeout_s == 5.0

    monkeypatch.setenv("INVENTORY_URL", "http://127.0.0.1:9000")
    monkeypatch.setenv("COMMAND_HTTP_TIMEOUT_S", "2.5")
    assert Settings().inventory_url == "http://127.0.0.1:9000"
    assert Settings().command_http_timeout_s == 2.5


def test_listen_address_comes_from_the_system_url():
    assert listen_address("http://127.0.0.1:8102") == ("127.0.0.1", 8102)
    assert listen_address("http://localhost:9000/") == ("localhost", 9000)
    with pytest.raises(ValueError):
        listen_address("http://127.0.0.1")


def test_app_runs_the_relay_while_the_process_lives():
    """El relay del componente arranca con su API y se detiene al pararla"""
    relays: list[_Relay] = []

    def start_relay() -> _Relay:
        relays.append(_Relay())
        return relays[-1]

    app = build_app("intake", {}, connect=lambda: None, start_relay=start_relay)
    with TestClient(app) as client:
        assert len(relays) == 1 and not relays[0].stopped
        response = client.post("/commands/Unknown", json={})
        assert response.status_code == 404
    assert relays[0].stopped


def _capture_uvicorn(monkeypatch) -> dict:
    """Arranque de un componente sin servidor real: guarda lo que se le pasaría a uvicorn."""
    import uvicorn

    from eda_retail.infra import outbox, tracing

    calls: dict = {}
    monkeypatch.setenv("EXECUTOR_MAX_CONCURRENCY", "3")
    monkeypatch.setattr(uvicorn, "run", lambda app, **kwargs: calls.update(kwargs))
    monkeypatch.setattr(tracing, "setup_tracing", lambda name, settings: None)
    monkeypatch.setattr(outbox, "start_relay", lambda settings, schema: _Relay())
    return calls


@pytest.mark.parametrize("action", ["notify_customer", "file_claim", "reissue_shipment", "issue_refund"])
def test_executor_limits_its_concurrent_operations(monkeypatch, action):
    """Pico de incidencias: cada ejecutor limita sus operaciones simultáneas a EXECUTOR_MAX_CONCURRENCY"""
    from eda_retail.step_executors import cli

    calls = _capture_uvicorn(monkeypatch)
    cli.main([action])
    assert calls["limit_concurrency"] == 3


def test_inventory_limits_its_concurrent_operations(monkeypatch):
    """inventory ejecuta reserve_stock y release_stock: también limita sus operaciones simultáneas"""
    from eda_retail.inventory import cli

    calls = _capture_uvicorn(monkeypatch)
    cli.main([])
    assert calls["limit_concurrency"] == 3


def test_systems_that_are_not_executors_keep_no_limit(monkeypatch):
    """intake no ejecuta pasos: arranca sin límite de operaciones simultáneas, como antes"""
    from eda_retail.intake import cli

    calls = _capture_uvicorn(monkeypatch)
    cli.main([])
    assert calls.get("limit_concurrency") is None
