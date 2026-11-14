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
