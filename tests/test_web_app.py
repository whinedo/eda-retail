"""Esqueleto del backend del front (spec web-front, "Acceso de solo lectura y local")."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from eda_retail.web import cli
from eda_retail.web.app import create_app
from eda_retail.web.db import read_only_connect


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_app(), base_url="http://localhost")


def test_ping_answers_without_origin(client: TestClient):
    """Una petición local sin cabecera Origin (curl, mismo origen) se atiende"""
    response = client.get("/api/ping")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


@pytest.mark.parametrize(
    "origin",
    ["http://localhost:5173", "http://127.0.0.1:8000", "http://localhost"],
)
def test_localhost_origins_are_allowed(client: TestClient, origin: str):
    """Orígenes de localhost (el servidor de Vite y el propio backend) se admiten"""
    response = client.get("/api/ping", headers={"Origin": origin})
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == origin


@pytest.mark.parametrize(
    "origin",
    ["http://evil.example", "http://localhost.evil.example", "https://192.168.1.10:8000", "null"],
)
def test_foreign_origins_are_rejected(client: TestClient, origin: str):
    """Acceso de solo lectura y local: no se admiten orígenes distintos de localhost"""
    response = client.get("/api/ping", headers={"Origin": origin})
    assert response.status_code == 403
    assert "access-control-allow-origin" not in response.headers


def test_foreign_origin_preflight_is_rejected(client: TestClient):
    """Un preflight CORS desde un origen ajeno también se rechaza"""
    response = client.options(
        "/api/ping",
        headers={"Origin": "http://evil.example", "Access-Control-Request-Method": "POST"},
    )
    assert response.status_code == 403


def test_cli_binds_only_to_loopback(monkeypatch: pytest.MonkeyPatch):
    """eda-web escucha solo en 127.0.0.1, aunque el entorno pida otra interfaz"""
    calls: dict = {}
    monkeypatch.setattr(cli.uvicorn, "run", lambda app, **kwargs: calls.update(app=app, **kwargs))
    monkeypatch.setenv("WEB_HOST", "0.0.0.0")
    monkeypatch.setenv("WEB_PORT", "8123")

    cli.main([])

    assert calls["host"] == "127.0.0.1"
    assert calls["port"] == 8123


def test_cli_port_flag_overrides_env(monkeypatch: pytest.MonkeyPatch):
    """--port tiene prioridad sobre WEB_PORT; por defecto el puerto es 8000"""
    calls: dict = {}
    monkeypatch.setattr(cli.uvicorn, "run", lambda app, **kwargs: calls.update(**kwargs))
    monkeypatch.delenv("WEB_PORT", raising=False)

    cli.main([])
    assert calls["port"] == 8000

    cli.main(["--port", "9001"])
    assert calls["port"] == 9001


def test_read_only_connection_forces_read_only_transactions(monkeypatch: pytest.MonkeyPatch):
    """Acceso de solo lectura: la conexión del front abre transacciones de solo lectura"""
    captured: dict = {}

    def fake_connect(url, **kwargs):
        captured.update(url=url, **kwargs)
        return object()

    monkeypatch.setattr("eda_retail.web.db.psycopg.connect", fake_connect)

    read_only_connect("postgresql://eda:eda@localhost:5432/eda")

    assert captured["url"] == "postgresql://eda:eda@localhost:5432/eda"
    assert "default_transaction_read_only=on" in captured["options"]


@pytest.mark.parametrize("host", ["evil.example", "localhost.evil.example", "192.168.1.10:8000"])
def test_foreign_host_is_rejected(host: str):
    """Host ajeno: se rechaza con 403 (DNS rebinding)"""
    client = TestClient(create_app(), base_url=f"http://{host}")
    assert client.get("/api/ping").status_code == 403


@pytest.mark.parametrize("host", ["localhost", "localhost:8000", "127.0.0.1:8000", "[::1]:8000"])
def test_local_host_is_allowed(host: str):
    """Las cabeceras Host de localhost se admiten, con o sin puerto"""
    client = TestClient(create_app(), base_url=f"http://{host}")
    assert client.get("/api/ping").status_code == 200
