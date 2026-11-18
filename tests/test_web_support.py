"""Soporte técnico desde el front (spec web-front, «Soporte técnico»): el backend lanza scripts/support.sh con una
lista blanca. Los tests lo sustituyen por un script falso: no tocan PostgreSQL ni Kafka."""

from __future__ import annotations

import stat
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from eda_retail.web.app import create_app

from .test_web_commands import ECHO_ARGS, _output, _wait_until

TOKEN = "0123456789abcdef"
SLOW = """#!/usr/bin/env bash
touch "$(dirname "$0")/launched"
sleep 5
"""


def _script(tmp_path: Path, name: str, body: str = ECHO_ARGS) -> Path:
    folder = tmp_path / name
    folder.mkdir()
    path = folder / f"{name}.sh"
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _client(tmp_path: Path, *, support: bool | None = True, notice_body: str = ECHO_ARGS) -> TestClient:
    app = create_app(notice_script=_script(tmp_path, "notice", notice_body),
                     support_script=_script(tmp_path, "support"), support=support)
    return TestClient(app, base_url="http://localhost")


def _launched(tmp_path: Path) -> bool:
    return (tmp_path / "support" / "launched").exists()


def _args(text: str) -> list[str]:
    return [line.removeprefix("arg:") for line in text.splitlines() if line.startswith("arg:")]


def test_support_disabled_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Soporte deshabilitado"""
    monkeypatch.delenv("WEB_SUPPORT", raising=False)
    client = _client(tmp_path, support=None)
    response = client.post("/api/support/runs", json={"action": "status"})
    assert response.status_code == 403 and "WEB_SUPPORT" in response.json()["detail"]
    assert not _launched(tmp_path)
    assert client.get("/api/config").json()["support_enabled"] is False


@pytest.mark.parametrize("raw", ['"x"', "{bad", "[]"])
def test_support_disabled_rejects_any_body_with_403(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw: str):
    """Soporte deshabilitado: 403 también con un cuerpo que no es un objeto JSON"""
    monkeypatch.delenv("WEB_SUPPORT", raising=False)
    response = _client(tmp_path, support=None).post("/api/support/runs", content=raw,
                                                    headers={"Content-Type": "application/json"})
    assert response.status_code == 403 and not _launched(tmp_path)


@pytest.mark.parametrize("raw", ['"x"', "{bad", "[]"])
def test_support_enabled_rejects_a_body_that_is_not_an_object(tmp_path: Path, raw: str):
    """Soporte habilitado: un cuerpo que no es un objeto JSON es 422 y no lanza nada"""
    response = _client(tmp_path).post("/api/support/runs", content=raw, headers={"Content-Type": "application/json"})
    assert response.status_code == 422 and not _launched(tmp_path)


@pytest.mark.parametrize(("value", "enabled"), [("true", True), ("1", True), ("no", False)])
def test_support_enabled_by_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, value: str, enabled: bool):
    """`WEB_SUPPORT=true` (o `1`) habilita el soporte y /api/config lo dice"""
    monkeypatch.setenv("WEB_SUPPORT", value)
    assert _client(tmp_path, support=None).get("/api/config").json()["support_enabled"] is enabled


def test_dry_run_from_the_front(tmp_path: Path):
    """Simulación desde el front"""
    client = _client(tmp_path)
    response = client.post("/api/support/runs", json={"action": "republish", "mode": "truncated"})
    assert response.status_code == 202
    body = response.json()
    assert body["argv"] == ["scripts/support.sh", "--json", "republish", "--mode", "truncated"]
    text, code = _output(client, body["run_id"])
    assert _args(text) == ["--json", "republish", "--mode", "truncated"] and code == 0


@pytest.mark.parametrize(("request_body", "argv"), [
    ({"action": "status"}, ["--json", "status"]),
    ({"action": "rebuild", "view": "orders"}, ["--json", "rebuild", "orders"]),
    ({"action": "republish", "mode": "recreated", "confirm": TOKEN},
     ["--json", "republish", "--mode", "recreated", "--confirm", TOKEN, "--origin", "web"]),
])
def test_each_action_maps_to_its_arguments(tmp_path: Path, request_body: dict, argv: list[str]):
    """Cada acción de la lista blanca da sus argumentos, siempre con --json"""
    client = _client(tmp_path)
    response = client.post("/api/support/runs", json=request_body)
    assert response.status_code == 202
    assert _args(_output(client, response.json()["run_id"])[0]) == argv


def test_confirm_from_the_front(tmp_path: Path):
    """Confirmación desde el front"""
    client = _client(tmp_path)
    response = client.post("/api/support/runs", json={"action": "replay-from", "component": "planner",
                                                      "outbox_id": 12, "confirm": TOKEN})
    assert response.status_code == 202
    assert _args(_output(client, response.json()["run_id"])[0]) == [
        "--json", "replay-from", "planner", "12", "--confirm", TOKEN, "--origin", "web"]


@pytest.mark.parametrize("request_body", [
    {"action": "drop"},
    {"action": "replay-from", "component": "views", "outbox_id": 1},
    {"action": "replay-from", "component": "planner", "outbox_id": 0},
    {"action": "replay-from", "component": "planner"},
    {"action": "rebuild", "view": "view_orders"},
    {"action": "republish", "mode": "deleted"},
    {"action": "republish"},
    {"action": "republish", "mode": "truncated", "confirm": "0123456789ABCDEZ"},
    {"action": "republish", "mode": "truncated", "confirm": "--origin cli"},
    {"action": "rebuild", "view": "orders", "mode": "truncated"},
    {"action": "status", "confirm": TOKEN},
    {"action": "status", "origin": "cli"},
    {"action": "replay-from", "component": "planner", "outbox_id": True},
    {"action": "replay-from", "component": "planner", "outbox_id": "12"},
    {"action": "replay-from", "component": "planner", "outbox_id": 12.0},
])
def test_support_request_outside_the_whitelist(tmp_path: Path, request_body: dict):
    """Petición de soporte fuera de la lista blanca"""
    response = _client(tmp_path).post("/api/support/runs", json=request_body)
    assert response.status_code == 422
    assert not _launched(tmp_path)


def test_support_while_another_command_runs(tmp_path: Path):
    """Soporte con otro comando en curso"""
    client = _client(tmp_path, notice_body=SLOW)
    first = client.post("/api/commands", json={"command": "seed"})
    assert first.status_code == 202
    _wait_until(lambda: (tmp_path / "notice" / "launched").exists())
    response = client.post("/api/support/runs", json={"action": "status"})
    assert response.status_code == 409
    assert not _launched(tmp_path)
    client.app.state.runner.shutdown()


def test_a_notice_command_waits_for_a_support_run(tmp_path: Path):
    """Ejecución única también al revés: con una ejecución de soporte en curso, el menú de comandos responde 409"""
    app = create_app(notice_script=_script(tmp_path, "notice"), support_script=_script(tmp_path, "support", SLOW),
                     support=True)
    client = TestClient(app, base_url="http://localhost")
    assert client.post("/api/support/runs", json={"action": "status"}).status_code == 202
    _wait_until(lambda: _launched(tmp_path))
    assert client.post("/api/commands", json={"command": "seed"}).status_code == 409
    app.state.runner.shutdown()


def test_the_support_script_loads_the_env_file_and_runs_eda_support():
    """scripts/support.sh ejecuta eda-support con .env, como scripts/notice.sh"""
    script = Path(__file__).resolve().parents[1] / "scripts" / "support.sh"
    text = script.read_text()
    assert script.stat().st_mode & stat.S_IXUSR
    assert "--env-file .env" in text and 'eda-support "$@"' in text


def test_support_endpoints_are_read_only_for_the_backend(tmp_path: Path):
    """La conexión del backend no escribe: el endpoint de soporte solo lanza el proceso (sin abrir la base)"""
    opened = threading.Event()
    import eda_retail.web.app as web_app

    original = web_app.read_only_connect
    web_app.read_only_connect = lambda *a, **k: opened.set() or original(*a, **k)
    try:
        client = _client(tmp_path)
        response = client.post("/api/support/runs", json={"action": "status"})
        assert response.status_code == 202
        _output(client, response.json()["run_id"])
    finally:
        web_app.read_only_connect = original
    assert not opened.is_set()
