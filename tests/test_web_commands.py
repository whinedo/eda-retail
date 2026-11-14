"""Ejecución de los comandos de scripts/notice.sh desde el front (spec web-front).

Los tests sustituyen scripts/notice.sh por un script falso: no tocan PostgreSQL, Kafka ni Ollama.
"""

from __future__ import annotations

import json
import stat
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from eda_retail.web.app import create_app

ECHO_ARGS = """#!/usr/bin/env bash
touch "$(dirname "$0")/launched"
echo "argc:$#"
for a in "$@"; do echo "arg:$a"; done
"""


def _script(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "notice.sh"
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _client(script: Path, **kwargs) -> TestClient:
    return TestClient(create_app(notice_script=script, **kwargs), base_url="http://localhost")


def _output(client: TestClient, run_id: str) -> tuple[str, int]:
    """Lee el stream de salida hasta el final: texto completo y código de salida."""
    response = client.get(f"/api/commands/{run_id}/stream")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    text, code, event = "", None, None
    for line in response.text.splitlines():
        if line.startswith("event: "):
            event = line.removeprefix("event: ")
        elif line.startswith("data: "):
            data = json.loads(line.removeprefix("data: "))
            if event == "output":
                text += data["text"]
            elif event == "exit":
                code = data["code"]
    assert code is not None, "el stream debe terminar con el código de salida"
    return text, code


def _wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "tiempo de espera agotado"
        time.sleep(0.05)


def test_run_an_example_notice(tmp_path: Path):
    """Ejecutar un aviso de ejemplo"""
    client = _client(_script(tmp_path, ECHO_ARGS))

    response = client.post("/api/commands", json={"command": "send", "example": "loss"})

    assert response.status_code == 202
    assert response.json()["argv"] == ["scripts/notice.sh", "send", "--example", "loss"]
    text, code = _output(client, response.json()["run_id"])
    assert "arg:send\narg:--example\narg:loss" in text.replace("\r\n", "\n")
    assert code == 0


@pytest.mark.parametrize(
    "body",
    [
        {"command": "rm"},
        {"command": "send", "example": "refund_everyone"},
        {"command": "send", "example": "loss", "text": "dos a la vez"},
        {"command": "send", "text": ""},
        {"command": "send", "text": "x" * 2001},
        {"command": "send", "text": "nul\x00byte"},
        {"command": "seed", "example": "loss"},
        {"command": "interactive", "timeout": 0},
        {"command": "interactive", "timeout": 601},
        {"command": "send", "extra": "--carrier evil"},
    ],
)
def test_argument_outside_the_whitelist(tmp_path: Path, body: dict):
    """Argumento fuera de la lista blanca"""
    client = _client(_script(tmp_path, ECHO_ARGS))

    assert client.post("/api/commands", json=body).status_code == 422
    assert not (tmp_path / "launched").exists()


def test_free_text_is_a_single_argument(tmp_path: Path):
    """Texto libre"""
    client = _client(_script(tmp_path, ECHO_ARGS))
    text = "Retraso; rm -rf / $(whoami) `id` 'x' \"y\" && echo pwned"

    run = client.post("/api/commands", json={"command": "send", "text": text}).json()

    output, code = _output(client, run["run_id"])
    lines = output.replace("\r\n", "\n").splitlines()
    assert lines[:3] == ["argc:2", "arg:send", f"arg:--text={text}"]
    assert code == 0


def test_free_text_that_looks_like_an_option(tmp_path: Path):
    """Un texto que empieza por guion sigue siendo el texto del aviso, no una opción"""
    client = _client(_script(tmp_path, ECHO_ARGS))

    run = client.post("/api/commands", json={"command": "send", "text": "--carrier evil"}).json()

    assert run["argv"] == ["scripts/notice.sh", "send", "--text=--carrier evil"]
    assert _output(client, run["run_id"])[1] == 0


def test_agents_stopped_warning_is_shown_as_is(tmp_path: Path):
    """Agentes parados"""
    warning = "aviso: los agentes no están en marcha (scripts/run-agents.sh); los avisos no avanzarán"
    client = _client(_script(tmp_path, f"#!/usr/bin/env bash\necho '{warning}' >&2\n"))

    run = client.post("/api/commands", json={"command": "send"}).json()

    output, _ = _output(client, run["run_id"])
    assert warning in output


def test_second_simultaneous_run(tmp_path: Path):
    """Segunda ejecución simultánea"""
    client = _client(_script(tmp_path, "#!/usr/bin/env bash\nread -r line\necho \"got:$line\"\n"))
    first = client.post("/api/commands", json={"command": "interactive", "timeout": 30}).json()

    assert client.post("/api/commands", json={"command": "seed"}).status_code == 409

    client.post(f"/api/commands/{first['run_id']}/input", json={"line": "q"})
    _output(client, first["run_id"])
    assert client.post("/api/commands", json={"command": "seed"}).status_code == 202


def test_answer_the_interactive_menu(tmp_path: Path):
    """Responder al menú"""
    client = _client(_script(tmp_path, "#!/usr/bin/env bash\necho 'elige:'\nread -r line\necho \"got:$line\"\n"))
    run = client.post("/api/commands", json={"command": "interactive", "timeout": 60}).json()
    assert run["argv"] == ["scripts/notice.sh", "interactive", "--timeout", "60"]

    assert client.post(f"/api/commands/{run['run_id']}/input", json={"line": "2"}).status_code == 202

    output, code = _output(client, run["run_id"])
    assert "elige:" in output and "got:2" in output
    assert code == 0


def test_interactive_runs_in_a_pseudo_terminal(tmp_path: Path):
    """interactive se ejecuta con una pseudoterminal; el resto, no"""
    body = "#!/usr/bin/env bash\nif [ -t 0 ]; then echo tty; else echo notty; fi\n"
    client = _client(_script(tmp_path, body + "read -r -t 5 line || true\n"))

    run = client.post("/api/commands", json={"command": "interactive", "timeout": 5}).json()
    client.post(f"/api/commands/{run['run_id']}/input", json={"line": ""})
    assert "tty" in _output(client, run["run_id"])[0].split()

    run = client.post("/api/commands", json={"command": "seed"}).json()
    assert "notty" in _output(client, run["run_id"])[0].split()


def test_input_rules(tmp_path: Path):
    """Líneas de entrada: solo a una sesión interactiva viva, sin saltos de línea y como mucho 200 caracteres"""
    client = _client(_script(tmp_path, "#!/usr/bin/env bash\nread -r line\necho \"got:$line\"\n"))
    run = client.post("/api/commands", json={"command": "interactive", "timeout": 30}).json()
    url = f"/api/commands/{run['run_id']}/input"

    assert client.post(url, json={"line": "a\nb"}).status_code == 422
    assert client.post(url, json={"line": "x" * 201}).status_code == 422
    assert client.post("/api/commands/does-not-exist/input", json={"line": "1"}).status_code == 404

    client.post(url, json={"line": "ok"})
    _output(client, run["run_id"])
    assert client.post(url, json={"line": "late"}).status_code == 409

    seed = client.post("/api/commands", json={"command": "seed"}).json()
    _output(client, seed["run_id"])
    assert client.post(f"/api/commands/{seed['run_id']}/input", json={"line": "1"}).status_code == 409


def test_unknown_run_stream_is_404(tmp_path: Path):
    """El stream de una ejecución desconocida responde 404"""
    client = _client(_script(tmp_path, ECHO_ARGS))
    assert client.get("/api/commands/does-not-exist/stream").status_code == 404


def test_run_that_never_ends(tmp_path: Path):
    """Ejecución que no termina: se termina el proceso y su grupo al pasar el tiempo máximo"""
    pid_file = tmp_path / "child.pid"
    script = _script(tmp_path, f"#!/usr/bin/env bash\nsleep 60 &\necho $! > '{pid_file}'\nwait\n")
    client = _client(script, run_max_seconds=1)

    run = client.post("/api/commands", json={"command": "seed"}).json()

    _, code = _output(client, run["run_id"])
    assert code != 0
    child = int(pid_file.read_text())
    _wait_until(lambda: not _alive(child))


def _alive(pid: int) -> bool:
    import os

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True
