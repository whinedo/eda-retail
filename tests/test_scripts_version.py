"""Versión de la PoC desde los scripts: `--version 1|2` en start-all.sh, run-agents.sh y run-web.sh.

Solo se prueba lo que pasa antes de arrancar nada (ayuda y una versión no válida): `docker`, `uv`, `npm` y
`curl` se sustituyen por programas que fallan, así que un script que siguiera adelante no arrancaría nada.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
SCRIPTS = ["start-all.sh", "run-agents.sh", "run-web.sh"]


@pytest.fixture
def no_tools(tmp_path) -> dict[str, str]:
    """Entorno con `docker`, `uv`, `npm` y `curl` falsos que fallan siempre."""
    for tool in ("docker", "uv", "npm", "curl"):
        stub = tmp_path / tool
        stub.write_text("#!/bin/sh\necho \"$0 no debe ejecutarse en este test\" >&2\nexit 97\n")
        stub.chmod(0o755)
    return {**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"}


def _run(script: str, *args: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(ROOT / "scripts" / script), *args], capture_output=True, text=True,
                          env=env, timeout=20, check=False)


@pytest.mark.parametrize("script", SCRIPTS)
def test_help_documents_the_version_option(script, no_tools):
    """--help: cada script documenta --version 1|2"""
    result = _run(script, "--help", env=no_tools)
    assert result.returncode == 0
    assert "--version 1|2" in result.stdout


@pytest.mark.parametrize("script", SCRIPTS)
@pytest.mark.parametrize("args", [("--version", "3"), ("--version", "dos"), ("--version",)])
def test_invalid_version_is_an_error_with_usage(script, args, no_tools):
    """Versión no válida: error con el uso y sin arrancar nada"""
    result = _run(script, *args, env=no_tools)
    assert result.returncode == 2
    assert "error: --version debe ser 1 o 2" in result.stderr
    assert "Uso: scripts/" in result.stderr
    assert "no debe ejecutarse" not in result.stderr


@pytest.mark.parametrize("script", SCRIPTS)
def test_the_version_flag_is_exported_to_the_processes(script):
    """--version exporta POC_VERSION, que gana a la de .env (uv --env-file y load_env_file no la pisan)"""
    text = (ROOT / "scripts" / script).read_text()
    assert 'export POC_VERSION="$2"' in text


def test_start_all_passes_the_version_to_run_agents_and_run_web():
    """start-all.sh lanza run-agents.sh y run-web.sh con el POC_VERSION exportado"""
    text = (ROOT / "scripts" / "start-all.sh").read_text()
    assert text.index('export POC_VERSION="$2"') < text.index("scripts/run-agents.sh --logs")


def test_stop_all_stops_the_version_2_processes():
    """stop-all.sh también para eda-mail-server y eda-carrier-agent"""
    text = (ROOT / "scripts" / "stop-all.sh").read_text()
    pattern = re.search(r"^PATTERN='(.*)'$", text, re.MULTILINE).group(1)
    for command in (".venv/bin/python .venv/bin/eda-mail-server", ".venv/bin/python .venv/bin/eda-carrier-agent",
                    ".venv/bin/python .venv/bin/eda-planner"):
        assert re.search(pattern, command), command
