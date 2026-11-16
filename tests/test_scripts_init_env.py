"""scripts/init-env.sh: creates `.env` from `.env.example` and generates `A2A_CARRIER_TOKEN`.

The script runs on a copy of the project layout in a temporary directory (`scripts/init-env.sh` and a
fake `.env.example`), so it never touches the real `.env`.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[1]
EXAMPLE = "POSTGRES_USER=eda\nLLM_MODEL=qwen2.5:7b-instruct\nA2A_CARRIER_TOKEN=\n"


def _project(tmp_path: Path, example: str = EXAMPLE) -> Path:
    (tmp_path / "scripts").mkdir()
    shutil.copy(ROOT / "scripts" / "init-env.sh", tmp_path / "scripts" / "init-env.sh")
    (tmp_path / ".env.example").write_text(example)
    return tmp_path


def _run(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(project / "scripts" / "init-env.sh"), *args], capture_output=True,
                          text=True, timeout=20, check=False)


def _token(env_text: str) -> str:
    match = re.search(r"^A2A_CARRIER_TOKEN=(.*)$", env_text, re.MULTILINE)
    assert match, env_text
    return match.group(1)


def test_creates_env_from_example_with_a_generated_token(tmp_path):
    """Sin .env: lo crea a partir de .env.example y genera A2A_CARRIER_TOKEN"""
    project = _project(tmp_path)
    result = _run(project)
    assert result.returncode == 0, result.stderr
    env = (project / ".env").read_text()
    assert "POSTGRES_USER=eda" in env
    assert "LLM_MODEL=qwen2.5:7b-instruct" in env
    token = _token(env)
    assert re.fullmatch(r"[0-9a-f]{64}", token)
    assert env.count("A2A_CARRIER_TOKEN=") == 1
    assert token not in result.stdout + result.stderr


def test_appends_the_token_when_the_example_has_no_line(tmp_path):
    """.env.example sin la línea del token: la añade al final"""
    project = _project(tmp_path, "POSTGRES_USER=eda\n")
    assert _run(project).returncode == 0
    env = (project / ".env").read_text()
    assert env.startswith("POSTGRES_USER=eda\n")
    assert re.fullmatch(r"[0-9a-f]{64}", _token(env))


def test_keeps_an_existing_env_and_its_token(tmp_path):
    """Con .env y token: no cambia nada"""
    project = _project(tmp_path)
    (project / ".env").write_text("LLM_MODEL=llama3.1\nA2A_CARRIER_TOKEN=mine\n")
    result = _run(project)
    assert result.returncode == 0
    assert (project / ".env").read_text() == "LLM_MODEL=llama3.1\nA2A_CARRIER_TOKEN=mine\n"
    assert "ya existe" in result.stdout


def test_fills_an_empty_token_in_an_existing_env(tmp_path):
    """Con .env y token vacío: solo rellena el token"""
    project = _project(tmp_path)
    (project / ".env").write_text("LLM_MODEL=llama3.1\nA2A_CARRIER_TOKEN=\n")
    assert _run(project).returncode == 0
    env = (project / ".env").read_text()
    assert env.startswith("LLM_MODEL=llama3.1\n")
    assert re.fullmatch(r"[0-9a-f]{64}", _token(env))


def test_fails_without_example(tmp_path):
    """Sin .env ni .env.example: error y no crea nada"""
    project = _project(tmp_path)
    (project / ".env.example").unlink()
    result = _run(project)
    assert result.returncode == 1
    assert "falta .env.example" in result.stderr
    assert not (project / ".env").exists()
