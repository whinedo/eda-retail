"""Ejecuciones de los servicios simulados de un proceso ejecutor, anotadas en un fichero JSONL por
`SERVICE_EXECUTIONS_LOG` (spec experiments, «Todos los componentes en procesos propios»)."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

from eda_retail.step_executors import executions
from eda_retail.step_executors.services import SERVICE_OF, FakeService


def test_without_the_env_the_executor_keeps_its_own_service():
    """Servicios simulados con su registro de ejecuciones: sin `SERVICE_EXECUTIONS_LOG` no cambia nada (el ejecutor
    usa su servicio simulado sin anotar)"""
    assert executions.service_from_env("file_claim", {}) is None
    assert executions.service_from_env("file_claim", {"SERVICE_EXECUTIONS_LOG": " "}) is None


def test_each_execution_is_recorded_once_with_its_key(tmp_path: Path):
    """Servicios simulados con su registro de ejecuciones: cada ejecución real del servicio deja una línea con su
    `idempotency_key`; una llamada repetida con la misma clave no ejecuta otra vez y no deja línea"""
    path = tmp_path / "runs" / "executor-file_claim.jsonl"
    service = executions.service_from_env("file_claim", {"SERVICE_EXECUTIONS_LOG": str(path)})
    assert isinstance(service, FakeService) and service.name == SERVICE_OF["file_claim"] == "claims"
    first = service.call("k1")
    assert service.call("k1") == first
    service.call("k2")
    assert executions.read_executions([path]) == Counter({("claims", "k1"): 1, ("claims", "k2"): 1})


def test_a_non_idempotent_service_records_every_execution(tmp_path: Path):
    """Sin deduplicación (E1) el servicio repite el efecto y cada repetición queda anotada"""
    path = tmp_path / "p.jsonl"
    service = FakeService("payments", "RF", idempotent=False, on_execute=executions.ExecutionLog(path).record)
    service.call("k")
    service.call("k")
    assert executions.read_executions([path]) == Counter({("payments", "k"): 2})


def test_read_executions_skips_missing_files_and_a_cut_last_line(tmp_path: Path):
    """Un proceso que muere a mitad de una línea no rompe la lectura: la línea cortada se ignora"""
    path = tmp_path / "a.jsonl"
    path.write_text('{"service": "mail", "idempotency_key": "k"}\n{"service": "ma', encoding="utf-8")
    assert executions.read_executions([path, tmp_path / "missing.jsonl"]) == Counter({("mail", "k"): 1})


@pytest.mark.parametrize("env", [{}, {"SERVICE_EXECUTIONS_LOG": "x.jsonl"}])
def test_the_executor_process_takes_its_service_from_the_env(monkeypatch, env):
    """El proceso `eda-step-executor` pasa a su acción el servicio de `SERVICE_EXECUTIONS_LOG` (ninguno sin ella)
    y el cliente LLM de `LLM_CLIENT`"""
    from eda_retail.step_executors import cli

    for name in ("SERVICE_EXECUTIONS_LOG",):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, str(Path.cwd() / value) if name == "SERVICE_EXECUTIONS_LOG" else value)
    sentinel = object()
    seen = {}
    monkeypatch.setattr(cli, "client_from_env", lambda: sentinel)
    monkeypatch.setattr(cli, "specs", lambda settings, action, llm, rules, **kw: seen.update(llm=llm, **kw) or {})
    monkeypatch.setattr(cli, "serve", lambda *a, **kw: None)
    monkeypatch.setattr(cli, "load_settings", lambda: SimpleNamespace(executor_max_concurrency=5))
    monkeypatch.setattr(cli, "url", lambda settings, action: "http://127.0.0.1:1")
    cli.main(["file_claim"])
    assert seen["llm"] is sentinel
    assert (seen["service"] is None) == (not env)
    if env:
        assert seen["service"].on_execute is not None
