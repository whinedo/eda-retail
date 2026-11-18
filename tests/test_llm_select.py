"""Cliente LLM de los procesos elegido por `LLM_CLIENT` (spec experiments, «Todos los componentes en procesos
propios»): Ollama por defecto y el oráculo de los experimentos con `LLM_CLIENT=oracle`."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from eda_retail.llm import select
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.llm.generation import LlmRequest
from eda_retail.llm.ollama_client import OllamaClient
from eda_retail.synthetic.generator import generate


def test_ollama_is_the_default_client():
    """LLM simulado en los procesos: sin `LLM_CLIENT` (o vacía, o `ollama`) cada proceso usa Ollama, como en
    producción y en `scripts/run-agents.sh`"""
    for environ in ({}, {"LLM_CLIENT": ""}, {"LLM_CLIENT": "ollama"}):
        assert isinstance(select.client_from_env(environ), OllamaClient)


def test_oracle_client_answers_like_the_harness_oracle():
    """LLM simulado en los procesos: con `LLM_CLIENT=oracle` el proceso responde con el oráculo sobre el conjunto
    `generate(ORACLE_SEED, ORACLE_INCIDENTS)`, el mismo que usa el harness"""
    client = select.client_from_env({"LLM_CLIENT": "oracle", "ORACLE_SEED": "7", "ORACLE_INCIDENTS": "4"})
    assert isinstance(client, FakeLlmClient) and client.model == "oracle"
    n = generate(7, 4).notices[2]
    out = client.complete(LlmRequest(agent="interpreter", incident_id="I1", system="s", prompt=n.text, schema={}))
    assert json.loads(out.text)["order_ids"] == [n.order_id]


@pytest.mark.parametrize(("environ", "message"), [
    ({"LLM_CLIENT": "openai"}, "LLM_CLIENT debe ser ollama u oracle, no 'openai'"),
    ({"LLM_CLIENT": "oracle", "ORACLE_INCIDENTS": "4"}, "LLM_CLIENT=oracle necesita ORACLE_SEED"),
    ({"LLM_CLIENT": "oracle", "ORACLE_SEED": "7", "ORACLE_INCIDENTS": "x"}, "ORACLE_INCIDENTS debe ser un entero"),
])
def test_an_invalid_llm_client_is_a_configuration_error(environ, message):
    """Un valor desconocido de `LLM_CLIENT`, o el oráculo sin su semilla, es un error de configuración en español"""
    with pytest.raises(ValueError, match=message):
        select.client_from_env(environ)


@pytest.mark.parametrize("module", ["planner", "replanner"])
def test_llm_components_take_their_client_from_the_env(monkeypatch, module):
    """Los procesos del planificador y del replanificador usan el cliente de `LLM_CLIENT`"""
    cli = __import__(f"eda_retail.{module}.cli", fromlist=["main"])
    sentinel = object()
    seen = {}
    monkeypatch.setattr(cli, "client_from_env", lambda: sentinel)
    monkeypatch.setattr(cli, "specs", lambda settings, llm, rules, **kw: seen.setdefault("llm", llm) and {})
    monkeypatch.setattr(cli, "serve", lambda *a, **kw: None)
    monkeypatch.setattr(cli, "load_settings", lambda: SimpleNamespace(planner_url="u", replanner_url="u",
                                                                       poc_version=1))
    cli.main([])
    assert seen["llm"] is sentinel


def test_interpreter_takes_its_client_from_the_env(monkeypatch):
    """El proceso del intérprete usa el cliente de `LLM_CLIENT`"""
    from eda_retail.interpreter import cli

    sentinel = object()
    seen = {}

    class _Consumer:
        def with_reconnect(self, _reconnect):
            return self

        def run_forever(self):
            return None

        def close(self):
            return None

    def build(conn, kafka, settings, llm, rules):
        seen["llm"] = llm
        return _Consumer()

    monkeypatch.setattr(cli, "client_from_env", lambda: sentinel)
    monkeypatch.setattr(cli, "build_consumer", build)
    monkeypatch.setattr(cli, "load_settings", lambda: SimpleNamespace(database_url="db", kafka_bootstrap_servers="k"))
    monkeypatch.setattr(cli, "setup_logging", lambda name: None)
    monkeypatch.setattr(cli, "setup_tracing", lambda name, settings: None)
    monkeypatch.setattr(cli, "start_relay", lambda settings, schema: SimpleNamespace(stop=lambda t: None))
    monkeypatch.setattr(cli, "connect", lambda url: None)
    monkeypatch.setattr(cli, "make_consumer", lambda servers, group_id: None)
    monkeypatch.setattr(cli, "_expire_forever", lambda settings, stop: None)
    cli.main([])
    assert seen["llm"] is sentinel
