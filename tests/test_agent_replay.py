"""Grabación, reproducción y evaluación por agente (spec agent-harness), sin base de datos ni modelo."""

from __future__ import annotations

import dataclasses
import json
import uuid

import pytest

from eda_retail.agent_eval import cli as eval_cli
from eda_retail.agent_eval.report import agent_report
from eda_retail.experiments.oracle import Oracle
from eda_retail.infra.commands import CommandEnvelope
from eda_retail.interpreter import handler as interpreter
from eda_retail.llm import ollama_client, select
from eda_retail.llm.agents import AGENTS
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.llm.generation import Generation, LlmCall
from eda_retail.llm.prompts.planner import plan_request
from eda_retail.llm.replay import REPLAY_MISS, OutcomeLog, RecordingClient, ReplayClient, ReplayMiss, read_jsonl
from eda_retail.queries.cost_per_incident import budget_from_cost
from eda_retail.rules import BusinessRules
from eda_retail.synthetic.generator import generate

RULES = BusinessRules()
BUDGET = budget_from_cost(None, RULES)


def _interpret(llm, notice) -> interpreter.Prepared:
    payload = {"carrier_notice_id": notice.carrier_notice_id, "carrier": notice.carrier, "raw_text": notice.text}
    command = CommandEnvelope(command_id=uuid.uuid4(), command_type="OpenIncident", correlation_id=str(uuid.uuid4()),
                              causation_id=uuid.uuid4(), payload=payload)
    preparer = interpreter.NoticePreparer(llm, RULES, interpreted=lambda n: False, budget=lambda i: BUDGET,
                                          known_orders=lambda order_ids: order_ids)
    return preparer(command, interpreter.OpenIncident.model_validate(payload))


def _summary(prepared: interpreter.Prepared):
    value = prepared.interpretation.model_dump(mode="json") if prepared.interpretation else None
    return value, prepared.reason, [(c.attempt, c.validation_passed, c.rejection) for c in prepared.calls]


def _with_one_invented_order(data):
    """El oráculo, salvo la primera respuesta, que inventa un pedido (para tener un rechazo grabado)."""
    oracle, first = Oracle(data), [True]

    def respond(request):
        answer = oracle(request)
        if first[0]:
            first[0] = False
            return {**answer, "order_ids": [*answer["order_ids"], "P-INVENTED"]}
        return answer

    return FakeLlmClient(responder=respond)


def test_replay_is_identical_without_a_model(tmp_path, monkeypatch):
    """Reproducción idéntica sin modelo"""
    data = generate(42, 6)
    monkeypatch.setenv("LLM_RECORD_DIR", str(tmp_path))
    recorded = [_summary(_interpret(RecordingClient(_with_one_invented_order(data), tmp_path), n))
                for n in data.notices]
    monkeypatch.delenv("LLM_RECORD_DIR")

    def no_ollama(*args, **kwargs):
        raise AssertionError("la reproducción no contacta con Ollama")

    monkeypatch.setattr(ollama_client.OllamaClient, "__init__", no_ollama)
    replay = select.client_from_env({"LLM_CLIENT": "replay", "LLM_REPLAY_DIR": str(tmp_path)})
    replayed = [_summary(_interpret(replay, n)) for n in data.notices]

    assert isinstance(replay, ReplayClient)
    assert replayed == recorded
    assert recorded[0][2][0] == (1, False, "ungrounded_reference")


def test_a_request_not_recorded_fails_with_replay_miss(tmp_path):
    """Petición sin grabar"""
    [notice] = generate(42, 1).notices

    with pytest.raises(ReplayMiss, match=REPLAY_MISS) as raised:
        _interpret(ReplayClient(tmp_path), notice)
    assert raised.value.code == REPLAY_MISS


def test_a_recording_of_another_prompt_version_misses(tmp_path):
    """Grabación de otra versión del prompt"""
    v3 = dataclasses.replace(AGENTS["planner"], prompt_version="3", system=AGENTS["planner"].system + "\nv3")
    v4 = dataclasses.replace(AGENTS["planner"], prompt_version="4", system=AGENTS["planner"].system + "\nv4")
    prompt = plan_request("I-1", "loss", "high", 0, "- P-1: importe 40").prompt
    recorder = RecordingClient(FakeLlmClient([{"steps": []}]), tmp_path)
    recorder.complete(v3.request("I-1", prompt))

    replay = ReplayClient(tmp_path)

    assert replay.complete(v3.request("I-1", prompt)).text == '{"steps": []}'
    with pytest.raises(ReplayMiss):
        ReplayClient(tmp_path).complete(v4.request("I-1", prompt))


def test_the_recording_has_no_personal_data_and_lives_outside_the_log(tmp_path):
    [notice] = generate(42, 1).notices
    RecordingClient(Oracle(generate(42, 1)).client(), tmp_path).complete(
        interpreter.notice_request("I-1", notice.carrier, notice.text))

    [record] = read_jsonl(tmp_path, "calls")

    assert set(record) == {"agent", "prompt_version", "prompt_hash", "request_hash", "response"}
    assert record["prompt_hash"] == AGENTS["interpreter"].prompt_hash


def test_replay_needs_its_directory():
    with pytest.raises(ValueError, match="LLM_REPLAY_DIR"):
        select.client_from_env({"LLM_CLIENT": "replay"})


def test_record_dir_wraps_the_chosen_client(tmp_path):
    client = select.client_from_env({"LLM_CLIENT": "oracle", "ORACLE_SEED": "7", "ORACLE_INCIDENTS": "2",
                                     "LLM_RECORD_DIR": str(tmp_path)})
    assert isinstance(client, RecordingClient)


# Evaluación por agente


def _call(attempt: int, passed: bool, rejection: str | None = None) -> LlmCall:
    return LlmCall(agent="planner", model="m", tokens_in=10, tokens_out=2, latency_ms=1, validation_passed=passed,
                   attempt=attempt, prompt_version="1", rejection=rejection)


def test_report_of_a_recorded_directory(tmp_path, monkeypatch, capsys):
    """Informe de un directorio grabado"""
    import confluent_kafka
    import psycopg

    def forbidden(*args, **kwargs):
        raise AssertionError("eda-agent-eval no escribe en la base ni en Kafka")

    monkeypatch.setattr(psycopg, "connect", forbidden)
    monkeypatch.setattr(confluent_kafka, "Producer", forbidden)
    log = OutcomeLog(tmp_path)
    request = AGENTS["planner"].request("I-1", "p")
    for _ in range(15):
        log(request, Generation(object(), None, [_call(1, True)]))
    for _ in range(5):
        log(request, Generation(object(), None, [_call(1, False, "refund_limit"), _call(2, True)]))

    assert eval_cli.main([str(tmp_path)]) == 0
    report = json.loads(capsys.readouterr().out)

    planner = report[f"planner@{AGENTS['planner'].prompt_version}"]
    assert planner["runs"] == 20
    assert planner["first_attempt_rate"] == 0.75
    assert planner["mean_attempts"] == 1.25
    assert planner["rejections"] == {"refund_limit": 5}
    assert planner["escalations"] == {}
    assert (planner["tokens_in"], planner["tokens_out"]) == (250, 50)


def test_report_counts_escalations_by_reason(tmp_path):
    log = OutcomeLog(tmp_path)
    request = AGENTS["interpreter"].request("I-1", "p")
    log(request, Generation(None, "input_too_long"))
    log(request, Generation(None, "llm_output_invalid", [_call(i, False, "ungrounded_reference") for i in (1, 2, 3)]))

    interp = agent_report(tmp_path)["interpreter@1"]

    assert interp["escalations"] == {"input_too_long": 1, "llm_output_invalid": 1}
    assert interp["rejections"] == {"ungrounded_reference": 3}
    assert interp["first_attempt_rate"] == 0.0


def test_the_outcome_log_keeps_the_full_text_of_each_rejection(tmp_path):
    """Texto completo de los rechazos"""
    log = OutcomeLog(tmp_path)
    request = AGENTS["replanner"].request("I-1", "p")
    detail = "invalid_plan (S4 depende de pasos inexistentes: ['S3'])"
    log(request, Generation(object(), None, [_call(1, False, "invalid_plan"), _call(2, True)], [detail]))
    log(request, Generation(None, "budget_exhausted"))

    first, second = read_jsonl(tmp_path, "outcomes")

    assert [a["error"] for a in first["attempts"]] == [detail, None]
    assert [a["rejection"] for a in first["attempts"]] == ["invalid_plan", None]
    assert second["attempts"] == []
    assert agent_report(tmp_path)["replanner@1"]["rejections"] == {"invalid_plan": 1}


def test_eval_rejects_a_missing_directory(tmp_path, capsys):
    assert eval_cli.main([str(tmp_path / "nope")]) == 2
    assert "no es un directorio" in capsys.readouterr().err
