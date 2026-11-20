# Agent harness

Objective: implement `specs/agent-harness/spec.md`, a single layer around every LLM call of the four LLM agents (interpreter, planner, replanner, writer). Branch `feat/agent-harness` from main after #116. Delivery strategy: ask-on-risk (forecast ~900 authored lines, so a chain of PRs).
Test runners: unit `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; lint `uv run ruff check`; integration only in CI.

## Specs

- S1 «Contrato declarado por agente»: `AgentSpec` per agent with `prompt_version`, output model, fixed business check (if any) and `max_tokens`; context-dependent checks are passed per run and the generation limit is `LLM_MAX_GENERATIONS` for every agent; no LLM call for an agent without one; a prompt change without a new `prompt_version` fails the contract test.
- S2 «El harness no escribe ni publica»: the harness runs before the component transaction and returns an `AgentOutcome` (accepted value or escalation reason, plus calls); it writes no table, publishes nothing, runs no effect.
- S3 «Entrada no confiable delimitada y acotada»: the notice goes delimited; a closing delimiter inside the text is neutralized; input over `MAX_UNTRUSTED_INPUT_CHARS` (default 8000) is rejected without calling the LLM and the incident is escalated with `input_too_long`.
- S4 «Grounding de las referencias»: an interpreter `order_id` must appear literally in the notice text; a plan step `order_id` must be one of the incident's orders in the command; otherwise a rejected generation with `ungrounded_reference` (counts for the limit, fed back to the LLM, `llm_output_invalid` when exhausted); grounding is checked before business rules; a cited but unregistered order still parks.
- S5 «Registro común de las llamadas con la versión del prompt»: one harness function writes `<schema>.llm_calls` and `LlmCallRecorded` in the component transaction; `LlmCallRecorded` v2 adds `prompt_version` and `prompt_hash` (SHA-256 of system prompt and template, without input); no rendered prompt or output in the event; v1 reads as v2 with `prompt_version` `"unknown"` and `prompt_hash` null.
- S6 «Grabación y reproducción de llamadas»: `LLM_RECORD_DIR` records request and response as JSONL keyed by `(agent, prompt_hash, rendered-request hash)`; `LLM_CLIENT=replay` with `LLM_REPLAY_DIR` returns the recording without contacting a model; a miss fails with `replay_miss`, without a real call and without counting as an LLM generation.
- S7 «Evaluación por agente»: `eda-agent-eval <directorio>` reads the harness runs recorded in a directory and reports per agent and `prompt_version`: first-attempt acceptance rate, mean attempts, rejections by reason, escalations by reason, tokens in and out; writes nothing to the database or Kafka.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1, S2 | inline | done (c9c9e9b): `llm/harness.py`, `llm/agents.py` (pinned fingerprints), prompts on `AgentSpec` (rendered prompts byte-identical to base); 7 unit tests GREEN (RED: missing modules) | C1 |
| T2 | S3, S4 | inline | done (c9c9e9b): `delimit`, `max_untrusted_input_chars`, grounding in interpreter, planner, replanner; 9 unit tests GREEN; 6 interpreter integration notices now cite their order (CI) | C1 |
| T3 | S5 | inline | done (d3db0d9): `llm/recording.py` (one recorder for the four agents), `LlmCallRecorded` v2 with upcaster, migration 0029 (`prompt_version`, `prompt_hash` on the five `llm_calls`); 3 unit tests GREEN, planner and migration integration tests (CI); 4 hand-built v1 test payloads moved to v2 | C2 |
| T4 | S6, S7 | inline | done: `llm/replay.py` (RecordingClient, ReplayClient, OutcomeLog), `LLM_CLIENT=replay`, `eda-agent-eval` (`agent_eval/`); 9 unit tests GREEN | C3 |

## Log

- L1 (2026-11-19) User request, verbatim: «implementa la spec del harness de agentes».
- L2 (2026-11-19) Route: inline for every task (no writer trigger: the units share `llm/` and the agent handlers, so they are not disjoint). T3 changes a consumed event contract (high-risk item 3) and gets its own commit.
- L3 (2026-11-19) T1 and T2 written inline. `LlmCall` gains `prompt_version`, `prompt_hash` and `rejection` (not in the event). The writer and baseline_sync replanner requests go through `AgentSpec.request`. Unit suite: 1114 passed; `test_answer_the_interactive_menu` and `test_topology_endpoint` fail locally with and without the change (environmental, green in CI).
- L4 (2026-11-19) T3 written inline. Risk: item 3 (consumed event contract `LlmCallRecorded` v2) and item 1 (migration 0029 adds columns). Own commit; independent verifier at the feature's end. `web/nodes.py` reads the raw event JSON without upcasting, but only fields present in v1 and v2.
- L5 (2026-11-19) T4 written inline. S7 corrected in the spec: the evaluation reads the recorded harness runs (`outcomes-*.jsonl`, with each attempt's rejection) instead of replaying them, because the planner and replanner checks need the incident context (amounts, current plan) that a recording does not carry. The invalid `LLM_CLIENT` message now lists `replay` («ollama, oracle o replay»). `.env.example` not updated: reading it is denied in this session.
- L6 (2026-11-19) Independent verifier: S1–S7 PASS; 1127 unit GREEN; 100 targeted integration tests GREEN locally (interpreter, planner, replanner, executors, migrations, audit, cost, web events); prompt differential 15/16 byte-identical, the difference being the requested `</ aviso >` neutralization. B1 (S1 said `AgentSpec` declares the generation limit): corrected in the spec, since the limit is `LLM_MAX_GENERATIONS` for every agent. Advisory 7 applied: integration test for `input_too_long` publishing `IncidentEscalated`. Open advisories: E5 and baseline_sync (E6) still call `generate` without grounding or input limit; the experiments harness forces `LLM_CLIENT=oracle` for its processes; `prompt_hash` covers declared templates only; `.env.example` not updated.
- L7 (2026-11-20) #117 merged (4eba572). `.env.example` updated by the user (`MAX_UNTRUSTED_INPUT_CHARS`, `LLM_RECORD_DIR`, `LLM_REPLAY_DIR`). Advisory closed on `feat/harness-experiments`: E5 and baseline_sync go through `AgentHarness` (planner and replanner grounding; interpreter input limit and grounding), with two new scenarios in `specs/experiments/spec.md`. E5 test RED (half the foreign-order plans accepted) then GREEN; unit 1128 passed, the two known environmental failures; baseline integration test in CI.
