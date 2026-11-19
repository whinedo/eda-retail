# Agent harness

Objective: implement `specs/agent-harness/spec.md`, a single layer around every LLM call of the four LLM agents (interpreter, planner, replanner, writer). Branch `feat/agent-harness` from main after #116. Delivery strategy: ask-on-risk (forecast ~900 authored lines, so a chain of PRs).
Test runners: unit `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; lint `uv run ruff check`; integration only in CI.

## Specs

- S1 «Contrato declarado por agente»: `AgentSpec` per agent with `prompt_version`, output model, business check, generation limit and `max_tokens`; no LLM call for an agent without one; a prompt change without a new `prompt_version` fails the contract test.
- S2 «El harness no escribe ni publica»: the harness runs before the component transaction and returns an `AgentOutcome` (accepted value or escalation reason, plus calls); it writes no table, publishes nothing, runs no effect.
- S3 «Entrada no confiable delimitada y acotada»: the notice goes delimited; a closing delimiter inside the text is neutralized; input over `MAX_UNTRUSTED_INPUT_CHARS` (default 8000) is rejected without calling the LLM and the incident is escalated with `input_too_long`.
- S4 «Grounding de las referencias»: an interpreter `order_id` must appear literally in the notice text; a plan step `order_id` must be one of the incident's orders in the command; otherwise a rejected generation with `ungrounded_reference` (counts for the limit, fed back to the LLM, `llm_output_invalid` when exhausted); grounding is checked before business rules; a cited but unregistered order still parks.
- S5 «Registro común de las llamadas con la versión del prompt»: one harness function writes `<schema>.llm_calls` and `LlmCallRecorded` in the component transaction; `LlmCallRecorded` v2 adds `prompt_version` and `prompt_hash` (SHA-256 of system prompt and template, without input); no rendered prompt or output in the event; v1 reads as v2 with `prompt_version` `"unknown"` and `prompt_hash` null.
- S6 «Grabación y reproducción de llamadas»: `LLM_RECORD_DIR` records request and response as JSONL keyed by `(agent, prompt_hash, rendered-request hash)`; `LLM_CLIENT=replay` with `LLM_REPLAY_DIR` returns the recording without contacting a model; a miss fails with `replay_miss`, without a real call and without counting as an LLM generation.
- S7 «Evaluación por agente»: `eda-agent-eval <directorio>` replays a recorded directory and reports per agent and `prompt_version`: first-attempt acceptance rate, mean attempts, rejections by reason, escalations by reason, tokens in and out; writes nothing to the database or Kafka.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1, S2 | inline | done (c9c9e9b): `llm/harness.py`, `llm/agents.py` (pinned fingerprints), prompts on `AgentSpec` (rendered prompts byte-identical to base); 7 unit tests GREEN (RED: missing modules) | C1 |
| T2 | S3, S4 | inline | done (c9c9e9b): `delimit`, `max_untrusted_input_chars`, grounding in interpreter, planner, replanner; 9 unit tests GREEN; 6 interpreter integration notices now cite their order (CI) | C1 |
| T3 | S5 | inline | done: `llm/recording.py` (one recorder for the four agents), `LlmCallRecorded` v2 with upcaster, migration 0029 (`prompt_version`, `prompt_hash` on the five `llm_calls`); 3 unit tests GREEN, planner and migration integration tests (CI); 4 hand-built v1 test payloads moved to v2 | C2 |
| T4 | S6, S7 | inline | pending | |

## Log

- L1 (2026-11-19) User request, verbatim: «implementa la spec del harness de agentes».
- L2 (2026-11-19) Route: inline for every task (no writer trigger: the units share `llm/` and the agent handlers, so they are not disjoint). T3 changes a consumed event contract (high-risk item 3) and gets its own commit.
- L3 (2026-11-19) T1 and T2 written inline. `LlmCall` gains `prompt_version`, `prompt_hash` and `rejection` (not in the event). The writer and baseline_sync replanner requests go through `AgentSpec.request`. Unit suite: 1114 passed; `test_answer_the_interactive_menu` and `test_topology_endpoint` fail locally with and without the change (environmental, green in CI).
- L4 (2026-11-19) T3 written inline. Risk: item 3 (consumed event contract `LlmCallRecorded` v2) and item 1 (migration 0029 adds columns). Own commit; independent verifier at the feature's end. `web/nodes.py` reads the raw event JSON without upcasting, but only fields present in v1 and v2.
