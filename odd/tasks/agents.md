# Feature: agents (fase 3: llm/ y agents/)

Objective: phase 3 of CLAUDE.md work order — LLM client (real and deterministic fake) with schema validation, and the interpreter, planner, replanner and writer agents; plus the `incident_status` view pulled forward from phase 5 (user decision L2).
Branch: `feat/infra` (continues) · Delivery: ask-on-risk · Runner: `uv run pytest` (LLM always the fake client; real runs are the user's, per ~/CLAUDE.md).

## Specs

Scenario names quoted verbatim; one test per scenario, docstring = scenario name.

- S1 LLM call record — observability-audit "Registro de las llamadas al LLM": scenario "Llamada del planificador". New event `LlmCallRecorded` (`incident_id`, `agent`, `model`, `tokens_in`, `tokens_out`, `latency_ms`, `validation_passed`, `attempt`) and command `RecordLlmCall`, one per call, accepted or not (user decision L3). Catalog (docs/README.md) and spec updated in the same change.
- S2 llm-boundaries "Salida con esquema validado" ("Plan sin campo obligatorio"), "El LLM no ejecuta efectos" ("Agente que intenta actuar"), "Reintentos de generación acotados" ("Tres planes inválidos seguidos", `llm_output_invalid`), "Presupuesto por incidencia" ("Presupuesto de tokens agotado", `budget_exhausted`).
- S3 incident-interpretation: "Aviso de retraso simple", "Aviso ambiguo", "Aviso con instrucciones incrustadas"; interpreter has no tools.
- S4 incident-planning "Plan según la gravedad": "Retraso leve", "Pérdida del paquete"; "Plan con una acción no permitida" (rejection asks the planner for a new plan within llm-boundaries limits).
- S5 incident-planning "Revisión del plan ante un fallo": "Sin stock en el almacén"; "Estabilidad de los step_id"; "Cierre de la incidencia": "Revisiones agotadas" (`max_revisions`); replanner reads the plan only from `incident_status`.
- S6 step-execution "Contenido redactado por el LLM": writer agent + deterministic draft validation ("Borrador con datos de otro pedido": validation part; `StepFailed invalid_draft` comes with executors in phase 4).
- S7 cqrs-views for `incident_status` (pulled forward): "Incidencia con plan en curso", "Evento duplicado en un proyector", "Reinicio de un proyector", "Consulta del estado", "Lectura justo después de escribir"; llm-boundaries "Incidencia escalada" (view shows `escalated` and reason).
- S8 CLAUDE.md rule 6: the LLM call happens before the transaction opens; inbox + command in one transaction; duplicates are detected before paying for an LLM call.

Assumptions (marked):
- A1 (rewritten by L15) LLM via local Ollama `/api/chat` (`OLLAMA_HOST`, `LLM_MODEL` default `llama3.1`, `LLM_SEED` 42, `temperature` 0, `LLM_TIMEOUT_SECONDS` 120); structured output via `format` JSON schema with `$ref` inlined, then pydantic validation in code. No tools are sent.
- A2 Limits by env: `LLM_MAX_GENERATIONS` 3, `LLM_MAX_TOKENS_PER_INCIDENT` 50000, `LLM_MAX_CALLS_PER_INCIDENT` 10, `MAX_PLAN_REVISIONS` 2.
- A3 `severity` `low` gets the fixed one-step `notify_customer` plan in code, without calling the LLM.
- A4 Budget is checked through the command handler (write-side validation), not a view.
- A5 `EscalateIncident` on an incident that does not exist yet is accepted when it belongs to a registered notice (interpreter exhausting its generations); the incident is created `escalated` with `cause` `unknown`.
- A6 `RecordLlmCall` does not take part in optimistic concurrency (append-only fact; it does not bump `version`).
- A7 Plan step `params` from the LLM are `order_id` and optional `amount`.
- A8 Agents decide from views, but take `expected_version` from the command handler (`current_version`) before the LLM call, outside the transaction: it is a write-side concurrency token, not decision data (design.md §4.1: writes are validated through the handler).
- A10 The fixed `low` plan has one `notify_customer` step per affected order (one step for the usual single-order notice).
- A9 Projectors stop at a `seq` gap (an uncommitted lower `seq` would otherwise be lost); a gap still open `gap_timeout` (default 5 s, process monotonic clock) after first seen is treated as a rollback and skipped.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1 (catalog + RecordLlmCall + docs/spec), A5 | inline | done | feat(command): record LLM calls |
| T2 | S2 (llm/ package) | inline | done | feat(llm): bounded, validated generation |
| T3 | S7 (projector infra + incident_status + query) | inline | done | feat(projectors): incident_status view |
| T4 | S8, S3 (agent unit of work + interpreter) | inline | done | feat(agents): interpreter and agent unit of work |
| T5 | S4 (planner) | inline | done | feat(agents): planner |
| T6 | S5 (replanner, budget, max revisions) | inline | done | feat(agents): replanner + fix(projectors) bbe1b39 |
| T7 | S6 (writer + draft validation) | inline | done | feat(agents): writer |
| T8 | agent CLI entry point with tracing | inline | done | feat(agents): eda-agent and eda-projector |

Route evidence: units share the agent runtime and handler; sequential, no Writer trigger reason.

## Log

- L1 (2026-11-13) user: "Sí, fase 3"
- L2 (2026-11-13) user chose "Adelantar incident_status (Recomendado)" for the replanner's state.
- L3 (2026-11-13) user chose "Evento LlmCallRecorded (Recomendado)" for LLM usage in the log.
- L4 (2026-11-13) T1: RED observed (ImportError RecordLlmCall); GREEN 7/7 in test_llm_calls.py; full suite 102 passed. tests/test_catalog.py EXPECTED_TYPES extended with LlmCallRecorded. No schema change (tokens_used, llm_calls already in incidents).
- L5 (2026-11-13) T2: RED (ModuleNotFoundError eda_retail.llm); GREEN 8/8 tests/test_llm.py. Escalation events for llm_output_invalid/budget_exhausted are asserted at agent level (T4–T6). .env.example could not be edited (deny rule): LLM_* vars pending there.
- L6 (2026-11-13) T3: tests and code written in one step (RED not observed separately). GREEN 10/10; suite 120 passed x3. First gap check used DB now() vs app occurred_at and was flaky under clock skew; replaced by a monotonic timer (A9). "Incidencia escalada" second clause (no more StepRequested) is enforced by the handler (incident_closed) and belongs to the phase 4 orchestrator. Risk: item 4 (gap handling).
- L7 (2026-11-13) T3 independent verifier: PASS, no blockers. Advisories: (a) A9 assumes no transaction holds a seq longer than gap_timeout (consider idle_in_transaction_session_timeout); (b) stale _gap_seen entries in a losing instance (memory only); (c) `-comp`/`compensating` view path untestable until phase 4 adds a compensation event; (d) no test escalates an incident that already has a plan.
- L8 (2026-11-13) T4: RED (ModuleNotFoundError eda_retail.agents); GREEN 8/8 test_interpreter.py; suite 128 passed. AgentRunner: inbox.seen before the LLM, decide outside transactions, one transaction for inbox + commands + RecordLlmCall; concurrency_conflict rolls back and re-decides (max 3), recording all spent calls. EventConsumer accepts UnitOfWork handlers and exposes process(event). Ambiguous notice: cause unknown with severity low is rejected in code (regenerated), not coerced.
- L9 (2026-11-13) T5: RED (ModuleNotFoundError agents.planner); GREEN 8/8 test_planner.py. Business check of LLM plans = CommandHandler.validate (dry run of the real command, rolled back before appending events: no seq gaps). Planner-level tests cover llm-boundaries scenarios "Plan sin campo obligatorio", "Tres planes inválidos seguidos", "Presupuesto de tokens agotado" with the escalation event.
- L10 (2026-11-13) T6: RED (ModuleNotFoundError agents.replanner); GREEN 5/5 test_replanner.py; suite 141 passed. Replanner reads the plan only from incident_status, waits (bounded, view_timeout 5 s) until the view shows the failed step, else raises ViewNotCaughtUp before touching the inbox. max_revisions = plan_version - 1 >= MAX_PLAN_REVISIONS, escalated without an LLM call. step_id stability enforced in code (step_id_changed). Retryable and -comp failures are ignored (orchestrator, phase 4).
- L11 (2026-11-13) T6 follow-up: 3833d1b was committed with 4 failing tests (pipeline exit masked by `tail`); cause: test_relay appends placeholder payloads and the incident_status projector raised on them. Fixed in bbe1b39: projectors skip events whose payload fails catalog validation (warning + span event), with a test. Suite 142 passed x2.
- L12 (2026-11-13) user: "activa los check del repositorio" -> chose "CI en GitHub Actions". Added .github/workflows/ci.yml (ebd5a5f): docker compose postgres+kafka, uv sync --locked, pytest. Not run on GitHub yet (no push). Risk: item 5.
- L13 (2026-11-13) T7: RED (ModuleNotFoundError agents.writer); GREEN 7/7 tests/test_writer.py. StepFailed invalid_draft publication stays with the phase 4 executors.
- L14 (2026-11-13) T8: RED (ModuleNotFoundError agents.cli); GREEN 4/4 tests/test_agent_cli.py; suite 153 passed. eda-agent <interpreter|planner|replanner> and eda-projector scripts; docs/README.md gets a Procesos section. Real Anthropic calls never exercised yet (user runs them). Phase 3 complete.
- L15 (2026-11-13) user: "no quiero usar Claude sino ollama". AnthropicClient and the anthropic dependency removed; OllamaClient (stdlib urllib, injectable post) added; tests 154 passed. Real Ollama runs are the user's (~/CLAUDE.md).
