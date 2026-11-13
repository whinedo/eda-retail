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
- A1 Model `claude-opus-5-5` by default (`LLM_MODEL`), effort `LLM_EFFORT` (default `medium`), server-side refusal fallback `fallbacks="default"` enabled; structured output via `output_config.format` JSON schema, then pydantic validation in code.
- A2 Limits by env: `LLM_MAX_GENERATIONS` 3, `LLM_MAX_TOKENS_PER_INCIDENT` 50000, `LLM_MAX_CALLS_PER_INCIDENT` 10, `MAX_PLAN_REVISIONS` 2.
- A3 `severity` `low` gets the fixed one-step `notify_customer` plan in code, without calling the LLM.
- A4 Budget is checked through the command handler (write-side validation), not a view.
- A5 `EscalateIncident` on an incident that does not exist yet is accepted when it belongs to a registered notice (interpreter exhausting its generations); the incident is created `escalated` with `cause` `unknown`.
- A6 `RecordLlmCall` does not take part in optimistic concurrency (append-only fact; it does not bump `version`).
- A7 Plan step `params` from the LLM are `order_id` and optional `amount`.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1 (catalog + RecordLlmCall + docs/spec), A5 | inline | done | feat(command): record LLM calls |
| T2 | S2 (llm/ package) | inline | done | feat(llm): bounded, validated generation |
| T3 | S7 (projector infra + incident_status + query) | inline | pending | |
| T4 | S8, S3 (agent unit of work + interpreter) | inline | pending | |
| T5 | S4 (planner) | inline | pending | |
| T6 | S5 (replanner, budget, max revisions) | inline | pending | |
| T7 | S6 (writer + draft validation) | inline | pending | |
| T8 | agent CLI entry point with tracing | inline | pending | |

Route evidence: units share the agent runtime and handler; sequential, no Writer trigger reason.

## Log

- L1 (2026-11-13) user: "Sí, fase 3"
- L2 (2026-11-13) user chose "Adelantar incident_status (Recomendado)" for the replanner's state.
- L3 (2026-11-13) user chose "Evento LlmCallRecorded (Recomendado)" for LLM usage in the log.
- L4 (2026-11-13) T1: RED observed (ImportError RecordLlmCall); GREEN 7/7 in test_llm_calls.py; full suite 102 passed. tests/test_catalog.py EXPECTED_TYPES extended with LlmCallRecorded. No schema change (tokens_used, llm_calls already in incidents).
- L5 (2026-11-13) T2: RED (ModuleNotFoundError eda_retail.llm); GREEN 8/8 tests/test_llm.py. Escalation events for llm_output_invalid/budget_exhausted are asserted at agent level (T4–T6). .env.example could not be edited (deny rule): LLM_* vars pending there.
