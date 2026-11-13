# Feature: orchestration (fase 4: orchestrator/ y executors/)

Objective: phase 4 of CLAUDE.md work order — plan execution with parallel steps, saga with code-derived compensations, executors per action with retries, dead letter queue, concurrency limit and incident closing.
Branch: `feat/infra` (continues) · Delivery: ask-on-risk · Runner: `uv run pytest` (LLM always the fake client; real runs are the user's).

## Specs

Scenario names quoted verbatim; one test per scenario, docstring = scenario name.

- S1 Design approved by the user (L2): new event `StepCancelled`, emitted by a new command `ApplyRevision` that the orchestrator sends on `PlanRevised`; it cancels the steps the revision does not keep and derives the `-comp` steps in code. `compensating` is inferred from the `StepRequested` of the `-comp` step. `StepCompleted` and `StepFailed` move to `event_version` 2 with `plan_version`, with an upcaster for v1. Executor retries with exponential backoff and jitter (`STEP_MAX_ATTEMPTS`); exhausted, `StepFailed` and escalation. Alembic migration: attempts counter in `plan_steps`, uniqueness (`incident_id`, `step_id`) in `refunds` and `customer_messages`, table `shipments`. Catalogs in docs/README.md updated in the same change (`RevisePlan` no longer derives compensations).
- S2 incident-planning "Pasos independientes en paralelo": "Pasos sin dependencias", "Paso con dependencia".
- S3 incident-planning saga: "Reembolso con una compensación pendiente", "Fallo del reenvío tras reservar stock", "Compensación repetida", "La liberación del stock falla" (`compensation_failed`, no refund), "Estados tras una compensación".
- S4 incident-planning "Cierre de la incidencia": "Plan completado" (`outcome` `resolved`, or `refunded` if the current plan ended with a refund); "Eventos obsoletos": "Resultado de una versión antigua" ("el sistema no lo usa para decidir", "queda registrado como obsoleto").
- S5 step-execution: "Enrutado por acción", "Reclamación creada", "Error de negocio" (`out_of_stock`, `retryable` false), "Error transitorio" (`retryable` true), "Importe por encima del máximo" (no `RefundIssued`, `IncidentEscalated` `refund_limit`), "Borrador con datos de otro pedido" (`StepFailed` `invalid_draft`, `retryable` true).
- S6 resilience: "Error transitorio resuelto en el segundo intento" ("el efecto se ejecuta una sola vez"), "Evento con esquema inválido" (`invalid_schema`, `eda.dlq` + `dead_letters`, `IncidentEscalated` if the incident is open), "Pico de incidencias" (limit 5, 50 steps, never more than 5 in flight, none lost), "Servicio de reclamaciones caído", "Caída de un ejecutor a mitad de un plan", "Reinicio del orquestador". Scope decision L3: DLQ and concurrency limit are in this phase.
- S7 idempotency (executors side): "Evento duplicado", "Evento nuevo que pide un efecto ya ejecutado", "Dos ejecutores a la vez" exercised through real executors (already covered at `effect_keys` level in phase 1).

Assumptions (marked):
- A1 Orchestration decisions live in the command handler: the orchestrator is a stateless consumer that translates events into commands (`ApplyRevision`, `AdvancePlan`, `EscalateIncident`); `AdvancePlan` requests every ready step in one command (parallel), applies the pivot rule and closes the incident when every step is final. State comes from `plan_steps` (design.md §4: no rehydration), never from process memory.
- A2 Executors copy `plan_version` from `StepRequested` into `RecordStepResult`. A result whose `plan_version` is older than the current one is appended (audit) but does not change `plan_steps`, and the orchestrator ignores it, recording `eda.stale=true` on its span. A kept step still `requested` is re-requested in the new version; its idempotency key is unchanged, so `effect_keys` returns the saved result.
- A3 The v1 → v2 upcaster sets `plan_version` 1 (v1 events carry none; the first plan is version 1). `incident_id` is the envelope `correlation_id`.
- A4 A non-kept step `pending`, `requested` or `failed` becomes `cancelled` (`StepCancelled`). If a cancelled compensable step later reports a stale completion, the handler derives its `-comp` step.
- A5 Retries are in-process in the executor (`STEP_MAX_ATTEMPTS` 3, `STEP_BACKOFF_BASE_SECONDS` 0.5, `STEP_BACKOFF_MAX_SECONDS` 10, full jitter with a seeded RNG in tests). Exhausted, `StepFailed` carries `retryable` true and `attempts`; the orchestrator escalates with `retries_exhausted` (`compensation_failed` for a `-comp` step) and the `StepRequested` goes to the dead letter queue. Non-retryable failures go to the replanner, unchanged.
- A6 The dead letter row is written in the consumer's transaction; the relay publishes pending `dead_letters` rows to `eda.dlq` (new column `published_at`), so business code never publishes to Kafka.
- A7 Concurrency limit `EXECUTOR_MAX_CONCURRENCY` (default 5): the executor takes up to N messages, processes them on N threads with one connection each, and commits the offsets only after all of them committed in PostgreSQL.
- A8 Simulated external services are injectable fakes (timeouts and outages for tests); `synthetic/` fault injection comes in phase 6.
- A10 Several compensations run in reverse order: each `-comp` depends on the `-comp` of the later original step, ordered by (`origin_plan_version`, step number).
- A9 `notify_customer` and `file_claim` use the writer agent for the text; the LLM call happens before the transaction and after checking `effect_keys`, and its cost is recorded with `RecordLlmCall`.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1 migration 0004 (+ `dead_letters.published_at`, A6) | inline | done | 4b8edc5 |
| T2 | S1 catalog: StepCancelled, v2 + upcaster, readers upcast, docs/README | inline | done | 0a4a348 |
| T3 | S1, S2, S3, S4 command side: ApplyRevision, AdvancePlan, stale results, attempts (+ migration 0005 `plan_steps.result`) | inline | done | 2899077, 584fb0c |
| T4 | S2, S3, S4, S6 "Reinicio del orquestador": orchestrator consumer | inline | done | 4c79188 |
| T5 | S5, S6 retries and limit, S7: executor runtime | inline | done | 2d79b6a |
| T6 | S5: the six executors | inline | done | ba700d7 |
| T7 | S6 DLQ | inline | done | 1df0b2f |
| T8 | S6 recovery and isolation; CLI entry points, docs/README processes | inline | done | (this commit) |

Route evidence: units share the command handler, the catalog and the executor runtime; sequential, no Writer trigger reason. Independent verifier after T1 and T2 (Risk items 1 and 3), as the user asked.

## Log

- L1 (2026-11-13) user: "Continúa con las tareas pendientes de la sesión anterior … fase 4 de la PoC de EDA (orchestrator/ y executors/: ejecución del plan, pasos en paralelo, saga y compensaciones, cierre de la incidencia) … Implementar tarea a tarea, con un test por escenario, y verificación independiente de la migración y de los cambios del catálogo."
- L2 (2026-11-13) user: "Apruebo" (design in S1).
- L3 (2026-11-13) user chose "Ambas (Recommended)": DLQ and concurrency limit enter phase 4.
- L4 (2026-11-13) forecast ~2500 authored changed lines; branch continues `feat/infra` as phases 1–3.
- L5 (2026-11-13) T1: RED (UndefinedColumn published_at, no shipments); GREEN 6/6 test_migration_0004.py; suite 164 passed; `alembic upgrade head` on the dev DB. Risk: item 1.
- L6 (2026-11-13) T2: RED (ImportError upcasters); GREEN 11/11 test_catalog.py; suite 168 passed. Readers upcast: EventConsumer.decode_message and Projector default registry. First commit had a ruff import-order error (pre-commit output was filtered by grep); fixed with --amend before anything else. Risk: item 3.
- L7 (2026-11-13) Independent verifier T1+T2: PASS, no blockers. Advisories: (1) 0004 re-upgrade fails if refunds/customer_messages hold rows (NOT NULL without default; documented); (2) plan_version/attempts stamped by the handler until T3 (fixed in T3); (3) `Projector(upcasters=...)` replaces the shared registry instead of extending it; (4) 0004 downgrade loses data, as 0003.
- L8 (2026-11-13) T3: RED (ImportError AdvancePlan); GREEN 15/15 test_saga_commands.py; suite 183 passed. Added migration 0005 `plan_steps.result` (the `-comp` params need the original `reservation_id`; effect_keys may be disabled for E1). Decisions: a failed non-kept step becomes `cancelled`; pivot rule also counts `-comp` steps not completed (a failed compensation blocks the refund); reverse order chains `-comp` steps by `depends_on`, ordered by (origin_plan_version, step number) (A10); `RecordStepResult.effect` optional (result replayed from effect_keys). incident_status view: StepCancelled, `-comp` added on request with the original `compensating`, stale results ignored.
- L9 (2026-11-13) T4: tests and code written together (RED not observed separately). GREEN 11/11 test_orchestrator.py; suite 194 passed. Orchestrator retries on concurrency_conflict (max 5) re-reading the version.
- L10 (2026-11-13) T5: tests and code written together (RED not observed separately). GREEN 9/9 (test_executor_runtime.py, test_backoff.py). First "Pico de incidencias" took 10 s because each batch held 5 steps of one incident (serial concurrency_conflict retries); events now arrive interleaved across incidents (0.5 s). New env: STEP_MAX_ATTEMPTS, STEP_BACKOFF_BASE_SECONDS, STEP_BACKOFF_MAX_SECONDS, EXECUTOR_MAX_CONCURRENCY (.env.example not editable: deny rule).
- L11 (2026-11-13) T6: GREEN 9/9 test_executors.py; suite 212 passed. The command handler already rejects plans above MAX_REFUND_AMOUNT, so "Importe por encima del máximo" is tested with an executor configured with a lower limit (defense in depth). Drafted actions read `estimated_delay_hours` from params (0 when absent): the write side does not store it.
- L12 (2026-11-13) T7: GREEN 3/3 test_dead_letters.py. Regression caught by the suite: test_effect_keys built StepRequested payloads with an extra `order_id` field (not catalog-valid), now dead-lettered; fixture fixed (order_id only in params). Escalation from infra goes through a hook (command/escalation.py) to keep infra free of command imports.
- L13 (2026-11-13) T8: GREEN 9/9 (test_recovery.py, test_executor_cli.py); suite 224 passed. eda-orchestrator, eda-executor <acción>; agents also dead-letter invalid events. Pending: no independent verifier run on T3–T8 (user asked only for migration and catalog); real Ollama run of the full flow is the user's.
