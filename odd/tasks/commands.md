# Feature: commands (fase 2: events/ y command/)

Objective: phase 2 of CLAUDE.md work order — event payload schemas and catalog with version upcasters, and the command handler with business rules and optimistic concurrency.
Branch: `feat/infra` (continues) · Delivery: ask-on-risk · Runner: `uv run pytest`.

## Specs

Scenario names quoted verbatim from the repo specs; one test per scenario, docstring = scenario name.

- S1 Catalog (docs/README.md "Catálogo de eventos"): one pydantic payload model per event type (15), `extra=forbid`; every event built by the handler is validated against it.
- S2 event-backbone / "Versionado de eventos": scenario "Evento de versión anterior" — upcasters convert on read, the log does not change.
- S3 event-backbone / "Sin datos personales en el log": scenario "Evento de aviso al cliente".
- S4 event-backbone / "Correlación por incidencia": scenario "Cadena de causalidad"; cqrs-views / "Catálogo de comandos": scenario "Comando derivado de un evento" (`command_id` = originating `event_id` = `causation_id`).
- S5 cqrs-views / "Escritura solo mediante comandos": scenarios "Comando válido", "Comando sobre una incidencia cerrada" (`incident_closed`).
- S6 cqrs-views / "Concurrencia optimista en el lado de escritura": scenario "Dos comandos simultáneos sobre la misma incidencia" (`concurrency_conflict`).
- S7 incident-interpretation: "Llega un aviso", "Aviso repetido" (`duplicate_notice`), "Pedido inexistente" (`no_valid_orders`); `cause` in `delay, loss, damage, wrong_address, strike, unknown`; "Aviso ambiguo" severity ≥ `medium` checked in code.
- S8 incident-planning / llm-boundaries (validation in the handler): "Plan con una acción no permitida", "Propuesta del LLM con una compensación incluida" (`compensation_not_allowed_in_plan`), "Plan sin campo obligatorio", "Reembolso por encima del máximo" (`refund_limit`), "Paso mantenido entre versiones" (step keeps `step_id` and origin version), revised plan MUST NOT include completed steps.
- S9 incident-planning "Paso con dependencia" (handler refuses `RequestStep` with pending dependencies) and "Reembolso con una compensación pendiente" (no `issue_refund` while a step is `compensating`); llm-boundaries "Incidencia escalada" (no more `StepRequested`).
- S10 `RequestStep` computes `idempotency_key` with the step's origin plan version (idempotency S8 of infra).

Assumptions (marked, change freely):
- A1 `RegisterCarrierNotice` assigns the `incident_id` deterministically from `carrier_notice_id` and uses it as `correlation_id` of `CarrierNoticeReceived`, so the whole incident is one trace and one `incident_id` per notice holds by construction.
- A2 Rejection reasons not named by the specs: `invalid_command`, `incident_not_found`, `notice_not_found`, `incident_exists`, `action_not_allowed`, `invalid_plan`, `plan_exists`, `no_plan`, `completed_step_in_plan`, `step_not_found`, `step_not_requestable`, `dependencies_pending`, `compensation_pending`, `steps_pending`, `invalid_severity`, `personal_data_in_event`.
- A3 `resolved` and `escalated` incidents both reject further commands with `incident_closed`.
- A4 Deferred to phase 3/4: planner severity rule for `low`, generation retries, budget, `max_revisions`, compensation derivation and cancellation on `PlanRevised`, stale `plan_version` results, `compensating → compensated` transitions.
- A5 Schema fix: `plan_steps.step_id` was a global primary key; migration `0003` makes it `(incident_id, step_id)` (with `compensates` as a composite reference).

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | A5 | inline | done (RED pk assert -> 47 passed; dev DB migrated) | 8b7e24d |
| T2 | S1, S2, S3 | inline | done (RED import error -> 8 passed) | next commit |
| T3 | S4, S5, S6, S7 (handler core, notices, incidents, close, escalate) | inline | pending | |
| T4 | S8 (CreatePlan, RevisePlan) | inline | pending | |
| T5 | S9, S10 (RequestStep, RecordStepResult) | inline | pending | |

Route evidence: sequential units over one module (handler) and shared rules; no Writer trigger reason. T1 is high risk (item 1) and gets its own commit.

## Log

- L1 (2026-11-13) user: "Sí, fase 2" (after phase 1 + observability were verified).
