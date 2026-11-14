# Feature: views (fase 5: projectors/ y queries/)

Objective: phase 5 of CLAUDE.md work order — projectors and queries for `cost_per_incident`, `audit_trail` and `carrier_dashboard`, plus view rebuild with fingerprint (needed by E4).
Branch: `feat/infra` (continues) · Delivery: ask-on-risk, one commit per task, user approves each commit · Runner: `uv run pytest`.

## Specs

Proposal approved by the user (L2). Existing scenarios in `specs/cqrs-views` and `specs/observability-audit` apply; one test per scenario, docstring = scenario name.

- S1 `cost_per_incident`: the view stores only facts from the log, per incident: `llm_calls`, `tokens_in`, `tokens_out` from every `LlmCallRecorded` (accepted or not), `claims`, `reissues`, `refunds_amount`. The query computes `llm_cost` and `effects_cost` from configuration: `LLM_PRICE_IN_PER_1K`, `LLM_PRICE_OUT_PER_1K` (default 0, Ollama is local), `EFFECT_COST_CLAIM`, `EFFECT_COST_REISSUE`; a refund costs its `amount`. Scenario "Incidencia con una revisión del plan": "incluye las llamadas del planificador y del replanner" "AND el importe de los efectos ejecutados".
- S2 `audit_trail`: one row per decision event, keyed by `seq` (`ON CONFLICT DO NOTHING`). `actor` = envelope `producer`; `decision` = event type (plus the action for `StepRequested`); `plan_version` when the event carries it; `reason` from `PlanRevised.reason`, `StepFailed.error`, `IncidentEscalated.reason`, and `validation_failed` for `LlmCallRecorded` with `validation_passed` false. `raw_text` is never stored (untrusted content). Limitation: a rejected command writes no events, so the audit only shows rejections that leave a trace in the log. Scenarios "Reembolso en lugar de reenvío" ("aparece el `StepFailed` que lo motivó, el `PlanRevised` y el reembolso resultante") and "Pérdida de la vista de auditoría".
- S3 `carrier_dashboard`: per carrier `incidents`, `delays` (`ShipmentDelayed` with `cause = delay`), `claims_filed`, `refunds_issued`. The auxiliary table `view_carrier_incidents (incident_id → carrier)` is filled from `CarrierNoticeReceived` (`correlation_id`); without a notice the carrier is `unknown`. Scenario "Vista de reclamaciones por transportista".
- S4 Counter idempotency: every row of `view_cost_per_incident` and `view_carrier_dashboard` keeps its `last_seq`; an event with `seq <= last_seq` is ignored. Scenario "Evento duplicado en un proyector" ("la vista no cambia").
- S5 Queries `get_incident_cost(id)`, `get_audit_trail(id)`, `get_carrier_dashboard()`: read only `view_*` and `projector_offsets`, never write, and return `last_applied_seq`. Scenario "Lectura justo después de escribir".
- S6 `eda-projector rebuild <view>`: empties the view, sets `last_seq = 0`, replays the log and prints the fingerprint (sha256 of the ordered rows) and the rebuild time. `eda-projector` without arguments runs the four projectors, each with its own position. Scenarios "Reconstrucción de incident_status", "Reinicio de un proyector".

Assumptions (marked):
- A1 Prices and effect costs are applied at query time so that a rebuilt view is identical regardless of configuration (H4).
- A2 Migration 0006 drops `llm_cost` and `effects_cost` from `view_cost_per_incident`; views are rebuildable from the log, so no data is lost.
- A3 `eda-projector rebuild` runs with no writers active (as in E4): a `seq` gap is an old rollback and is skipped at once (`gap_timeout` 0).

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1, S3, S4 migration 0006 + round-trip test | inline | done (RED 2 failed → GREEN 237 passed) | 78a2987 |
| T2 | S1, S4, S5 cost projector + query | inline | done (RED collection error → GREEN 242 passed) | c98364d |
| T3 | S2, S5 audit projector + query | inline | done (RED collection error → GREEN 246 passed) | ccade9b |
| T4 | S3, S4, S5 carrier projector + query | inline | done (RED collection error → GREEN 250 passed) | 07f0ec8 |
| T5 | S6 rebuild with fingerprint + CLI with four projectors | inline | done (RED collection error → GREEN 257 passed; dev DB: 4 views identical over 49 events) | c8b5f91 |
| T6 | specs/ scenarios, docs/README, `.env.example` text for the user | inline | done (257 passed; .env.example text handed to the user) | f7b0639 |

Route evidence: each task is one understood unit of about 100–200 lines; no writer trigger (no parallel units, no context backstop).

## Log

- L1 (2026-11-14, user): "Proponerme las specs y tareas de la fase 5 (vistas view_cost_per_incident, view_audit_trail, view_carrier_dashboard y sus consultas) y esperar mi visto bueno antes de escribir código."
- L2 (2026-11-14, user): "Aprobada" — proposal S1–S6 with T1–T6 approved.
- L3 (2026-11-14): T5 rebuild failed in the full suite for cost and carrier views. Cause: `test_view_counters_start_at_zero` (T1) inserted rows straight into the views, which no event backs; a correct rebuild drops them. Fix: that test now rolls back its rows (`force_rollback=True`). A replay upper bound was tried and reverted (not the cause; A3 covers quiet rebuilds). Assumption A3: rebuild runs with no writers active, so `seq` gaps are skipped at once (`gap_timeout` 0).
