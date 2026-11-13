# Feature: observability (OpenTelemetry + Jaeger)

Objective: distributed tracing with OpenTelemetry for synchronous operations (PostgreSQL calls, in-process functions) and asynchronous ones (outbox → relay → Kafka → consumer), visualised in Jaeger from `docker compose`.
Branch: `feat/infra` (continues phase 1, before phase 2) · Delivery: ask-on-risk · Runner: `uv run pytest`.

## Specs

- S1 User request (L1): "añade observabilidad con Opentelemtry (opentracing?) tanto para operaciones síncronas como asíncronas con un Jaeger en el docker compose para visualizarlo". OpenTracing is archived (merged into OpenTelemetry); OpenTelemetry is used.
- S2 Jaeger runs in `docker compose`, receives OTLP and shows the UI on `localhost:16686`.
- S3 Trace context (W3C `traceparent`) crosses the outbox in a new column of `outbox` (user decision L2); the relay copies it to Kafka headers and the consumer extracts it, so producer → relay → consumer → derived events share one trace. `events` does not change.
- S4 Schema change goes through Alembic with migrations (CLAUDE.md: "Si el esquema cambia, añade Alembic y migraciones; no edites el volumen a mano").
- S5 Synchronous operations are traced: PostgreSQL statements (psycopg instrumentation) and a helper for in-process functions.
- S6 observability-audit / "Traza por incidencia": spans carry `incident_id` (`correlation_id`), `event_id`, `causation_id`, `event_type` and component (`service.name`); a span per event consumed. LLM-call and effect spans arrive with their phases (assumption).
- S7 Tracing can be switched off by configuration and the system works without a collector.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S2 | inline | done (UI API answers on :16686) | 8bd3026 |
| T2 | S4, S3 (schema) | inline | done (RED import error -> 3 passed; suite 34 passed) | 770c0fa |
| T3 | S3, S5, S6, S7 (tracing in event_store, relay, consumer, db) | inline | done (RED import error -> 42 passed) | 3eed716 |
| T4 | S1, S2 end-to-end check in Jaeger + docs | inline | done (demo trace: 17 spans, 4 services, one trace) | next commit |

Route evidence: sequential units (T3 needs T2's column); no Writer trigger reason.

## Log

- L1 (2026-11-13) user: "antes añade observabilidad con Opentelemtry (opentracing?) tanto para operaciones síncronas como asíncronas con un Jaeger en el docker compose para visualizarlo"
- L2 (2026-11-13) user chose "Columna en outbox (Recomendado)" over derived IDs for carrying trace context through the outbox.
- L3 T4 evidence: `scripts/trace_demo.py` produced one Jaeger trace with 17 spans across `command-handler`, `relay`, `interpreter`, `planner` (create → publish → process chain, SQL spans nested). Dev DB `eda` migrated with `uv run alembic upgrade head` (outbox now has `trace_context`). design.md §13 added; only that block committed, user's uncommitted edits in §4.1/§4.2/§12 left untouched.
- L4 (2026-11-13) user: "Sí, verifícalo". Independent verifier on `408c7c5..2f9daf9`: no blockers, S1–S7 pass, 42 passed; schema identical across empty-DB and existing-volume upgrades, downgrade/upgrade round-trips, baseline differential identical except the added `traceparent` header. Scratch, 5 DBs, 4 topics and 11 consumer groups removed by parent.
  Advisories:
  - A1 fixed: `%` in the database URL broke `migrations._config` (ConfigParser interpolation); now escaped, test `test_database_url_with_percent_encoded_password`.
  - A2: with tracing disabled, a consumer still forwards an upstream `traceparent` into `outbox.trace_context` (NoOp tracer keeps the parent context). Accepted: keeps traces continuous across mixed deployments.
  - A3: `provider.shutdown()` takes ~7–10 s when the collector is unreachable (OTLP retry backoff).
  - A4 (phase 2 handlers): exception messages are recorded on `process` spans; handlers must not put PII in exception text. Headers are decoded as UTF-8 (a foreign non-UTF-8 header would fail).
  - A5: IGNORED events get no `process` span; no consumer CLI calls `setup_tracing` yet (components arrive in later phases).
