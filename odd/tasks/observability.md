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
| T2 | S4, S3 (schema) | inline | done (RED import error -> 3 passed; suite 34 passed) | next commit |
| T3 | S3, S5, S6, S7 (tracing in event_store, relay, consumer, db) | inline | pending | |
| T4 | S1, S2 end-to-end check in Jaeger + docs | inline | pending | |

Route evidence: sequential units (T3 needs T2's column); no Writer trigger reason.

## Log

- L1 (2026-11-13) user: "antes añade observabilidad con Opentelemtry (opentracing?) tanto para operaciones síncronas como asíncronas con un Jaeger en el docker compose para visualizarlo"
- L2 (2026-11-13) user chose "Columna en outbox (Recomendado)" over derived IDs for carrying trace context through the outbox.
