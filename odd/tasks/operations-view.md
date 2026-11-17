# Operations view and incident metrics

Objective: add the user's specs `specs/incident-metrics/spec.md` and `specs/operations-view/spec.md` to the repository and implement them (projector `operations`, its views and queries, `GET /api/operations/*`, the front screen `#/analytics/operations`). Branch `feat/operations-view` from main 41a2794; delivery `single-pr`.
Test runners: unit `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; front `cd web && npm test && npm run typecheck`; integration only in CI.

## Specs

- S1 specs/incident-metrics/spec.md, every requirement and scenario (one test each): «Proyector de operaciones», «Vista incident_metrics», «Hechos de apoyo», «Sin datos personales ni contenido no confiable», «Consultas de operaciones», «Retraso de las vistas», «Idempotencia», «Reconstrucción», «Nueva vista sin tocar la escritura».
- S2 specs/operations-view/spec.md, every requirement and scenario: «Acceso desde la analítica de pedidos», «Resumen de estado y motivos de escalado», «Tabla de incidencias», «Coste por escenario», «Llamadas al LLM por agente», «Replanificación y pasos fallidos», «Transportistas y consultas A2A», «Frescura de las vistas», «Actualización en vivo», «Estados vacíos y errores», «Acceso de solo lectura y local».
- S3 «Cambios a aplicar en otras specs al implementarla» (operations-view): `web-front` (link «Operaciones», incident filter by link, API routes), `cqrs-views` («Vistas mínimas»), `experiments` (E4 fingerprint), `README.md` and `docs/manual-usuario-eda-retail.md` §9.
- S4 incident-metrics «Supuestos a verificar contra el código», checked against events/catalog.py: `StepFailed` has no `action` (taken from the `StepRequested` of the same `step_id`, spec fallback) and its code is `error`; `LlmCallRecorded` validation flag is `validation_passed` and `incident_id` is required; `CarrierNoticeReceived` and `StockReleased` carry the incident only as `correlation_id`. The spec section is rewritten as verified facts.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1 (projector, views, migration 0026, idempotency, rebuild), S4 | inline | done: projectors/operations.py, 0026, spec facts verified; integration tests in CI | bb2e322 |
| T2 | S1 (queries, lag) | inline | done: queries/operations.py (p95 nearest-rank, lag_of, kafka_end_offsets 2 s); unit GREEN | bb2e322 |
| T3 | S2 (API endpoints, read-only) | inline | done: 5 GET routes, 405 on POST, read-only SQL check; unit GREEN | bb2e322 |
| T4 | S2 (front screen, route, link, live/pause, empty/error states) | inline | done: OperationsView, useOperations, route enum in App, link in OrderAnalytics, client methods; vitest 139 GREEN, typecheck clean; not checked visually in a browser | (this commit) |
| T5 | S3 (other specs, README, manual) | inline | pending | |

## Log

- L1 (2026-11-17) User requests, verbatim: "Después del merge, empieza E13 con su spec. Además debe haber una spec nueva para una vista analítica de operaciones"; "Es esa misma, puedes usarla y editarla"; "añadirla e implementarla. Hazlo antes de empezar E13".
- L2 (2026-11-17) #82 merged (41a2794). Map from a read-only explorer: projector template `projectors/carrier_dashboard.py`, registry `PROJECTORS`, migration template 0020/0018, queries `applied()` pattern, web routes in `web/app.py create_app`, front React/Vite/vitest with hand-rolled hash routing in `web/src/App.tsx`, E4 iterates `PROJECTORS`.
- L3 (2026-11-17) Backend done. Spec edits: table `views.view_incident_metrics`, `kafka_partition`/`kafka_offset`, `view_operations_steps` helper for the failed step action, `validated` from `validation_passed`, lag reads `event_log.offsets` too (operations-view read-only list updated), incidents API returns `next_before`. Assumptions section rewritten as verified facts.
