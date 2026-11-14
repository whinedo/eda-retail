# Feature: web-front (FastAPI + React/Vite)

Objective: local web UI with a command menu (`scripts/notice.sh`), a systems map and a horizontally scrollable events panel, per `specs/web-front/spec.md` and the mockups in `docs/front/`.
Branch: `feat/web-front` · Delivery: ask-on-risk · Runner: `uv run pytest` (backend), `npm test` (front).

## Specs

- S1 User request (L1): front with agents, systems, integrations and communications; left menu with the `scripts/notice.sh` commands; main area with the systems map; bottom panel with events (emisor, destino, tipo, timestamp) and right-hand horizontal scroll.
- S2 Stack (L2): FastAPI backend + React/Vite front; commands run for real through `scripts/notice.sh`, localhost only.
- S3 Layout and visuals follow the approved mockups (L3); palette: LLM `#E3F0FF/#2B7DE0`, code `#E6F6EC/#2E9E5B`, data `#F1F1EE/#8A8A80`, external `#FFF3E0/#E08A2B`; fonts Inter and JetBrains Mono.
- S4 Command menu with a whitelist of arguments, one run at a time, output streamed (spec: Menú de comandos, Segunda ejecución simultánea).
- S5 Systems map from a static topology plus live process/health status (spec: Mapa de sistemas).
- S6 Events panel: columns, sticky header, horizontal scroll, filters, pause, SSE by `seq` (spec: Panel de eventos, Actualización en vivo).
- S7 Sender/receiver derived from the event catalogue (spec: Origen de emisor y destino).
- S8 Read-only access to `events` and `view_*`; bound to `127.0.0.1` (spec: Acceso de solo lectura y local). The front MUST NOT write to the log (cqrs-views).
- S9 Node detail with LLM metrics from `LlmCallRecorded` (spec: Detalle de un nodo).
- S10 `interactive` through a pty with stdin lines (spec: Comando interactivo).
- S11 Partition/offset: open decision. Default "—" until the relay records them; recording them needs an Alembic migration (CLAUDE.md).
- S12 Jaeger (L5): header link to the Jaeger UI and a per-event "ver traza" link to `<JAEGER_UI_URL>/trace/<trace_id>`, `trace_id` read from `outbox.trace_context` (spec: Acceso a las trazas en Jaeger).

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S2, S8 backend skeleton `src/eda_retail/web/` (FastAPI app, config, localhost binding, `eda-web` script) | | pending | |
| T2 | S7, S8 events API (`/api/events`, sender/receiver from catalogue, filters, pagination) | | pending | |
| T3 | S6 SSE stream by `seq` with `Last-Event-ID` | | pending | |
| T4 | S4, S10 command runner (whitelist, single run, streamed output, pty for `interactive`) | | pending | |
| T5 | S5, S9 topology, `/api/health` (pgrep, Kafka, PostgreSQL, Ollama) and node detail with LLM metrics | | pending | |
| T6 | S2, S3 front skeleton `web/` (Vite + React + TS), layout, API client | | pending | |
| T7 | S4, S10 command menu and output area | | pending | |
| T8 | S3, S5 systems map component (nodes, edges, status, highlight) | | pending | |
| T9 | S6, S9 events panel and node detail drawer with filter | | pending | |
| T10 | S11 decision with the user: relay records partition/offset (migration) or keep "—" | | pending | |
| T12 | S12 `trace_id` in `/api/events` from `outbox.trace_context`, `JAEGER_UI_URL` in config, header link and per-event link (after T2, T6 and T9) | | pending | |
| T11 | all end-to-end check against the compose stack and docs (README, design.md section, `scripts/run-web.sh`) | | pending | |

Order: T1 → T2 → T3 → T4 → T5 (backend), then T6 → T7 → T8 → T9; T10 can run any time; T12 after T9; T11 last. Each task goes test first (RED, then green) as in the other task files.

## Log

- L1 (2026-11-14) user: "quiero preparar un front también para este proyecto ... a la izquierda un menú con los comandos del cli scripts/notice.sh y ocupando el espacio principal el mapa de sistemas ... en la parte inferior los eventos que se intercambian con emisor, destino, tipo de evento y timestamp (tiene que tener un scroll lateral derecha)"
- L2 (2026-11-14) user chose "Prototipo navegable + spec", "FastAPI + React/Vite" and "Ejecutarlo de verdad". User: "el código lo desarrollará Code".
- L3 (2026-11-14) user: "adelante el diseño es perfecto" on the three mockups (`docs/front/*.png`).
- L5 (2026-11-14) user: "sería interesante que desde el front se pudiera acceder a la interfaz web de Jaeger para revisar la observabilidad"; chose "Enlace y traza por evento". `outbox` rows are kept after publishing and `outbox.trace_context` holds the W3C `traceparent`, so every published event keeps its trace id.
