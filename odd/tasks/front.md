# Feature: web-front (FastAPI + React/Vite)

Objective: local web UI with a command menu (`scripts/notice.sh`), a systems map and a horizontally scrollable events panel, per `specs/web-front/spec.md` and the mockups in `docs/front/`.
Branch: `feat/web-front` · Delivery: ask-on-risk · Runner: `uv run pytest` (backend), `npm test` and `npm run typecheck` in `web/` (front).

## Specs

- S1 User request (L1): front with agents, systems, integrations and communications; left menu with the `scripts/notice.sh` commands; main area with the systems map; bottom panel with events (emisor, destino, tipo, timestamp) and right-hand horizontal scroll.
- S2 Stack (L2): FastAPI backend + React/Vite front; commands run for real through `scripts/notice.sh`, localhost only.
- S3 Layout and visuals follow the approved mockups (L3); palette: LLM `#E3F0FF/#2B7DE0`, code `#E6F6EC/#2E9E5B`, data `#F1F1EE/#8A8A80`, external `#FFF3E0/#E08A2B`; fonts Inter and JetBrains Mono.
- S4 Command menu with a whitelist of arguments, one run at a time, output streamed (spec: Menú de comandos, Segunda ejecución simultánea).
- S5 Systems map from a static topology plus live process/health status (spec: Mapa de sistemas).
- S6 Events panel: columns, sticky header, horizontal scroll, filters, pause, SSE by `seq` (spec: Panel de eventos, Actualización en vivo).
- S7 Sender from `producer`; receiver from the event types each component declares, projectors last (spec: Origen de emisor y destino, corrected in T2: the catalogue module declares no consumers).
- S8 Read-only access to `events` and `view_*`; bound to `127.0.0.1` (spec: Acceso de solo lectura y local). The front MUST NOT write to the log (cqrs-views).
- S9 Node detail with LLM metrics from `LlmCallRecorded` (spec: Detalle de un nodo).
- S10 `interactive` through a pty with stdin lines (spec: Comando interactivo).
- S11 Partition/offset: open decision. Default "—" until the relay records them; recording them needs an Alembic migration (CLAUDE.md).
- S12 Jaeger (L5): header link to the Jaeger UI and a per-event "ver traza" link to `<JAEGER_UI_URL>/trace/<trace_id>`, `trace_id` read from `outbox.trace_context` (spec: Acceso a las trazas en Jaeger).

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S2, S8 backend skeleton `src/eda_retail/web/` (FastAPI app, config, localhost binding, `eda-web` script) | inline | done (RED ModuleNotFoundError eda_retail.web → GREEN 12 passed; non-integration suite 108 passed) | feat(web-front): backend skeleton |
| T2 | S7, S8 events API (`/api/events`, sender/receiver from catalogue, filters, pagination) | inline | done (RED ModuleNotFoundError eda_retail.web.routing → GREEN unit 113 passed; integration green in CI) | feat(web-front): events API (PR #19) |
| T3 | S6 SSE stream by `seq` with `Last-Event-ID` | inline | done (RED ModuleNotFoundError eda_retail.web.stream → GREEN unit 120 passed; integration green in CI) | feat(web-front): live events stream (PR #20) |
| T4 | S4, S10 command runner (whitelist, single run, streamed output, pty for `interactive`) | inline | done (RED TypeError create_app(notice_script) + Host 200≠403 → GREEN unit 147 passed, command tests stable over 5 runs; integration green in CI; judgment-day APPROVED, 0 critical) | feat(web-front): command runner (PR #21) |
| T5 | S5, S9 topology, `/api/health` (pgrep, Kafka, PostgreSQL, Ollama) and node detail with LLM metrics | inline | done (RED ModuleNotFoundError eda_retail.web.health → GREEN unit 160 passed; integration green in CI) | feat(web-front): topology, health and node detail (PR #22) |
| T6 | S2, S3 front skeleton `web/` (Vite + React + TS), layout, API client | inline | in review (RED unresolved ./App and ./client → GREEN vitest 10 passed, tsc clean, build ok; layout checked in the browser without backend) | feat(web-front): front scaffolding |
| T7 | S4, S10 command menu and output area | | pending | |
| T8 | S3, S5 systems map component (nodes, edges, status, highlight) | | pending | |
| T9 | S6, S9 events panel and node detail drawer with filter | | pending | |
| T10 | S11 decision with the user: relay records partition/offset (migration) or keep "—" | | pending | |
| T12 | S12 `trace_id` in `/api/events` from `outbox.trace_context`, `JAEGER_UI_URL` in config, header link and per-event link (after T2, T6 and T9) | | pending | |
| T13 | S4, S10 harden the command runner (judgment-day W1–W3 on PR #21): close the pty master and answer a controlled error when the launch fails, write input outside the run lock, kill the running process group on backend shutdown; also S3 (non-ASCII Last-Event-ID) and S4 (always set exit_code) | | pending | |
| T11 | all end-to-end check against the compose stack and docs (README, design.md section, `scripts/run-web.sh`) | | pending | |

Order: T1 → T2 → T3 → T4 → T5 (backend), then T6 → T7 → T8 → T9; T10 can run any time; T12 after T9; T11 last. Each task goes test first (RED, then green) as in the other task files.

## Log

- L1 (2026-11-14) user: "quiero preparar un front también para este proyecto ... a la izquierda un menú con los comandos del cli scripts/notice.sh y ocupando el espacio principal el mapa de sistemas ... en la parte inferior los eventos que se intercambian con emisor, destino, tipo de evento y timestamp (tiene que tener un scroll lateral derecha)"
- L2 (2026-11-14) user chose "Prototipo navegable + spec", "FastAPI + React/Vite" and "Ejecutarlo de verdad". User: "el código lo desarrollará Code".
- L3 (2026-11-14) user: "adelante el diseño es perfecto" on the three mockups (`docs/front/*.png`).
- L4 (2026-11-14) T1 in worktree `eda-retail-worktrees/web-front` (E6 running in the main checkout). New deps: `fastapi`, `uvicorn`; dev `httpx2` (Starlette deprecates `httpx` for `TestClient`). `eda-web` always binds 127.0.0.1 (`WEB_PORT`, `--port`). `LocalOriginGuard` returns 403 for any non-localhost `Origin` (preflight included) before CORS. `read_only_connect` sets `default_transaction_read_only=on`. `WEB_PORT` pending in `.env.example` (deny rule). Integration suite not run (needs Postgres on 5432 during E6). Follow-up for T4: also check the `Host` header against DNS rebinding (needs a spec line).
- L5 (2026-11-14) user: "sería interesante que desde el front se pudiera acceder a la interfaz web de Jaeger para revisar la observabilidad"; chose "Enlace y traza por evento". `outbox` rows are kept after publishing and `outbox.trace_context` holds the W3C `traceparent`, so every published event keeps its trace id.
- L6 (2026-11-14) T2: spec corrected ("Origen de emisor y destino" now names `agents.cli.AGENTS`, `orchestrator.runner.EVENT_TYPES` and `executor-<acción>`; `events/catalog.py` has no consumers) and new requirement "Página de eventos" (limit 1..500, `before_seq`, `incident_id`, `node`, `next_before_seq`). Endpoints use `read_only_connect`. Integration tests not run yet: they recreate `eda_test` on 5432 while E6 runs.
- L7 (2026-11-14) T3: spec "Actualización en vivo" extended: `GET /api/events/stream`, `after_seq` for the first connection (EventSource cannot send `Last-Event-ID` on the first request), `Last-Event-ID` wins, no position starts after the last `seq`, poll at least every 0.5 s. Stream reads `events` by `seq` through its own read-only connection; `create_app(stream_max_polls=)` bounds the stream in tests. No keep-alive comments yet.
- L8 (2026-11-14) T4: spec extended: `Host` header must be localhost/127.0.0.1/[::1] (DNS rebinding, closes the L4 follow-up), runs capped at 15 min (process group killed: one run at a time means a hung process would block the menu), free text 1..2000 chars, input lines up to 200 chars and 409 for non-interactive or finished runs. Free text goes as one `--text=<text>` argument so argparse never reads it as an option. Tests use a fake `notice.sh` in `tmp_path`; the real script was not run. Only the last run is kept in memory.
- L9 (2026-11-14) judgment-day on PR #21: APPROVED (0 critical); warnings W1–W3 confirmed by both judges, merged as is; ledger in the session notes folder, follow-up T13. T5: spec adds the insurer (`claims`) to the simulated services (executors/services.py), node ids equal event sender/receiver names (`intake` for eda-notice), `/api/health` checks with a 2 s cap each (moved from open decisions to decisions), node detail with `writer` metrics for notify_customer and file_claim. `/api/health` not tried against the real services (it would call Ollama during E6).
- L10 (2026-11-14) T6: `web/` with React 19, Vite 8, Vitest 5, TypeScript 7 (latest at install), fonts from `@fontsource` (no external requests). Container-presentational: `App` + `useHealth` poll `/api/health` every 5 s, `Header` renders the pills. Vite proxies `/api` to 127.0.0.1:8000 keeping Host `localhost:5173` (allowed by the guard). New pre-commit hook `web` (vitest + tsc) and CI installs Node 24 + `npm ci --prefix web`. User: "no uses andamiaje di scaffolding".
