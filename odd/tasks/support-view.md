# Support view (soporte técnico)

Objective: a support CLI `eda-support` (diagnosis plus the four recovery actions) and a front view that launches it. Branches `feat/support-cli` (PR1), then the API (PR2) and the front (PR3), each from main after the previous merge; delivery `stacked-to-main`, one PR per piece.
Test runners: unit `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; front `cd web && npm test && npm run typecheck`; integration only in CI.

## Specs

- S1 specs/support/spec.md, every requirement and scenario (one test each): «Diagnóstico», «Simulación y confirmación», «Sistema parado», «Republicación tras perder el topic», «Republicación desde una fila», «Reconstrucción de una vista», «Registro de las ejecuciones».
- S2 web-front: a support view that lists the actions, shows `status`, runs a dry run and then the confirmed action, with streamed output; enabled only with `WEB_SUPPORT=true`; the web keeps its read-only database connection and only launches `eda-support` through a whitelist (spec to be written in PR2/PR3).
- S4 User requirement, verbatim: «la vista de soporte debe tener un enlace para acceder a ella desde la vista principal de la EDA». Planned: a «Soporte» link in the header of the main view, next to «Analítica», to a route `#/support`; shown only when `WEB_SUPPORT=true` (read from `/api/config`).
- S3 Scope approved by the user: the four actions `status`, `republish --mode truncated|recreated`, `replay-from <schema> <outbox_id>`, `rebuild <view>`. Out of scope: reprocessing dead letters and retrying failed jobs.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1 spec, S3 | inline | done: specs/support/spec.md | C1 |
| T2 | S1 «Registro» (migration 0028, schema `support`, table `runs`) | inline | done: migration and revert test (CI) | C2 |
| T3 | S1 (CLI `eda-support`: status, dry run/confirm, preconditions, four actions, record) | inline | done: 28 unit tests GREEN (RED: missing package); 5 integration tests run in CI | C2 |
| T4 | S2 (API `POST /api/support/runs`, whitelist, `WEB_SUPPORT`) | inline | done: web-front «Soporte técnico», `scripts/support.sh`, `web/support.py`, shared `CommandRunner.start_script`; 22 tests RED then GREEN | PR2 |
| T5 | S2, S4 (front view, header link) | inline | pending (PR3) | |
| T6 | docs: README, manual, design.md | inline | partial: README command table, design.md, event-backbone ownership note; manual in PR3 | C2 |

## Log

- L1 (2026-11-18) User requests, verbatim: «para el CLI vamos a montar una nueva vista (soporte técnico) que permita ejecutar este comando (y otros que sean necesarios para recuperar el sistema ante un problema)»; «mientras tanto, propón el diseño de la vista de soporte técnico»; «Las cuatro acciones».
- L2 (2026-11-18) Design approved: the web never writes (its PostgreSQL connection stays read-only, spec web-front «Acceso de solo lectura y local»); it launches `eda-support` through a pydantic whitelist like `scripts/notice.sh`. Write actions dry-run by default and need `--confirm <token>`, the token hashes the action, its arguments and the observed state. «System stopped» is checked: no monitored process alive (pgrep patterns of `web.topology`) and no member in the Kafka groups `interpreter` and `orchestrator` (the read side assigns partitions and has no group members). Executions go to `support.runs`. `eda-support` writes other schemas' tables only as a maintenance tool; the spec states this exception to the ownership rule.
- L3 (2026-11-18) Risk: item 1 (republish rewrites outbox positions, resets group offsets and read-side positions; new table), item 2 in PR2 (the web gains a way to launch writing commands). An independent verifier is needed per PR.
- L4 (2026-11-18) PR1 written inline (no writer trigger: one unit). `positioned_lags` moved from the harness to `infra/positions.py` so `eda-support` reuses it. `status` skips the group lag when the topic has no partitions (deleted topic reads as `topic_lost`, not `kafka_down`). Unit suite 987 GREEN, ruff clean; integration tests (`tests/integration/test_support.py`, 0028 revert) only in CI. The manual (`docs/manual-usuario-eda-retail.md`) has the user's uncommitted edits in the main checkout: left for PR3.
- L5 (2026-11-18) Independent verifier: PASS, no blockers (987 unit GREEN, ruff clean; integration judged from code). Applied: A1, the watermark reads retry transient leader/metadata errors through the new `infra.positions.watermark_offsets` (`high_watermark` now delegates to it), so a just-created topic does not read as `kafka_down`; A3 partly, a unit test for the `postgres_down` alert. Not applied: A2 (a partial republish leaves the topic non-empty, documented in `republish_in_order`), A3 rest (no `truncated` integration test, group offsets at 0 not asserted), A4 (`finish_run` failing after the effects exits 1 with no outcome), A5 (`summary.error` keeps the raw exception text), A6 (the republish token ignores `pending`, harmless).
- L6 (2026-11-18) User request, verbatim: «la vista de soporte debe tener un enlace para acceder a ella desde la vista principal de la EDA». Added as S4 for T5 (PR3).
- L7 (2026-11-18) User request, verbatim: «mientras tanto, empieza la pieza 2 de la API». Branch `feat/support-api` stacked on `feat/support-cli` (#98 not merged yet). The support API shares the single `CommandRunner` with `scripts/notice.sh` because web-front says «ejecutar un único comando a la vez»; disabled it answers 403 before validating the body. Output goes through the existing `GET /api/commands/{run_id}/stream`. `.env.example` not updated (the session may not read it): `WEB_SUPPORT` must be added there by the user.
