# Feature: infra (fase 1)

Objective: phase 1 of CLAUDE.md work order — DB connection, `events` + `outbox` + relay, `inbox`, `effect_keys`, with integration tests against real PostgreSQL and Kafka.
Branch: `feat/infra` · Delivery: ask-on-risk · Runner: `uv run pytest` (Postgres + Kafka from `docker compose`).

## Specs

Source of truth is the repo spec files; scenario names are quoted verbatim. Each scenario gets one test whose docstring is the scenario name.

- S1 `docker compose up -d` brings `kafka` and `postgres` to `healthy` and creates topics `eda.events` (3 partitions) and `eda.dlq` (1 partition) — CLAUDE.md: "comprueba que el broker queda `healthy` y que se crean los topics".
- S2 event-backbone / "Estructura común del evento": scenarios "Evento completo", "Evento sin correlation_id".
- S3 event-backbone / "Log de eventos inmutable" + "Log en PostgreSQL": scenarios "Intento de modificar un evento", "Intento de borrar un evento".
- S4 event-backbone / "Publicación atómica con el estado (Outbox)": scenario "Fallo entre el cambio y la publicación".
- S5 event-backbone / "Orden por incidencia" + "Kafka como bus de entrega": scenarios "Orden de pasos", "Pérdida del topic" (relay `--replay-from <seq>`, key = `incident_id`).
- S6 event-backbone / "Entrega al menos una vez": scenario "Reentrega tras falta de confirmación" (`enable.auto.commit=false`, offset commit only after PostgreSQL `COMMIT`).
- S7 idempotency / "Inbox por consumidor": scenarios "Reentrega de Kafka", "Caída antes de confirmar el offset".
- S8 idempotency / "Clave de idempotencia por paso": scenarios "Misma clave para el mismo paso", "Paso mantenido en una revisión".
- S9 idempotency / "Inbox y claves de efectos son mecanismos distintos", "Consulta del almacén de claves antes de actuar", "Registro atómico de la clave", "Conservación de las claves": scenarios "Evento nuevo que pide un efecto ya ejecutado", "Evento duplicado", "Dos ejecutores a la vez", "Duplicado tardío".
- S10 Switches `INBOX_ENABLED` and `EFFECT_KEYS_ENABLED` (default on) disable each mechanism for E1.

Out of scope (later phases, assumption): "Cadena de causalidad" (needs planner), "Evento de versión anterior" (upcasters in `events/`), "Evento de aviso al cliente" and the per-action uniqueness requirements (executors), "Caída tras enviar el correo" (simulated external mail service).

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1 | inline | done (healthy + topics verified) | 41165ba |
| T2 | S2, S3, S4 (writer side) | inline | done (RED import error -> 7 passed) | a185de3 |
| T3 | S4, S5 (relay) | inline | done (RED import error -> 10 passed); "Pérdida del topic" consumer-side clause covered in T5 | 8e742c6 |
| T4 | S6, S7 (consumer loop + inbox) | inline | done (RED import error -> 14 passed, 3 runs) | 991464e |
| T5 | S8, S9, S10 (effect_keys + switches) | inline | done (RED import error -> 31 passed, 3 runs) | next commit |

Route evidence: one sequential feature, units depend on each other (relay needs writer, consumer needs relay); no Writer trigger reason.

## Log

- L1 (2026-11-13) user: "en /Users/hector/develop/ai/agents/eda-retail quiero empezar a desarrollar el proyecto. Tienes ahí las specs"
- L2 (2026-11-13) user approved: initial commit on `main` with existing files and phase 1 on `feat/infra` ("Sí, adelante"). Initial commit `b41b967`.
- L3 Test isolation decision: tests create a fresh `eda_test` database from `db/init/001_schema.sql` per session (events cannot be truncated) and a unique Kafka topic per test.
- L4 (2026-11-13) user: "he movido unos ficheros y he creado un dir llamado docs" — `specs/README.md` and diagrams moved to `docs/` (uncommitted, user-owned). An earlier commit of mine swept them in via `git add -A`; redone as `41165ba` with only my files. From now on stage explicit paths only.
- L5 (2026-11-13) user approved fixing references and committing the docs move ("Sí, y sigue con T4"): `e806764`. T4 adds `EventConsumer` (offset commit only after PostgreSQL COMMIT, unhandled types ignored without inbox) and `inbox.register`; DLQ/retries stay in `resilience`.
- L6 (2026-11-13) user: "estoy corrigiendo design.md no lo tengas en cuenta hasta que te avise", then "ya está corregido design.md". User reverted the `docs/` links and the §12 note in design.md and deleted docs/*.png|svg (uncommitted, user-owned, not touched). §7 unchanged, so T5 followed it.
- L7 T5: `effect_keys.acquire/complete` (ACQUIRED/COMPLETED/BUSY, takeover after `locked_until`), `compute_idempotency_key` = sha256 of JSON [incident_id, step_id, origin_plan_version]. Key purge after the safety period is not implemented (keys are never deleted). Phase 1 complete.
- L8 (2026-11-13) user: "Sí, verifícalo". Independent verifier on `b41b967..5dfe201`: no blockers, S1–S10 pass, 31 passed; concurrency probes (16 concurrent acquirers, takeover race after `locked_until`, rollback of winner/takeover holder) all held. Scratch dir and `eda_verify` DB removed by parent; parent spot check 31 passed.
  Advisories carried to later phases:
  - A1 (resilience): after a BUSY/handler exception, not committing the offset is not enough if the same consumer keeps polling; a later commit skips the failed message. Retry loop must `seek` back or pause the partition.
  - A2 (key purge, when added): `effect_keys.acquire` needs a None guard if a row can vanish between the INSERT conflict and `SELECT … FOR UPDATE`; `complete` does not check `rowcount` and has no lease fencing (consistent with spec).
  - A3 (executors): `settings.effect_keys_enabled` and `effect_lock_seconds` are parsed but not yet consumed; executors must pass them to `acquire`/`complete` or E1 ignores the switch.
