# Feature: experiments (fase 6: synthetic/, baseline_sync/ y experiments/)

Objective: phase 6 of CLAUDE.md work order — seeded notice generator with labels, fault injection, in-process experiment harness with reports, synchronous baseline, and experiments E1–E7.
Branch: `feat/experiments` from `main` (39bb140) · Delivery: stacked PRs to `main`, one per PR row below; user approves each commit and each push · Runner: `uv run pytest` (LLM always the fake client; real Ollama runs for E5/E6 are the user's).

## Specs

Proposal approved by the user (L2). Scenario names quoted verbatim from `specs/synthetic-data` and `specs/experiments`; one test per scenario, docstring = scenario name.

- S1 Generator (synthetic-data «Generador de avisos», «Repetibilidad», «Datos simulados de negocio»): variants `delay`, `loss`, `damage`, `wrong_address`, `strike`, `unknown`, "en texto libre y en formato de API, y con una etiqueta de la `cause` y del plan esperado". Scenarios "Conjunto de evaluación" ("produce 50 avisos con su causa y su plan esperado", "incluye al menos cinco de cada variante") and "Dos ejecuciones con la misma semilla" ("los dos conjuntos son idénticos"). Orders carry references only (no personal data).
- S2 Fault injection (synthetic-data «Inyección de fallos», «Fallos reproducibles», «Datos simulados de negocio»): "duplicar eventos (de 1 a N veces), retrasar su entrega, hacer fallar un tipo de paso con una probabilidad dada, caer un consumidor en un instante dado y dejar sin stock un almacén". Scenarios "Duplicación de eventos", "Misma semilla, mismos fallos", "Reserva de stock simulada".
- S3 Harness and report (experiments «Informe reproducible»): one command per experiment, `uv run eda-experiment <exp> --seed N`, report with seed, configuration, prompt and model versions and metrics (`experiments/results/<exp>-<timestamp>/report.json` and `report.md`). Scenario "Ejecución de un experimento".
- S4 E1, E2, E3, E4, E7 with the fake oracle LLM: scenarios "Con idempotencia", "Sin idempotencia", "Caída a mitad del plan", "Almacén sin existencias", "Reconstrucción completa", "Explicación de una incidencia".
- S5 Synchronous baseline (experiments «Base síncrona de referencia»): same agents, executors' services and compensation rules, direct calls without `events`, outbox, inbox or views. Scenario "Misma entrada en ambas versiones".
- S6 E5 and E6 with repetitions: scenarios "Evaluación del planificador", "Puerta de calidad", "Comparación sin fallos", "Comparación con fallos", "Tres repeticiones de E5".

Design decisions (approved, L2):
- D1 In-process harness: one process runs relay, projectors, agents, orchestrator and executors as consumers served in turns, against real PostgreSQL and Kafka with a database and topics of its own per run. E6 measures both versions inside the same harness.
- D2 E1–E4 and E7 use a fake oracle LLM answering from the generator labels; E5 and E6 use Ollama (three repetitions, run by the user) with `--llm fake` for smoke runs.
- D3 Duplicates, delays and crashes are injected at message delivery (each consumer gets each message N times; held messages; a consumer stops after K messages without committing the last offset and restarts in the same group).
- D4 Business faults are seeded: failure probability per action in the simulated services, warehouses with no stock for chosen orders.
- D5 The baseline reuses interpreter, planner, replanner, writer, effect services and code-derived compensations through direct calls with in-process retries.
- D6 Reports as in S3; prompt version = hash of the prompt templates.
- D7 Expected plans derive from `incident-planning`.

Assumptions (marked):
- A1 Expected plan per variant (the spec fixes only `delay`/`low` and `loss`/`high`). Severity: `delay` `low` up to 24 h, `medium` up to 72 h, `high` above; `loss` and `damage` `high`; `wrong_address`, `strike` and `unknown` `medium`. Actions: any `low` → `notify_customer`; `delay` `medium` → `notify_customer`; `delay` `high` → `notify_customer`, `file_claim`; `loss` and `damage` → `notify_customer`, `file_claim`, `reserve_stock`, `reissue_shipment` (S4 depends on S3); `wrong_address` → `notify_customer`, `reserve_stock`, `reissue_shipment`; `strike` and `unknown` → `notify_customer`. To be reviewed by the user in PR 1.
- A2 Variants are assigned in a seeded shuffled round-robin, so a set of 50 has 8 or 9 of each.

## Tasks

| ID | PR | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- | --- |
| T1 | 1 | S1 generator, labels, dataset loading | inline | done (RED collection error → GREEN 264 passed) | (pending commit) |
| T2 | 2 | S2 fault injection | inline | pending | |
| T3 | 3 | S3, S4 harness, report, E1, E2 | inline | pending | |
| T4 | 4 | S4 E3, E4, E7 | inline | pending | |
| T5 | 5 | S5 baseline_sync | inline | pending | |
| T6 | 6 | S6 E5, E6, repetitions | inline | pending | |

Route evidence: sequential units, each depends on the previous one; no writer trigger.

## Log

- L1 (2026-11-14, user): "continua. Además se han definido specs para un front que se desarrollará cuando se haya terminado el resto"
- L2 (2026-11-14, user): "Aprobada" — phase 6 proposal (D1–D7, S1–S6, six stacked PRs) approved.
