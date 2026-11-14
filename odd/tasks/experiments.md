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
- A1 Expected plan per variant (the spec fixes only `delay`/`low` and `loss`/`high`). Severity: `delay` `low` up to 24 h, `medium` up to 72 h, `high` above; `loss` and `damage` `high`; `wrong_address`, `strike` and `unknown` `medium`. Actions: any `low` → `notify_customer`; `delay` `medium` → `notify_customer`; `delay` `high` → `notify_customer`, `file_claim`; `loss` and `damage` → `notify_customer`, `file_claim`, `reserve_stock`, `reissue_shipment` (S4 depends on S3); `wrong_address` → `notify_customer`, `reserve_stock`, `reissue_shipment`; `strike` and `unknown` → `notify_customer`. Reviewed and approved as is by the user (L15); the policy is written in the planner prompt (`planner.POLICY`).
- A2 Variants are assigned in a seeded shuffled round-robin, so a set of 50 has 8 or 9 of each.
- A3 Idempotency modes for E1 (user approved the harness design, L5): `full` (inbox everywhere, `effect_keys`, services keyed), `no_effect_keys` (executors lose inbox, `effect_keys` and service deduplication; agents and orchestrator keep their inbox) and `none` (everything off). H1 compares `full` and `no_effect_keys`; `none` is reported as an observation. A duplicate is a repeated external execution of a step (`FakeService.executions`) or an extra reservation row. The UNIQUE constraints on the effect tables stay: a repeated step crashes the executor after the external effect, Kafka redelivers, and the harness skips the message after 3 crashes (poison message).
- A4 Generated order ids are `P-S<seed>N<nnnn>` (no inner hyphens), so the writer's validation reads them as one order reference.
- A5 E3 uses only `loss` incidents (taken from a seeded set of `incidents × 6`) with both warehouses empty; every reservation fails, so H3 expects a refund or an escalation in each one.
- A6 E4 rebuilds the four views with `projectors.rebuild` after the run, with no writers active.
- A7 E7 injects a 50 % failure rate on `reserve_stock` so revised plans appear, and samples the incidents with `random.Random(seed)`. A decision is a `StepRequested`; its agent is the actor of the plan event with the same version; its reason is `PlanRevised.reason` or, for a created plan, the cause and severity of the `ShipmentDelayed` reached through `causation_id`.
- A8 The baseline keeps its business state in PostgreSQL (user choice, L11): a database of its own per run with `orders`, `stock`, `reservations`, effect tables and `incidents`, and no `events`, outbox, inbox, views or Kafka. It calls the same action `prepare`/`execute`, `validate_plan`, agent prompts and checks, and ports the saga rules (ready steps, refund pivot, compensations) to an in-memory step map. Ready steps run one at a time in (origin version, `step_id`) order. It applies business faults and duplicates (the same notice received N times; a repeated notice returns the existing incident); consumer crashes and delivery delays have no synchronous equivalent.
- A9 E5 evaluates the planner on its own (user choice, L15): the labelled cause, severity and delay, the same prompt (`planner.plan_request`), checks (`validate_plan`) and retries. A plan is a hit when it proposes the same actions as the expected plan, regardless of order and `step_id`. `low` severity is planned by code without the LLM, so those incidents are counted apart (`code_planned`) and stay out of the accuracy. H5 holds when the mean accuracy of the repetitions reaches the threshold.
- A10 E6 runs, per repetition, the same notices with the same LLM in both versions and two scenarios: `no_faults`, and `faults` (every event delivered 2 times, the same notice received 2 times in the baseline, and 30 % failures on `reserve_stock`; consumer crashes have no synchronous equivalent). Money cost = tokens × configured prices. H6 has no threshold, so the report has no verdict. The events latency includes the harness turn scheduling in one process (short polls per consumer), so it is not a one-process-per-component deployment latency; with the oracle the harness dominates, with Ollama the LLM does.

## Tasks

| ID | PR | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- | --- |
| T1 | 1 | S1 generator, labels, dataset loading | inline | done (RED collection error → GREEN 264 passed) | dc84a8e (PR #7, merged 6d8aa24) |
| T2 | 2 | S2 fault injection | inline | done (RED collection error → GREEN 272 passed) | 22b42f6 (PR #8, merged b625e3a) |
| T3a | 3a | S4 oracle LLM, writer-compatible order ids (A4) | inline | done (RED → GREEN 284 passed) | e3086c1 (PR #9, merged 0755746) |
| T3b | 3b | S3, S4 harness, idempotency modes (A3), non-idempotent services | inline | done (slice alone: 280 passed) | e061343 (PR #10, merged 5e337e5) |
| T3c | 3c | S3, S4 report, E1, E2, `eda-experiment`, docs | inline | done (RED → GREEN 284 passed) | 253a642 (PR #11, merged 6f89ab8) |
| T4 | 4 | S4 E3, E4, E7 (A5–A7), per-experiment default size | inline | done (RED collection error → GREEN) | 39ba3e2 (PR #12, merged fa4f265) |
| T5a | 5a | S5 saga rules in memory (`baseline_sync/saga.py`), shared harness helpers | inline (mapping by one read-only explorer) | done (unit tests GREEN; slice alone 292 passed) | cecb2cc (PR #13, merged 1386145) |
| T5b | 5b | S5 synchronous runner (A8) | inline | done (RED collection error → GREEN) | 72010cf (PR #14, merged 80891ee) |
| T6a | 6a | S6 A1 policy in the planner prompt, E5 (A9), `--llm`, `--repetitions`, `--threshold` | inline | done (RED → GREEN) | 13e0c9e (PR #15, merged f84fbdd) |
| T6b | 6b | S6 E6 events vs synchronous (A10) | inline | done (RED collection error → GREEN) | PR 6b (this commit) |

Route evidence: sequential units, each depends on the previous one; no writer trigger.

## Log

- L1 (2026-11-14, user): "continua. Además se han definido specs para un front que se desarrollará cuando se haya terminado el resto"
- L2 (2026-11-14, user): "Aprobada" — phase 6 proposal (D1–D7, S1–S6, six stacked PRs) approved.
- L3 (2026-11-14, user): "mergea tú" — PR #7 (T1) merged after CI passed (6d8aa24).
- L4 (2026-11-14, user): "Sí, todo" — T2 committed, PR #8 opened and merged after CI passed.
- L5 (2026-11-14, user): "describe el harness que se ha aplicado" → design described; "Sí, sigue".
- L6 (2026-11-14): T3 findings. (a) The writer's order-reference regex read `P-11-0001` as two orders; generator ids changed (A4) with a test. (b) Without any inbox, a repeated `ShipmentDelayed` reaches the planner again, the handler rejects the second plan and the planner escalates the incident with `llm_output_invalid` before any step runs, so no effect duplicates can appear; E1 now runs three modes (A3). Seed 7, 12 incidents: `full` 0 duplicates; `no_effect_keys` 75 (2 deliveries) and 81 (3 deliveries), 69 consumer crashes, 23 poison messages; `none` 12/12 escalated. H1 holds.
- L7 (2026-11-14): PR 3 measured ~797 changed lines; one slicing pass split it into 3a (oracle), 3b (harness) and 3c (report, E1, E2, CLI, docs), each within 400.
- L8 (2026-11-14, user): "Yes, commit 3a" / "Yes, push and PR" / "mergeada PR" — 3a and 3b committed, PRs #9 and #10 merged after CI passed; each slice was tested alone before its commit.
- L9 (2026-11-14, user): "mergeada" — PR #11 (3c) merged after CI passed (6f89ab8); T4 started.
- L10 (2026-11-14): T4 finding. The oracle replanner kept completed steps in the revised plan, against the replanner rule "No incluyas los pasos ya completados"; the handler rejected every proposal and E3 escalated 4/4 with `llm_output_invalid`. The oracle now drops completed steps and the oracle test pins it; E3 then resolves with refunds.
- L11 (2026-11-14, user): "Merge it yourself" — PR #12 merged (fa4f265). Baseline design question answered "A: PostgreSQL" (A8).
- L12 (2026-11-14): T5. Mapping showed agents' prompts/checks, `validate_plan`, `Writer.draft`, services and `compute_idempotency_key` reuse without the log, while the saga rules live in SQL on `plan_steps`; they were ported to `baseline_sync/runner.py`. The harness gained `isolated_database` and `effect_duplicates`, shared by both versions.
- L13 (2026-11-14): T5 measured ~510 changed lines; one slicing pass split it into 5a (saga rules as pure functions with unit tests, harness helpers) and 5b (runner, integration tests, docs).
- L14 (2026-11-14, user): "Yes, commit 5a" / "Yes, push and PR" / "mergeada" — PR #13 (5a) merged after CI passed.
- L15 (2026-11-14, user): "Merge it yourself" — PR #14 merged (80891ee). E5 design "A: planner on its own"; A1 "Approve as is"; policy in the prompt "Yes, add it". The planner prompt only had the `loss` example, so E5 would have measured whether the model guesses the table; the six policy rows are now in `planner.SYSTEM`, with a test that ties them to `expected_plan`. This changes the prompt version of the reports.
- L16 (2026-11-14, user): "mergeada" — PR #15 (6a) merged (f84fbdd). The user launched `LLM_MODEL=qwen2.5:7b-instruct uv run eda-experiment e5`; result pending.
- L17 (2026-11-14): T6b. Smoke run of E6 with the oracle (6 incidents, 1 repetition): both versions close 6/6 with 0 duplicates and the same LLM calls; events latency ~1.5 s vs sync ~12 ms, driven by the harness scheduling (A10). Alembic INFO logs are now silenced in `eda-experiment`.
