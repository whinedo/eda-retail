# E22 command races and the effect–record window

Objective: implement the E22 injections (concurrent command delivery, reinvocation with another `command_id`, `crash_between_effect_and_record`) and experiment E22 (H22). Branch `feat/e22-command-races` from main e93c648; delivery `single-pr` (one PR per experiment, user rule).
Test runners: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"` (unit); integration and experiments only in CI (CI already takes ~16–17 min of a 20 min timeout: keep the E22 integration run small).

## Specs

- S1 "Empezar E22 (carreras de comandos y ventana efecto–registro, H22): lee su requisito en specs/experiments/spec.md y «Entrega concurrente de comandos» en specs/synthetic-data/spec.md (crash_between_effect_and_record está sin implementar en FaultPlan)."
  - specs/experiments «E22 Carreras de comandos y ventana efecto–registro (H22)», verbatim scope: "El escenario son 50 incidencias con cuatro variantes: (a) el mismo `ExecuteStep` entregado desde 2, 4 y 8 hilos con conexiones propias; (b) el mismo `CreatePlan`, `RevisePlan`, `RegisterCarrierNotice` y `SeedSampleData` concurrentes con el mismo `command_id`; (c) dos `command_id` distintos con la misma `idempotency_key` (por ejemplo, una reinvocación de la saga tras un plazo) para `notify_customer`, `file_claim`, `reserve_stock` y `issue_refund`; (d) `crash_between_effect_and_record` en `notify_customer` y en `issue_refund`, con reintento."
  - Report, verbatim: "El informe MUST dar, por variante, las ejecuciones del servicio simulado por clave, las respuestas distintas recibidas por un mismo `command_id`, los eventos escritos y las veces que una petición esperó a una clave `in_progress`."
  - Scenarios (one test each): «Dos ejecutores a la vez», «Comando concurrente con trabajo diferido», «Misma clave, otro command_id», «Caída tras el efecto y antes de registrar la clave», «Clave en curso vencida», «Mismo command_id con otro payload» (observation only), «Cumplimiento de H22».
  - specs/synthetic-data «Entrega concurrente de comandos», verbatim: "La inyección SHALL permitir entregar un mismo comando desde N hilos (o procesos) a la vez, con conexiones propias, para ejercer las carreras del inbox y de `effect_keys`, y SHALL permitir un punto de caída entre el efecto externo y el registro de su clave (`crash_between_effect_and_record` por acción)." Scenario «Caída entre efecto y registro».
- S2 "un PR por experimento, con cada inyección (specs/synthetic-data) junto al experimento que la usa."
- S3 "El CI no sube informes: para diagnosticar un fallo de integración, mete las métricas en el mensaje del assert."
- S4 "Si una spec y el código chocan, pregúntame y corrige la spec en el mismo cambio."

Design decisions (assumptions, flagged to the user):

- `ExecuteStep`, `CreatePlan` and `RevisePlan` are deferred commands: the request only registers the inbox and one `command_jobs` row. Concurrent deliveries of the same `command_id` therefore race on the inbox, and only one job reaches `effect_keys`. The `effect_keys` race is exercised by variant (c): the two `command_id`s are delivered at the same time and E22 runs two job workers per component, so both jobs can prepare and handle at once.
- "las veces que una petición esperó a una clave `in_progress`" is measured as the `BUSY` claims of `effect_keys.acquire` in the job (a new `EffectInProgress` error, subclass of `RuntimeError`, so behaviour is unchanged). A concurrent `INSERT … ON CONFLICT` blocks on the other transaction and then sees `completed`; that is not counted as `BUSY`.
- "ejecuciones del servicio simulado por clave": `FakeService.executions` for `notify_customer`, `file_claim`, `issue_refund`; for `reserve_stock` (no external service) the `StockReserved` events per key.
- "eventos escritos": effect events and `StepCompleted` per key, from `event_log.events` joined by `causation_id` to the delivered `command_id`s.
- (c) is an injection (`reinvoke_steps` in `FaultPlan`): the dispatcher's `ExecuteStep` of those actions is delivered with its own `command_id` and, at the same time, with a fresh one (same payload, same key). The saga ignores the second `StepCompleted` (stale step).
- `crash_between_effect_and_record`: the first attempt of each key of those actions calls the service and then fails before the handle transaction; the job retries with the same `command_id` and key.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1 (injections), S2 | inline | done | a49f8e6 |
| T2 | S1 (experiment, report, scenarios), S3, S4 | inline | done | be82764, 808243a, fix in next commit |
| T3 | Risk item 3 | delegated verifier over the whole document | done | 1 blocker fixed (RED/GREEN), rest PASS |

## Log

- L1 (2026-11-17) User request, verbatim: "Empezar E22 (carreras de comandos y ventana efecto–registro, H22): lee su requisito en specs/experiments/spec.md y «Entrega concurrente de comandos» en specs/synthetic-data/spec.md (crash_between_effect_and_record está sin implementar en FaultPlan). Antes de escribir código, crea odd/tasks/<feature>.md."
- L2 (2026-11-17) Map: `ExecuteStep` is deferred (step_executors/component.py), jobs retry on any exception with the same job (infra/jobs.py), one `JobWorker` per component in the harness, `effect_keys` key and effect commit together (so `BUSY` needs a committed `in_progress` row), the saga ignores a `StepCompleted` for a step that is no longer `requested` (orchestrator/saga.py:332), effect events carry no `step_id`, `FakeService` dedups by key.
- L3 (2026-11-17) T1 a49f8e6 (injections, spec synthetic-data scenarios «Mismo comando desde varios hilos» and «Reinvocación con otro command_id»), T2 be82764 (E22, CLI, expexecutor, guide, integration tests seed 7 / 4 incidents / 8 threads; expired in-progress key test in test_step_executors). Unit 702 passed, ruff ok; unit tests of E22 written after the code (no observed RED). Integration only in CI. Risk item 3 (shared harness command delivery): independent verifier launched.
- L4 (2026-11-17) User, verbatim: "Sí, precisa el WHEN en este PR". specs/experiments «Dos ejecutores a la vez»: the WHEN now says the 8 deliveries reach the executor inbox at once, and the THEN that the inbox registers one delivery, whose single job registers the key and executes the effect.
- L5 (2026-11-17) T3 independent verifier: scenarios PASS; BLOCKER: with `duplicates > 1` `InProcessCommands` returned the first response instead of the last (baseline), which changes E1 mode `none` (the second `CreatePlan` without inbox is rejected with `plan_exists` and the saga escalates with `command_rejected`). Fix: return the last delivery of the command; RED `test_duplicated_commands_answer_with_the_last_delivery` failed, GREEN after the fix. Advisory taken: inventory BUSY now raises `EffectInProgress` too (moved to infra/effect_keys.py) so `busy_claims` counts it; `counting_busy` got a unit test. Advisories kept: the payload observation covers a stored rejection (`duplicate_notice`), not an accepted C1; `busy_claims` is expected to be 0; seed 7 data dependence is checked only by the integration asserts; E22 integration time estimated at 50–70 % of E23 (measure in the first CI run). Unit 704 passed, ruff ok.
