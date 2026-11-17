# E13 compensation failure

Objective: write the E13 requirement in specs/experiments and implement experiment E13 (H13) with the existing `fail_probability` injection on `release_stock`. Branch `feat/e13-compensation-failure` from main 348b4c3; delivery `single-pr` (one PR per experiment, user rule).
Test runners: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"` (unit); integration and experiments only in CI (sharded; new slow files land in `slow-b`).

## Specs

- S1 "Empieza E13 «Fallo de compensación» con su spec." "Aún no hay requirement de E13 en specs/experiments/spec.md: escríbelo (hipótesis, escenario y escenarios GIVEN/WHEN/THEN)." Each scenario gets a test.
- S2 Expected behaviour, verbatim: "una compensación que termina en StepFailed y agota sus reintentos escala con compensation_failed, sin issue_refund y sin pedir más compensaciones" (specs/incident-planning, «Fallo de una compensación», «Escalado de la incidencia»).
- S3 "La inyección existe: FaultPlan.fail_probability["release_stock"] (src/eda_retail/synthetic/faults.py). Comprueba contra la saga qué significa «agota sus reintentos»."

Design (assumptions):

- «Agota sus reintentos»: the saga never retries a step. Retries belong to the executor (`StepPreparer`, `STEP_MAX_ATTEMPTS` for retryable `EffectError`); it emits `StepFailed` only after them, and `rules.on_failure` escalates any `StepFailed` of a compensation with `compensation_failed`, retryable or not. The injected failure (`harness.failing`) is non-retryable with `attempts` 1, so it is final at once. No spec–code clash.
- Compensations need a completed `reserve_stock` whose dependent fails: `fail_probability` `reissue_shipment` 1.0 (every reissue fails, the oracle replaces it with `issue_refund` and the saga derives `release_stock` `-comp` at plan version 2) and `release_stock` 0.5 (about half the compensations fail; the rest complete and the held refund is issued, as a control branch).
- Offline count (`should_fail` over the generated dataset): seed 42 / 50 incidents, 25 compensations, 11 fail; seed 13 / 10 incidents (integration test), 6 and 3.
- H13: at least one failed compensation; every incident with a failed compensation ends escalated with `compensation_failed`, with no `issue_refund` requested and no compensation requested after that escalation; I1 to I5 without violations.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1, S2, S3 | inline | done | see L3 |

## Log

- L1 (2026-11-18) User request, verbatim: "1. Empieza E13 «Fallo de compensación» con su spec. Crea antes odd/tasks/e13-compensation-failure.md y una rama desde main. Aún no hay requirement de E13 en specs/experiments/spec.md: escríbelo (hipótesis, escenario y escenarios GIVEN/WHEN/THEN). El comportamiento esperado ya está en specs/incident-planning/spec.md: una compensación que termina en StepFailed y agota sus reintentos escala con compensation_failed, sin issue_refund y sin pedir más compensaciones. La inyección existe: FaultPlan.fail_probability["release_stock"] (src/eda_retail/synthetic/faults.py). Comprueba contra la saga qué significa «agota sus reintentos»."
- L2 (2026-11-18) Map: `Saga.fail` → `rules.on_failure` returns `("escalate", compensation_failed)` for any step with `compensates`; `rules.escalation` derives no compensations for that reason; the oracle replanner replaces a failed `reissue_shipment` with `issue_refund`, so `rules.revise` derives the `-comp` of the completed reserve and the refund waits for it.
- L3 (2026-11-18) T1: requirement «E13 Fallo de compensación (H13)» in specs/experiments with four scenarios; `experiments/e13_compensation_failure.py` (`summarize`, `h13`, `run_e13`), CLI `e13`, expexecutor entry, guide section and table row. RED observed (collection error, missing module), then GREEN: 10 unit tests; unit suite 767 passed, ruff ok, hygiene ok. Integration test (seed 13, 10 incidents, marked slow, lands in `slow-b`) runs only in CI. Risk: none (new experiment and CLI choice; no shared behaviour changed), so no independent verifier. Open point: `slow-b` already takes about 16 of 20 minutes.
