# E12 executor down and hung

Objective: implement the «Ejecutor caído o colgado» injection (in-process mode) and experiment E12 (H12a, H12b). Branch `feat/e12-executor-down-hung` from main 687d880; delivery `single-pr` (one PR per experiment, user rule).
Test runners: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"` (unit); integration and experiments only in CI (sharded; new slow files land in `slow-b`).

## Specs

- S1 "Después, E12." Requirement specs/experiments «E12 Ejecutor caído y ejecutor colgado (H12)», scenarios «Ejecutor caído para siempre», «Ejecutor colgado», «Recuperación del ejecutor», «Compensación de un paso con plazo vencido», each with a test. Verbatim hypothesis: "H12a (vivacidad) toda incidencia que usa el ejecutor afectado termina escalada o resuelta dentro de `SAGA_STEP_TIMEOUT_S` más un margen, y las que no lo usan terminan como siempre; H12b (aislamiento) la latencia p95 de las incidencias que no usan el ejecutor afectado no supera el doble de la de una ejecución sin fallo." H12b is exploratory: "un resultado negativo de H12b es un hallazgo y no un fallo del experimento, y el informe lo MUST decir."
- S2 Injection specs/synthetic-data «Ejecutor caído o colgado», verbatim: "`down` (rechaza la conexión) y `hang` (acepta la conexión y no responde nunca). En el modo en proceso se simula haciendo que `InProcessCommands` lance `CommandUnavailable` o espere el timeout".
- S3 User decision (2026-11-17): E12 runs in-process ("En proceso"), and the E12 spec is corrected in this PR: it currently says "El experimento SHALL ejecutarse en modo multiproceso".

Design (assumptions):

- `ExecutorFault(target, mode, action=None, duration_s=None)` in `FaultPlan.executor_faults`: the window opens at the first command delivered to that target (and action, if given) and lasts `duration_s` (None: forever). `down` raises `CommandUnavailable` at once; `hang` keeps the request (backlog) and waits the command timeout before raising it. When a `hang` window ends, the backlog runs once (the stuck process answers what it had accepted), so late results arrive stale or to steps still current.
- The dispatcher runs in its own thread for E12 (`RunConfig.dispatcher_thread`), as in `orchestrator/cli.py`; otherwise a hang would also freeze relays and projectors in the harness turn and distort H12b. Deadlines and the rest stay in the turn.
- Variants: `baseline` (no fault), `down`, `hang`, `hang_recovery` (recovers after 1.5 × step timeout) on `executor_file_claim`, and `compensation`: `inventory` down from the first reserve for 1.5 × step timeout, so the reserve times out, the saga derives its `release_stock` `-comp` (orchestrator/saga.py `fail`: a timed-out compensable step) and it completes when `inventory` is back, before its own deadline. A release with no reservation completes without effect.
- The harness keeps working while a hung executor that will recover has accepted requests (`awaiting_recovery`), so the backlog is answered even if every incident already closed.
- H12a: no incident left open; every incident using `file_claim` closes within `SAGA_STEP_TIMEOUT_S` + margin (baseline max latency + `MARGIN_S`); incidents not using it end with the same status as in `baseline`. H12b: p95 latency of incidents not using `file_claim` ≤ 2 × their p95 in `baseline`.
- Seed 42 gives 18 of 50 incidents with `file_claim` (36 %); the spec no longer asks for "aproximadamente la mitad" and the report gives the share (`config.file_claim_share`, `incidents.file_claim_share`).

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S2 | inline | done | 6bcc9e5 |
| T2 | S1, S3 | inline | done | ed85a31, fix in next commit |
| T3 | Risk items 3, 4 | delegated verifier over the whole document | done | 1 blocker fixed (RED/GREEN), rest PASS |

## Log

- L1 (2026-11-17) User request, verbatim: "Sí, empieza E12" (plan: "2. Después, E12."). Decision on the mode clash, verbatim answer: "En proceso (Recommended)".
- L2 (2026-11-17) Map: dispatcher `dispatch_once` takes one due command (`LIMIT 1`), starts the deadline at first send and retries `CommandUnavailable` with backoff (orchestrator/dispatcher.py); `expire_once` fails a timed-out step with `timeout` and marks its pending command `expired`; compensation only for completed `reserve_stock` (orchestrator/rules.py); production runs dispatcher and expiry in their own threads (orchestrator/cli.py); `StockReleased` carries `reservation_id`.
- L3 (2026-11-17) T1 6bcc9e5: `ExecutorFault`, `ExecutorOutage`, `InProcessCommands` down/hang/backlog/`flush_recovered`, `RunConfig.dispatcher_thread` and `command_timeout_seconds`. RED observed (5 failed), GREEN.
- L4 (2026-11-17) T2: experiment `e12_executor_down_hung.py`, CLI, expexecutor (checks H12a only; H12b exploratory), guide, spec E12 now says in-process (user decision). Compensation variant reworked after reading `Saga.fail`: the saga already compensates a timed-out `reserve_stock`. `awaiting_recovery` added after noticing the run could end before the recovery. Unit tests of the experiment written after the code (no observed RED). Unit 722 passed, ruff ok. Integration (seed 7, 6 incidents) CI only. Open point for the user: seed 42 gives 18/50 incidents with `file_claim`, the spec says "aproximadamente la mitad".
- L5 (2026-11-17) T3 independent verifier: regression and concurrency PASS. BLOCKER: the integration test expected the final file_claim steps to be `failed:timeout`, but the revision turns a dropped `failed` step into `cancelled` and keeps its error (`rules.revise`, `_WITHOUT_EFFECT`). Fix: `summarize_claim_steps` counts `timed_out` (error `timeout`, status failed or cancelled) and the test also asserts every file_claim `ExecuteStep` is `expired`; RED (collection error, missing function) then GREEN; unit 723 passed. Advisories taken: E21 `redeliver` passes the interruptible sleep too. Advisories kept: a reserve accepted right at its deadline can derive a second `-comp` (pre-existing A4 path); CI estimate `slow-b` ~12–12.5 min. Also fixed before push: `git add docs` had committed the user's untracked docs/specs-experimentos-resiliencia.md; removed from the commit with `git rm --cached` and amend (file untouched).
- L6 (2026-11-17) User, verbatim: "Sí, corrige la spec". specs/experiments E12 now says the 50 incidents of the experiment seed, part with `file_claim` (18 with seed 42), and the report MUST give the share (already in `config.file_claim_share`, tested by `test_incidents_summary_splits_affected_and_unaffected` and `test_incidents_using_file_claim_come_from_the_expected_plan`).
