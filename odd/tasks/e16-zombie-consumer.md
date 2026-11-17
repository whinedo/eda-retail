# E16 zombie consumer and rebalance

Objective: implement the harness multiprocess mode, the «Congelación de un proceso» injection (SIGSTOP/SIGCONT) and experiment E16 (H16). Branch `feat/e16-zombie-consumer` from main f4bcbb6; delivery `single-pr` (one PR per experiment, user rule).
Test runners: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"` (unit); integration and experiments only in CI (sharded; new slow files land in `slow-b`, 20 min job timeout untouched).

## Specs

- S1 Requirement specs/experiments «Modo multiproceso del harness», verbatim: "El harness SHALL ofrecer un modo multiproceso (`--mode multiprocess`) que arranque cada componente como proceso propio contra PostgreSQL y Kafka reales (los mismos procesos que `scripts/run-agents.sh`), con una base de datos y un topic propios por ejecución." Scenario «Aislamiento de la ejecución»: "**THEN** sus procesos están parados y su base de datos y su topic se han borrado".
- S2 Injection specs/synthetic-data «Congelación de un proceso», verbatim: "En modo multiproceso, la inyección SHALL permitir congelar un proceso con `SIGSTOP` durante un tiempo y reanudarlo con `SIGCONT`, de modo que un consumidor de Kafka pierda su sesión (`session.timeout.ms` o `max.poll.interval.ms`) y vuelva a vivir después de que otra instancia haya asumido sus particiones." Scenario «Consumidor zombie»: "**THEN** el experimento registra que Kafka había reasignado sus particiones antes de que reanudara y qué mensajes procesó después".
- S3 Requirement specs/experiments «E16 Consumidor zombie y rebalanceo (H16)», verbatim: "El experimento SHALL ejecutarse en modo multiproceso con dos instancias del orquestador en el mismo grupo, 50 incidencias en curso y un `SIGSTOP` a la instancia que posee más particiones durante un tiempo mayor que `session.timeout.ms`; después `SIGCONT`." and "el informe MUST dar, por incidencia, si dos instancias procesaron mensajes de ella solapados en el tiempo y qué transacciones esperaron al bloqueo de la incidencia." Scenarios, one test each: «Rebalanceo mientras la instancia está congelada» ("**AND** el informe registra el instante del rebalanceo"), «Reanudación del zombie» ("el inbox descarta el mensaje si B ya lo registró, o la confirmación falla sin dejar estado a medias" / "`orchestrator.plan_steps` no retrocede ningún paso de `completed` a otro estado"), «Cumplimiento de H16» ("H16 se cumple si no hay dos comandos con `command_id` distinto para el mismo paso y la misma versión del plan, ni transiciones inválidas, y I1 a I5 no tienen violaciones").
- S4 Spec correction (follows the E12 user decision "En proceso", L1 of odd/tasks/e12-executor-down-hung.md, and L3): «Modo multiproceso del harness» still says "Los experimentos E12 y E16 MUST usar ese modo"; E12 runs in process, so only E16 must. Per L3 the multiprocess mode runs only the orchestrator instances as separate processes; the rest stays in the in-process harness with the simulated LLM client. The requirement text is rewritten to say so.

Map (explorer, 2026-11-17):

- Processes start as console scripts `uv run eda-<name>` (scripts/run-agents.sh:38-51, pyproject.toml:22-37); all config comes from env vars (config.py:52-152) and `.env` never overrides an env var already set. A signal to the `uv run` PID hits uv, not python: launch the python entrypoint directly or signal the process group.
- The orchestrator has no HTTP listener and a fixed group id `orchestrator` (orchestrator/cli.py:51-79), so two instances can run together. It runs a consumer, an outbox relay, a dispatcher thread and a deadlines thread.
- `make_consumer` (infra/kafka.py:18-26) sets no `session.timeout.ms`, `max.poll.interval.ms` or rebalance callbacks; librdkafka defaults are 45 s and 300 s.
- Handlers lock the incident with `SELECT ... FOR UPDATE` (orchestrator/saga.py `lock`); the dispatcher and the deadlines thread lock it too. A frozen instance mid-transaction keeps the inbox row and the incident lock, so the other instance waits on them.
- No per-message processing record with instance identity exists: the inbox has only `processed_at`.
- `dispatch_once._due` has no `FOR UPDATE SKIP LOCKED`, so two instances can deliver the same `command_outbox` row, with the same `command_id` (the receiver's inbox dedupes it).
- Reusable: `isolated_database`, topic creation and deletion from `Harness.__enter__/__exit__`, `group_lag`, `high_watermark`, `RunResult` SQL, `invariants.check` (I2 uses in-memory fake services and must be re-sourced from effect tables in multiprocess).
- The LLM components' CLIs (planner, replanner, interpreter) always build `OllamaClient()`; there is no switch to the simulated client, and CI has no Ollama.

Decision (L3): only the orchestrators run as separate processes.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1, S4 | inline | done: harness `orchestrator_processes`, `processes.py` (CommandGateway, start_orchestrators), spec rewritten; unit GREEN, integration in CI | 2d8aaa5 |
| T2 | S2 | inline | done: `OrchestratorProcess.freeze/resume` (SIGSTOP/SIGCONT), unit test on a real process | 2d8aaa5 |
| T3 | S3 (instance identity, processing and lock-wait record, configurable session timeout) | inline | done: KAFKA_SESSION_TIMEOUT_MS, rebalance/start/lock-wait log lines parsed by `read_log`; instance identity = process log file | 2d8aaa5 |
| T4 | S3 (experiment, invalid-transition check, report, CLI, guide) | inline | done: `e16_zombie_consumer.py` (SagaSampler, rebalance, zombie, double_processed, overlaps, lock_waits, h16), CLI, expexecutor, guide; unit GREEN, integration in CI (10 incidents) | (this commit) |
| T5 | Risk items, whole document | delegated verifier | pending | |

## Log

- L1 (2026-11-17) User request, verbatim: "Después, el modo multiproceso con SIGSTOP para E16 (specs/experiments «E16 Consumidor zombie y rebalanceo», specs/synthetic-data «Congelación de un proceso»). Antes de escribir código, crea odd/tasks/<feature>.md."
- L2 (2026-11-17) Created after merging #81 (f4bcbb6). Map above from a read-only explorer.
- L3 (2026-11-17) User decision on how LLM components run in multiprocess mode, verbatim: "Solo orquestadores aparte".
- L4 (2026-11-17) T1-T3 implemented. Design: the external instances run their own outbox relay (the harness drops its orchestrator relay); receivers stay in process behind a local HTTP gateway (`command_router` per target prefix); no production behavior change beyond optional session timeout and DEBUG/INFO log lines. A zombie whose Kafka commit fails after SIGCONT keeps today's behavior (the process exits); E16 reports its exit code.
- L5 (2026-11-17) T4 implemented. Invalid transitions = a sampled step leaving `completed` other than to `compensating`/`compensated`, or a closed incident changing status. Integration test runs 10 incidents (CI time), the CLI default is 50.
