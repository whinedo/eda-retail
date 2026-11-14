Objective: replace the shared data model (one command handler and shared `events`/`outbox`/`incidents`/`plan_steps`/`inbox` written by every process) with a real EDA: each component owns its tables, has its own command handler and outbox, and talks to the others only through Kafka; the read side (CQRS) is made of query components that consume Kafka.
Branch: `docs/eda-component-ownership` (design and specs), then one branch per task. Delivery: chained PRs (`feature-branch-chain` is not needed; each task lands on main in order). Test runner: pytest (unit with a closed DB/Kafka port; integration in CI) and vitest for the front.
Risk: item 1 (persisted format and data move to per-component schemas), item 3 (event and command contracts between components), item 4 (eventual consistency, ordering). Each task names its own items.

## Specs

- S1 User (L1): "Postgresql es leído por todos como log de eventos. Los eventos se emiten en el broker pero, por otro lado, todos los agentes y sistemas escriben en la misma tabla de eventos: esto es un antipatrón (usar una base de datos como proceso de "comunicación") INADMISIBLE. Adapta el código para que, si bien puede ser un log para las vistas, nunca un sistema (código) escriba en una tabla que no sea suya."
- S2 User (L2): "los agentes no escriben los eventos, pero los sistemas (código) sí" — the rule applies to the code around each agent, not to the LLM.
- S3 User (L4): "Puede ser una base de datos común "para la PoC vale" pero cada uno tiene sus tablas y éstas no se relacionan directamente con los de otros. adapta el design.md para arreglarlo y luego el código necesario. Quiero una EDA simple, pero una EDA no un acoplamiento en base de datos." No PostgreSQL roles: one database, one schema per component, no foreign keys or joins across components (spec event-backbone "Propiedad de los datos").
- S4 User (L5): "habrá tantos command handlers como sistemas que actúen por comandos y cada uno escribirá en su tabla y en el broker. De ahí que se necesiten outbox table patterns y CQRS" (spec cqrs-views "Escritura solo mediante comandos", "Catálogo de comandos", "Un único dueño por estado"; event-backbone "Publicación atómica con el estado (Outbox)").
- S5 User (L6): "si no para qué quiero CQRS y no habría consistencia eventual, una casuística con la que hay que lidiar sí o sí en una EDA" — replicas and views lag, and each case is handled explicitly (design.md §4.3; cqrs-views "Consistencia eventual declarada", "Réplica atrasada"; llm-boundaries "Presupuesto por incidencia").
- S6 User (L7): "puedes montar un sistema adicional (query de CQRS) que mantenga una vista del log de eventos (ordenados correctamente) y que sirva para alimentar el front" (cqrs-views "Vista del log de eventos", "Vistas en PostgreSQL con posición en Kafka", "Reconstrucción de vistas"; web-front "Página de eventos").

## Tasks

| ID | Specs | Task | Route | Status | Commit |
| --- | --- | --- | --- | --- | --- |
| T1 | S1–S6 | design.md §2–§8, §10, §13, §14 and specs (event-backbone, cqrs-views, idempotency, llm-boundaries, resilience, web-front) | inline | in review | docs(eda): component ownership |
| T2 | S3, S4 | Reusable per-schema infra: `<schema>.outbox` + relay thread per component, `<schema>.inbox`, `<schema>.dead_letters`, `<schema>.effect_keys`; Alembic migration that creates the schemas; static test "no SQL writes outside the component's schema" | pending | pending | |
| T3 | S6 | Query side: `event_log` component (Kafka → `event_log.events` with `seq`, partition, offset, trace context; offsets in the same transaction) and projectors consuming Kafka with per-partition offsets; rebuild by re-consuming; web reads `event_log.events` | pending | pending | |
| T4 | S3, S4 | `intake` (own `carrier_notices` + outbox) and `inventory` (`orders`, `stock`, `reservations`; `SeedSampleData` emits `OrderRegistered`; runs `reserve_stock`/`release_stock`) | pending | pending | |
| T5 | S2, S4, S5 | `interpreter`: own command handler, `interpretations`, `llm_calls`, replicas `orders` and `budgets`; retry while an order is not replicated yet | pending | pending | |
| T6 | S2, S4 | `planner`: own command handler (plan validation with its `orders` replica), `plans`, replicas | pending | pending | |
| T7 | S4, S5 | `orchestrator`: owns `incidents` and `plan_steps`, consumes `ShipmentDelayed`/`PlanCreated`/`PlanRevised`/step results/escalations, discards stale proposals; `StepRequested` carries what executors need | pending | pending | |
| T8 | S2, S4 | `replanner`: own command handler, `revisions`, replica `incidents` (no view reads) | pending | pending | |
| T9 | S4 | executors: own effect tables, `effect_keys`, `ExecuteStep` handler, replicas `orders` and `budgets` | pending | pending | |
| T10 | S1, S3 | remove the shared command handler and shared tables; data move; README, scripts (`run-agents.sh` starts `event_log` and `inventory`), web topology | pending | pending | |
| T11 | S5 | experiments harness and `baseline_sync` on the new components; the user reruns E1–E7 with Ollama | pending | pending | |

Order: T1 (approve design and specs) → T2 → T3 → T4 → T5–T9 (one per PR, the orchestrator T7 before T8 and T9) → T10 → T11. Routes and writer delegation are decided per task when it starts (Writer trigger only for parallel independent units).

## Log

- L1 (2026-11-14) User: "hay una decisión que me desagrada profundamente. Postgresql es leído por todos como log de eventos. Los eventos se emiten en el broker pero, por otro lado, todos los agentes y sistemas escriben en la misma tabla de eventos: esto es un antipatrón (usar una base de datos como proceso de "comunicación") INADMISIBLE. Adapta el código para que, si bien puede ser un log para las vistas, nunca un sistema (código) escriba en una tabla que no sea suya."
- L2 (2026-11-14) User: "los agentes no escriben los eventos, pero los sistemas (código) sí"; "como digo es innadmisible que escriban en el mismo modelo de datos. Te has cargado completamente una arquitectura EDA sólo con esto".
- L3 (2026-11-14) Verified with an explorer map: every process calls the in-process `command/handler.py`, which writes `events`, `outbox`, `incidents`, `plan_steps`, `carrier_notices`; agents, orchestrator and executors read `incidents` and `orders`; the replanner decides from `view_incident_status`; `inbox`, `effect_keys`, `dead_letters` are shared; projectors read `events` by `seq` instead of Kafka; FKs `outbox.seq → events`, `plan_steps.incident_id → incidents`. About 200 tests touch the handler, `append_events` or the projectors.
- L4 (2026-11-14) User: "el rol de postgre no sería conveniente. Puede ser una base de datos común "para la PoC vale" pero cada uno tiene sus tablas y éstas no se relacionan directamente con los de otros. adapta el design.md para arreglarlo y luego el código necesario. Quiero una EDA simple, pero una EDA no un acoplamiento en base de datos. Es un diseño que habría propuesto un junior no un sistema competente como debes ser".
- L5 (2026-11-14) User: "habrá tantos command handlers como sistemas que actúen por comandos y cada uno escribirá en su tabla y en el broker. De ahí que se necesiten outbox table patterns y CQRS".
- L6 (2026-11-14) User: "si no para qué quiero CQRS y no habría consistencia eventual, una casuística con la que hay que lidiar sí o sí en una EDA. Si pido una EDA no montes una mierda". Added design.md §4.3 with each lag case and how it is handled.
- L7 (2026-11-14) User: "puedes montar un sistema adicional (query de CQRS) que mantenga una vista del log de eventos (ordenados correctamente) y que sirva para alimentar el front". `event_log` becomes a query component; projectors also consume Kafka (no component reads another one's tables); positions are Kafka offsets per partition, rebuild re-consumes the topic (infinite retention).
- L8 (2026-11-14) T1 written: design.md and specs updated; implementation not started. Next: approval of the design/spec PR, then T2.
