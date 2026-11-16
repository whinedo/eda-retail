# Spec scenario coverage

Objective: one test per spec scenario (CLAUDE.md); 15 scenarios had none. Branch: `test/spec-scenario-coverage` from `main` (5e9e2f9). Delivery: one PR to `main`. Runner: pytest (unit locally with closed ports; integration only in CI), vitest for `web/`.

## Specs

- S1 (A) `cqrs-views` / «Catálogo de comandos» / Scenario «El orquestador no recibe comandos»:
  - **GIVEN** el catálogo de comandos
  - **WHEN** se revisan sus destinatarios
  - **THEN** ninguno es `orchestrator`
- S2 (A) `cqrs-views` / «Un único dueño por estado» / Scenario «Dos revisiones sobre la misma incidencia»:
  - **GIVEN** una incidencia en la versión de plan 2 y dos `PlanRevised` del replanificador basados en la versión 2
  - **WHEN** el orquestador los consume
  - **THEN** aplica el primero y la versión pasa a 3
  - **AND** descarta el segundo como obsoleto, sin cambiar el estado
- S3 (A) `cqrs-views` / «Separación entre el modelo de escritura y el de lectura» / Scenario «Lectura del panel»:
  - **GIVEN** un panel que muestra incidencias abiertas
  - **WHEN** consulta los datos
  - **THEN** la consulta solo lee tablas `views.view_*` y la posición aplicada de cada vista en `views.offsets` (la `applied` de «Consistencia eventual declarada»)
  - **AND** no lee ninguna tabla de estado de escritura de otro componente
- S4 (A) `event-backbone` / «Kafka como registro de eventos» / Scenario «Vista reconstruida desde Kafka»:
  - **GIVEN** la vista del log vacía y el topic `eda.events` con todos los eventos
  - **WHEN** `event_log` vuelve a consumir el topic desde el principio
  - **THEN** la vista vuelve a tener todos los eventos, una vez cada uno
- S5 (A) `event-backbone` / «Invocación de comandos» / Scenario «El resultado llega como evento»:
  - **GIVEN** un `ExecuteStep` de `reserve_stock` aceptado con 202
  - **WHEN** el dispatcher recibe la respuesta
  - **THEN** el paso sigue `requested`
  - **AND** pasa a `completed` cuando la saga consume el `StepCompleted` de `inventory`
- S6 (A) `event-backbone` / «Invocación de comandos» / Scenario «Solo eventos en Kafka»:
  - **GIVEN** el topic `eda.events` tras procesar una incidencia completa
  - **WHEN** se revisan sus mensajes
  - **THEN** todos son eventos del catálogo y ninguno es un comando
- S7 (A) `event-backbone` / «Propiedad de los datos» / Scenario «Dato de otro componente»:
  - **GIVEN** un planificador que necesita el importe del pedido P-1002 para validar un reembolso
  - **WHEN** valida el plan
  - **THEN** lee el importe de la proyección `view_orders`, alimentada por el evento `OrderRegistered` de `inventory`
  - **AND** no consulta ninguna tabla de `inventory`
- S8 (B) `idempotency` / «Caída entre el efecto y el registro» / Scenario «Caída tras enviar el correo»:
  - **GIVEN** un ejecutor que envía un aviso y cae antes de registrar la clave
  - **WHEN** se reentrega el `ExecuteStep`
  - **THEN** el reintento usa la misma clave hacia el servicio de correo
  - **AND** el cliente recibe un solo aviso
- S9 (B) `idempotency` / «Un aviso por cliente y pedido» / Scenario «Revisión del plan tras un aviso enviado»:
  - **GIVEN** un `CustomerNotified` ya publicado para el pedido P1
  - **WHEN** el replanner publica una versión nueva que mantiene `notify_customer`
  - **THEN** no se envía otro aviso a ese cliente por ese paso
- S10 (B) `idempotency` / «Una reclamación por incidencia y pedido» / Scenario «Reclamación solicitada dos veces»:
  - **GIVEN** una reclamación ya creada para el pedido P1 en la incidencia I1
  - **WHEN** llega otro `ExecuteStep` de `file_claim` para P1
  - **THEN** el ejecutor devuelve el `claim_id` existente
  - **AND** no crea otra reclamación
- S11 (B) `idempotency` / «Reservas, reenvíos y reembolsos sin duplicar» / Scenario «Reproducción del log con ejecutores activos»:
  - **GIVEN** un log reproducido por error ante los ejecutores
  - **WHEN** reciben de nuevo `ExecuteStep` ya ejecutados
  - **THEN** no se crea ninguna reserva ni reembolso nuevo
- S12 (C) `observability-audit` / «Traza por incidencia» / Scenario «Seguimiento de una incidencia»:
  - **GIVEN** una incidencia resuelta
  - **WHEN** se consulta su traza
  - **THEN** muestra la secuencia aviso, interpretación, plan, pasos y cierre
  - **AND** cada span indica su duración y el componente que lo produjo
- S13 (B) `resilience` / «Confirmación del offset tras procesar» / Scenario «Fallo durante el procesamiento»:
  - **GIVEN** un consumidor que recibe un evento y falla antes de confirmar su transacción
  - **WHEN** se reinicia
  - **THEN** Kafka le entrega de nuevo el evento porque su offset no se confirmó
- S14 (C) `web-front` / «Reinicio de la vista» / Scenario «Reinicio con un comando en ejecución»:
  - **GIVEN** un comando en ejecución
  - **WHEN** el usuario mira la cabecera
  - **THEN** «Resetear simulación» está desactivado
- S15 (C) `web-front` / «Origen de emisor y destino» / Scenario «Resultado de un ejecutor»:
  - **GIVEN** un `StepCompleted` publicado por `executor-notify_customer`
  - **WHEN** se muestra en el panel
  - **THEN** su emisor es `executor-notify_customer` y su destino `orchestrator`

## Tasks

| ID | Specs | Task | Route | Status | Commit |
| --- | --- | --- | --- | --- | --- |
| T1 | group A | cqrs-views + event-backbone scenario tests | delegated (parallel writers) | done | |
| T2 | group B | idempotency + resilience scenario tests | delegated (parallel writers) | done | |
| T3 | group C | observability-audit + web-front scenario tests | delegated (parallel writers) | done | |
| T4 | S10 | file_claim returns the existing claim; migration 0022 | inline | done (RED → GREEN; verifier PASS after one correction) | this PR |

Route evidence: three independent units with disjoint new test files, launched together (Writer trigger a). Risk: none expected (tests only); a scenario the code does not meet is reported, not patched.

## Log

- L1 (2026-11-16) User: "añade los Tests para los 15 escenarios de las specs que aún no tienen uno. Por qué hay requisitos sin escenario? era una regla del proyecto". Checked: 0 requirements without scenario; 15 scenarios whose name appears in no test docstring (script over specs/*/spec.md vs tests/ and web/src/*.test.*).
- L2 (2026-11-16) T1–T3 done by three parallel writers; each stayed in its edit surface. 15 tests: unit S1, S3, S7, S8, S15; vitest S14; integration S2, S4, S5, S6, S9, S10, S11, S12, S13 (collected only, CI runs them). No scenario left without a test.
- L3 (2026-11-16) S10 «Reclamación solicitada dos veces» does not hold: `executor_file_claim.claims` (migration 0015) has no uniqueness on (incident_id, order_id) and `file_claim` deduplicates only by `idempotency_key`, so a revised plan with a new `file_claim` step for the same order creates a second claim. The test is `xfail(strict=True)`; the fix is a production change pending the user's decision.
- L4 (2026-11-16) S3 spec corrected in the same change: the panel queries also read `views.offsets` to return `applied`, required by «Consistencia eventual declarada» in the same spec; the scenario now says so and adds that no write-side table of another component is read.
- L5 (2026-11-16) User: "Sí, arréglalo" (the S10 gap). Branch `fix/file-claim-unique` from `main` (fed258f). T4 (S10), inline: `StepPreparer` takes an optional `existing(step)` lookup by the action's business key; `file_claim` uses `existing_claim` (same incident_id and order_id) so a revised plan's new `file_claim` step returns the existing `claim_id` with no draft, no service call, no `ClaimFiled` and no new row, and its own effect key is completed with that result. Migration 0022 adds the unique index `claims (incident_id, order_id)` that design.md §8 already promised; it stops with an error and deletes nothing if duplicates exist. RED: 2 unit tests in tests/test_step_executors.py (`existing` keyword missing) → GREEN 576 unit tests; S10 xfail removed; migration tests added (integration, CI). Finding: design.md §8 also promises `refunds (incident_id, step_id)` and `customer_messages (incident_id, step_id)`, which migration 0015 never created; left for the user to decide. Risk: item 1 (migration) → independent verifier.
- L6 (2026-11-16) Verifier on T4: items 1, 3, 4, 5 PASS (advisory: a race of two jobs with different keys calls the external claims service twice; one DB claim survives). BLOCKER item 2: the lookup also ran in the E1 no-idempotency modes and hid the `file_claim` duplicates E1 measures. Correction: the lookup only runs with `EFFECT_KEYS_ENABLED`, and the harness drops `claims_incident_order_key` in non-`full` modes (design.md §8: the no-idempotency variant removes these constraints in the test DB). Tests: unit `test_the_existing_claim_lookup_follows_effect_keys_enabled` (RED not observed: written with the fix), integration `test_e1_drops_the_business_uniques_without_idempotency`. 577 unit tests pass.
