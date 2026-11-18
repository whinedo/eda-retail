# E8 Matriz de puntos de caída

Objetivo: codificar E8 (H8): cada componente cae una vez en cada punto aplicable de su transacción y se reinicia; toda incidencia se cierra sin efectos repetidos ni eventos perdidos. Rama `feat/e8-crash-matrix`. Entrega: 2 PRs (PR1 puntos de caída, PR2 experimento); push, PR y merge solo con el OK del usuario. Tests: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; la integración corre en CI.

## Specs

- S1 (H8): «si cualquier componente cae en cualquier punto de su transacción, toda incidencia acaba cerrada, sin efectos duplicados ni eventos perdidos, y se cumplen I1 a I5.»
- S2 (modo): «En proceso, como dice el catálogo, con el harness actual. Así cada caída es determinista y no hace falta matar procesos.»
- S3 (puntos): `before_tx` «Mensaje recibido (y LLM ya llamado), antes de abrir la transacción»; `after_commit_before_offset` «Tras el `COMMIT` de PostgreSQL, antes del commit del offset»; `after_commit_before_response` «Command handler REST: commit hecho, sin responder (E2 generalizado)»; `relay_after_publish_before_mark` «Relay: publicado en Kafka, fila del `outbox` sin marcar». «La caída entre el efecto y su clave no entra en la matriz: ya la cubre E22 (`crash_between_effect_and_record`).»
- S4 (matriz): «Cruza los componentes (intake, interpreter, planner, replanner, orchestrator, cada ejecutor, proyectores) con los puntos que les aplican. Salen unas 25 celdas.» «20 incidencias, con una sola caída en el mensaje K (el de la mitad, según la semilla) y reinicio del componente.»
- S5 (inyección): «Añado a `FaultPlan` un `crash_at: {(componente, punto): K}`. Generaliza el `crash_after` que ya usan `FaultyDelivery` y `_crash_after_commit`. En el relay hace falta un hook nuevo.»
- S6 (informe): «incidencias cerradas y abiertas; efectos por `idempotency_key` (máximo); eventos con `event_id` repetido en Kafka frente a filas en `event_log.events`; descartes del inbox; llamadas al LLM repetidas; tiempo de recuperación; I1 a I5.» «Una celda solo cuenta si la caída se llegó a producir.»
- S7 (entrega): «1. Los puntos de caída en `FaultPlan`, consumer, relay y command handler, con la spec synthetic-data y sus tests unitarios. 2. El experimento: spec experiments (H8), `e8_crash_matrix.py`, CLI, guía y un test de integración en un shard.»

Decisiones al escribir la spec (L2), pendientes de revisión del usuario:
- `before_tx` aplica al intérprete (consumer de Kafka con `prepare` antes de la transacción) y al worker de trabajos del planificador y del replanificador. No aplica a los ejecutores, porque su `prepare` es el efecto (E22).
- K de cada celda = ⌈n/2⌉ de las n unidades que ese componente atiende en ese punto en `baseline` con la misma semilla.
- Todas las celdas con un 30 % de fallos en `reserve_stock`, para que el replanificador tenga trabajo.
- Proyectores: una celda con `incident_status`, porque todos comparten el consumidor posicionado.

## Tasks

- T0 (S1-S7), inline: spec de E8 en `specs/experiments/spec.md` («E8 Matriz de puntos de caída (H8)») y `specs/synthetic-data/spec.md` («Caída en un punto de la transacción»). Pendiente de revisión del usuario.
- T1 (S3, S5), PR1: `FaultPlan.crash_at` con validación de los puntos, hooks en `EventConsumer`/`FaultyDelivery`, en `PositionedConsumer` (event_log y proyectores), en el worker de trabajos, en `InProcessCommands` y en `ComponentRelay`; registro de cada caída y del reinicio; tests unitarios. Risk: item 3 (código compartido de consumer y relay), con verificador independiente. Pendiente.
- T2 (S1, S2, S4, S6), PR2: `experiments/e8_crash_matrix.py`, CLI y catálogo `e8`, guía, test de integración en un shard. Pendiente.

## Log

- L1 (2026-11-18): el usuario preguntó «propón el diseño de E8. Es el último experimento?» y aprobó el diseño propuesto: «Yes, write it».
- L2 (2026-11-18): al revisar el código se confirma que el LLM no se llama en un consumer genérico. El intérprete llama a `prepare` antes de su transacción (`execute_command`), y el planificador y el replanificador lo hacen en su `JobWorker`, fuera de toda transacción. `event_log` deduplica con `ON CONFLICT (event_id)` y los proyectores con `views.applied_events`. Spec escrita en T0. Siguiente paso: revisión del usuario antes de implementar T1.
