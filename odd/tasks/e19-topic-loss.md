# E19 Pérdida del topic

Objetivo: codificar E19 (H19). Rama `feat/e19-topic-loss`, apilada sobre `feat/e15-llm-degradation` (comparten la CLI, el catálogo, el CI y las specs). Entrega: un PR. Tests: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; la integración corre en CI (shard slow-a).

## Specs

- S1: specs/experiments/spec.md, «E19 Pérdida del topic (H19)» (escenarios: Topic vaciado, Topic recreado, Avisos nuevos tras la recuperación, Cumplimiento de H19).
- S2: specs/synthetic-data/spec.md, «Pérdida del topic» (escenarios: Topic vaciado con offsets que siguen, Topic recreado desde 0).
- S3: guía de experimentos con la sección de E19 y `eda-expexecutor run e19` con su catálogo.
- S4: el test slow de E19 en slow-a.

## Tasks

- T1 (S1–S4), inline: `outbox.republish_in_order`, `Harness.lose_topic`, `Harness.republish`, `send_notices(indices)` y `run_until_closed(upto=)`; `experiments/e19_topic_loss.py`, CLI, catálogo, guía, tests unitarios e integración, CI.

## Log

- L1 (2026-11-18): «Sigue con E14, E15, E19 y E20 (tabla en docs/guia-experimentos-eda-retail.md).» E19 no tenía spec; el usuario aprobó el diseño propuesto («Sí, adelante»): 200 incidencias cerradas, variantes `truncated` (`delete_records`) y `recreated` (borrar y crear), republicación con `replay_from`, H19 = todos los eventos de cada outbox en su orden, 0 efectos, eventos y cambios de estado nuevos, vistas idénticas, I1 a I5.
- L2: el lado de lectura guarda su posición en PostgreSQL y descarta un offset no posterior al guardado (`positions.is_new`); con el topic recreado, los eventos nuevos se perderían sin avisar. El procedimiento de `recreated` pone a -1 esa posición (la idempotencia por `event_id` de `views.applied_events` y el `ON CONFLICT` de `event_log` absorben lo ya aplicado). Para que el experimento lo demuestre, llegan 20 avisos nuevos tras la recuperación (añadido al diseño aprobado) y I5 compara las vistas con las reconstruidas desde el topic.
- L3 (2026-11-18): el verificador independiente encontró que H19 no podía cumplirse: republicar outbox a outbox rompe el orden causal entre componentes dentro de cada partición (un `StepCompleted` antes del `PlanCreated` de su incidencia) y algunas vistas guardan `kafka_partition` y `kafka_offset`, así que las vistas reconstruidas no coincidían con las mantenidas (I5). Decisión del usuario: «Republicar en orden». Cambios: `outbox.republish_in_order` republica a la vez todos los outbox mezclados por su posición en el topic perdido (un solo productor idempotente); la spec event-backbone y design.md lo piden; E19 mide además las filas que no respetan ese orden (`reordered`) e I5 no compara la posición en Kafka de las vistas (`invariants.check(volatile=...)`). No hay CLI para lanzarlo fuera del harness (hueco anterior: design.md citaba un `--replay-from` que no existía).
- L4: recheck del verificador: B1 resuelto. Su advisory 1 (tras un lote fallido, las filas de los lotes anteriores ya tenían su posición nueva y no se podía rehacer el orden original) se corrige: `republish_in_order` guarda las posiciones solo cuando se han confirmado todos los lotes. `Harness.republish` cierra su conexión.
