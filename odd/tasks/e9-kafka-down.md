# E9 Caída de Kafka

Objetivo: codificar E9 (H9): broker parado o congelado y reiniciado; el outbox acumula y el relay publica al volver, sin pérdidas. Rama `feat/e9-kafka-down` desde `main` (aa218f2). Entrega: un PR. Tests: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; la integración corre en CI (shard slow-a).

## Specs

- S1: specs/experiments/spec.md, «E9 Caída de Kafka (H9)»: en proceso, 50 incidencias; con la mitad planificadas se para el broker 30 s y llegan 20 avisos más; después vuelve y se ejecuta hasta cerrar. Variantes `baseline`, `stopped` (`docker stop`/`start`) y `paused` (`docker pause`/`unpause`). Escenarios: Broker parado, Broker congelado, Avisos durante la caída, Cumplimiento de H9.
- S2: specs/synthetic-data/spec.md, «Caída del broker» (parar o congelar el contenedor de Kafka y esperar a que vuelva; restaurarlo siempre).
- S3: métricas por variante: pendientes máximos del outbox por componente, avisos aceptados durante la caída, errores de entrega del relay, fallos de commit de offset, tiempo hasta vaciar el outbox, mensajes en el topic frente a filas de outbox y `event_id` repetidos, descartes del inbox, cerradas y escaladas por motivo, latencia frente a `baseline`, I1 a I5.
- S4: H9: en todas las variantes, 0 filas de outbox sin publicar, todo `event_id` del outbox en el topic, mensajes repetidos sin efectos repetidos (I2) ni filas duplicadas en `event_log`, todos los avisos cerrados (también los de la caída), I1 a I5 sin violaciones; cada variante con caída provoca su efecto (algún error de entrega del relay y algún pendiente en el outbox).
- S5: el turno del harness tolera los errores de entrega del relay como `ComponentRelay.run_until` (con un `flush` corto) y un fallo de commit de offset (`KafkaException`) reinicia el consumidor sin contar como veneno.
- S6: guía de experimentos con la sección de E9, `eda-expexecutor run e9` con su catálogo y el test slow en slow-a.

## Tasks

- T1 (S2, S5), inline: `synthetic/broker_faults.py` y los cambios del harness, con tests unitarios.
- T2 (S1, S3, S4, S6), inline: `experiments/e9_kafka_down.py`, CLI, catálogo, guía, specs, tests unitarios e integración, CI.

## Log

- L1 (2026-11-18): el usuario eligió «E9 Caída de Kafka» como siguiente experimento y aprobó el diseño propuesto: «Sí, adelante».
- L2: hallazgo: en producción, el intérprete y el orquestador consumen con `EventConsumer.run_forever`; un commit de offset fallido con el broker caído lanza `KafkaException` y termina el proceso. La entrega al menos una vez lo absorbe si algo reinicia el proceso (el inbox descarta lo ya procesado). E9 lo modela como caída y reinicio del consumidor y cuenta los fallos de commit; no cambia el código de producción.
- L3: T1 y T2 escritas inline. ruff y suite unitaria en verde (863). `tests/test_experiment_cli.py` usaba `e9` como experimento inexistente: ahora usa `e8`. Escenario «Broker congelado»: se mide lo que más tarda un relay en un turno (`relay_max_seconds`), con la cota de dos plazos de entrega más 1 s. El test de integración (semilla 9, 12 incidencias, 4 avisos en el corte, corte de 10 s) va a slow-a. Risk: item 3 (turno y caídas del harness, compartidos; por defecto sin cambios) e item 5 (el test corta el Kafka del shard): verificador independiente.
- L4: verificador independiente: sin blockers confirmados; R1, blocker probable con evidencia medida (librdkafka 2.16): sin coordinador, un commit síncrono espera `session.timeout.ms` (45 s) y un consumidor recreado espera a que caduque el miembro anterior, así que el corte se alargaba y `run_until_closed` podía parar con incidencias abiertas. Corrección (una tanda): `RunConfig.consumer_session_timeout_ms` (6000 ms en E9) para los consumidores en proceso, temporizador en `BrokerOutage` (`duration_s`) que termina el corte a su hora, `idle_seconds` 30 tras el recheck, `published_during_outage` en H9 (escenario «Broker parado»), restauración si el propio corte falla (R4), error fatal del productor registrado (R3) y muestra final del outbox (R5). Suite unitaria 866 en verde. Recheck limitado a R1 pedido al mismo verificador.
- L5: recheck de R1: resuelto, sin blockers nuevos. Avisos aplicados: `BrokerOutage.restoring` se marca antes del comando de restauración (tras `docker unpause` el broker responde antes de que vuelva el CLI) y `published_during_outage` deja de contar en cuanto se marca; `IDLE_SECONDS` a 30 s (reconexión de librdkafka, hasta 10 s, más la recomposición del grupo).
