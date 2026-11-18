# E10 Caída de PostgreSQL

Objetivo: codificar E10 (H10): PostgreSQL parado o congelado 30 s y de vuelta; cada proceso se reconecta solo, sin pérdidas ni efectos repetidos. Entrega: 3 PRs encadenados (PR1 reconexión, PR2 modo multiproceso completo, PR3 experimento), cada uno con merge tras el CI en verde. Tests: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; la integración corre en CI.

## Specs

- S1 (H10): «si PostgreSQL se para o se congela y vuelve, los componentes se reconectan solos, sin reiniciarlos a mano. No se pierde ningún evento, ningún efecto se repite, toda incidencia se cierra (también las de los avisos que llegan durante el corte) y se mantienen I1 a I5.»
- S2 (escenario): «50 incidencias. Cuando la mitad tienen un `PlanCreated`, se corta el contenedor `eda-postgres` durante 30 s.» «Durante el corte llegan 20 avisos por REST. Fallarán con 5xx, y quien los envía los reintenta hasta que se aceptan.» «Después se ejecuta hasta cerrar todas las incidencias.»
- S3 (variantes): «`baseline`: sin corte.» «`stopped`: `docker stop` y `docker start`. Las conexiones se cortan y las transacciones abiertas se deshacen.» «`paused`: `docker pause` y `docker unpause`. Las conexiones quedan abiertas sin respuesta.»
- S4 (modo multiproceso): «cada componente corre como proceso propio, como en `scripts/run-agents.sh`, y el harness solo genera los datos y mide.» LLM y servicios simulados en los procesos (PR2).
- S5 (métricas por variante): incidencias cerradas, resueltas y escaladas; duración real del corte; peticiones REST rechazadas durante el corte y reintentos de avisos y comandos; reconexiones por proceso y procesos que murieron; tiempo desde la vuelta de la base hasta que el lag llega a 0 y el outbox se vacía; mensajes de Kafka que se vuelven a entregar porque su offset no se confirmó, y cuántos descarta el inbox; ejecuciones del servicio simulado por `idempotency_key` (debe ser 1); filas de outbox frente a filas de `event_log`; latencia (media, mediana y p95) y su cociente frente a `baseline`; I1 a I5.
- S6 (criterio): H10 se cumple si, en todas las variantes: ningún proceso muere ni queda colgado; no queda ninguna fila de outbox pendiente y cada evento del outbox está en `event_log`; ningún efecto se repite; todas las incidencias se cierran, también las de los avisos del corte; I1 a I5 no tienen violaciones; cada variante con corte provoca su efecto: al menos un error de conexión por proceso y al menos un aviso rechazado.
- S7 (PR1, reconexión): «`connect_timeout` y keepalive en `db.connect`, y reconexión en el relay, en `EventConsumer`, en los bucles del orquestador e interpreter y en projector/event-log. Con unit tests.»
- S8 (PR2): «lanzador genérico, LLM y servicios simulados elegibles por variable de entorno y logs de cada proceso.»
- S9 (PR3): «el experimento, `PostgresOutage` (copiado de `BrokerOutage`), la spec y el test de integración.»

## Tasks

- T1 (S7), inline, PR1 `feat/e10-pg-reconnect`: reconexión a PostgreSQL en `infra/db.py`, `ComponentRelay`, `EventConsumer`, `PositionedConsumer`, los bucles de orchestrator e interpreter y los CLI de projector y event-log; spec resilience; tests unitarios. Commit 49447e0; verificador: PASS. Hecho.
- T2 (S4, S8), PR2: modo multiproceso con todos los componentes. Pendiente.
- T3 (S1-S3, S5, S6, S9), PR3: E10. Pendiente.

## Log

- L1 (2026-11-18): el usuario pidió «propón el diseño de E10 mientras esperamos» y lo aprobó: «ok al diseño de E10». Tras el mapa de la reconexión, eligió «Sí, 3 PRs» y dijo «mergéalas tú»: push, PR y merge de cada PR de E10 tras ver su CI en verde, fijado al SHA del head.
- L2: mapa (explorador de solo lectura): solo `JobWorker` y el router REST (una conexión por petición) se recuperan de una conexión rota. El relay conserva su conexión cerrada para siempre; `EventConsumer.run_forever` no captura errores (el proceso muere sin confirmar el offset); los bucles de orchestrator e interpreter, projector y event-log no se reconectan; `db.connect` no tiene ningún timeout. Todos los CLI con LLM crean `OllamaClient()` sin forma de cambiarlo.
- L3: rama de PR1 creada desde `feat/e9-kafka-down` (2ff3dc4) mientras #92 espera su CI; se rebasa sobre main tras el merge de #92.
- L4: T1 inline (RED observado: `tests/test_pg_reconnect.py` sin `reconnect`, `reopen` ni `run_loop`). `db.connect` con `connect_timeout` 5 s y TCP keepalive; `db.reopen`/`run_loop`; `reconnect` opcional en `ComponentRelay`, `EventConsumer` (`run_forever` reintenta el mismo mensaje y confirma después) y `PositionedConsumer` (`recover`, `run_resilient`); cada proyector con su conexión. Sin `reconnect` (harness en proceso) nada cambia. Suite unitaria 883 y ruff en verde. Risk: item 3 (helpers compartidos) e item 4 (hilos, orden reintento/commit): verificador independiente.
- L5: verificador independiente: PASS sin blockers (diferencial de `run_forever` sin `reconnect` idéntico a la base; el harness no usa los caminos nuevos). Avisos aplicados: A3 (la spec acota «los errores que no son de la base» a los consumidores) y A5 (docstrings con el nombre del escenario). Sin cambio: A1 (más de 300 s sin poll saca al consumidor del grupo; E10 corta 30 s), A2 (`OperationalError` incluye deadlocks y timeouts: se reintentan, como antes el reinicio), A4 (espera de 1 s tras un error en los bucles), A6 (previo).
