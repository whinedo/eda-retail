# Guía de experimentos de eda-retail

Experimentos actuales (E1–E7) y experimentos de resiliencia e idempotencia propuestos (E8–E23)

## Índice

1. Qué se mide y cómo se ejecuta
2. Experimentos actuales (E1–E7)
3. Experimentos nuevos: catálogo (E8–E23)
4. Invariantes comunes
5. Experimentos nuevos en detalle
6. Limitaciones del arnés y orden de implantación

## 1. Qué se mide y cómo se ejecuta

La PoC de eda-retail gestiona incidencias logísticas con una arquitectura dirigida por eventos: Kafka (KRaft, un broker, topic `eda.events` con 3 particiones, retención infinita y clave `incident_id`), PostgreSQL con un schema por componente, outbox con relay, inbox por consumidor, `effect_keys` en los ejecutores y una saga (orquestador) que invoca comandos REST con plazos durables. Los experimentos ponen a prueba esas garantías con datos sintéticos y fallos inyectados, y cada uno contrasta una hipótesis.

**Cómo se ejecuta.** Un solo comando por experimento:

```
uv run eda-experiment <exp> --seed N
```

Opciones útiles: `--incidents` (tamaño del escenario), `--out` (carpeta de informes), y solo en E5 y E6 `--llm oracle|ollama`, `--repetitions`, `--temperature` y (solo E5) `--threshold`. Cada ejecución crea y borra su propia base de datos y su propio topic, y escribe `experiments/results/<exp>-<fecha>/report.json` y `report.md` con la semilla, la configuración, el modelo y la versión de los prompts. El código sale con 0 si todas las hipótesis se cumplen.

**Con informe de resultados (`eda-expexecutor`).** El expexecutor (spec `expexecutor`) es un agente determinista, sin LLM, que ejecuta el experimento con `eda-experiment` y documenta la ejecución:

```
uv run eda-expexecutor run <exp> [--seed N] [--incidents N] [--config experiments/config.py]
uv run eda-expexecutor report experiments/results/<exp>-<fecha>
uv run eda-expexecutor delete experiments/results/<exp>-<fecha>
uv run eda-expexecutor delete --all
```

- `run` guarda la salida del experimento en `experiment.log`, dentro de su carpeta, y escribe `results.md` con el propósito, el resultado (esperado, obtenido y si coinciden) y las evidencias, cada una con su origen (clave de `report.json` o línea de `experiment.log`). Termina con el mismo código que `eda-experiment`; si el experimento falla sin escribir `report.json`, el informe lo dice y cita el final del log.
- Los parámetros salen de la línea de comandos, de `experiments/config.py` (diccionario `EXPERIMENTS` por experimento) o del valor por defecto de `eda-experiment`, en ese orden, y el informe dice de dónde sale cada uno. El fichero se valida antes de ejecutar nada.
- `report` regenera `results.md` sin repetir la ejecución; `delete <carpeta>` borra solo ese `results.md`, y `delete --all` borra todas las carpetas de ejecución de `experiments/results` (no toca `experiments/config.py`). Los resultados no están en el control de versiones.

**Arnés actual.** Monta los componentes en un solo proceso contra PostgreSQL y Kafka reales: relays, proyectores, agentes, orquestador y ejecutores se atienden por turnos, y lo que tarda (el LLM, los servicios) corre en su propio hilo. La inyección de fallos (`FaultPlan`) admite hoy: duplicados de entrega (1 a N), retraso de la entrega, probabilidad de fallo por tipo de paso, caída de un consumidor tras K mensajes sin confirmar el último offset, almacenes sin stock, reinicio de los offsets de los grupos de consumidores al principio del topic (E21), reenvío de avisos del transportista, en orden o a la vez (E23), entrega concurrente de un comando desde N hilos con conexiones propias, reinvocación de un `ExecuteStep` con otro `command_id` y la misma clave, y caída entre el efecto y el registro de su clave (E22). Los mensajes veneno (E18) se publican aparte, directamente en el topic (`synthetic/poison.py`). La elección de los pasos que fallan depende de la semilla, la acción y la `idempotency_key`, no del orden de ejecución, así que es reproducible.

**Todos los componentes en procesos propios.** Con `RunConfig.component_processes=True` (E10), el arnés no monta los componentes: lanza los procesos de `scripts/run-agents.sh` en su versión 1 (`experiments/processes.py`, `COMPONENTS`) contra la base de datos y el topic de la ejecución, cada uno con un puerto libre y su log en un directorio temporal de la ejecución, y solo siembra los datos, envía los avisos por REST y mide. Antes de empezar espera a que los procesos que atienden comandos acepten conexiones y a que el intérprete y el orquestador tengan particiones; al terminar (o si la ejecución falla) los para con `SIGINT` y guarda su código de salida (0 si salen limpios). `Harness.process_deaths()` da los que murieron solos, `ComponentProcess.log_text()` el log de cada uno y `Harness.service_executions()` las ejecuciones de los servicios simulados por `idempotency_key`. En este modo, `Harness.lags()` da el lag de los grupos del orquestador y del intérprete (su offset confirmado) y el de `event_log` y cada proyector (su posición guardada en `event_log.offsets` y `views.offsets`), `Harness.reconnect()` cambia la conexión del arnés si una caída de la base la rompió (E10) y `catch_up_log` con los procesos ya parados lee una sola vez, sin esperar. Variables que reciben los procesos:

- `LLM_CLIENT`: `ollama` (por defecto, también sin definir) u `oracle`. Con `oracle`, `ORACLE_SEED` y `ORACLE_INCIDENTS` dan el conjunto `generate(seed, incidents)` del oráculo, el mismo que usa el arnés. Otro valor impide arrancar el proceso.
- `SERVICE_EXECUTIONS_LOG`: fichero JSONL donde cada `eda-step-executor` anota cada ejecución de su servicio simulado (servicio e `idempotency_key`). Sin ella, el ejecutor no anota nada.

El modo rechaza lo que solo inyecta el arnés en proceso: los fallos del `FaultPlan`, los modos de idempotencia distintos de `full`, la versión 2 y un LLM, unos datos o un transportista propios.

**Referencias de componentes** (para leer los informes): `PENDING_NOTICE_TIMEOUT_S` (300 s) aparca avisos; `SAGA_STEP_TIMEOUT_S` (60 s) y `SAGA_PLAN_TIMEOUT_S` (300 s) son los plazos de la saga; `COMMAND_HTTP_TIMEOUT_S` (5 s) es el timeout HTTP del dispatcher; `EXECUTOR_MAX_CONCURRENCY` (5) limita a los ejecutores; `JOB_LEASE_SECONDS`, `JOB_MAX_ATTEMPTS` y el backoff gobiernan los trabajos diferidos.

## 2. Experimentos actuales (E1–E7)

| Exp. | Hipótesis | Qué prueba | Escenario por defecto | Estado |
|---|---|---|---|---|
| E1 | H1 | Duplicados en el bus | 50 incidencias, cada evento y comando 2 y 3 veces | Cumplida |
| E2 | H2 | Caída de un ejecutor tras confirmar y antes de responder | 50 incidencias | Cumplida |
| E3 | H3 | Fallo de un paso y replanificación | 30 pérdidas, almacenes sin stock | Cumplida |
| E4 | H4 | Reconstrucción de vistas desde Kafka | 200 incidencias | Cumplida |
| E5 | H5 | Calidad del plan del LLM | 40 incidencias, 3 repeticiones, umbral 0,8 | No cumplida |
| E6 | H6 | Eventos frente a versión síncrona | 50 incidencias, 3 repeticiones | Informativa |
| E7 | H7 | Auditabilidad | 50 incidencias, 50 % de fallos en `reserve_stock` | Cumplida |

### E1 Duplicados en el bus (H1)

Entrega cada evento a cada consumidor y cada comando de la saga 2 y 3 veces con el mismo `command_id`, y compara tres modos de idempotencia: `full` (inbox, `effect_keys` y servicios con clave), `no_effect_keys` (los ejecutores e `inventory` pierden inbox de comandos, `effect_keys` y la deduplicación de los servicios) y `none` (todo desactivado, solo observación). **H1:** con idempotencia, 0 efectos duplicados en todas las ejecuciones; sin `effect_keys`, más de 0. Un duplicado es una ejecución externa repetida del mismo paso o una reserva de más. **Mide:** cierres, abiertas, escaladas, duplicados por tipo, caídas.

### E2 Caída de un ejecutor (H2)

El ejecutor de `notify_customer` (todas las incidencias lo usan) cae tras completar la mitad de sus pasos: confirma la transacción del último y cae antes de responder. La saga reinvoca con el mismo `command_id` y el inbox devuelve la respuesta guardada. **H2:** todas las incidencias terminan en `IncidentResolved` o `IncidentEscalated` y no se repite ningún paso. **Mide:** latencia por incidencia, caídas, pasos repetidos.

### E3 Fallo de un paso y replanificación (H3)

Solo incidencias de pérdida (`loss`: aviso, reclamación, reserva y reenvío) con los dos almacenes sin existencias. La reserva falla, el replanner quita reserva y reenvío y propone un reembolso. **H3:** todas las incidencias terminan con un reembolso o un escalado, ningún paso completado se repite y ningún reembolso supera el importe del pedido ni el máximo configurado.

### E4 Reconstrucción de vistas (H4)

Ejecuta las incidencias hasta cerrarlas y, con el sistema parado, borra cada vista y la reconstruye volviendo a consumir el topic. **H4:** la huella de cada vista reconstruida coincide con la original. **Mide:** eventos aplicados y segundos por vista.

### E5 Calidad del plan (H5)

El planificador aislado recibe la interpretación etiquetada del generador (causa, gravedad, retraso) y los pedidos, con el mismo prompt, las mismas comprobaciones (`validate_plan`) y los mismos reintentos que el agente. Un plan acierta si propone las mismas acciones que el esperado, sin importar el orden ni los `step_id`. Con gravedad `low` el plan lo fija el código y esas incidencias no cuentan. **H5:** la media del acierto de las repeticiones alcanza 0,8. **Resultado:** con Ollama (qwen2.5:7b-instruct) el acierto fue de 0,75 a 0,78 y H5 no se cumple. Errores sistemáticos: en `strike` añade `file_claim`; en `delay` de gravedad alta añade `reserve_stock` y `reissue_shipment`.

### E6 Eventos frente a síncrono (H6)

Cada repetición ejecuta los mismos avisos, con el mismo LLM, en la versión de eventos y en la base síncrona (`baseline_sync`), en dos escenarios: `no_faults` y `faults` (cada evento entregado 2 veces y un 30 % de fallos en `reserve_stock`). El informe da latencia (media, mediana, p95), tokens, llamadas, coste, incidencias resueltas, reembolsadas y escaladas, efectos duplicados y escaladas por motivo. **H6** no fija umbral: describe diferencias. **Resultado destacado:** ambas versiones con 0 duplicados; latencia de eventos de unos 1019 s frente a unos 32 s en síncrono, porque con un único Ollama las 50 incidencias simultáneas hacen cola en el LLM; todas las escaladas fueron `llm_output_invalid`. Los plazos de la saga se escalan con las incidencias en este experimento y el informe lo declara.

### E7 Auditabilidad (H7)

Ejecuta con un 50 % de fallos en `reserve_stock` para que haya planes revisados y explica, desde el log y la traza (cadena de `causation_id`), diez incidencias elegidas con la semilla. Cada decisión es un `StepRequested` de `audit_trail`; su explicación necesita agente, versión del plan y motivo. **H7:** todas las decisiones de las incidencias elegidas tienen los tres.

**Cobertura.** E1 a E4 y E7 prueban idempotencia, caída de un ejecutor, replan, reconstrucción y auditoría en condiciones de laboratorio benignas; no prueban caídas de Kafka o de PostgreSQL, procesos congelados, mensajes envenenados, proyecciones atrasadas ni reproducción completa del log. Los experimentos nuevos cubren esos huecos.

## 3. Experimentos nuevos: catálogo (E8–E23)

Las hipótesis nuevas se llaman como su experimento (H11, H12...). Los marcados con (S) tienen spec completa en `specs-experimentos-resiliencia.md`.

| Exp. | Nombre | Fallo inyectado | Qué se comprueba | Modo |
|---|---|---|---|---|
| E8 | Matriz de puntos de caída | Caída de cada componente en cada punto de la transacción | Recuperación sin duplicados ni pérdidas | En proceso |
| E9 | Caída de Kafka | Broker parado y reiniciado | El outbox acumula y el relay publica al volver, sin pérdidas | Implementado (en proceso) |
| E10 | Caída de PostgreSQL | Base parada o congelada 30 s | Reconexión sola, sin pérdidas ni efectos repetidos | Implementado (multiproceso) |
| E11 (S) | Entrega retrasada y replanes | Resultados tardíos, 50 % de fallos en `reserve_stock` | Eventos obsoletos ignorados, compensaciones correctas | Implementado |
| E12 (S) | Ejecutor caído y colgado | `file_claim` en `down` y en `hang` | Plazos, aislamiento, cabeza de línea del dispatcher | Implementado (en proceso) |
| E13 (S) | Fallo de compensación | `release_stock` falla | Escalado `compensation_failed`, sin reembolso | Implementado (en proceso) |
| E14 | Ráfaga y contrapresión | Pico de avisos, 503 de los ejecutores | Dispatcher y cola de trabajos se recuperan | Implementado (en proceso) |
| E15 | Degradación del LLM | Latencia o salidas inválidas | `llm_output_invalid`, plazos, trabajos reintentados | Implementado (en proceso) |
| E16 (S) | Consumidor zombie | `SIGSTOP` más allá de la sesión y rebalanceo | Sin transiciones inválidas ni comandos duplicados | Implementado (multiproceso) |
| E17 (S) | Proyección atrasada | Proyectores parados | Avisos aparcados, reanudación y escalado | Implementado |
| E18 (S) | Mensajes veneno | Mensajes inválidos en mitad de la partición | DLQ sin bloquear la partición | Implementado |
| E19 | Pérdida del topic | Topic vaciado y republicación desde el outbox | Se recupera el log sin repetir efectos | Implementado (en proceso) |
| E20 | Dependencias de V2 | MCP de correo y agente A2A inestables | Timeouts, 401, tarjeta inválida | Implementado (en proceso) |
| E21 (S) | Reproducción total del log | Offsets de todos los grupos al principio | 0 efectos, eventos y cambios de estado nuevos | Implementado (en proceso) |
| E22 (S) | Carreras de comandos y ventana efecto–registro | Entrega concurrente y caída entre efecto y clave | 0 efectos duplicados | Implementado |
| E23 (S) | Duplicados de negocio | Avisos reenviados y replanes que repiten acciones | Máximos de las specs de idempotencia | Implementado (en proceso) |

## 4. Invariantes comunes

Todo experimento de resiliencia comprueba, además de su hipótesis, estos cinco invariantes y los incluye en el informe:

| Id | Invariante |
|---|---|
| I1 | Toda incidencia termina en `IncidentResolved` o `IncidentEscalated` |
| I2 | Ningún efecto externo se ejecuta dos veces para la misma `idempotency_key` |
| I3 | Como máximo un `issue_refund` por incidencia y pedido, y nunca con compensaciones pendientes |
| I4 | Toda fila de `outbox` está publicada o pendiente; ninguna se pierde |
| I5 | Las vistas reconstruidas desde `eda.events` tienen la misma huella que las mantenidas |

Si I1 o I2 se violan, la hipótesis no se da por cumplida aunque su criterio propio sí lo esté.

**Implementación.** `experiments/invariants.py` los comprueba al terminar, con el sistema parado: I1 con el log (incidencias con `CarrierNoticeReceived` sin `IncidentResolved` ni `IncidentEscalated`), I2 con los efectos repetidos que mide E1, I3 con `executor_issue_refund.refunds` y con el orden de los `StepRequested` y `StepCompleted` del log (un `issue_refund` pedido con una compensación pedida y sin completar), I4 con cada `outbox` (filas sin publicar o cuyo evento no está en `event_log.events`) e I5 reconstruyendo cada vista desde el topic. E9, E10, E11, E13, E14, E15, E19 y E20 (por variante; E10, con los procesos ya parados), E16, E17, E18, E21, E22 y E23 los incluyen en `metrics.invariants` (E16, con las instancias del orquestador aún en marcha y ociosas); E21 los comprueba antes de sus observaciones y de su control, que repiten efectos a propósito.

## 5. Experimentos nuevos en detalle

### E9 Caída de Kafka (H9)

- **Objetivo.** Comprobar que el outbox protege los eventos cuando el broker cae: los componentes siguen aceptando comandos y guardando eventos, y el relay publica todo lo pendiente al volver, sin pérdidas y sin efectos repetidos.
- **Escenario.** En proceso, 50 incidencias. Cuando la mitad tienen un `PlanCreated` en el log, se corta el broker 30 s y en ese tiempo llegan 20 avisos más; después vuelve y se ejecuta hasta cerrar todas las incidencias. Tres variantes (`synthetic.broker_faults.BrokerOutage`): `baseline`, sin corte; `stopped`, `docker stop` y `docker start` (conexión rechazada); `paused`, `docker pause` y `docker unpause` (conexiones abiertas sin respuesta).
- **Mide.** Por variante: cerradas, resueltas y escaladas por motivo; duración real del corte; filas publicadas durante el corte; filas de outbox pendientes, máximas por componente durante el corte y al terminar; avisos aceptados durante el corte; errores de entrega del relay; caídas de consumidores por un commit de offset fallido; lo que más tardó un relay en un turno; tiempo hasta vaciar el outbox; mensajes del topic, filas de outbox y de `event_log`, y mensajes repetidos; descartes del inbox; latencia y su cociente frente a `baseline`; I1 a I5.
- **Criterio.** H9: en las tres variantes, ninguna fila de outbox pendiente al terminar ni publicada durante el corte, cada evento del outbox en `event_log`, todas las incidencias cerradas (también las de los avisos del corte) e I1 a I5 sin violaciones; además, cada variante con corte provoca algún error de entrega del relay y alguna fila pendiente durante el corte.
- **Implementación.** `uv run eda-expexecutor run e9`. Corta de verdad el contenedor de Kafka (`KAFKA_CONTAINER`, por defecto `eda-kafka`): mientras dura, cualquier otra cosa que use ese broker se queda sin él. `BrokerOutage` devuelve el broker a su estado al terminar el corte, también si el experimento falla, y espera a que responda. Un temporizador termina el corte a su hora aunque un turno quede bloqueado en Kafka, y los consumidores en proceso usan un `session.timeout.ms` de 6 s (`RunConfig.consumer_session_timeout_ms`): sin broker, un commit síncrono espera un coordinador hasta ese plazo (45 s por defecto) y un consumidor recreado espera a que caduque el miembro anterior. También usan un `max.poll.interval.ms` de 15 s (`RunConfig.consumer_max_poll_interval_ms`), que el broker usa como rebalance timeout: con el broker congelado, un consumidor que pierde sus particiones manda un `JoinGroup` que el broker puede retener hasta ese plazo (300 s por defecto), y E9 se daba por quieto con avisos sin cerrar. El turno del harness trata un lote sin confirmar como `ComponentRelay.run_until` (`RunConfig.tolerate_relay_errors`, con un plazo de entrega de 1 s): la transacción se deshace y las filas siguen pendientes. El productor puede reenviar ese lote al volver el broker mientras el relay lo publica otra vez: los mensajes repetidos son los mensajes del topic menos las filas de `event_log`, que tiene un `event_id` único, y el inbox los descarta. En producción, el intérprete y el orquestador consumen con `EventConsumer.run_forever`: un commit de offset fallido con el broker caído lanza `KafkaException` y termina el proceso. El harness lo trata como caída y reinicio del consumidor (`commit_failures`), sin contarlo como veneno, porque el mensaje ya se procesó.

### E10 Caída de PostgreSQL (H10)

- **Objetivo.** Comprobar que, si PostgreSQL se para o se congela y vuelve, los componentes se reconectan solos, sin reiniciarlos a mano: no se pierde ningún evento, ningún efecto se repite, toda incidencia se cierra (también las de los avisos que llegan durante el corte) y se mantienen I1 a I5.
- **Escenario.** Con todos los componentes en procesos propios, 50 incidencias. Cuando la mitad tienen un `PlanCreated`, se corta el contenedor `eda-postgres` 30 s. Durante el corte llegan 20 avisos por REST: fallan con 5xx o sin respuesta y quien los envía los reintenta, con el mismo `command_id`, hasta que se aceptan. Después se ejecuta hasta cerrar todas las incidencias. Tres variantes (`synthetic.pg_faults.PostgresOutage`): `baseline`, sin corte; `stopped`, `docker stop` y `docker start` (las conexiones se cortan y las transacciones abiertas se deshacen); `paused`, `docker pause` y `docker unpause` (las conexiones quedan abiertas sin respuesta).
- **Mide.** Por variante: cerradas, resueltas y escaladas por motivo; duración real del corte; avisos rechazados durante el corte y sus reintentos; sondeos de la base sin respuesta; por proceso, errores de conexión, mensajes vueltos a atender (reintento en el proceso o entrega repetida de Kafka), descartes del inbox y reintentos de comandos; procesos que murieron y código de salida de cada uno; tiempo desde la vuelta de la base hasta lag 0 con el outbox vacío; filas de outbox y de `event_log`; ejecuciones por `idempotency_key`; latencia y su cociente frente a `baseline`; I1 a I5.
- **Criterio.** H10: en las tres variantes ningún proceso muere ni queda colgado (código de salida 0 al pararlo), no queda ninguna fila de outbox pendiente y cada evento del outbox está en `event_log`, ningún efecto se repite, todas las incidencias se cierran e I1 a I5 no tienen violaciones; además, cada variante con corte provoca su efecto: algún aviso rechazado y, en `stopped`, al menos un error de conexión en cada proceso; en `paused`, algún sondeo con una conexión nueva sin respuesta.
- **Implementación.** `uv run eda-expexecutor run e10` (con `caffeinate -i` en macOS: tarda varios minutos). Corta de verdad el contenedor de PostgreSQL (`POSTGRES_CONTAINER`, por defecto `eda-postgres`): mientras dura, cualquier otra cosa que use esa base se queda sin ella. `PostgresOutage` tiene la mecánica de `BrokerOutage`: un temporizador termina el corte a su hora, al salir devuelve la base aunque el experimento falle y espera a que acepte una conexión nueva y responda a `SELECT 1`. El arnés no lee su conexión durante el corte; al volver la base la cambia si se rompió (`Harness.reconnect`), y cada muestra posterior tolera un error reconectándose. Las métricas de cada proceso salen de su log (nivel DEBUG). Un error de conexión es una línea WARNING o ERROR con «la base no responde», «sin conexión a la base», «: error; se reintenta» o «worker de trabajos de»; todo proceso tiene un relay que consulta la base sin parar, así que con la base parada todos registran alguno. Con la base congelada, en cambio, las conexiones abiertas esperan sin error (TCP keepalive no lo detecta) y solo fallan las conexiones nuevas, con `connect_timeout`: por eso esa variante exige un sondeo sin respuesta y no un error en cada proceso. Un mensaje que el proceso vuelve a atender (otra línea `-> tipo incidencia pX oY` del mismo offset) es un reintento en el proceso si le precede «se reintenta el mensaje pX oY» y, si no, una entrega repetida de Kafka porque su offset no se confirmó; un descarte del inbox es una línea `<- ...: duplicate`.

### E11 Entrega retrasada y replanes forzados (H11)

- **Objetivo.** Comprobar que los resultados de una `plan_version` anterior, o posteriores al plazo, no cambian el estado de la saga.
- **Escenario.** 50 incidencias, 50 % de fallos en `reserve_stock`, planes con pasos en paralelo y retraso del resultado (`step_delay_seconds`) en `reserve_stock`, `reissue_shipment` y `file_claim`. Como Kafka mantiene el orden por incidencia dentro de la partición, los resultados obsoletos nacen del paralelismo y de los plazos, no del reordenamiento del bus.
- **Mide.** `PlanRevised` por incidencia, resultados obsoletos por versión y tras el plazo, los que cambiaron el estado de la saga fuera del supuesto A4 y las compensaciones A4, compensaciones pedidas y completadas, escaladas por motivo e I1 a I5.
- **Criterio.** H11: I1 a I5 sin violaciones, ningún resultado obsoleto cambia el estado de la saga salvo la excepción A4 (el `StepCompleted` tardío de un `reserve_stock` cancelado se marca completado y pide su `release_stock`) y ninguna compensación se pide más de una vez.
- **Implementación.** `uv run eda-expexecutor run e11`. El orquestador descarta los obsoletos sin registrarlos, así que se cuentan desde el log: obsoleto por versión si su `plan_version` es anterior a la vigente al consumirlo, salvo el de una compensación (`-comp`), que la saga trata mientras siga pedida; tras el plazo si el harness anotó que su plazo venció con el paso pedido (el plazo no deja evento). Un obsoleto cambia el estado si el orquestador emite algún evento con él como causa. Retrasos base: `reserve_stock` 1 s, `reissue_shipment` 2 s y `file_claim` 6 s (jitter de 0,5 a 1,5), con un plazo de `ExecuteStep` de 4 s. El retraso deja ejecutar el efecto a su hora y añade su resultado al outbox del ejecutor más tarde.

### E12 Ejecutor caído y ejecutor colgado (H12)

- **Objetivo.** Comprobar la vivacidad ante un ejecutor permanentemente fuera y medir el aislamiento entre incidencias.
- **Escenario.** En proceso, 50 incidencias (con la semilla 42, 18 llevan `file_claim`), `file_claim` en `down`, en `hang` (acepta la conexión y no responde) y en `hang` con recuperación.
- **Hipótesis a contrastar.** El dispatcher invoca un comando por turno (`LIMIT 1`): con `hang`, cada intento ocupa `COMMAND_HTTP_TIMEOUT_S` (5 s) y puede retrasar los comandos de otras incidencias (cabeza de línea). Es una suposición a verificar, no un hecho conocido.
- **Mide.** Intentos por comando, tiempo del dispatcher esperando al ejecutor, latencia p95 de las incidencias que no usan `file_claim`, escaladas por `step_timeout`, resultados obsoletos tras la recuperación.
- **Criterio.** H12a (vivacidad, obligatoria): todo termina dentro de `SAGA_STEP_TIMEOUT_S` más margen. H12b (aislamiento, exploratoria): p95 de las no afectadas ≤ 2 × el de una ejecución sin fallo; un resultado negativo es un hallazgo que se documenta.
- **Necesita.** Modos `down`/`hang` de ejecutor y el dispatcher en su propio hilo.
- **Implementación.** `uv run eda-expexecutor run e12`, en proceso: `executor_faults` del `FaultPlan` deja el sistema destino `down` (`CommandUnavailable` al momento) o `hang` (acepta la petición, se espera `COMMAND_HTTP_TIMEOUT_S` y queda en un backlog), durante un intervalo que empieza con el primer comando o para siempre. Al recuperarse, el ejecutor colgado atiende una vez lo que tenía aceptado y el informe cuenta los resultados obsoletos y vigentes. El dispatcher va en su propio hilo (`RunConfig.dispatcher_thread`), como en el orquestador: un ejecutor colgado lo bloquea a él y no a los relays ni al lado de lectura. Variantes: `baseline`, `down`, `hang`, `hang_recovery` y `compensation` (`inventory` cae con la primera reserva durante 1,5 plazos: la reserva vence, la saga deriva su `release_stock` `-comp` y este se completa cuando `inventory` vuelve). Plazos cortos: `SAGA_STEP_TIMEOUT_S` 6 s y `COMMAND_HTTP_TIMEOUT_S` 1 s.

### E13 Fallo de compensación (H13)

- **Objetivo.** Comprobar que una compensación que falla escala la incidencia con `compensation_failed`, sin reembolso y sin pedir más compensaciones (spec incident-planning, «Fallo de una compensación»).
- **Escenario.** En proceso, 50 incidencias con `fail_probability` 1,0 en `reissue_shipment` y 0,5 en `release_stock`. Cada reenvío falla, el replanner lo sustituye por `issue_refund` y la saga deriva el `release_stock` `-comp` de la reserva completada, que retiene el reembolso. Con la semilla 42 hay 25 compensaciones y fallan 11; las que se completan son la rama de control y terminan reembolsadas.
- **«Agota sus reintentos».** La saga no reintenta pasos: el ejecutor publica `StepFailed` cuando agota los suyos (`STEP_MAX_ATTEMPTS` para un error reintentable) y la saga escala cualquier `StepFailed` de una compensación (`rules.on_failure`). El fallo inyectado no es reintentable (`attempts` 1), así que es definitivo al momento.
- **Mide.** Compensaciones pedidas, completadas y fallidas; incidencias con una compensación fallida y, de ellas, las escaladas con `compensation_failed`, los `issue_refund` pedidos y las compensaciones pedidas tras el escalado; incidencias con la compensación completada y reembolsadas; escaladas por motivo; I1 a I5.
- **Criterio.** H13: al menos una compensación fallida; toda incidencia con una compensación fallida escala con `compensation_failed`, sin `issue_refund` pedido ni compensaciones tras el escalado; I1 a I5 sin violaciones.
- **Implementación.** `uv run eda-expexecutor run e13`. Las métricas salen del log (`event_log.events`, en orden de `seq`).

### E14 Ráfaga y contrapresión (H14)

- **Objetivo.** Comprobar que una ráfaga de avisos con ejecutores que responden 503 por encima de su capacidad no pierde ni duplica comandos y que el dispatcher y las colas de trabajos se recuperan (spec resilience, «Pico de incidencias»).
- **Escenario.** En proceso, 200 avisos enviados de golpe y dos variantes: `baseline`, sin límite, y `backpressure`, con capacidad 3 en cada ejecutor. En las dos, el `prepare` del planner tarda 0,05 s (el LLM) y el de los ejecutores 0,1 s (el servicio externo), para que los pasos lleguen más deprisa de lo que se atienden.
- **Mide.** Por variante: cerradas, resueltas y escaladas por motivo; 503 por ejecutor; intentos por comando (máximo y reintentados); máximo de trabajos abiertos por componente y de comandos pendientes; tiempo hasta vaciar las colas; colas al terminar; pasos con `ExecuteStep` de `command_id` distinto; latencia (media, mediana y p95); I1 a I5. El informe da también el cociente de la p95 frente a `baseline`.
- **Criterio.** H14, sobre `backpressure`: al menos un 503, ningún ejecutor por encima de su capacidad, colas vacías al terminar, sin comandos duplicados, todas las incidencias cerradas e I1 a I5 sin violaciones. La latencia es un dato, no parte de H14.
- **Implementación.** `uv run eda-expexecutor run e14`. La capacidad se cuenta en trabajos abiertos (`pending` o `running`) en el `command_jobs` del ejecutor: en proceso el dispatcher entrega un comando cada vez y un `ExecuteStep` solo se acepta, así que contar peticiones simultáneas no limitaría nada. `InProcessCommands` responde 503 (`CommandUnavailable`) a un `ExecuteStep` nuevo con el ejecutor lleno y deja pasar una reentrega cuyo trabajo ya existe; el dispatcher lo reprograma con backoff y el mismo `command_id`, y el plazo del paso sigue contando desde el primer envío. `QueueSampler` mide las colas en cada turno, como mucho cada 0,05 s.

### E15 Degradación del LLM (H15)

- **Objetivo.** Comprobar que el LLM propone y el código dispone también cuando el LLM falla: ninguna salida inválida se convierte en plan, los plazos cierran las incidencias y los trabajos se reintentan (spec llm-boundaries).
- **Escenario.** En proceso, 50 incidencias con un 30 % de fallos en `reissue_shipment` (para que trabaje el replanner) y cuatro variantes del oráculo del planner y del replanner (`synthetic.llm_faults.DegradedLlm`): `baseline`; `invalid`, un 60 % de respuestas que no son JSON o sin campos; `slow`, 0,5 s por llamada; `error`, un 30 % de llamadas que lanzan una excepción. `slow` y `error` usan un plazo de `CreatePlan` y `RevisePlan` de 4 s: en `error`, un trabajo que agota `JOB_MAX_ATTEMPTS` queda `failed` sin eventos y solo el plazo cierra la incidencia (con la semilla 42, el replanner de una incidencia falla 6 veces seguidas).
- **Mide.** Por variante: cerradas, resueltas y escaladas por motivo; degradaciones inyectadas; llamadas al LLM registradas y las inválidas; trabajos del planner y del replanner reintentados, su máximo de intentos y los `failed`; planes cuya última llamada no pasó la validación; planes publicados tras el escalado; pasos pedidos tras el escalado; latencia; I1 a I5.
- **Criterio.** H15: en todas las variantes, ninguna incidencia abierta, ningún plan de una llamada inválida, ningún paso (salvo compensaciones `-comp`) pedido tras el escalado e I1 a I5 sin violaciones; además, cada variante provoca su efecto (`invalid`, una llamada inválida; `slow`, un `plan_timeout`; `error`, un trabajo reintentado).
- **Implementación.** `uv run eda-expexecutor run e15`. La degradación es determinista dada la semilla, el agente, la incidencia y el número de llamada; el intérprete y el redactor de avisos no se degradan. Un plan «viene de una llamada inválida» si el último `LlmCallRecorded` con su misma causa (el `command_id`) tiene `validation_passed` falso. Las métricas salen del log en orden de `seq` y de los `command_jobs`.

### E16 Consumidor zombie y rebalanceo (H16)

- **Objetivo.** Sondear el supuesto de «único dueño del estado de la saga, sin concurrencia optimista» cuando dos instancias coexisten por un rebalanceo.
- **Escenario.** Multiproceso, dos instancias del orquestador en el mismo grupo, 50 incidencias en curso, `SIGSTOP` a la que posee más particiones más tiempo que `session.timeout.ms`, luego `SIGCONT`.
- **Mide.** Instante del rebalanceo, mensajes que procesó la zombie tras reanudar, solapamientos por incidencia, esperas al bloqueo de la incidencia, descartes del inbox, comandos con `command_id` distinto para el mismo paso.
- **Criterio.** H16: sin transiciones inválidas, sin retrocesos de `completed` y sin comandos duplicados con `command_id` distinto; I1 a I5 sin violaciones.
- **Necesita.** Modo multiproceso y congelación de procesos.
- **Implementación.** `uv run eda-expexecutor run e16`, en modo multiproceso (`RunConfig.orchestrator_processes`): dos procesos `eda-orchestrator` (el módulo `eda_retail.orchestrator.cli` lanzado con el Python del harness, para que las señales lleguen a Python, con SIGINT activo para pararlas limpiamente) contra la base de datos y el topic de la ejecución, con `KAFKA_SESSION_TIMEOUT_MS` 6000 y su propio relay; el resto de componentes sigue en el harness con el cliente LLM simulado y atiende sus comandos por REST en un servidor HTTP local de la ejecución. Los resultados de `reserve_stock`, `file_claim` y `reissue_shipment` llegan con 3 s de retraso para que las incidencias sigan en curso; cuando el orquestador las tiene todas, se congela con `SIGSTOP` durante 12 s la instancia con más particiones y se reanuda con `SIGCONT`. Las medidas salen del log de cada instancia (rebalanceos, inicio y fin de cada mensaje con su partición y offset, esperas al bloqueo de la incidencia con `LOG_LEVEL=DEBUG`), de `orchestrator.command_outbox` y de un muestreo de `plan_steps` e `incidents` en cada turno del harness. Si la confirmación del offset falla al reanudar (la zombie ya no está en el grupo), el proceso termina, como en producción: el informe da su código de salida.

### E17 Proyección atrasada y avisos aparcados (H17)

- **Objetivo.** Verificar el tratamiento explícito de la consistencia eventual: el intérprete no sondea la vista; aparca el aviso con plazo y reanuda con el evento.
- **Escenario.** Proyectores parados al llegar los avisos, 50 avisos, `PENDING_NOTICE_TIMEOUT_S` en 30 s. Un aviso aparcado no se reanuda al reanudar los proyectores: se reanuda al consumir de Kafka el `OrderRegistered` de su pedido o, si ese evento llegó antes de aparcarlo, al vencer el plazo consultando la vista una vez. Por eso las variantes se definen por el orden de los eventos: (a) pedidos registrados después de aparcar los avisos (se reanudan por `OrderRegistered` con los proyectores aún parados); (b) pedidos registrados antes y proyectores parados más que el plazo (escalan con `no_valid_orders`); (c) pedidos registrados antes y proyectores reanudados antes del plazo (se reanudan por el plazo con el pedido proyectado). En (a) se reenvía además un aviso aparcado.
- **Mide.** Por variante: aparcados, reanudados por `OrderRegistered`, reanudados por plazo con pedido proyectado, escalados con `no_valid_orders`, tiempo en aparcamiento, avisos que quedan aparcados (`interpreter.pending_notices` y `parked_notices`), pedidos en `view_orders` con los proyectores parados y lag del intérprete con los avisos aparcados.
- **Criterio.** H17: (a) todos reanudados por `OrderRegistered`, (b) todos escalados, (c) todos reanudados por el plazo sin escalar; ninguno aparcado al final, lag 0 del intérprete con los avisos aparcados y el aviso reenviado descartado con `duplicate_notice` sin un segundo aparcamiento.
- **Implementación.** `uv run eda-expexecutor run e17`. Cada variante usa su propia base y su topic. El harness para y reanuda los proyectores (`pause_projectors`, `resume_projectors`; `event_log` sigue consumiendo) y siembra los pedidos cuando lo pide el experimento (`RunConfig.seed_orders=False` y `seed_orders(wait=False)`). Las cuentas salen del log: un `ShipmentDelayed` de un aviso aparcado cuya causa es un `OrderRegistered` es una reanudación por el pedido; si no, por el plazo.

### E18 Mensajes veneno en mitad de la partición (H18)

- **Objetivo.** Comprobar que un mensaje inválido ni detiene al consumidor ni se pierde.
- **Escenario.** 100 incidencias; se inyectan en `eda.events`, en posiciones distintas: JSON inválido, evento sin un campo obligatorio de la estructura común (`producer`), `event_type` desconocido, `event_version` sin upcaster, payload fuera de schema, payload con datos personales y payload de 900 KB.
- **Mide.** Por tipo y por consumidor: presencia en `<componente>.dead_letters` y en `eda.dlq`, motivo, lag final, reintentos antes de saltar.
- **Criterio.** H18: lag final 0 en todos los grupos, todo veneno en la DLQ de cada consumidor que atiende su tipo exactamente una vez (y en ninguna más), el `event_type` desconocido en `event_log.events` sin dead letter, ninguna otra dead letter y las 100 incidencias terminan.
- **Implementación.** `uv run eda-expexecutor run e18`. Los venenos de tipo legible son `ShipmentDelayed` con el `correlation_id` de una incidencia abierta (se inyectan cuando el orquestador ya tiene incidencias abiertas), repartidos por las particiones; los atienden el orquestador, `event_log` y los proyectores con handler para ese tipo, y el JSON inválido lo atienden todos. El lado de lectura guarda sus dead letters en `event_log.dead_letters` y `views.dead_letters` (migración 0024). La reconciliación se hace con las filas de cada `dead_letters` (`published_at` indica que el relay lo llevó a la DLQ) y el informe da también los mensajes del topic DLQ y si cada incidencia afectada se escaló con `invalid_schema` (solo se escala si sigue abierta al leer el veneno).
- **Nota.** El consumidor trata de forma distinta un mensaje que incumple el schema (va a la DLQ y se confirma el offset) y un fallo del handler (la excepción se propaga y el offset no se confirma); el experimento observa también el segundo caso, que es el que puede producir un bucle de reintentos.

### E19 Pérdida del topic (H19)

- **Objetivo.** Comprobar el procedimiento de «Pérdida del topic» de la spec event-backbone: se republica el outbox de todos los componentes en el orden que tenía cada partición y el log se recupera sin repetir efectos.
- **Escenario.** En proceso, 200 incidencias cerradas con un 30 % de fallos en `reserve_stock`. Con el sistema parado se pierde `eda.events`, se republica el outbox entero de todos los componentes (`outbox.republish_in_order`), se arranca hasta lag 0 y llegan 20 avisos nuevos. Variantes: `truncated` (`delete_records`: los offsets siguen) y `recreated` (el topic se borra y se crea: los offsets vuelven a 0).
- **Mide.** Por variante: cerradas antes y después; filas republicadas por componente; mensajes publicados desde la pérdida; filas de outbox sin republicar o desordenadas dentro de su partición; filas que no respetan, entre todos los outbox, el orden que tenían en el topic perdido; cambios entre la foto de antes y la de después (las de E21); I1 a I5 al terminar.
- **Criterio.** H19: en las dos variantes, tantos mensajes como filas de outbox, en el orden de cada partición también entre componentes, ningún cambio tras la republicación, lag 0, todas las incidencias cerradas (también las nuevas) e I1 a I5 sin violaciones.
- **Implementación.** `uv run eda-expexecutor run e19`. En `recreated`, el procedimiento lleva a 0 los offsets de los grupos de Kafka y pone a -1 la posición que `event_log` y los proyectores guardan en PostgreSQL. Sin ello, los consumidores quedarían más allá del final del topic nuevo y el lado de lectura descartaría los mensajes nuevos, porque solo aplica offsets posteriores al guardado; los avisos nuevos lo pondrían de manifiesto en I5. Lo ya aplicado lo absorben el inbox, `views.applied_events` y el `ON CONFLICT` de `event_log`. La primera versión republicaba outbox a outbox (`replay_from`): el verificador vio que así un `StepCompleted` quedaba antes del `PlanCreated` de su incidencia y las vistas reconstruidas no coincidían con las mantenidas; por eso la spec event-backbone pide ahora el orden de cada partición también entre componentes. I5 no compara `kafka_partition` ni `kafka_offset` de las vistas, que cambian legítimamente al republicar.

### E20 Dependencias de V2 (H20)

- **Objetivo.** Comprobar que los guardrails de V2 aguantan cuando sus dependencias externas fallan: el servidor MCP de correo (spec mcp-mail) y el agente A2A del transportista (spec a2a-carrier). Cada fallo debe terminar en el resultado que dice su spec y el planificador debe seguir planificando sin las opciones del transportista.
- **Escenario.** En proceso, con `POC_VERSION=2` y 50 incidencias. El servidor de correo corre en proceso, el agente del transportista es la app A2A en proceso y `A2A_TIMEOUT_S` es de 1 s. Seis variantes con un solo fallo cada una (`synthetic.v2_faults`): `baseline`; `mail_down`, un 50 % de errores de `send_email`; `mail_invalid`, un 30 % de salidas sin `message_id` o con la dirección; y `carrier_slow`, `carrier_401` y `carrier_bad_card`, un 50 % de consultas con el agente lento (2 s), con un token que responde 401 o con una Agent Card que anuncia otra skill.
- **Mide.** Por variante: cerradas, resueltas y escaladas por motivo; fallos inyectados; `CarrierConsulted` por `outcome` y las consultas con un `outcome` inesperado; latencia máxima de la consulta; tareas que recibió el agente en las consultas con fallo; incidencias cuyo primer plan difiere del de `baseline`; `StepFailed` por `error`; llamadas y claves de `send_email`; `CustomerNotified`; máximo de correos por `idempotency_key`; eventos con algo que parece una dirección de correo; latencia; I1 a I5.
- **Criterio.** H20: en todas las variantes, ninguna incidencia abierta; todo `outcome` es el del fallo inyectado (`timeout`, `failed` o `rejected_by_guardrail`) o `answered` sin fallo; la consulta no supera `A2A_TIMEOUT_S` más 1,5 s; con el 401 y con la Agent Card distinta el agente no recibe ninguna tarea; los planes coinciden con los de `baseline`; ninguna clave tiene más de un correo; ningún evento contiene una dirección; I1 a I5 sin violaciones. Además, cada variante provoca su efecto: algún fallo inyectado, algún reintento de `send_email` en `mail_down` y un `StepFailed` con `mcp_output_invalid` por cada salida inválida en `mail_invalid`.
- **Implementación.** `uv run eda-expexecutor run e20`. El servidor MCP no tiene autenticación, así que el 401 solo existe en A2A, y el cliente A2A lo clasifica como `failed`. La elección de los fallos es determinista: en el correo depende de la semilla, la `idempotency_key` y el número de llamada con esa clave, de modo que un reintento puede salir bien; en el transportista, de la incidencia. El agente se monta como app ASGI nueva en cada consulta (`httpx.ASGITransport`), porque cada consulta corre en su propio bucle de asyncio. El oráculo planifica con las etiquetas del generador, así que las opciones del transportista no cambian el plan, y comparar con `baseline` mide solo si la consulta fallida bloquea o altera la planificación.

### E21 Reproducción total del log (H21)

- **Objetivo.** Demostrar que reprocesar todo el log, por error o por una recuperación, no repite ningún efecto (spec `idempotency`: «Reproducción del log con ejecutores activos»).
- **Escenario.** 50 incidencias cerradas con 30 % de fallos en `reserve_stock` (hay revisiones y compensaciones). Con el sistema parado se llevan los offsets de todos los grupos a 0 y se reinicia hasta lag 0. Variante de control `no_effect_keys`: los `ExecuteStep` antiguos se reentregan a los ejecutores sin inbox de comandos ni `effect_keys`. Como observación, se reproduce además sin el inbox del orquestador y luego sin el del intérprete.
- **Mide.** Ejecuciones de cada servicio simulado por clave, filas de cada `outbox`, `inbox` y `command_outbox`, huella de `plan_steps` y de las vistas, mensajes descartados por inbox; además, `ExecuteStep` antiguos reentregados a los ejecutores activos.
- **Criterio.** H21: nada cambia con el inbox activo y el control produce efectos repetidos (demuestra que el experimento detecta la falta de idempotencia).
- **Hallazgo.** Apagar el inbox de un solo consumidor no repite nada: la saga descarta por estado los eventos de incidencias cerradas y los resultados obsoletos, y el intérprete deduplica el aviso con el inbox de comandos de `OpenIncident`. Por eso el control está en la capa de efectos y la variante `inbox_off` queda como observación.
- **Necesita.** Reinicio de offsets de un grupo.
- **Diferencia con E1.** E1 duplica en caliente durante una ejecución; E21 reproduce el log completo después, con todo el estado ya escrito.
- **Implementación.** `uv run eda-expexecutor run e21`. Los grupos de Kafka (intérprete y orquestador) vuelven al offset 0 con el sistema parado; el lado de lectura guarda su posición en PostgreSQL (`views.offsets`), así que se rebobina desde el offset 0 y descarta lo ya aplicado. Los `ExecuteStep` antiguos se reentregan con el sistema en marcha y las incidencias ya cerradas (duplicado tardío). Las observaciones `inbox_off` y el control `no_effect_keys` (el último, porque quita las restricciones de unicidad de negocio) se ejecutan sobre la misma base después de la reproducción con inbox, y cada uno se compara con la foto anterior.

### E22 Carreras de comandos y ventana efecto–registro (H22)

- **Objetivo.** Ejercer lo que E1 no toca: carreras reales sobre el inbox y `effect_keys`, y la caída entre el efecto externo y el registro de su clave.
- **Variantes.** (a) mismo `ExecuteStep` desde 2, 4 y 8 hilos con conexiones propias; (b) mismo `CreatePlan`, `RevisePlan`, `RegisterCarrierNotice` y `SeedSampleData` concurrentes; (c) dos `command_id` con la misma `idempotency_key`; (d) caída tras enviar el correo o emitir el reembolso y antes de registrar la clave, con reintento.
- **Mide.** Ejecuciones del servicio por clave, respuestas distintas para un mismo `command_id`, eventos escritos, esperas por clave `in_progress` y recuperación tras `locked_until`.
- **Observación sin hipótesis.** Un mismo `command_id` con payload distinto: la spec no lo define; el informe lo anota y propone cerrar el caso.
- **Criterio.** H22: como mucho una ejecución del servicio por clave y la misma respuesta para todas las entregas de un `command_id`.
- **Necesita.** Entrega concurrente de comandos y punto de caída `crash_between_effect_and_record`.
- **Implementación.** `uv run eda-expexecutor run e22`, una ejecución por variante con un 50 % de fallos en `reserve_stock` (fuerzan `RevisePlan` e `issue_refund`). (a) y (b) usan `concurrent_commands` del `FaultPlan`: el comando llega desde N hilos a la vez, cada uno con su conexión y el mismo `command_id` (`deliver_concurrently`). (c) usa `reinvoke_steps`: el `ExecuteStep` llega a la vez con dos `command_id` y la misma clave, con dos workers de trabajos por componente (`RunConfig.job_workers`) para que los dos trabajos compitan por `effect_keys`. (d) usa `crash_between_effect_and_record`: en el primer intento de cada clave, el ejecutor llama al servicio y cae antes de la transacción que registra la clave; el trabajo se reintenta con el mismo `command_id` y la misma clave. `ExecuteStep`, `CreatePlan` y `RevisePlan` tienen trabajo largo: la petición solo registra el inbox y el trabajo, así que en (a) y (b) las entregas compiten en el inbox y solo un trabajo llega a `effect_keys`. Las esperas por una clave `in_progress` son los trabajos que encuentran la clave `in_progress` con su plazo vigente (`busy_claims`); la recuperación tras `locked_until` la cubre un test de integración del ejecutor. Las ejecuciones de `reserve_stock`, sin servicio externo, son sus `StockReserved` por clave. Cada variante debe provocar su carrera; si no, no cuenta como cumplida.

### E23 Duplicados de negocio (H23)

- **Objetivo.** Cubrir los duplicados que no vienen del bus sino del negocio: avisos reenviados por el transportista, replanes que repiten acciones.
- **Variantes.** (a) cada aviso reenviado de 1 a 5 veces, en orden y a la vez; (b) replanes que vuelven a pedir `file_claim` y `notify_customer`; (c) reembolso pedido otra vez en un plan posterior; (d) el mismo pedido en dos incidencias.
- **Mide.** Un `incident_id` por `carrier_notice_id`, descartes `duplicate_notice`, reclamaciones por (incidencia, pedido), avisos por (incidencia, pedido, versión del plan), reembolsos por incidencia y pedido, reservas activas por (incidencia, pedido).
- **Criterio.** H23: todos los recuentos igualan los máximos que fijan las specs `idempotency` e `incident-interpretation`.
- **Implementación.** `uv run eda-expexecutor run e23`, una ejecución por variante. (a) reenvía cada aviso con `Harness.resend_notice` (en orden) o `Harness.resend_notice_concurrently` (a la vez, un hilo y una conexión por envío). (b) y (c) usan oráculos de replan propios: `RerequestOracle` pide otra vez `file_claim` y `notify_customer` ya completados con `step_id` nuevos (la reserva tarda 2 s en responder para que ya estén completados); `RefundAgainOracle` mantiene tras el reembolso la reserva fallida, con su `step_id` y su clave, así que vuelve a fallar y el replan siguiente propone `issue_refund` otra vez. Los dos proponen planes que el replanner acepta (`check_revision`): no repiten pasos completados ni cambian el `step_id` de un paso abierto. (d) da a uno de cada cuatro pedidos un segundo aviso con otro `carrier_notice_id` y las mismas etiquetas, porque el oráculo indexa por pedido. Para que el sistema cumpla H23, el ejecutor de `issue_refund` devuelve el reembolso existente de la incidencia y el pedido, y el de `notify_customer` el aviso del mismo pedido en la misma versión del plan (migración 0025). Cada variante debe provocar su duplicado; si no, no cuenta como cumplida.

## 6. Limitaciones del arnés y orden de implantación

**Limitaciones.** El arnés actual es en proceso: sirve para duplicados, retrasos, fallos por probabilidad, caída de un consumidor y almacenes vacíos, y todo corre en un mismo proceso, de modo que no puede reproducir congelaciones de procesos, rebalanceos reales ni cortes de Kafka y PostgreSQL. E10 y E16 usan un modo multiproceso (E10, con todos los componentes en procesos propios; E9 se hace en proceso: corta el contenedor de Kafka con Docker) (E12 se hace en proceso, con el dispatcher en su propio hilo) (procesos de `scripts/run-agents.sh`, `docker stop/pause`, `SIGSTOP` o un proxy tipo toxiproxy). El modo multiproceso de E16 solo saca el orquestador a procesos propios: los componentes con LLM solo arrancan como proceso con Ollama. Las propuestas de esta guía se basan en las specs, `design.md` y el código del arnés, no en haber ejecutado los experimentos.

**Orden sugerido.**

1. En proceso y con poco trabajo nuevo: E21, E23, E17, E18 y E11, que reutilizan el arnés con extensiones pequeñas (reinicio de offsets, replan forzado, parada de proyectores, inyección de veneno, retraso por acción).
2. E22: requiere hilos con conexiones propias y un punto de caída nuevo en los ejecutores.
3. E12, en proceso con el dispatcher en su propio hilo. Con modo multiproceso: E16 y E10 (todos los componentes en procesos propios). E9 se hace en proceso, cortando el contenedor de Kafka.
4. El resto (E8, E14, E15, E19, E20) según el interés del trabajo.

Los ficheros de especificación de los experimentos (S) están en `specs-experimentos-resiliencia.md`, en formato OpenSpec (Purpose, Requirements, Scenarios con GIVEN/WHEN/THEN).
