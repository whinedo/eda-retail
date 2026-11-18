# Experiments Specification

## Purpose

Definir las hipótesis, los experimentos y las métricas de la PoC académica. La arquitectura orientada a eventos se compara con una base síncrona equivalente. Los umbrales indicados son valores iniciales ajustables. Los experimentos de resiliencia (E11 en adelante) comprueban además que la arquitectura mantiene sus garantías (una resolución por incidencia, sin efectos duplicados, vistas reconstruibles) cuando fallan la entrega, los consumidores, los ejecutores, las proyecciones y los mensajes, y que la idempotencia de comandos y eventos se cumple también bajo concurrencia y reproducción del log.

## Requirements

### Requirement: Base síncrona de referencia

El sistema SHALL incluir una implementación síncrona con la misma lógica de negocio, los mismos agentes con LLM y los mismos ejecutores, pero con llamadas directas en lugar de bus, log y vistas. MUST usar los mismos datos sintéticos y los mismos fallos inyectados.

#### Scenario: Misma entrada en ambas versiones

- **GIVEN** un conjunto de 50 avisos con una semilla
- **WHEN** se ejecuta en la versión de eventos y en la síncrona
- **THEN** ambas reciben exactamente los mismos avisos y fallos

### Requirement: Informe reproducible

Cada experimento SHALL poder ejecutarse con un único comando y generar un informe con la semilla, la configuración, la versión de los prompts y de los modelos y las métricas. MUST ser posible repetirlo con el mismo resultado de los componentes deterministas.

#### Scenario: Ejecución de un experimento

- **GIVEN** la configuración del experimento E1
- **WHEN** se ejecuta
- **THEN** se genera un informe con sus métricas y la configuración usada

### Requirement: E1 Duplicados en el bus (hipótesis H1)

El experimento E1 SHALL entregar cada evento a cada consumidor de Kafka y cada comando REST que invoca la saga 2 y 3 veces (el mismo `command_id`, como un reintento HTTP de entrega al menos una vez) y comparar el sistema con y sin Idempotency key. Sin idempotencia, los ejecutores e `inventory` pierden su inbox de comandos, `effect_keys` y la clave en los servicios simulados. Hipótesis H1: con idempotencia el número de efectos duplicados es 0 en todas las ejecuciones; sin ella, mayor que 0.

#### Scenario: Con idempotencia

- **GIVEN** 50 incidencias y cada evento y cada comando entregados 3 veces
- **WHEN** se ejecuta E1 con las claves activas
- **THEN** el informe indica 0 avisos, reclamaciones, reservas y reembolsos duplicados

#### Scenario: Sin idempotencia

- **GIVEN** las mismas 50 incidencias y las claves desactivadas
- **WHEN** se ejecuta E1
- **THEN** el informe cuantifica los efectos duplicados por tipo

### Requirement: E2 Caída de un ejecutor (hipótesis H2)

El experimento E2 SHALL matar un ejecutor a mitad de un plan y reiniciarlo: cae después de confirmar su transacción y antes de responder al `ExecuteStep`, y la saga lo vuelve a invocar con el mismo `command_id`. Hipótesis H2: el 100 % de los planes se completa sin repetir ningún paso ya completado.

#### Scenario: Caída a mitad del plan

- **GIVEN** 50 incidencias y un ejecutor que cae tras completar la mitad de sus pasos
- **WHEN** se reinicia
- **THEN** todas las incidencias terminan en `IncidentResolved` o `IncidentEscalated`
- **AND** el informe indica 0 pasos repetidos

### Requirement: E3 Fallo de un paso y replanificación (hipótesis H3)

El experimento E3 SHALL dejar sin stock el almacén en una parte de las incidencias. Hipótesis H3: en el 100 % de los casos el replanner genera un plan que no repite pasos completados y que respeta el límite de reembolso.

#### Scenario: Almacén sin existencias

- **GIVEN** 30 incidencias de pérdida con el almacén sin stock
- **WHEN** se ejecuta E3
- **THEN** cada incidencia termina con un reembolso o con un escalado
- **AND** el efecto de ningún `notify_customer` ni `file_claim` completado se repite (volver a confirmar con `StepCompleted` un paso mantenido en una revisión, sin efecto nuevo, no cuenta como repetición)

### Requirement: E4 Reconstrucción de vistas (hipótesis H4)

El experimento E4 SHALL borrar cada vista y reconstruirla volviendo a consumir el topic `eda.events` desde el principio; las vistas incluyen las del proyector `operations` (spec `incident-metrics`, «Reconstrucción»). Hipótesis H4: la huella de cada vista reconstruida coincide con la original, y el tiempo de reconstrucción se registra.

#### Scenario: Reconstrucción completa

- **GIVEN** un log de 200 incidencias y las vistas pobladas
- **WHEN** se borran y se reproducen
- **THEN** la huella de cada vista coincide con la original
- **AND** el informe indica el tiempo por vista

### Requirement: E5 Calidad del plan (hipótesis H5)

El experimento E5 SHALL medir el acierto del planificador sobre un conjunto etiquetado de 30 a 50 incidencias, comparando el plan generado con el esperado. Hipótesis H5: el acierto en la selección de acciones es al menos del 80 % (valor inicial).

#### Scenario: Evaluación del planificador

- **GIVEN** un conjunto etiquetado de 40 incidencias
- **WHEN** se ejecuta E5
- **THEN** el informe indica el acierto global y por tipo de causa
- **AND** lista los planes que no coinciden

#### Scenario: Puerta de calidad

- **GIVEN** un acierto inferior al umbral configurado
- **WHEN** termina E5
- **THEN** el informe marca la hipótesis H5 como no cumplida

### Requirement: E6 Eventos frente a síncrono (hipótesis H6)

El experimento E6 SHALL comparar las dos versiones en latencia total por incidencia, tokens, coste, comportamiento ante fallos y número de incidencias resueltas y escaladas, con las escaladas contadas por motivo. H6 no fija un umbral: el objetivo es medir y describir las diferencias, con la media, la mediana y el percentil 95. E6 SHALL escalar los plazos de la saga con el número de incidencias, sin bajar de los valores por defecto, porque la base síncrona no tiene plazos y el LLM es una sola cola compartida; el informe SHALL decir qué plazos se usaron.

#### Scenario: Comparación sin fallos

- **GIVEN** 50 incidencias sin fallos inyectados
- **WHEN** se ejecutan en ambas versiones
- **THEN** el informe incluye la latencia, los tokens y el coste de cada versión
- **AND** el informe incluye las incidencias escaladas de cada versión, contadas por motivo

#### Scenario: Comparación con fallos

- **GIVEN** las mismas incidencias con fallos y duplicados inyectados
- **WHEN** se ejecutan en ambas versiones
- **THEN** el informe incluye las incidencias resueltas y los efectos duplicados de cada versión

### Requirement: E7 Auditabilidad (hipótesis H7)

El experimento E7 SHALL seleccionar incidencias al azar y reconstruir su explicación solo con el log y la traza. Hipótesis H7: para el 100 % de las seleccionadas se puede indicar qué agente propuso cada paso, con qué versión del plan y por qué.

#### Scenario: Explicación de una incidencia

- **GIVEN** 10 incidencias elegidas al azar
- **WHEN** se reconstruye su explicación con `audit_trail` y la traza
- **THEN** cada decisión tiene agente, versión del plan y motivo

### Requirement: Repetición para el componente no determinista

Los experimentos que dependen del LLM (E5, E6) MUST repetirse al menos tres veces y el informe SHALL mostrar la variación entre ejecuciones.

#### Scenario: Tres repeticiones de E5

- **GIVEN** E5 configurado con tres repeticiones
- **WHEN** termina
- **THEN** el informe muestra el acierto de cada repetición y su rango

### Requirement: Invariantes comunes de resiliencia

Todo experimento de resiliencia (E11 en adelante) MUST comprobar al terminar los invariantes siguientes y MUST incluirlos en el informe, con su recuento, aunque la hipótesis del experimento hable solo de algunos:

- I1: toda incidencia termina en `IncidentResolved` o `IncidentEscalated` (ninguna queda abierta tras el tiempo máximo del experimento).
- I2: ningún efecto externo se ejecuta dos veces para la misma `idempotency_key` (`repeated_steps` = 0).
- I3: como máximo un `issue_refund` por incidencia y pedido, y nunca con compensaciones pendientes de esa incidencia.
- I4: toda fila de `outbox` de cada componente está publicada o pendiente; ninguna se pierde.
- I5: la huella de cada vista reconstruida desde `eda.events` coincide con la de la vista mantenida durante el experimento.

#### Scenario: Informe con invariantes

- **GIVEN** cualquier experimento de resiliencia terminado
- **WHEN** se escribe el informe
- **THEN** `report.json` contiene `invariants` con I1 a I5 y su recuento de violaciones
- **AND** la hipótesis del experimento no se da por cumplida si I1 o I2 tienen alguna violación

### Requirement: Convención de ejecución y de informe

Cada experimento nuevo SHALL ejecutarse con `uv run eda-experiment <exp> --seed N`, usar el generador de avisos y la inyección de fallos con la misma semilla y escribir `experiments/results/<exp>-<fecha>/report.json` y `report.md` con semilla, configuración, modelo y versión de los prompts. Salvo que el experimento diga otra cosa, MUST usar `--llm oracle` (sin coste), porque mide la arquitectura y no el LLM.

#### Scenario: Reproducibilidad

- **GIVEN** la semilla 42 y la misma configuración
- **WHEN** se ejecuta dos veces un experimento nuevo
- **THEN** los pasos que fallan y los mensajes retrasados o inyectados son los mismos en las dos ejecuciones

### Requirement: Modo multiproceso del harness

El harness SHALL ofrecer un modo multiproceso (`RunConfig.orchestrator_processes`) que arranque una o varias instancias del orquestador como procesos propios (`eda-orchestrator`, el mismo proceso que `scripts/run-agents.sh`) contra PostgreSQL y Kafka reales, con la base de datos y el topic propios de la ejecución. El resto de componentes sigue en el harness, con el cliente LLM simulado, y atiende los comandos de esas instancias por REST en un servidor HTTP local de la ejecución. El experimento E16 MUST usar ese modo; el modo en proceso sigue siendo el de E1 a E7 y el de los demás experimentos, E12 incluido.

#### Scenario: Aislamiento de la ejecución

- **GIVEN** una ejecución multiproceso
- **WHEN** termina o se aborta
- **THEN** sus procesos están parados y su base de datos y su topic se han borrado

### Requirement: Todos los componentes en procesos propios

El harness SHALL ofrecer un modo (`RunConfig.component_processes`) en el que cada componente corre como proceso propio: exactamente los procesos de `scripts/run-agents.sh` en su versión 1 (proyector, log de eventos, intake, inventory, orquestador, intérprete, planificador, replanificador y los cuatro `eda-step-executor <acción>`), contra PostgreSQL y Kafka reales, con la base de datos y el topic propios de la ejecución y un puerto libre por proceso que atiende comandos. El harness solo siembra los datos, envía los avisos por REST al proceso de intake y mide; espera a que los procesos estén listos antes de empezar y los para al terminar, también si la ejecución falla. Los procesos con LLM eligen su cliente con `LLM_CLIENT`: `ollama` (por defecto, el de producción) u `oracle`, el oráculo sobre `generate(ORACLE_SEED, ORACLE_INCIDENTS)`; cualquier otro valor MUST impedir que el proceso arranque. Con `SERVICE_EXECUTIONS_LOG`, cada ejecutor anota cada ejecución de su servicio simulado con su `idempotency_key`; sin ella, nada cambia. El modo MUST rechazar lo que solo inyecta el harness en proceso: los fallos del `FaultPlan`, los modos de idempotencia distintos de `full`, la versión 2 y un LLM, unos datos o un transportista propios. El informe de cada proceso da si murió solo, su código de salida y su log. Sin el interruptor, el harness y los procesos de `scripts/run-agents.sh` no cambian.

#### Scenario: Los mismos procesos que scripts/run-agents.sh

- **GIVEN** el modo con todos los componentes en procesos propios
- **WHEN** el harness lanza los procesos
- **THEN** son exactamente los de `scripts/run-agents.sh`, con el mismo nombre, el módulo de su comando `eda-*` y sus argumentos

#### Scenario: LLM simulado en los procesos

- **GIVEN** un proceso con LLM
- **WHEN** arranca sin `LLM_CLIENT` o con `LLM_CLIENT=oracle`
- **THEN** usa Ollama o el oráculo sobre el conjunto de la ejecución, respectivamente
- **AND** con otro valor no arranca y el error lo dice en español

#### Scenario: Servicios simulados con su registro de ejecuciones

- **GIVEN** un ejecutor con `SERVICE_EXECUTIONS_LOG`
- **WHEN** su servicio simulado ejecuta un efecto
- **THEN** queda una línea con el servicio y la `idempotency_key`, y una llamada repetida con la misma clave no deja otra
- **AND** sin la variable el ejecutor no anota nada

#### Scenario: Procesos listos antes de empezar

- **GIVEN** los procesos recién lanzados
- **WHEN** el harness espera a que estén listos
- **THEN** cada proceso que atiende comandos acepta conexiones y cada consumidor de grupo tiene particiones
- **AND** si uno muere al arrancar, el error da su nombre, su código de salida y el final de su log

#### Scenario: Parada y procesos caídos

- **GIVEN** una ejecución con todos los componentes en procesos
- **WHEN** el harness los para
- **THEN** cada proceso recibe `SIGINT`, ejecuta su limpieza y sale con código 0
- **AND** un proceso que terminó antes sin que el harness lo parase consta como caído, con su código de salida

#### Scenario: Ejecución de punta a punta con todos los componentes en procesos

- **GIVEN** una ejecución pequeña con todos los componentes en procesos y el oráculo como LLM
- **WHEN** se ejecuta hasta cerrar las incidencias
- **THEN** todas se cierran, ningún proceso muere y todos salen limpios al pararlos
- **AND** I1 a I5 no tienen violaciones y cada `idempotency_key` se ejecuta una vez en su servicio simulado

### Requirement: E9 Caída de Kafka (H9)

El sistema SHALL cumplir la hipótesis H9: si el broker de Kafka se para o se congela y vuelve, los componentes siguen aceptando comandos y guardando sus eventos en el outbox, el relay publica todo lo pendiente al volver el broker, no se pierde ningún evento, los mensajes que el broker recibe más de una vez (un lote sin confirmar que el productor reenvía y el relay vuelve a publicar) no repiten efectos ni filas, toda incidencia se cierra y se mantienen I1 a I5. El experimento SHALL ejecutarse en modo en proceso con 50 incidencias: cuando la mitad tienen un `PlanCreated` en el log, se corta el broker durante 30 s y en ese tiempo llegan 20 avisos más; después el broker vuelve y se ejecuta hasta cerrar todas las incidencias. Tres variantes (spec synthetic-data, «Caída del broker»): `baseline`, sin corte (los 20 avisos llegan en el mismo momento); `stopped`, el contenedor de Kafka se para y se arranca (conexión rechazada); y `paused`, el contenedor se congela y se descongela (conexiones abiertas sin respuesta). Mientras el broker no está, el relay de cada componente reintenta como `ComponentRelay.run_until` (spec event-backbone): un lote sin confirmar se deshace y sus filas siguen pendientes. Un commit de offset que falla con el broker caído, o que no se confirma en su plazo (spec event-backbone, «Entrega al menos una vez»), hace caer al consumidor, que se reinicia sin confirmar el offset; Kafka vuelve a entregar el mensaje y el inbox lo descarta. El corte lo termina un temporizador a los 30 s, aunque un turno quede bloqueado en una llamada a Kafka, y los consumidores usan un `session.timeout.ms` de 6 s: sin broker, un commit síncrono espera un coordinador hasta ese plazo, y un consumidor recreado espera a que caduque el miembro anterior del grupo. El informe MUST dar, por variante: incidencias cerradas, resueltas y escaladas por motivo; duración real del corte; filas pendientes de outbox, máximas por componente durante el corte y al terminar; filas publicadas durante el corte; avisos aceptados durante el corte; errores de entrega del relay por componente; caídas de consumidores por fallo de commit; tiempo desde la vuelta del broker hasta vaciar el outbox; mensajes en el topic, filas de outbox, filas de `event_log` y mensajes repetidos (mensajes menos filas de `event_log`); descartes del inbox por consumidor; latencia (media, mediana y p95) y su cociente frente a `baseline`; e I1 a I5. Como diagnóstico, el informe da también, al terminar cada variante y antes de que `event_log` se ponga al día, el lag de cada grupo y, por consumidor, las particiones asignadas con su posición o, si tiene hilo propio, si el hilo sigue vivo. Cada variante con corte MUST provocar su efecto (al menos un error de entrega del relay y una fila de outbox pendiente durante el corte); si no, no cuenta como cumplida. Los consumidores en proceso SHALL usar un `session.timeout.ms` de 6 s y un `max.poll.interval.ms` de 15 s, menor que el tiempo sin avance con el que la ejecución se da por terminada (30 s): con el broker congelado, el broker, que usa ese plazo como rebalance timeout, no retiene más de 15 s, una vez que vuelve a responder, el `JoinGroup` de un consumidor que perdió sus particiones.

#### Scenario: Broker parado

- **GIVEN** la variante `stopped`, con incidencias en curso
- **WHEN** el broker se para y vuelve a los 30 s
- **THEN** durante el corte el relay no marca ninguna fila como publicada y el outbox acumula las pendientes
- **AND** al volver, el relay las publica todas: no queda ninguna fila pendiente y cada evento del outbox está en `event_log`

#### Scenario: Broker congelado

- **GIVEN** la variante `paused`, con incidencias en curso
- **WHEN** el broker se congela y se descongela a los 30 s
- **THEN** ningún turno del relay se bloquea más que su plazo de entrega y no queda ninguna fila pendiente al terminar
- **AND** los mensajes repetidos en el topic no repiten efectos (I2) ni filas en `event_log`

#### Scenario: Avisos durante la caída

- **GIVEN** el broker cortado en `stopped` o `paused`
- **WHEN** llegan 20 avisos nuevos
- **THEN** el intake los acepta todos, porque los comandos no pasan por Kafka
- **AND** sus incidencias se cierran cuando el broker vuelve

#### Scenario: Cumplimiento de H9

- **GIVEN** la ejecución completa de E9
- **WHEN** se evalúa la hipótesis
- **THEN** H9 se cumple si en todas las variantes no queda ninguna fila de outbox pendiente, no se publicó ninguna durante el corte, cada evento del outbox está en `event_log`, todas las incidencias (también las de los avisos del corte) se cerraron, I1 a I5 no tienen violaciones y cada variante con corte provocó su efecto

### Requirement: E10 Caída de PostgreSQL (H10)

El sistema SHALL cumplir la hipótesis H10: si PostgreSQL se para o se congela y vuelve, los componentes se reconectan solos, sin reiniciarlos a mano; no se pierde ningún evento, ningún efecto se repite, toda incidencia se cierra (también las de los avisos que llegan durante el corte) y se mantienen I1 a I5. El experimento SHALL ejecutarse con todos los componentes en procesos propios (requirement «Todos los componentes en procesos propios») y 50 incidencias: cuando la mitad tienen un `PlanCreated`, se corta el contenedor `eda-postgres` durante 30 s (spec synthetic-data, «Caída de la base de datos»), con un temporizador que lo devuelve a su hora. Durante el corte llegan 20 avisos por REST: fallan con 5xx o sin respuesta, y quien los envía los reintenta, con el mismo `command_id`, hasta que se aceptan (si un intento llegó a registrar el aviso y perdió la respuesta, el inbox de intake repite su 202; un 422 `duplicate_notice` solo llega si el aviso ya estaba registrado con otro `command_id`, no cuenta como aceptado y se informa aparte). Después se ejecuta hasta cerrar todas las incidencias. Tres variantes: `baseline`, sin corte (los 20 avisos llegan en el mismo momento); `stopped`, `docker stop` y `docker start` (las conexiones se cortan y las transacciones abiertas se deshacen); y `paused`, `docker pause` y `docker unpause` (las conexiones quedan abiertas sin respuesta). El harness MUST NOT leer su conexión durante el corte: solo envía los avisos, sondea la base con una conexión nueva y mira si algún proceso murió; al volver la base cambia su conexión si el corte la rompió, y una lectura posterior que falla se cuenta y se reintenta tras reconectarse, sin tumbar la ejecución. `event_log` se pone al día con los procesos aún en marcha; las invariantes se comprueban con los procesos ya parados.

El informe MUST dar, por variante: incidencias cerradas, resueltas y escaladas por motivo; duración real del corte y tiempo hasta que la base responde; avisos enviados, aceptados, rechazados durante y después del corte, reintentos y rechazos de negocio; sondeos de la base sin respuesta durante el corte; por proceso, sus errores de conexión, los mensajes que volvió a atender tras un reintento en el proceso, los que Kafka le volvió a entregar porque su offset no se confirmó, los que descartó su inbox y los reintentos de comandos del dispatcher; procesos que murieron y código de salida de cada uno al pararlo; tiempo desde la vuelta de la base hasta que el lag (los grupos de Kafka y la posición guardada de `event_log` y de cada proyector) llega a 0 con el outbox vacío; filas de outbox pendientes al terminar, filas de outbox, filas de `event_log` y eventos del outbox que faltan en `event_log`; claves ejecutadas en los servicios simulados, máximo de ejecuciones por `idempotency_key` (debe ser 1) y ejecuciones repetidas; latencia (media, mediana y p95) y su cociente frente a `baseline`; e I1 a I5.

Las métricas de cada proceso salen de su log. Un error de conexión es una línea WARNING o ERROR del propio proceso con «la base no responde» o «sin conexión a la base» (consumidores de Kafka, proyector, event-log y relay), «: error; se reintenta» (el relay de cada proceso y los bucles del dispatcher, de los plazos y de los avisos aparcados) o «worker de trabajos de» (el worker de trabajos largos). Un mensaje vuelto a atender es una segunda línea `-> tipo incidencia pX oY` del mismo offset: tras «se reintenta el mensaje pX oY» es un reintento en el proceso; sin esa línea, una entrega repetida de Kafka. Un descarte del inbox es una línea `<- tipo incidencia: duplicate`; un reintento de comando, una línea «sin respuesta (intento n)» del dispatcher. Un proceso queda colgado si no sale tras `SIGINT` y hay que matarlo: su código de salida no es 0.

Cada variante con corte MUST provocar su efecto; si no, no cuenta como cumplida: al menos un aviso rechazado durante el corte y, en `stopped`, al menos un error de conexión en cada proceso (todos tienen un relay que consulta la base sin parar); en `paused`, al menos un sondeo con una conexión nueva sin respuesta, porque las conexiones abiertas esperan sin error (TCP keepalive no detecta una base congelada) y un proceso puede no registrar ninguno.

#### Scenario: Base parada

- **GIVEN** la variante `stopped`, con incidencias en curso y todos los componentes en procesos propios
- **WHEN** PostgreSQL se para y vuelve a los 30 s
- **THEN** cada proceso registra al menos un error de conexión y se reconecta solo, sin morir ni reiniciarse a mano
- **AND** al terminar no queda ninguna fila de outbox pendiente, cada evento del outbox está en `event_log` y ninguna `idempotency_key` se ejecuta más de una vez

#### Scenario: Base congelada

- **GIVEN** la variante `paused`, con incidencias en curso y todos los componentes en procesos propios
- **WHEN** PostgreSQL se congela y se descongela a los 30 s
- **THEN** la base no acepta conexiones nuevas durante el corte y los procesos siguen al descongelarse, sin morir
- **AND** al terminar no queda ninguna fila de outbox pendiente y ningún efecto se repite (I2)

#### Scenario: Avisos durante la caída de la base

- **GIVEN** la base cortada en `stopped` o `paused`
- **WHEN** llegan 20 avisos por REST
- **THEN** fallan con 5xx o sin respuesta y quien los envía los reintenta hasta que se aceptan
- **AND** sus incidencias se cierran cuando la base vuelve

#### Scenario: Procesos al terminar E10

- **GIVEN** una variante de E10 terminada
- **WHEN** el harness para los procesos
- **THEN** el informe da los que murieron solos y el código de salida de cada uno, que es 0 si salió limpio

#### Scenario: Cumplimiento de H10

- **GIVEN** la ejecución completa de E10
- **WHEN** se evalúa la hipótesis
- **THEN** H10 se cumple si en todas las variantes ningún proceso murió ni quedó colgado, no queda ninguna fila de outbox pendiente, cada evento del outbox está en `event_log`, ningún efecto se repite, todas las incidencias (también las de los avisos del corte) se cerraron, I1 a I5 no tienen violaciones y cada variante con corte provocó su efecto

### Requirement: E11 Entrega retrasada y planes revisados (H11)

El sistema SHALL cumplir la hipótesis H11: con revisiones forzadas del plan y resultados de pasos que llegan tarde, ningún resultado obsoleto cambia el estado de la saga, salvo la excepción del supuesto A4, y se mantienen los invariantes I1 a I5. El escenario son 50 incidencias con un 50 % de fallos en `reserve_stock` (`fail_probability`), planes con pasos en paralelo, `step_delay_seconds` en `reserve_stock`, `reissue_shipment` y `file_claim` para que haya resultados de una `plan_version` anterior después de un `PlanRevised`, y un plazo de `ExecuteStep` más corto que parte de esos retrasos. El orquestador descarta un resultado obsoleto sin registrarlo en ningún sitio, así que el experimento los cuenta desde el log: es obsoleto el `StepCompleted` o `StepFailed` cuya `plan_version` es anterior a la vigente cuando el orquestador lo consume (salvo el de una compensación, que no lo es por la versión: spec incident-planning, «Eventos obsoletos»), y el de un paso cuyo plazo ya venció con el paso pedido (el plazo no deja evento: lo anota el harness al tratarlo). Un obsoleto cambia el estado de la saga si el orquestador emite algún evento con él como causa. La excepción A4: el `StepCompleted` tardío de un `reserve_stock` cancelado se marca completado y pide su `release_stock`, para no dejar la reserva sin liberar; esa compensación MUST pedirse una sola vez. El informe MUST dar: `PlanRevised` por incidencia, resultados obsoletos por versión y tras el plazo, los que cambiaron el estado de la saga (fuera de A4) y las compensaciones A4, compensaciones pedidas y completadas, y el número de incidencias escaladas por motivo.

#### Scenario: Resultado de una versión antigua tras una revisión

- **GIVEN** un plan v1 con `reserve_stock` y `file_claim` en paralelo, y la reserva falla
- **WHEN** la saga aplica `PlanRevised` v2 y después llega el `StepCompleted` de `file_claim` de la v1
- **THEN** el orquestador lo descarta como obsoleto sin cambiar el estado de la saga (no emite ningún evento por él)
- **AND** el efecto de `file_claim` no se ejecuta otra vez (I2)

#### Scenario: Resultado después del plazo

- **GIVEN** un `ExecuteStep` cuyo plazo vence mientras su ejecutor tarda más de lo previsto
- **WHEN** el resultado llega después del plazo
- **THEN** la saga ya ha tratado el paso como fallido con `error` `timeout` y descarta el resultado como obsoleto
- **AND** si el paso era `reserve_stock`, su compensación se pide una sola vez

#### Scenario: Reembolso con compensaciones pendientes

- **GIVEN** una incidencia con un `reserve_stock` completado cuya compensación sigue pendiente
- **WHEN** el plan revisado propone `issue_refund`
- **THEN** la saga no invoca `issue_refund` hasta que la compensación termine (I3)

#### Scenario: Cumplimiento de H11

- **GIVEN** la ejecución completa de E11
- **WHEN** se evalúa la hipótesis
- **THEN** H11 se cumple si I1 a I5 no tienen violaciones, ningún resultado obsoleto cambió el estado de la saga salvo las compensaciones A4 y ninguna compensación se pidió más de una vez

#### Scenario: Reserva cancelada completada tarde (supuesto A4)

- **GIVEN** un `reserve_stock` cancelado por una revisión del plan cuyo `StepCompleted` llega después
- **WHEN** el orquestador lo consume
- **THEN** marca la reserva como completada y pide su `release_stock` una sola vez, y no cuenta como un cambio de estado indebido

### Requirement: E12 Ejecutor caído y ejecutor colgado (H12)

El sistema SHALL cumplir la hipótesis H12, que tiene dos partes: H12a (vivacidad) toda incidencia que usa el ejecutor afectado termina escalada o resuelta dentro de `SAGA_STEP_TIMEOUT_S` más un margen, y las que no lo usan terminan como siempre; H12b (aislamiento) la latencia p95 de las incidencias que no usan el ejecutor afectado no supera el doble de la de una ejecución sin fallo. El experimento SHALL ejecutarse en modo en proceso (`InProcessCommands` simula el ejecutor caído o colgado y el dispatcher va en su propio hilo, como en el orquestador, para que un ejecutor colgado lo bloquee a él y no al resto del arnés) con `file_claim` en `down`, en `hang` y en `hang` con recuperación al cabo de un intervalo, con las 50 incidencias que genera la semilla del experimento, una parte con `file_claim` (con la semilla 42, 18); el informe MUST dar la proporción de incidencias que llevan `file_claim`. La parte H12b se declara exploratoria: se parte de la hipótesis de que el dispatcher, que invoca un comando por turno (`LIMIT 1`), sufre bloqueo de cabeza de línea con el modo `hang` (cada intento ocupa `COMMAND_HTTP_TIMEOUT_S`); un resultado negativo de H12b es un hallazgo y no un fallo del experimento, y el informe lo MUST decir.

#### Scenario: Ejecutor caído para siempre

- **GIVEN** el ejecutor de `file_claim` en modo `down` durante toda la ejecución
- **WHEN** vence el plazo de cada `ExecuteStep` de `file_claim`
- **THEN** cada paso pasa a `failed` con `error` `timeout` y la saga revisa el plan o escala
- **AND** las incidencias sin `file_claim` terminan en `IncidentResolved`

#### Scenario: Ejecutor colgado

- **GIVEN** el ejecutor de `file_claim` en modo `hang`
- **WHEN** el dispatcher lo invoca repetidamente
- **THEN** el informe da cuántos intentos hizo el dispatcher por comando, el tiempo que el dispatcher pasó esperando a este ejecutor y la latencia p95 de las incidencias que no lo usan
- **AND** el plazo del paso empieza a contar en el primer envío, no en el encolado

#### Scenario: Recuperación del ejecutor

- **GIVEN** un ejecutor `hang` que se recupera cuando algunos plazos ya han vencido
- **WHEN** responde a los comandos pendientes
- **THEN** los resultados llegan como obsoletos o a pasos aún vigentes y no se ejecuta ningún efecto dos veces (I2)

#### Scenario: Compensación de un paso con plazo vencido

- **GIVEN** un `reserve_stock` cuyo ejecutor está caído al vencer el plazo
- **WHEN** la saga pide su compensación
- **THEN** la compensación se invoca con el sufijo `-comp` y se completa cuando el ejecutor vuelve, sin liberar dos veces

### Requirement: E13 Fallo de compensación (H13)

El sistema SHALL cumplir la hipótesis H13: cuando una compensación termina en `StepFailed`, la saga escala la incidencia con `compensation_failed`, no emite `issue_refund` y no pide más compensaciones, y se mantienen I1 a I5 (spec incident-planning, «Fallo de una compensación» y «Escalado de la incidencia»). La saga no reintenta pasos: los reintentos son del ejecutor, que solo publica `StepFailed` cuando los agota (`STEP_MAX_ATTEMPTS` para un error reintentable, uno para uno que no lo es), así que todo `StepFailed` de una compensación es definitivo. El escenario son 50 incidencias en modo en proceso con `fail_probability` 1,0 en `reissue_shipment` (cada reenvío falla, el replanner lo sustituye por `issue_refund` y la saga deriva el `release_stock` `-comp` de la reserva completada, que retiene el reembolso) y 0,5 en `release_stock` (el fallo inyectado no es reintentable, `attempts` 1). Las compensaciones que se completan son la rama de control: su reembolso retenido se emite. El informe MUST dar: compensaciones pedidas, completadas y fallidas; incidencias con una compensación fallida y, de ellas, las escaladas con `compensation_failed`, los `issue_refund` pedidos y las compensaciones pedidas después del escalado; incidencias con la compensación completada y reembolsadas; escaladas por motivo; e I1 a I5. El experimento MUST provocar al menos una compensación fallida; si no, no cuenta como cumplido.

#### Scenario: La liberación del stock falla

- **GIVEN** un `reserve_stock` completado cuyo `reissue_shipment` falla y un plan revisado con `issue_refund`
- **WHEN** su `release_stock` `-comp` termina en `StepFailed`
- **THEN** la saga publica `IncidentEscalated` con el motivo `compensation_failed`
- **AND** no se pide ningún `issue_refund` en esa incidencia

#### Scenario: Sin compensaciones tras el escalado

- **GIVEN** una incidencia escalada con `compensation_failed`
- **WHEN** la saga trata el escalado y los resultados que llegan después
- **THEN** no pide ninguna compensación más en esa incidencia

#### Scenario: La compensación se completa

- **GIVEN** un `release_stock` `-comp` que termina en `StepCompleted`
- **WHEN** la saga lo trata
- **THEN** pide el `issue_refund` retenido y la incidencia termina en `IncidentResolved` con `outcome` `refunded`

#### Scenario: Cumplimiento de H13

- **GIVEN** la ejecución completa de E13
- **WHEN** se evalúa la hipótesis
- **THEN** H13 se cumple si hubo al menos una compensación fallida, toda incidencia con una compensación fallida terminó escalada con `compensation_failed`, sin `issue_refund` pedido y sin compensaciones pedidas tras el escalado, y I1 a I5 no tienen violaciones

### Requirement: E14 Ráfaga y contrapresión (H14)

El sistema SHALL cumplir la hipótesis H14: ante una ráfaga de avisos con ejecutores que rechazan con 503 por encima de su capacidad, el dispatcher reintenta cada comando con el mismo `command_id` y backoff, ningún ejecutor tiene más trabajos abiertos que su capacidad, las colas (`orchestrator.command_outbox` y los `command_jobs` de cada componente) se vacían, ningún comando se pierde ni se duplica con otro `command_id` y se mantienen I1 a I5 (spec resilience, «Pico de incidencias»). El experimento SHALL ejecutarse en modo en proceso con 200 incidencias enviadas de golpe y dos variantes: `baseline`, sin límite de capacidad, y `backpressure`, con capacidad 3 en cada uno de los cuatro ejecutores (`executor_<acción>`). En las dos, el `prepare` de los trabajos del planner tarda 0,05 s (el LLM) y el de los ejecutores 0,1 s (el servicio externo), para que los pasos lleguen a cada ejecutor más deprisa de lo que los atiende y las colas de trabajos crezcan. La capacidad se cuenta en trabajos abiertos (`pending` o `running`) en el `command_jobs` del ejecutor, porque en proceso el dispatcher entrega un comando cada vez y un `ExecuteStep` solo se acepta. El informe MUST dar, por variante: incidencias cerradas, resueltas y escaladas por motivo; respuestas 503 por ejecutor; intentos por comando (máximo y comandos reintentados); máximo de trabajos abiertos por componente y de comandos pendientes en `command_outbox`; tiempo hasta vaciar las colas; comandos pendientes y trabajos abiertos al terminar; pasos con `ExecuteStep` de `command_id` distinto; latencia (media, mediana y p95); e I1 a I5. La variante `backpressure` MUST provocar al menos un 503; si no, no cuenta como cumplida. La latencia frente a `baseline` se da como dato y no forma parte de H14.

#### Scenario: Ejecutor saturado

- **GIVEN** un ejecutor con capacidad 3 y 3 trabajos abiertos
- **WHEN** el dispatcher le invoca otro `ExecuteStep`
- **THEN** la invocación falla con 503 (`CommandUnavailable`) sin crear el trabajo
- **AND** el dispatcher reprograma el comando con backoff y el mismo `command_id`

#### Scenario: Las colas se vacían

- **GIVEN** la ráfaga de 200 avisos de la variante `backpressure`
- **WHEN** termina la ejecución
- **THEN** no queda ningún comando `pending` en `orchestrator.command_outbox` ni ningún trabajo `pending` o `running` en los `command_jobs`
- **AND** ningún ejecutor tuvo nunca más trabajos abiertos que su capacidad

#### Scenario: Sin comandos duplicados

- **GIVEN** comandos reintentados tras un 503
- **WHEN** se cuentan los `ExecuteStep` de `command_outbox`
- **THEN** ningún paso tiene dos `ExecuteStep` con `command_id` distinto en la misma versión del plan

#### Scenario: Cumplimiento de H14

- **GIVEN** la ejecución completa de E14
- **WHEN** se evalúa la hipótesis
- **THEN** H14 se cumple si en `backpressure` hubo al menos un 503, ningún ejecutor superó su capacidad, las colas terminaron vacías, ningún paso tiene comandos duplicados, todas las incidencias se cerraron y I1 a I5 no tienen violaciones
- **AND** en `baseline` I1 e I2 tampoco tienen violaciones (si se violan, la hipótesis no se da por cumplida)

### Requirement: E15 Degradación del LLM (H15)

El sistema SHALL cumplir la hipótesis H15: cuando el LLM del planner y del replanner responde con salidas inválidas, tarda o falla, ninguna salida inválida se convierte en un evento de plan, toda incidencia se cierra (resuelta o escalada con su motivo), no se pide ningún paso del plan tras el escalado de la incidencia (las compensaciones `-comp` que deriva el escalado sí) y se mantienen I1 a I5 (spec llm-boundaries). El experimento SHALL ejecutarse en modo en proceso con 50 incidencias, un 30 % de fallos en `reissue_shipment` para que también trabaje el replanner, y cuatro variantes del cliente oráculo (spec synthetic-data, «Degradación del LLM»): `baseline`, sin degradación; `invalid`, un 60 % de las respuestas no son JSON o les faltan campos; `slow`, 0,5 s por llamada; y `error`, un 30 % de las llamadas lanzan una excepción (como un timeout de Ollama) y el trabajo se reintenta con backoff hasta `JOB_MAX_ATTEMPTS`. `slow` y `error` usan un plazo de `CreatePlan` y `RevisePlan` de 4 s: en `slow`, para que la cola del planner lo supere; en `error`, porque un trabajo que agota sus intentos queda `failed` sin eventos y solo el plazo cierra la incidencia. El informe MUST dar, por variante: incidencias cerradas, resueltas y escaladas por motivo; degradaciones inyectadas; llamadas al LLM registradas y las que no pasaron la validación; trabajos del planner y del replanner reintentados, su máximo de intentos y los `failed`; eventos de plan cuya última llamada al LLM no pasó la validación; eventos de plan publicados después del escalado de su incidencia; pasos pedidos después del escalado; latencia (media, mediana y p95); e I1 a I5. Cada variante con degradación MUST provocar su efecto (`invalid`: al menos una llamada que no pasa la validación; `slow`: al menos un escalado por `plan_timeout`; `error`: al menos un trabajo reintentado); si no, no cuenta como cumplida.

#### Scenario: Salidas inválidas del LLM

- **GIVEN** la variante `invalid`
- **WHEN** el planner agota `LLM_MAX_GENERATIONS` sin una salida válida
- **THEN** escala la incidencia con `llm_output_invalid` sin publicar ningún `PlanCreated`
- **AND** todo `PlanCreated` o `PlanRevised` publicado viene de una llamada que pasó la validación

#### Scenario: LLM lento y plazo del plan

- **GIVEN** la variante `slow`, con un plazo de plan menor que la cola de trabajos del planner
- **WHEN** vence el plazo de un `CreatePlan`
- **THEN** la saga escala la incidencia con `plan_timeout`
- **AND** si el plan llega después, no se pide ninguno de sus pasos

#### Scenario: Errores del LLM y trabajos reintentados

- **GIVEN** la variante `error`
- **WHEN** la llamada al LLM lanza una excepción
- **THEN** el trabajo vuelve a `pending` con backoff y se reintenta con el mismo `command_id`
- **AND** el informe cuenta los trabajos reintentados
- **AND** si el trabajo agota `JOB_MAX_ATTEMPTS`, la saga escala la incidencia con `plan_timeout` al vencer el plazo

#### Scenario: Cumplimiento de H15

- **GIVEN** la ejecución completa de E15
- **WHEN** se evalúa la hipótesis
- **THEN** H15 se cumple si en todas las variantes se cerraron todas las incidencias, ningún evento de plan viene de una llamada inválida, no se pidió ningún paso tras el escalado, I1 a I5 no tienen violaciones y cada variante con degradación provocó su efecto

### Requirement: E16 Consumidor zombie y rebalanceo (H16)

El sistema SHALL cumplir la hipótesis H16: cuando una instancia del orquestador se congela más que el timeout de sesión, otra toma sus particiones y la primera reanuda, no se produce ninguna transición de saga inválida ni ningún comando duplicado con `command_id` distinto para el mismo paso, y se mantienen I1 a I5. El experimento SHALL ejecutarse en modo multiproceso con dos instancias del orquestador en el mismo grupo, 50 incidencias en curso y un `SIGSTOP` a la instancia que posee más particiones durante un tiempo mayor que `session.timeout.ms`; después `SIGCONT`. El experimento sondea el supuesto de «único dueño sin concurrencia optimista» del diseño: el informe MUST dar, por incidencia, si dos instancias procesaron mensajes de ella solapados en el tiempo y qué transacciones esperaron al bloqueo de la incidencia.

#### Scenario: Rebalanceo mientras la instancia está congelada

- **GIVEN** la instancia A del orquestador congelada con mensajes en curso
- **WHEN** vence su sesión y Kafka reasigna sus particiones a la instancia B
- **THEN** B procesa los mensajes pendientes de esas particiones
- **AND** el informe registra el instante del rebalanceo

#### Scenario: Reanudación del zombie

- **GIVEN** la instancia A reanudada tras el rebalanceo con un mensaje a medio procesar
- **WHEN** A termina su transacción o intenta confirmar su offset
- **THEN** el inbox descarta el mensaje si B ya lo registró, o la confirmación falla sin dejar estado a medias
- **AND** `orchestrator.plan_steps` no retrocede ningún paso de `completed` a otro estado

#### Scenario: Cumplimiento de H16

- **GIVEN** la ejecución completa de E16
- **WHEN** se evalúa la hipótesis
- **THEN** H16 se cumple si no hay dos comandos con `command_id` distinto para el mismo paso y la misma versión del plan, ni transiciones inválidas, y I1 a I5 no tienen violaciones

### Requirement: E17 Proyección atrasada y avisos aparcados (H17)

El sistema SHALL cumplir la hipótesis H17: con los proyectores parados, los avisos sobre pedidos que aún no están en `view_orders` se aparcan; cada uno se reanuda con un `ShipmentDelayed` o escala con `no_valid_orders` según lo que ocurra antes de su plazo, y ningún aviso queda aparcado al terminar. Un aviso aparcado no depende de que los proyectores se reanuden: el intérprete lo reanuda al consumir de Kafka el `OrderRegistered` de su pedido, y si ese evento ya lo consumió antes de aparcarlo, solo lo resuelve al vencer el plazo, consultando `view_orders` una vez (spec cqrs-views, «Proyección atrasada»). El escenario son 50 avisos y tres variantes con `PENDING_NOTICE_TIMEOUT_S` acortado (30 s), siempre con los proyectores parados al llegar los avisos: (a) pedidos registrados después de aparcar los avisos; (b) pedidos registrados antes de los avisos y proyectores parados más que el plazo; (c) pedidos registrados antes de los avisos y proyectores reanudados antes del plazo. El informe MUST dar, por variante, avisos aparcados, reanudados por `OrderRegistered`, reanudados por el plazo con el pedido proyectado y escalados, el tiempo en aparcamiento, los avisos que quedan aparcados, los pedidos que tenía `view_orders` mientras los proyectores estaban parados, y comprobar que el intérprete no consulta `view_orders` en bucle: el aviso se aparca y se confirma el offset, así que el lag de su grupo es 0 con los avisos aparcados.

#### Scenario: Reanudación por el pedido registrado

- **GIVEN** 50 avisos aparcados sobre pedidos que aún no existen y los proyectores parados
- **WHEN** se registran sus pedidos antes de que venza el plazo
- **THEN** el intérprete publica un `ShipmentDelayed` por aviso al consumir su `OrderRegistered`, aunque los proyectores sigan parados
- **AND** `parked_notices` queda vacía y ningún aviso escala

#### Scenario: Plazo vencido sin proyección

- **GIVEN** avisos aparcados cuyo `OrderRegistered` el intérprete consumió antes de aparcarlos, y los proyectores parados más tiempo que el plazo
- **WHEN** vence el plazo
- **THEN** el intérprete publica `IncidentEscalated` con `no_valid_orders` para cada aviso
- **AND** `parked_notices` los borra al consumir el escalado

#### Scenario: Pedido proyectado antes del plazo

- **GIVEN** un aviso aparcado cuyo `OrderRegistered` el intérprete consumió antes de aparcarlo, y los proyectores reanudados antes del plazo
- **WHEN** vence su plazo y la vista ya tiene el pedido
- **THEN** publica `ShipmentDelayed` y no escala

#### Scenario: Cumplimiento de H17

- **GIVEN** la ejecución completa de E17
- **WHEN** se evalúa la hipótesis
- **THEN** H17 se cumple si en (a) todos se reanudan por `OrderRegistered`, en (b) todos escalan con `no_valid_orders`, en (c) todos se reanudan por el plazo sin escalar, en ninguna variante queda un aviso aparcado, el lag del intérprete es 0 con los avisos aparcados y el aviso reenviado se descarta sin un segundo aparcamiento

#### Scenario: Aviso aparcado y repetido

- **GIVEN** un aviso aparcado
- **WHEN** el transportista lo reenvía mientras está aparcado
- **THEN** se descarta con `duplicate_notice` y no hay un segundo aparcamiento

### Requirement: E18 Mensajes veneno en mitad de la partición (H18)

El sistema SHALL cumplir la hipótesis H18: un mensaje inválido en mitad de una partición no detiene a ningún grupo de consumidores ni se descarta en silencio. El escenario son 100 incidencias con una inyección de cada tipo de veneno, repartidas en distintas posiciones y particiones. Los consumidores de un veneno son los que atienden su `event_type`, como fija la spec `resilience` («Dead letter queue»): todos para un mensaje sin `event_type` legible; `event_log` y los consumidores con handler para el tipo, en los demás casos. Un `event_type` desconocido con la estructura común válida no es un veneno para nadie (tolerant reader): se comprueba que ningún consumidor lo lleva a la dead letter y que `event_log` lo guarda. El informe MUST dar, por tipo de veneno y por consumidor: si llegó a `<componente>.dead_letters` y a `eda.dlq`, el motivo, el lag de cada grupo al final y cuántos reintentos hizo antes de saltarlo. Cuando el veneno lleva un `correlation_id` identificable, su incidencia SHALL escalarse; en los demás casos queda solo en la dead letter.

#### Scenario: JSON inválido

- **GIVEN** un mensaje que no es JSON en la posición 50 de una partición
- **WHEN** lo recibe cada grupo
- **THEN** cada consumidor lo guarda en su `dead_letters` con el motivo `invalid_schema` y confirma el offset
- **AND** los mensajes siguientes de la partición se procesan

#### Scenario: Evento de un tipo desconocido

- **GIVEN** un evento con la estructura común válida y un `event_type` que no está en el catálogo
- **WHEN** lo reciben los consumidores
- **THEN** ninguno lo lleva a la dead letter queue ni bloquea la partición
- **AND** `event_log` lo guarda en `event_log.events`

#### Scenario: Evento de una versión desconocida

- **GIVEN** un evento de un tipo del catálogo con `event_version` sin upcaster
- **WHEN** lo reciben los consumidores
- **THEN** va a la dead letter queue de cada consumidor que atiende su tipo y no bloquea la partición

#### Scenario: Evento con datos personales

- **GIVEN** un evento cuyo payload incluye datos personales no permitidos
- **WHEN** lo reciben los consumidores que atienden su tipo
- **THEN** va a la dead letter queue de cada uno y no se escribe en las vistas ni en el log

#### Scenario: Payload muy grande

- **GIVEN** un payload de 900 KB, por debajo de `message.max.bytes`
- **WHEN** lo reciben los consumidores
- **THEN** se procesa o se rechaza por schema sin retrasar a las demás incidencias de la partición

#### Scenario: Reconciliación de la DLQ

- **GIVEN** el final de la ejecución
- **WHEN** se compara lo inyectado con lo que hay en `eda.dlq` y en los `dead_letters`
- **THEN** cada veneno aparece exactamente una vez por cada consumidor que atiende su tipo, y en ninguno más
- **AND** la ejecución no tiene ningún otro mensaje en la DLQ

#### Scenario: Cumplimiento de H18

- **GIVEN** la ejecución completa de E18
- **WHEN** se evalúa la hipótesis
- **THEN** H18 se cumple si el lag final de todos los grupos es 0, todo veneno está en la DLQ de cada consumidor que atiende su tipo, el evento de tipo desconocido está en `event_log.events` y las 100 incidencias terminan (I1)

### Requirement: E19 Pérdida del topic (H19)

El sistema SHALL cumplir la hipótesis H19: si se pierde el contenido de `eda.events` y se republica el outbox de todos los componentes en el orden que tenía cada partición (spec event-backbone, «Pérdida del topic»), el topic vuelve a tener todos los eventos en ese orden, también entre componentes, no se repite ningún efecto, no aparece ningún evento nuevo, el estado de la saga y las vistas no cambian, el sistema sigue atendiendo avisos nuevos y se mantienen I1 a I5. El experimento SHALL ejecutarse en modo en proceso con 200 incidencias cerradas, un 30 % de fallos en `reserve_stock` (revisiones y compensaciones) y el sistema parado; después se pierde el topic, se republica el outbox completo de todos los componentes (`outbox.republish_in_order`), se arranca hasta que el lag sea 0 y llegan 20 avisos nuevos que deben cerrarse. Dos variantes: `truncated`, el topic se vacía con `delete_records` y sus offsets siguen creciendo; y `recreated`, el topic se borra y se crea de nuevo y sus offsets vuelven a 0. En `recreated`, el procedimiento de recuperación lleva a 0 los offsets de los grupos de Kafka y pone a -1 la posición que el lado de lectura guarda en PostgreSQL: sin ello, los consumidores quedarían más allá del final del topic nuevo y el lado de lectura descartaría los mensajes nuevos por tener un offset no posterior al guardado. Lo ya aplicado lo absorben el inbox, `views.applied_events` y el `ON CONFLICT` de `event_log`. I5 no compara la posición en Kafka (`kafka_partition`, `kafka_offset`) que guardan algunas vistas: cambia al republicar, y las vistas mantenidas conservan la original porque `views.applied_events` descarta el evento repetido. El informe MUST dar, por variante: incidencias cerradas antes y después de los avisos nuevos; filas republicadas por componente; mensajes publicados desde la pérdida; filas de outbox cuyo offset no crece en el orden del outbox dentro de su partición; filas que cambiaron de partición o cuyo offset nuevo no respeta, entre todos los outbox, el orden que tenían en el topic perdido; los cambios entre la foto de antes de la pérdida y la de después de la republicación (ejecuciones de los servicios, filas de outbox, inbox, `command_outbox`, reservas, reembolsos y log, huella de `orchestrator.plan_steps` y de cada vista); e I1 a I5 al terminar.

#### Scenario: Topic vaciado

- **GIVEN** la variante `truncated` con 200 incidencias cerradas
- **WHEN** se republica el outbox de todos los componentes
- **THEN** se publican tantos mensajes como filas tienen los outbox, en el orden que tenía cada partición, también entre componentes
- **AND** no cambia ninguna ejecución de servicio, fila, huella de `plan_steps` ni vista

#### Scenario: Topic recreado

- **GIVEN** la variante `recreated`, con los offsets de los grupos en 0 y la posición del lado de lectura en -1
- **WHEN** se republica el outbox de todos los componentes
- **THEN** el topic nuevo tiene tantos mensajes como filas tienen los outbox, en el orden que tenía cada partición, también entre componentes
- **AND** no cambia ninguna ejecución de servicio, fila, huella de `plan_steps` ni vista

#### Scenario: Avisos nuevos tras la recuperación

- **GIVEN** el topic recuperado en cualquiera de las dos variantes
- **WHEN** llegan 20 avisos nuevos
- **THEN** sus incidencias se cierran y las vistas reconstruidas desde el topic coinciden con las mantenidas (I5)

#### Scenario: Cumplimiento de H19

- **GIVEN** la ejecución completa de E19
- **WHEN** se evalúa la hipótesis
- **THEN** H19 se cumple si en las dos variantes se publicaron tantos mensajes como filas de outbox, en el orden de cada partición también entre componentes, la republicación no cambió nada, todas las incidencias (también las nuevas) se cerraron e I1 a I5 no tienen violaciones

### Requirement: E20 Dependencias de V2 (H20)

El sistema SHALL cumplir la hipótesis H20: cuando el servidor MCP de correo (spec mcp-mail) o el agente A2A del transportista (spec a2a-carrier) fallan, tardan o no superan los guardrails, cada fallo termina en el resultado que dicen sus specs, el planificador sigue planificando sin las opciones del transportista, ningún correo se envía dos veces, ninguna dirección de correo llega al log, toda incidencia se cierra y se mantienen I1 a I5. El experimento SHALL ejecutarse en modo en proceso con `POC_VERSION=2`, 50 incidencias, el servidor de correo en proceso, el agente del transportista como app A2A en proceso, `A2A_TIMEOUT_S` de 1 s y seis variantes con un solo fallo cada una (spec synthetic-data, «Fallos de las dependencias de V2»): `baseline`, sin fallos; `mail_down`, un 50 % de las llamadas a `send_email` terminan en un error de la herramienta; `mail_invalid`, un 30 % devuelven una salida sin `message_id` o con la dirección del destinatario; `carrier_slow`, un 50 % de las consultas tardan 2 s; `carrier_401`, un 50 % llevan un token que el agente no acepta y responde 401; y `carrier_bad_card`, un 50 % encuentran una Agent Card con otra skill. El 401 es un `outcome` `failed` (spec a2a-carrier, «Consulta del planificador»). El informe MUST dar, por variante: incidencias cerradas, resueltas y escaladas por motivo; fallos inyectados; `CarrierConsulted` por `outcome`; consultas cuyo `outcome` no es el esperado (`timeout` en `carrier_slow`, `failed` en `carrier_401`, `rejected_by_guardrail` en `carrier_bad_card` y `answered` sin fallo inyectado); latencia máxima de la consulta; tareas que recibió el agente en las consultas con fallo; incidencias cuyo primer plan difiere del de `baseline` (con o sin `PlanCreated`); `StepFailed` de `notify_customer` por `error`; `CustomerNotified`; máximo de ejecuciones del correo por `idempotency_key`; eventos con algo que parece una dirección de correo; latencia (media, mediana y p95); e I1 a I5. Cada variante con fallos MUST provocar su efecto (al menos un fallo inyectado; en `mail_down`, además, al menos un reintento de `send_email`; y en `mail_invalid`, un `StepFailed` con `mcp_output_invalid` por cada salida inválida); si no, no cuenta como cumplida.

#### Scenario: Servidor de correo caído

- **GIVEN** la variante `mail_down`
- **WHEN** `send_email` falla en las llamadas elegidas
- **THEN** el ejecutor reintenta con espera exponencial y, si agota `STEP_MAX_ATTEMPTS`, publica `StepFailed` con `error` `mail_unavailable` y `retryable` verdadero
- **AND** ninguna `idempotency_key` tiene más de una ejecución del correo

#### Scenario: Salidas inválidas del servidor de correo

- **GIVEN** la variante `mail_invalid`
- **WHEN** `send_email` devuelve una salida sin `message_id` o con la dirección del destinatario
- **THEN** cada salida inválida termina en un `StepFailed` no reintentable con `error` `mcp_output_invalid`
- **AND** ningún evento del log contiene una dirección de correo

#### Scenario: Agente del transportista inestable

- **GIVEN** la variante `carrier_slow`, `carrier_401` o `carrier_bad_card`
- **WHEN** el planificador consulta al agente en las incidencias elegidas
- **THEN** `CarrierConsulted` tiene el `outcome` del fallo inyectado en todas ellas y `answered` en las demás, con una latencia de como mucho `A2A_TIMEOUT_S` más 1,5 s
- **AND** en `carrier_401` y `carrier_bad_card` el agente no recibe ninguna tarea de esas consultas
- **AND** cada incidencia tiene el mismo primer plan que en `baseline`

#### Scenario: Cumplimiento de H20

- **GIVEN** la ejecución completa de E20
- **WHEN** se evalúa la hipótesis
- **THEN** H20 se cumple si en todas las variantes se cerraron todas las incidencias, todo `outcome` es el esperado, la latencia de la consulta no superó su cota, ninguna consulta con fallo de autenticación o de Agent Card llegó al agente, los planes coinciden con `baseline`, ninguna `idempotency_key` tiene más de un correo, ningún evento contiene una dirección de correo, I1 a I5 no tienen violaciones y cada variante con fallos provocó su efecto

### Requirement: E21 Reproducción total del log (H21)

El sistema SHALL cumplir la hipótesis H21: reproducir el log completo ante todos los consumidores y los ejecutores no produce ningún efecto nuevo, ningún evento nuevo y ningún cambio de estado. El escenario son 50 incidencias cerradas con un 30 % de fallos en `reserve_stock` (para que haya revisiones y compensaciones); con el sistema parado se llevan los offsets de todos los grupos al principio de `eda.events` y se arranca de nuevo hasta que el lag sea 0. Como control, una variante `no_effect_keys` reentrega los `ExecuteStep` antiguos a los ejecutores sin inbox de comandos ni `effect_keys` (el modo de E1) y MUST producir efectos repetidos, para demostrar que el experimento detecta la ausencia de idempotencia. Desactivar el inbox de un solo consumidor no sirve de control: la saga descarta por estado los eventos de incidencias cerradas y los resultados obsoletos, y el intérprete deduplica cada aviso con el inbox de comandos de `OpenIncident`. Por eso la variante `inbox_off` (el orquestador, y después el intérprete) se mantiene como observación, fuera de H21. El informe MUST comparar, antes y después, las ejecuciones del servicio simulado por `idempotency_key`, el número de filas de cada `outbox`, `inbox` y `command_outbox`, la huella de `orchestrator.plan_steps` y de cada vista, y el número de mensajes descartados por el inbox.

#### Scenario: Reproducción con inbox activo

- **GIVEN** 50 incidencias cerradas y los offsets de todos los grupos al principio
- **WHEN** los consumidores reprocesan todo el topic
- **THEN** el número de ejecuciones de cada servicio simulado no cambia
- **AND** ningún `outbox` recibe filas nuevas y `command_outbox` no recibe comandos nuevos

#### Scenario: Estado intacto

- **GIVEN** el mismo escenario
- **WHEN** termina la reproducción
- **THEN** la huella de `orchestrator.plan_steps` y las de las vistas coinciden con las de antes
- **AND** el inbox descartó tantos mensajes como tenía registrados cada consumidor

#### Scenario: Ejecutores activos ante comandos antiguos

- **GIVEN** los ejecutores activos y los `ExecuteStep` antiguos entregados de nuevo con su `command_id` original
- **WHEN** los atienden
- **THEN** devuelven la respuesta guardada y no se crea ninguna reserva ni reembolso nuevo, también tras vencer los plazos de la saga («Duplicado tardío» de la spec `idempotency`)

#### Scenario: Control sin idempotencia en los efectos

- **GIVEN** la variante de control `no_effect_keys`: los ejecutores sin inbox de comandos ni `effect_keys`
- **WHEN** se reentregan los `ExecuteStep` antiguos con su `command_id` original
- **THEN** el experimento detecta efectos repetidos en los servicios simulados

#### Scenario: Reproducción sin el inbox de un consumidor

- **GIVEN** la variante `inbox_off` en el orquestador, y después en el intérprete
- **WHEN** se reproduce el log
- **THEN** el informe anota lo que cambia como observación, fuera de H21

#### Scenario: Cumplimiento de H21

- **GIVEN** la variante con inbox activo
- **WHEN** se evalúa la hipótesis
- **THEN** H21 se cumple si el recuento de efectos, de filas y de huellas no cambia, y la variante de control produce alguna diferencia

### Requirement: E22 Carreras de comandos y ventana efecto–registro (H22)

El sistema SHALL cumplir la hipótesis H22: el mismo comando entregado a la vez desde varios hilos, la misma clave de idempotencia con distinto `command_id` y la caída entre el efecto y el registro de su clave nunca producen un efecto duplicado. El escenario son 50 incidencias con cuatro variantes: (a) el mismo `ExecuteStep` entregado desde 2, 4 y 8 hilos con conexiones propias; (b) el mismo `CreatePlan`, `RevisePlan`, `RegisterCarrierNotice` y `SeedSampleData` concurrentes con el mismo `command_id`; (c) dos `command_id` distintos con la misma `idempotency_key` (por ejemplo, una reinvocación de la saga tras un plazo) para `notify_customer`, `file_claim`, `reserve_stock` y `issue_refund`; (d) `crash_between_effect_and_record` en `notify_customer` y en `issue_refund`, con reintento. El informe MUST dar, por variante, las ejecuciones del servicio simulado por clave, las respuestas distintas recibidas por un mismo `command_id`, los eventos escritos y las veces que una petición esperó a una clave `in_progress`.

#### Scenario: Dos ejecutores a la vez

- **GIVEN** 8 hilos que entregan el mismo `ExecuteStep` con el mismo `command_id`
- **WHEN** las 8 entregas llegan a la vez al inbox del ejecutor
- **THEN** el inbox registra una sola entrega, que crea el único trabajo que registra la clave, y solo ese ejecuta el efecto
- **AND** todos reciben la misma respuesta y se escribe un solo `StepCompleted`

#### Scenario: Comando concurrente con trabajo diferido

- **GIVEN** dos entregas simultáneas del mismo `CreatePlan`
- **WHEN** ambas pasan por la aceptación
- **THEN** `planner.command_jobs` tiene una sola fila y se publica un solo `PlanCreated`

#### Scenario: Misma clave, otro command_id

- **GIVEN** un `ExecuteStep` completado con la clave K y `command_id` C1
- **WHEN** llega otro `ExecuteStep` con la clave K y `command_id` C2
- **THEN** el ejecutor devuelve el resultado guardado sin repetir el efecto

#### Scenario: Caída tras el efecto y antes de registrar la clave

- **GIVEN** un ejecutor que envía el aviso de un pedido y cae antes de registrar la clave
- **WHEN** se reentrega el `ExecuteStep`
- **THEN** el reintento usa la misma clave hacia el servicio de correo
- **AND** el cliente recibe un solo aviso

#### Scenario: Clave en curso vencida

- **GIVEN** una clave `in_progress` cuyo ejecutor cayó y cuyo `locked_until` ya venció
- **WHEN** otro ejecutor recibe el mismo paso
- **THEN** lo toma y lo completa sin duplicar el efecto externo

#### Scenario: Mismo command_id con otro payload

- **GIVEN** un comando registrado con `command_id` C1
- **WHEN** llega otro con C1 y un payload distinto
- **THEN** el informe anota la respuesta como observación; la spec no define este caso, así que queda fuera de H22 y el experimento propone cerrarlo

#### Scenario: Cumplimiento de H22

- **GIVEN** las cuatro variantes
- **WHEN** se evalúa la hipótesis
- **THEN** H22 se cumple si ninguna variante tiene ejecuciones del servicio por encima de una por clave y todas las entregas de un mismo `command_id` reciben la misma respuesta

### Requirement: E23 Duplicados de negocio: avisos, reclamaciones, avisos al cliente y reembolsos (H23)

El sistema SHALL cumplir la hipótesis H23: los duplicados que nacen del negocio (no solo de la entrega) no producen efectos repetidos. El escenario son 50 incidencias con un 50 % de fallos en `reserve_stock` para forzar replanes y cuatro variantes: (a) cada aviso del transportista reenviado de 1 a 5 veces, en orden y a la vez, con el mismo `carrier_notice_id`; (b) replanes que vuelven a pedir `file_claim` y `notify_customer` de pedidos ya atendidos; (c) un reembolso pedido otra vez tras un replan posterior; (d) el mismo pedido en dos incidencias distintas (dos avisos con distinto `carrier_notice_id`). El informe MUST dar, por variante: `incident_id` por `carrier_notice_id`, descartes con motivo `duplicate_notice`, reclamaciones por (incidencia, pedido), avisos por (incidencia, pedido, versión del plan), reembolsos por incidencia y pedido, y reservas activas por (incidencia, pedido). Cada variante MUST provocar el duplicado que mide; una variante que no lo provoca no cuenta como cumplida.

#### Scenario: Aviso repetido

- **GIVEN** un aviso N1 ya interpretado
- **WHEN** el transportista lo reenvía 5 veces, algunas a la vez
- **THEN** hay un solo `incident_id` para N1 y un solo `ShipmentDelayed`
- **AND** cada repetición queda registrada como descarte con `duplicate_notice`

#### Scenario: Reclamación solicitada dos veces

- **GIVEN** una reclamación ya creada para el pedido P1 en la incidencia I1
- **WHEN** un replan vuelve a solicitar `file_claim` para P1
- **THEN** el ejecutor devuelve el `claim_id` existente y no crea otra

#### Scenario: Aviso al cliente tras una revisión

- **GIVEN** un `CustomerNotified` ya publicado para P1
- **WHEN** el replanner mantiene `notify_customer` en la versión nueva
- **THEN** no se envía otro aviso por ese paso

#### Scenario: Reembolso único

- **GIVEN** una incidencia con un reembolso emitido
- **WHEN** un plan posterior vuelve a proponer `issue_refund` para el mismo pedido
- **THEN** no se emite un segundo reembolso (I3) y el total reembolsado no supera el importe del pedido

#### Scenario: Pedido en dos incidencias

- **GIVEN** dos avisos distintos sobre el pedido P1
- **WHEN** se procesan las dos incidencias
- **THEN** cada incidencia mantiene sus propias claves de efecto y el informe separa los efectos por incidencia
- **AND** las comprobaciones de reclamación, reserva y reembolso se hacen por (incidencia, pedido)

#### Scenario: Cumplimiento de H23

- **GIVEN** las cuatro variantes
- **WHEN** se evalúa la hipótesis
- **THEN** H23 se cumple si todos los recuentos del informe son los máximos que fijan las specs `idempotency` e `incident-interpretation`
