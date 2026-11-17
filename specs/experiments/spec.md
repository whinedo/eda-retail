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

El experimento E4 SHALL borrar cada vista y reconstruirla volviendo a consumir el topic `eda.events` desde el principio. Hipótesis H4: la huella de cada vista reconstruida coincide con la original, y el tiempo de reconstrucción se registra.

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

El harness SHALL ofrecer un modo multiproceso (`--mode multiprocess`) que arranque cada componente como proceso propio contra PostgreSQL y Kafka reales (los mismos procesos que `scripts/run-agents.sh`), con una base de datos y un topic propios por ejecución. Los experimentos E12 y E16 MUST usar ese modo; el modo en proceso actual sigue siendo el de E1 a E7 y el de los demás experimentos que no exijan procesos aparte.

#### Scenario: Aislamiento de la ejecución

- **GIVEN** una ejecución multiproceso
- **WHEN** termina o se aborta
- **THEN** sus procesos están parados y su base de datos y su topic se han borrado

### Requirement: E11 Entrega retrasada y planes revisados (H11)

El sistema SHALL cumplir la hipótesis H11: con revisiones forzadas del plan y resultados de pasos que llegan tarde, ningún resultado obsoleto cambia el estado de la saga, salvo la excepción del supuesto A4, y se mantienen los invariantes I1 a I5. El escenario son 50 incidencias con un 50 % de fallos en `reserve_stock` (`fail_probability`), planes con pasos en paralelo, `step_delay_seconds` en `reserve_stock`, `reissue_shipment` y `file_claim` para que haya resultados de una `plan_version` anterior después de un `PlanRevised`, y un plazo de `ExecuteStep` más corto que parte de esos retrasos. El orquestador descarta un resultado obsoleto sin registrarlo en ningún sitio, así que el experimento los cuenta desde el log: es obsoleto el `StepCompleted` o `StepFailed` cuya `plan_version` es anterior a la vigente cuando el orquestador lo consume, y el de un paso cuyo plazo ya venció con el paso pedido (el plazo no deja evento: lo anota el harness al tratarlo). Un obsoleto cambia el estado de la saga si el orquestador emite algún evento con él como causa. La excepción A4: el `StepCompleted` tardío de un `reserve_stock` cancelado se marca completado y pide su `release_stock`, para no dejar la reserva sin liberar; esa compensación MUST pedirse una sola vez. El informe MUST dar: `PlanRevised` por incidencia, resultados obsoletos por versión y tras el plazo, los que cambiaron el estado de la saga (fuera de A4) y las compensaciones A4, compensaciones pedidas y completadas, y el número de incidencias escaladas por motivo.

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

El sistema SHALL cumplir la hipótesis H12, que tiene dos partes: H12a (vivacidad) toda incidencia que usa el ejecutor afectado termina escalada o resuelta dentro de `SAGA_STEP_TIMEOUT_S` más un margen, y las que no lo usan terminan como siempre; H12b (aislamiento) la latencia p95 de las incidencias que no usan el ejecutor afectado no supera el doble de la de una ejecución sin fallo. El experimento SHALL ejecutarse en modo en proceso (`InProcessCommands` simula el ejecutor caído o colgado y el dispatcher va en su propio hilo, como en el orquestador, para que un ejecutor colgado lo bloquee a él y no al resto del arnés) con `file_claim` en `down`, en `hang` y en `hang` con recuperación al cabo de un intervalo, con 50 incidencias de las que aproximadamente la mitad llevan `file_claim`. La parte H12b se declara exploratoria: se parte de la hipótesis de que el dispatcher, que invoca un comando por turno (`LIMIT 1`), sufre bloqueo de cabeza de línea con el modo `hang` (cada intento ocupa `COMMAND_HTTP_TIMEOUT_S`); un resultado negativo de H12b es un hallazgo y no un fallo del experimento, y el informe lo MUST decir.

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
