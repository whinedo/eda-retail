# Synthetic data Specification

## Purpose

Disponer de avisos de transportistas y de fallos inyectables que sean repetibles, para que los experimentos se puedan ejecutar y comparar sin datos reales.

## Requirements

### Requirement: Generador de avisos

El generador SHALL producir avisos con las variantes `delay`, `loss`, `damage`, `wrong_address`, `strike` y `unknown`, en texto libre y en formato de API, y con una etiqueta de la `cause` y del plan esperado.

#### Scenario: Conjunto de evaluación

- **GIVEN** una semilla y un tamaño de 50 avisos
- **WHEN** se ejecuta el generador
- **THEN** produce 50 avisos con su causa y su plan esperado
- **AND** incluye al menos cinco de cada variante

### Requirement: Repetibilidad

Con la misma semilla, el generador MUST producir exactamente el mismo conjunto.

#### Scenario: Dos ejecuciones con la misma semilla

- **GIVEN** la semilla 42
- **WHEN** se ejecuta el generador dos veces
- **THEN** los dos conjuntos son idénticos

### Requirement: Datos simulados de negocio

El sistema SHALL simular pedidos, existencias por almacén, transportistas, correo y pagos, sin conexión con sistemas reales. Los pedidos y el stock de ejemplo son de `inventory`, que los carga con el comando `SeedSampleData` y publica un `OrderRegistered` por cada pedido nuevo.

#### Scenario: Datos de ejemplo por comando

- **GIVEN** `inventory` sin el pedido P-1001
- **WHEN** atiende un `SeedSampleData` que lo incluye
- **THEN** escribe el pedido en `inventory.orders` y `OrderRegistered` de P-1001 en `inventory.outbox`
- **AND** otro `SeedSampleData` con los mismos datos no escribe ningún `OrderRegistered`

#### Scenario: Reserva de stock simulada

- **GIVEN** un almacén simulado con 3 unidades del artículo A1
- **WHEN** se reservan 4 unidades
- **THEN** la reserva falla con `out_of_stock`

### Requirement: Inyección de fallos

El sistema SHALL permitir inyectar fallos configurables: duplicar eventos y comandos (de 1 a N veces), retrasar la entrega de los eventos y el resultado de un tipo de paso, hacer fallar un tipo de paso con una probabilidad dada, caer un consumidor o un ejecutor en un instante dado, caer un componente en un punto de su transacción, dejar un ejecutor caído o colgado, congelar un proceso, parar los proyectores, inyectar mensajes veneno, reiniciar los offsets de un grupo, entregar un comando de forma concurrente, caer entre el efecto y el registro de su clave y dejar sin stock un almacén.

#### Scenario: Duplicación de eventos

- **GIVEN** la inyección configurada para entregar cada evento 3 veces
- **WHEN** se ejecuta una incidencia
- **THEN** cada consumidor recibe cada evento 3 veces

#### Scenario: Combinación de fallos

- **GIVEN** una configuración con duplicados, fallos en `reserve_stock` y resultados retrasados
- **WHEN** se ejecuta dos veces con la misma semilla
- **THEN** los mismos pasos fallan y se retrasan en las dos ejecuciones

### Requirement: Caída en un punto de la transacción

La inyección SHALL permitir que un componente caiga una sola vez en un punto dado de su transacción, al atender su unidad K (`crash_at`: {(componente, punto): K}), y que el harness lo reinicie con el mismo plan sin que vuelva a caer. La unidad es un mensaje de Kafka, un comando REST, un trabajo o una fila de outbox, según el punto. Los puntos son:

- `before_tx`: la llamada al LLM (`prepare`) ya se hizo y la transacción aún no se abrió, en el intérprete al atender un `CarrierNoticeReceived` y en el worker de trabajos del planificador y del replanificador. No se escribe nada: el consumidor vuelve a recibir el mensaje (su offset no se confirmó), el trabajo se retoma cuando vence su lease, y el LLM se vuelve a llamar. En los ejecutores, `prepare` es el efecto externo: esa caída es `crash_between_effect_and_record` (E22), no un punto de esta inyección.
- `after_commit_before_offset`: un consumidor de Kafka (intérprete, orquestador, `event_log` o un proyector) confirmó la transacción de PostgreSQL del mensaje y cae antes de confirmar el offset.
- `after_commit_before_response`: un command handler REST (intake, inventory, planificador, replanificador o un ejecutor) confirmó la transacción del comando (en un comando `deferred`, la que acepta el trabajo) y cae antes de responder.
- `relay_after_publish_before_mark`: el relay de un componente publicó en Kafka el lote que contiene la fila K, con su `flush` completo, y cae antes de marcar las filas de `outbox` como publicadas.

`crash_after` se mantiene para E2. Un punto que no aplica al componente (p. ej. `before_tx` en el orquestador o `relay_after_publish_before_mark` en `event_log`, que no tiene outbox) MUST rechazarse al crear el plan. Cada caída MUST quedar registrada con su componente, punto, unidad y hora, y también la hora a la que el componente reiniciado atiende su unidad siguiente.

#### Scenario: Caída de un consumidor antes de la transacción

- **GIVEN** `crash_at` con (`interpreter`, `before_tx`) y K = 3
- **WHEN** el intérprete atiende su tercer `CarrierNoticeReceived`
- **THEN** llama al LLM, cae sin abrir la transacción y sin confirmar el offset, y el harness lo reinicia
- **AND** el mensaje se vuelve a entregar, el LLM se vuelve a llamar y la incidencia se abre una sola vez

#### Scenario: Caída de un worker de trabajos antes de la transacción

- **GIVEN** `crash_at` con (`replanner`, `before_tx`) y K = 1
- **WHEN** el worker del replanificador toma su primer trabajo y el LLM responde
- **THEN** el worker cae sin escribir nada y el trabajo sigue `running` hasta que vence su lease
- **AND** otro worker lo retoma, vuelve a llamar al LLM y escribe un solo `PlanRevised`

#### Scenario: Caída tras el commit, antes del offset

- **GIVEN** `crash_at` con (`event_log`, `after_commit_before_offset`) y K = 5
- **WHEN** `event_log` guarda su quinto evento y confirma la transacción
- **THEN** cae sin confirmar el offset y, al reiniciarse, Kafka le vuelve a entregar ese evento
- **AND** `event_log.events` tiene una sola fila con ese `event_id`

#### Scenario: Caída tras el commit, antes de la respuesta

- **GIVEN** `crash_at` con (`planner`, `after_commit_before_response`) y K = 2
- **WHEN** el planificador acepta su segundo `CreatePlan` y confirma la transacción
- **THEN** no responde a la saga, que lo reinvoca con el mismo `command_id`
- **AND** el inbox devuelve la respuesta guardada y hay un solo trabajo para ese comando

#### Scenario: Caída del relay tras publicar

- **GIVEN** `crash_at` con (`orchestrator`, `relay_after_publish_before_mark`) y K = 4
- **WHEN** el relay del orquestador publica el lote con su cuarta fila de outbox
- **THEN** cae sin marcar las filas y el relay reiniciado las vuelve a publicar en orden, con el mismo `event_id`
- **AND** al terminar no queda ninguna fila del outbox pendiente (I4)

#### Scenario: Punto que no aplica

- **GIVEN** `crash_at` con (`orchestrator`, `before_tx`)
- **WHEN** se crea el plan de fallos
- **THEN** se rechaza con un error que dice que `before_tx` no aplica al orquestador

### Requirement: Inyección de entrega retrasada por acción

La inyección de fallos SHALL permitir retrasar el resultado de un tipo de paso (`step_delay_seconds` por acción, con un jitter determinista dada la semilla), de modo que el `StepCompleted` o `StepFailed` de un paso llegue después de los de otros pasos de la misma incidencia y de la revisión del plan que provoquen.

#### Scenario: Resultado tardío determinista

- **GIVEN** `step_delay_seconds` de 4 s para `file_claim` y la semilla 42
- **WHEN** se ejecuta dos veces el mismo conjunto
- **THEN** los mismos pasos `file_claim` retrasan su resultado la misma cantidad en las dos ejecuciones

### Requirement: Ejecutor caído o colgado

La inyección SHALL permitir dejar un ejecutor en uno de dos modos durante un intervalo o para siempre: `down` (rechaza la conexión) y `hang` (acepta la conexión y no responde nunca). En el modo en proceso se simula haciendo que `InProcessCommands` lance `CommandUnavailable` o espere el timeout; en el modo multiproceso se para el proceso o se interpone un proxy que no contesta.

#### Scenario: Ejecutor colgado

- **GIVEN** el ejecutor de `file_claim` en modo `hang` y `COMMAND_HTTP_TIMEOUT_S` de 5 s
- **WHEN** el dispatcher le invoca un `ExecuteStep`
- **THEN** la petición vence a los 5 s con `CommandUnavailable`
- **AND** el dispatcher reprograma el comando con backoff y el mismo `command_id`

### Requirement: Capacidad limitada de un ejecutor y trabajo lento

La inyección SHALL permitir dar a un ejecutor una capacidad (`executor_capacity`, trabajos abiertos como máximo) y retrasar el `prepare` de los trabajos de un componente (`prepare_delay_seconds`). En el modo en proceso, `InProcessCommands` responde 503 (`CommandUnavailable`) a un `ExecuteStep` nuevo cuando el ejecutor ya tiene tantos trabajos `pending` o `running` como su capacidad, como uvicorn con `EXECUTOR_MAX_CONCURRENCY`; un comando cuyo trabajo ya existe se entrega igual (es una reentrega).

#### Scenario: Ejecutor lleno

- **GIVEN** `executor_capacity` 3 en `executor_file_claim` y 3 trabajos abiertos en su `command_jobs`
- **WHEN** el dispatcher le invoca un `ExecuteStep` nuevo
- **THEN** la invocación falla con `CommandUnavailable` y el informe cuenta un 503 de `executor_file_claim`

#### Scenario: Reentrega con el ejecutor lleno

- **GIVEN** el mismo ejecutor lleno y un `ExecuteStep` cuyo trabajo ya está en su `command_jobs`
- **WHEN** se entrega otra vez con su `command_id`
- **THEN** se entrega al command handler, que lo trata como repetido

### Requirement: Degradación del LLM

La inyección SHALL permitir degradar el LLM de los agentes elegidos (por defecto, `planner` y `replanner`) con tres modos: `invalid` (la respuesta no es JSON o le faltan campos, con una probabilidad), `slow` (cada llamada tarda unos segundos más) y `error` (la llamada lanza una excepción, con una probabilidad). La elección es determinista dada la semilla, el agente, la incidencia y el número de llamada de ese agente en esa incidencia. Los demás agentes reciben la respuesta del cliente sin cambios.

#### Scenario: Elección reproducible

- **GIVEN** dos ejecuciones con la misma semilla y la misma degradación
- **WHEN** el planner llama al LLM por n-ésima vez para la misma incidencia
- **THEN** las dos reciben la misma respuesta degradada o la misma respuesta sin degradar

#### Scenario: Agente no degradado

- **GIVEN** la degradación limitada a `planner` y `replanner`
- **WHEN** el intérprete llama al LLM
- **THEN** recibe la respuesta del cliente sin cambios

### Requirement: Congelación de un proceso

En modo multiproceso, la inyección SHALL permitir congelar un proceso con `SIGSTOP` durante un tiempo y reanudarlo con `SIGCONT`, de modo que un consumidor de Kafka pierda su sesión (`session.timeout.ms` o `max.poll.interval.ms`) y vuelva a vivir después de que otra instancia haya asumido sus particiones.

#### Scenario: Consumidor zombie

- **GIVEN** dos instancias del orquestador en el mismo grupo y una congelada más tiempo que el timeout de sesión
- **WHEN** se reanuda la congelada
- **THEN** el experimento registra que Kafka había reasignado sus particiones antes de que reanudara y qué mensajes procesó después

### Requirement: Caída del broker

La inyección SHALL permitir cortar el broker de Kafka de dos formas, sobre su contenedor de Docker (`KAFKA_CONTAINER`, por defecto `eda-kafka`): pararlo y arrancarlo (`stopped`, las conexiones se rechazan) o congelarlo y descongelarlo (`paused`, las conexiones quedan abiertas sin respuesta). Al terminar el corte, la inyección SHALL esperar a que el broker responda (los metadatos del topic) antes de devolver el control, y MUST devolver el broker a su estado aunque el experimento falle durante el corte.

#### Scenario: Corte del broker

- **GIVEN** el broker en marcha
- **WHEN** se corta en modo `stopped` o `paused`
- **THEN** se ejecuta `docker stop` o `docker pause` sobre su contenedor
- **AND** al terminar, `docker start` o `docker unpause`, y se espera a que el broker responda

#### Scenario: Fallo durante el corte

- **GIVEN** el broker cortado
- **WHEN** el experimento lanza una excepción antes de terminar el corte
- **THEN** el broker vuelve igualmente a su estado y la excepción sigue su curso

### Requirement: Caída de la base de datos

La inyección SHALL permitir cortar PostgreSQL de dos formas, sobre su contenedor de Docker (`POSTGRES_CONTAINER`, por defecto `eda-postgres`), con la misma mecánica que la caída del broker: pararlo y arrancarlo (`stopped`, las conexiones se cortan y las transacciones abiertas se deshacen) o congelarlo y descongelarlo (`paused`, las conexiones quedan abiertas sin respuesta). Con una duración, un temporizador SHALL devolver la base a su estado a su hora aunque quien la usa esté bloqueado. Al terminar el corte, la inyección SHALL esperar a que la base acepte una conexión nueva y responda a `SELECT 1` antes de devolver el control, y MUST devolver la base a su estado aunque el experimento falle durante el corte o falle el propio corte.

#### Scenario: Corte de la base de datos

- **GIVEN** PostgreSQL en marcha
- **WHEN** se corta en modo `stopped` o `paused`
- **THEN** se ejecuta `docker stop` o `docker pause` sobre su contenedor
- **AND** al terminar, `docker start` o `docker unpause`, y se espera a que la base responda

#### Scenario: Fallo durante el corte de la base de datos

- **GIVEN** la base de datos cortada
- **WHEN** el experimento lanza una excepción antes de terminar el corte, o falla el propio `docker stop` o `docker pause`
- **THEN** la base vuelve igualmente a su estado y la excepción sigue su curso

### Requirement: Parada de proyectores

La inyección SHALL permitir parar los proyectores (lado de lectura) durante un intervalo mientras los demás componentes siguen publicando, y reanudarlos después.

#### Scenario: Proyectores parados

- **GIVEN** los proyectores parados durante 20 s
- **WHEN** `inventory` publica `OrderRegistered` en ese intervalo
- **THEN** `views.view_orders` no tiene esos pedidos hasta que se reanudan los proyectores

### Requirement: Mensajes veneno inyectados

La inyección SHALL permitir publicar directamente en `eda.events`, en posiciones elegidas de una partición con carga, mensajes de estos tipos: JSON inválido, evento sin un campo obligatorio de la estructura común, `event_type` desconocido, `event_version` sin upcaster, payload que no cumple el schema del evento, payload con datos personales no permitidos y payload muy grande (por debajo de `message.max.bytes`). Cada mensaje inyectado MUST quedar anotado con su partición y su offset.

#### Scenario: Veneno en mitad de la partición

- **GIVEN** una partición con 100 eventos válidos pendientes
- **WHEN** se inyecta un JSON inválido en la posición 50
- **THEN** el informe guarda su partición y su offset
- **AND** los eventos válidos siguen su orden relativo

### Requirement: Pérdida del topic

La inyección SHALL permitir, con el sistema parado, perder el contenido de `eda.events` de dos formas: vaciarlo (`delete_records` hasta la marca de agua alta de cada partición, los offsets siguen creciendo) o borrarlo y crearlo de nuevo con las mismas particiones (los offsets vuelven a 0). Al recrearlo, el arnés crea de nuevo los productores de los relays y los consumidores del lado de lectura, lleva a 0 los offsets de los grupos y pone a -1 la posición guardada del lado de lectura. Después, el arnés republica el outbox de todos los componentes en el orden que tenía cada partición (`outbox.republish_in_order`, spec event-backbone).

#### Scenario: Topic vaciado con offsets que siguen

- **GIVEN** un topic con mensajes y el sistema parado
- **WHEN** se vacía
- **THEN** la marca de agua baja de cada partición sube hasta la alta y los mensajes republicados reciben offsets posteriores

#### Scenario: Topic recreado desde 0

- **GIVEN** un topic con mensajes y el sistema parado
- **WHEN** se borra y se crea de nuevo
- **THEN** tiene las mismas particiones, sin mensajes, y el primer mensaje republicado de cada partición recibe el offset 0

### Requirement: Fallos de las dependencias de V2

La inyección SHALL permitir hacer inestables las dependencias de la versión 2 (design.md §15). En el servidor MCP de correo, dos modos: `down` (la llamada a `send_email` termina en un error de la herramienta sin guardar el correo) e `invalid` (devuelve una salida sin `message_id` o con la dirección del destinatario, sin guardar el correo). En el agente A2A del transportista, tres: `slow` (tarda más que `A2A_TIMEOUT_S` en completar la tarea), `unauthorized` (la consulta lleva un token que el agente no acepta y responde 401) y `bad_card` (la Agent Card anuncia otra skill). Cada modo tiene una probabilidad. La elección es determinista dada la semilla y el modo: en el correo, la `idempotency_key` y el número de llamada con esa clave; en el transportista, la incidencia. Las llamadas y consultas no elegidas se atienden como sin fallos.

#### Scenario: Elección reproducible de los fallos de V2

- **GIVEN** dos ejecuciones con la misma semilla, el mismo modo y la misma probabilidad
- **WHEN** llega la n-ésima llamada con la misma `idempotency_key`, o la consulta de la misma incidencia
- **THEN** las dos fallan o las dos se atienden sin fallo

#### Scenario: Servidor de correo inestable

- **GIVEN** el servidor de correo con el modo `down` o `invalid` y una llamada elegida
- **WHEN** se invoca `send_email`
- **THEN** responde con un error de la herramienta (`down`) o con una salida sin `message_id` o con la dirección (`invalid`)
- **AND** el buzón no cambia

#### Scenario: Agente del transportista inestable con fallos inyectados

- **GIVEN** el agente del transportista con el modo `slow`, `unauthorized` o `bad_card` y una incidencia elegida
- **WHEN** el planificador lo consulta
- **THEN** la tarea no se completa dentro de `A2A_TIMEOUT_S` (`slow`), la petición recibe un 401 (`unauthorized`) o la Agent Card no anuncia `delivery_options` (`bad_card`)

### Requirement: Reinicio de offsets de un grupo

La inyección SHALL permitir llevar los offsets de uno o todos los grupos de consumidores al principio de `eda.events` con el sistema parado, para reproducir el log completo ante los consumidores y los ejecutores.

#### Scenario: Grupo reiniciado al principio

- **GIVEN** todas las incidencias cerradas y los consumidores parados
- **WHEN** se reinician los offsets de todos los grupos y se arrancan otra vez
- **THEN** cada consumidor vuelve a recibir todos los mensajes del topic desde el offset 0

### Requirement: Entrega concurrente de comandos

La inyección SHALL permitir entregar un mismo comando desde N hilos (o procesos) a la vez, con conexiones propias, para ejercer las carreras del inbox y de `effect_keys`, SHALL permitir entregar a la vez el `ExecuteStep` de una acción con otro `command_id` y la misma `idempotency_key` (como una reinvocación de la saga tras un plazo), y SHALL permitir un punto de caída entre el efecto externo y el registro de su clave (`crash_between_effect_and_record` por acción). Cada entrega anota la respuesta que recibió.

#### Scenario: Mismo comando desde varios hilos

- **GIVEN** la entrega concurrente de `ExecuteStep` con 4 hilos
- **WHEN** la saga invoca un paso
- **THEN** el comando llega 4 veces a la vez con el mismo `command_id`
- **AND** se anotan las 4 respuestas de ese `command_id`

#### Scenario: Reinvocación con otro command_id

- **GIVEN** la reinvocación de `notify_customer`
- **WHEN** la saga invoca un paso `notify_customer` con la clave K
- **THEN** llegan a la vez dos `ExecuteStep` con la clave K y `command_id` distintos
- **AND** el resto de acciones llega una sola vez

#### Scenario: Caída entre efecto y registro

- **GIVEN** `crash_between_effect_and_record` para `notify_customer` en el primer envío
- **WHEN** el ejecutor envía el correo y cae antes de registrar la clave
- **THEN** el experimento registra una ejecución del servicio de correo con la clave K
- **AND** el siguiente intento llega con el mismo `command_id` y la misma clave K

### Requirement: Reenvío de avisos del transportista

La inyección SHALL permitir reenviar un aviso ya registrado de 1 a N veces con el mismo `carrier_notice_id`, en orden o a la vez (cada envío desde su hilo y con su conexión), con un `command_id` nuevo en cada envío, y SHALL decidir con la semilla cuántas veces y de qué forma se reenvía cada aviso.

#### Scenario: Avisos reenviados en orden y a la vez

- **GIVEN** una semilla y 20 avisos
- **WHEN** se genera el plan de reenvíos
- **THEN** cada aviso se reenvía de 1 a 5 veces, unos en orden y otros a la vez
- **AND** la misma semilla da el mismo plan

### Requirement: Pedido compartido entre avisos

El generador SHALL poder dar a una parte de los pedidos un segundo aviso con otro `carrier_notice_id` y las mismas etiquetas (causa, gravedad, retraso y plan esperado), sin repetir pedidos en los datos de ejemplo.

#### Scenario: Segundo aviso de un pedido

- **GIVEN** un conjunto de 12 avisos
- **WHEN** se genera con pedidos compartidos
- **THEN** uno de cada cuatro pedidos tiene un segundo aviso con otro `carrier_notice_id` y las mismas etiquetas
- **AND** los pedidos de los datos de ejemplo no se repiten

### Requirement: Fallos reproducibles

La inyección de fallos MUST ser determinista dada una semilla.

#### Scenario: Misma semilla, mismos fallos

- **GIVEN** una semilla y una probabilidad de fallo del 20 % en `reserve_stock`
- **WHEN** se ejecuta dos veces el mismo conjunto
- **THEN** los mismos pasos fallan en las dos ejecuciones
