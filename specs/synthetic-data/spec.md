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

El sistema SHALL permitir inyectar fallos configurables: duplicar eventos y comandos (de 1 a N veces), retrasar la entrega de los eventos y el resultado de un tipo de paso, hacer fallar un tipo de paso con una probabilidad dada, caer un consumidor o un ejecutor en un instante dado, dejar un ejecutor caído o colgado, congelar un proceso, parar los proyectores, inyectar mensajes veneno, reiniciar los offsets de un grupo, entregar un comando de forma concurrente, caer entre el efecto y el registro de su clave y dejar sin stock un almacén.

#### Scenario: Duplicación de eventos

- **GIVEN** la inyección configurada para entregar cada evento 3 veces
- **WHEN** se ejecuta una incidencia
- **THEN** cada consumidor recibe cada evento 3 veces

#### Scenario: Combinación de fallos

- **GIVEN** una configuración con duplicados, fallos en `reserve_stock` y resultados retrasados
- **WHEN** se ejecuta dos veces con la misma semilla
- **THEN** los mismos pasos fallan y se retrasan en las dos ejecuciones

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

La inyección SHALL permitir, con el sistema parado, perder el contenido de `eda.events` de dos formas: vaciarlo (`delete_records` hasta la marca de agua alta de cada partición, los offsets siguen creciendo) o borrarlo y crearlo de nuevo con las mismas particiones (los offsets vuelven a 0). Al recrearlo, el arnés crea de nuevo los productores de los relays y los consumidores del lado de lectura, lleva a 0 los offsets de los grupos y pone a -1 la posición guardada del lado de lectura. Después, cada componente con outbox lo marca entero como pendiente (`ComponentRelay.replay_from`).

#### Scenario: Topic vaciado con offsets que siguen

- **GIVEN** un topic con mensajes y el sistema parado
- **WHEN** se vacía
- **THEN** la marca de agua baja de cada partición sube hasta la alta y los mensajes republicados reciben offsets posteriores

#### Scenario: Topic recreado desde 0

- **GIVEN** un topic con mensajes y el sistema parado
- **WHEN** se borra y se crea de nuevo
- **THEN** tiene las mismas particiones, sin mensajes, y el primer mensaje republicado de cada partición recibe el offset 0

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
