# Event backbone Specification

## Purpose

Definir cómo se representan, publican y reparten los eventos, y cómo se invocan los comandos. Es la base de todas las demás capacidades: cada componente publica desde su outbox, Kafka conserva el registro y entrega los mensajes a los consumidores con garantía de al menos una vez.

## Requirements

### Requirement: Estructura común del evento

Todo evento publicado MUST llevar los campos `event_id` (único), `event_type`, `event_version`, `occurred_at`, `correlation_id`, `causation_id`, `producer` y `payload`. Un comando lleva `command_id`, `command_type`, `correlation_id`, `causation_id` y `payload`, y no se publica en Kafka (Requirement "Invocación de comandos"). El sistema SHALL rechazar la publicación de un evento al que le falte alguno.

#### Scenario: Evento completo

- **GIVEN** un productor con un evento con todos los campos de la estructura común
- **WHEN** lo publica
- **THEN** el sistema lo acepta y lo escribe en el outbox del productor

#### Scenario: Evento sin correlation_id

- **GIVEN** un evento al que le falta `correlation_id`
- **WHEN** el productor intenta publicarlo
- **THEN** el sistema lo rechaza con un error que nombra el campo ausente
- **AND** el evento no se escribe en el outbox

### Requirement: Correlación por incidencia

El `correlation_id` de todos los eventos de una incidencia SHALL ser su `incident_id`. El `causation_id` MUST apuntar al `event_id` del evento que originó el nuevo.

#### Scenario: Cadena de causalidad

- **GIVEN** un `ShipmentDelayed` con `event_id` E1 e `incident_id` I1
- **WHEN** la saga invoca `CreatePlan` con `causation_id` E1 y el planificador publica `PlanCreated` como consecuencia
- **THEN** el `PlanCreated` lleva `correlation_id` I1 y como `causation_id` el `command_id` de ese `CreatePlan`, cuyo `causation_id` es E1

### Requirement: Log de eventos inmutable

El log de eventos (el topic de Kafka y la vista `event_log.events`) SHALL ser de solo añadir. El sistema MUST NOT permitir modificar ni borrar un evento ya escrito. Un error en un evento se corrige con un evento posterior.

#### Scenario: Intento de modificar un evento

- **GIVEN** un evento ya escrito en el log
- **WHEN** un componente intenta sobrescribirlo
- **THEN** el sistema rechaza la operación
- **AND** el evento original permanece intacto

### Requirement: Kafka como registro de eventos

Kafka SHALL conservar todos los eventos del topic `eda.events` con retención infinita: es el registro del que se reconstruyen las vistas. En PostgreSQL, el histórico consultable es la vista del log que mantiene el componente de consulta `event_log` (spec `cqrs-views`, "Vista del log de eventos"); ningún componente decide leyéndola.

#### Scenario: Vista reconstruida desde Kafka

- **GIVEN** la vista del log vacía y el topic `eda.events` con todos los eventos
- **WHEN** `event_log` vuelve a consumir el topic desde el principio
- **THEN** la vista vuelve a tener todos los eventos, una vez cada uno

### Requirement: Orden por incidencia

Los eventos de una misma incidencia SHALL entregarse a cada consumidor en el orden en que se escribieron. Para ello el bus MUST particionar por `incident_id`.

#### Scenario: Orden de pasos

- **GIVEN** una incidencia con los eventos `PlanCreated` y `StepCompleted` de uno de sus pasos, escritos en ese orden
- **WHEN** un consumidor los recibe
- **THEN** recibe `PlanCreated` antes que `StepCompleted`

### Requirement: Kafka como bus de entrega

Los eventos SHALL repartirse a los consumidores mediante Apache Kafka; los comandos MUST NOT publicarse en Kafka. El relay del outbox de cada componente MUST publicar en el topic `eda.events` con `incident_id` como clave de partición. Si el contenido del topic se pierde, cada componente SHALL poder republicar su outbox. Al confirmar el broker cada mensaje, el relay MUST guardar en su fila de `outbox` la partición y el offset que le asignó Kafka (`kafka_partition`, `kafka_offset`); si el evento se republica, se guarda la posición de la última publicación.

#### Scenario: Pérdida del topic

- **GIVEN** un log con 200 incidencias y un topic `eda.events` vaciado
- **WHEN** cada componente republica su outbox
- **THEN** el topic recibe de nuevo todos los eventos, en el orden de cada outbox
- **AND** los consumidores no repiten ningún efecto gracias al inbox y a las claves de idempotencia

#### Scenario: Posición del mensaje publicado

- **GIVEN** un evento pendiente en el outbox
- **WHEN** el relay lo publica y el broker confirma la entrega
- **THEN** su fila de `outbox` guarda la partición y el offset del mensaje en `eda.events`
- **AND** tras republicarlo desde su outbox, la fila guarda la partición y el offset de la nueva publicación

### Requirement: Entrega al menos una vez

El bus MUST entregar cada evento a cada consumidor al menos una vez y reentregarlo si el consumidor no confirma el procesamiento. Los consumidores SHALL asumir que pueden recibir duplicados.

#### Scenario: Reentrega tras falta de confirmación

- **GIVEN** un consumidor que recibe un evento y falla antes de confirmar su offset
- **WHEN** se reinicia en el mismo grupo de consumidores
- **THEN** el bus le vuelve a entregar el evento

### Requirement: Invocación de comandos

Los comandos SHALL invocarse de forma síncrona por HTTP (REST): cada sistema expone su command handler como `POST /commands/<Comando>`. La saga MUST guardar cada comando en `orchestrator.command_outbox` en la misma transacción que su transición, y su dispatcher SHALL invocarlo y reintentarlo con backoff, siempre con el mismo `command_id`, hasta recibir una respuesta. Todo comando MUST ser idempotente: el handler registra el `command_id` en su inbox en la misma transacción que su estado y sus eventos, y un comando repetido devuelve la misma respuesta sin repetir estado, eventos ni efectos. La respuesta solo dice si el comando se acepta (202) o se rechaza (422); el resultado le llega a la saga como evento por Kafka. Un comando rechazado SHALL tratarse como un fallo no reintentable.

#### Scenario: Caída antes de invocar

- **GIVEN** una transición de la saga confirmada con un `CreatePlan` en `orchestrator.command_outbox`
- **WHEN** el orquestador cae antes de invocarlo y se reinicia
- **THEN** el dispatcher invoca `CreatePlan` con su `command_id`

#### Scenario: Comando invocado dos veces

- **GIVEN** un `ExecuteStep` de `file_claim` ya aceptado con `command_id` C1
- **WHEN** el dispatcher lo invoca otra vez con C1
- **THEN** el handler devuelve la misma respuesta que la primera vez
- **AND** no escribe estado ni eventos nuevos ni repite la reclamación

#### Scenario: Sistema que no responde

- **GIVEN** el ejecutor de reclamaciones sin responder
- **WHEN** el dispatcher invoca su `ExecuteStep`
- **THEN** reintenta con backoff y el mismo `command_id`
- **AND** la saga sigue procesando los eventos de otras incidencias

#### Scenario: El resultado llega como evento

- **GIVEN** un `ExecuteStep` de `reserve_stock` aceptado con 202
- **WHEN** el dispatcher recibe la respuesta
- **THEN** el paso sigue `requested`
- **AND** pasa a `completed` cuando la saga consume el `StepCompleted` de `inventory`

#### Scenario: Comando rechazado

- **GIVEN** un `ExecuteStep` cuyo payload no cumple el esquema del comando
- **WHEN** el handler lo valida
- **THEN** responde 422 sin escribir eventos
- **AND** la saga marca el paso `failed` con `error` `rejected`

#### Scenario: Solo eventos en Kafka

- **GIVEN** el topic `eda.events` tras procesar una incidencia completa
- **WHEN** se revisan sus mensajes
- **THEN** todos son eventos del catálogo y ninguno es un comando

### Requirement: Propiedad de los datos

Cada componente SHALL escribir únicamente en sus propias tablas, agrupadas en un esquema propio de PostgreSQL (`intake`, `interpreter`, `planner`, `replanner`, `orchestrator`, `executor_<acción>`, `inventory`, `event_log`, `views`). Los componentes MUST comunicarse solo por eventos en Kafka y por comandos REST: ninguno escribe ni lee las tablas de escritura de otro, ni hay claves foráneas o joins entre tablas de componentes distintos. Lo que un sistema necesita de otro dominio lo consulta en las proyecciones del lado de lectura (spec `cqrs-views`, "Consultas al lado de lectura") o le llega en el comando. La base de datos es una sola para la PoC.

#### Scenario: Dato de otro componente

- **GIVEN** un planificador que necesita el importe del pedido P-1002 para validar un reembolso
- **WHEN** valida el plan
- **THEN** lee el importe de la proyección `view_orders`, alimentada por el evento `OrderRegistered` de `inventory`
- **AND** no consulta ninguna tabla de `inventory`

#### Scenario: Escritura fuera de su esquema

- **GIVEN** el código de cualquier componente
- **WHEN** se revisan las sentencias SQL que ejecuta
- **THEN** solo escriben en tablas de su propio esquema

### Requirement: Publicación atómica con el estado (Outbox)

Cuando un componente cambia su estado y publica un evento, ambas operaciones MUST ocurrir de forma atómica mediante su propio outbox transaccional: el cambio de estado y la fila del evento en `<componente>.outbox` se escriben en la misma transacción de PostgreSQL, y el relay de ese componente publica su outbox en Kafka. El sistema MUST NOT publicar un evento de un cambio que no se llegó a confirmar, ni confirmar un cambio sin publicar su evento.

#### Scenario: Fallo entre el cambio y la publicación

- **GIVEN** un componente que ha guardado su cambio de estado y su evento en su outbox
- **WHEN** el proceso cae antes de enviar el evento al bus
- **THEN** al reiniciar, el relay del componente publica en Kafka el evento pendiente
- **AND** el evento aparece una sola vez en la vista del log de `event_log`

### Requirement: Versionado de eventos

Cada tipo de evento SHALL tener un `event_version`. Un cambio incompatible de esquema MUST incrementar la versión. Los consumidores MUST aceptar las versiones anteriores mediante un conversor al leer, sin reescribir el log.

#### Scenario: Evento de versión anterior

- **GIVEN** un evento `StepCompleted` de la versión 1 en el log
- **AND** un proyector que espera la versión 2 (la del catálogo)
- **WHEN** el proyector lee el evento
- **THEN** lo convierte a la versión 2 al leerlo
- **AND** el evento del log no cambia

### Requirement: Sin datos personales en el log

Los eventos MUST NOT contener datos personales en claro (nombre, correo, dirección). Contendrán referencias (`order_id`, `customer_ref`) que el ejecutor resuelve en el momento de actuar.

#### Scenario: Evento de aviso al cliente

- **GIVEN** un `ExecuteStep` con la acción de avisar al cliente
- **WHEN** se escribe en el log
- **THEN** el payload contiene `order_id` y no el correo del cliente
