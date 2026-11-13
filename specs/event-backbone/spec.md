# Event backbone Specification

## Purpose

Definir cómo se representan, almacenan y reparten los eventos. Es la base de todas las demás capacidades: el log de eventos es la fuente de verdad y el bus entrega los eventos a los consumidores con garantía de al menos una vez.

## Requirements

### Requirement: Estructura común del evento

Todo evento publicado MUST llevar los campos `event_id` (único), `event_type`, `event_version`, `occurred_at`, `correlation_id`, `causation_id`, `producer` y `payload`. El sistema SHALL rechazar la publicación de un evento al que le falte alguno.

#### Scenario: Evento completo

- **GIVEN** un productor con un evento con todos los campos de la estructura común
- **WHEN** lo publica
- **THEN** el sistema lo acepta y lo añade al log

#### Scenario: Evento sin correlation_id

- **GIVEN** un evento al que le falta `correlation_id`
- **WHEN** el productor intenta publicarlo
- **THEN** el sistema lo rechaza con un error que nombra el campo ausente
- **AND** el evento no aparece en el log

### Requirement: Correlación por incidencia

El `correlation_id` de todos los eventos de una incidencia SHALL ser su `incident_id`. El `causation_id` MUST apuntar al `event_id` del evento que originó el nuevo.

#### Scenario: Cadena de causalidad

- **GIVEN** un `ShipmentDelayed` con `event_id` E1 e `incident_id` I1
- **WHEN** el planificador publica `PlanCreated` como consecuencia
- **THEN** el `PlanCreated` lleva `correlation_id` I1 y `causation_id` E1

### Requirement: Log de eventos inmutable

El log de eventos SHALL ser de solo añadir. El sistema MUST NOT permitir modificar ni borrar un evento ya escrito. Un error en un evento se corrige con un evento posterior.

#### Scenario: Intento de modificar un evento

- **GIVEN** un evento ya escrito en el log
- **WHEN** un componente intenta sobrescribirlo
- **THEN** el sistema rechaza la operación
- **AND** el evento original permanece intacto

### Requirement: Log en PostgreSQL

El log de eventos SHALL residir en una tabla `events` de PostgreSQL. La base de datos MUST rechazar `UPDATE`, `DELETE` y `TRUNCATE` sobre esa tabla. Cada evento SHALL recibir un número de secuencia global creciente (`seq`).

#### Scenario: Intento de borrar un evento

- **GIVEN** un evento escrito en la tabla `events`
- **WHEN** cualquier sesión ejecuta un `DELETE` o un `UPDATE` sobre él
- **THEN** la base de datos rechaza la sentencia con un error
- **AND** el evento permanece intacto

### Requirement: Orden por incidencia

Los eventos de una misma incidencia SHALL entregarse a cada consumidor en el orden en que se escribieron. Para ello el bus MUST particionar por `incident_id`.

#### Scenario: Orden de pasos

- **GIVEN** una incidencia con los eventos `PlanCreated` y `StepRequested` escritos en ese orden
- **WHEN** un consumidor los recibe
- **THEN** recibe `PlanCreated` antes que `StepRequested`

### Requirement: Kafka como bus de entrega

Los eventos SHALL repartirse a los consumidores mediante Apache Kafka. El relay del outbox MUST publicar en el topic `eda.events` con `incident_id` como clave de partición. Kafka no es la fuente de verdad: si el contenido de un topic se pierde, el relay SHALL poder republicar desde el log de PostgreSQL.

#### Scenario: Pérdida del topic

- **GIVEN** un log con 200 incidencias y un topic `eda.events` vaciado
- **WHEN** se ejecuta la republicación desde el log
- **THEN** el topic recibe de nuevo todos los eventos en su orden de `seq`
- **AND** los consumidores no repiten ningún efecto gracias al inbox y a las claves de idempotencia

### Requirement: Entrega al menos una vez

El bus MUST entregar cada evento a cada consumidor al menos una vez y reentregarlo si el consumidor no confirma el procesamiento. Los consumidores SHALL asumir que pueden recibir duplicados.

#### Scenario: Reentrega tras falta de confirmación

- **GIVEN** un consumidor que recibe un evento y falla antes de confirmarlo
- **WHEN** vence el tiempo de confirmación
- **THEN** el bus reentrega el evento al consumidor

### Requirement: Publicación atómica con el estado (Outbox)

Cuando un componente cambia su estado y publica un evento, ambas operaciones MUST ocurrir de forma atómica mediante un outbox transaccional: el cambio de estado, la fila de `events` y la fila de `outbox` se escriben en la misma transacción de PostgreSQL. El sistema MUST NOT publicar un evento de un cambio que no se llegó a confirmar, ni confirmar un cambio sin publicar su evento.

#### Scenario: Fallo entre el cambio y la publicación

- **GIVEN** un componente que ha guardado su cambio de estado y su evento en el outbox
- **WHEN** el proceso cae antes de enviar el evento al bus
- **THEN** al reiniciar, el relay del outbox publica en Kafka el evento pendiente
- **AND** el evento aparece una sola vez en el log

### Requirement: Versionado de eventos

Cada tipo de evento SHALL tener un `event_version`. Un cambio incompatible de esquema MUST incrementar la versión. Los consumidores MUST aceptar las versiones anteriores mediante un conversor al leer, sin reescribir el log.

#### Scenario: Evento de versión anterior

- **GIVEN** un evento `ShipmentDelayed` de la versión 1 en el log
- **AND** un proyector que espera la versión 2
- **WHEN** el proyector lee el evento
- **THEN** lo convierte a la versión 2 al leerlo
- **AND** el evento del log no cambia

### Requirement: Sin datos personales en el log

Los eventos MUST NOT contener datos personales en claro (nombre, correo, dirección). Contendrán referencias (`order_id`, `customer_ref`) que el ejecutor resuelve en el momento de actuar.

#### Scenario: Evento de aviso al cliente

- **GIVEN** un `StepRequested` con la acción de avisar al cliente
- **WHEN** se escribe en el log
- **THEN** el payload contiene `order_id` y no el correo del cliente
