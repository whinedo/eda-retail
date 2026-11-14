# CQRS views Specification

## Purpose

Separar quien acepta cambios (command handler, que añade eventos al log) de quien responde consultas (vistas construidas por proyectores). Aplica CQRS (9.4): el log es la fuente de verdad y las vistas se reconstruyen reproduciéndolo.

## Requirements

### Requirement: Escritura solo mediante comandos

Todo cambio de estado SHALL pasar por el command handler, que valida las reglas de negocio contra el estado actual y, si se cumplen, añade eventos al log. Un comando rechazado MUST NOT escribir eventos.

#### Scenario: Comando válido

- **GIVEN** una incidencia abierta
- **WHEN** el command handler recibe el comando de registrar el resultado de un paso
- **THEN** añade los eventos correspondientes al log
- **AND** responde que el comando se aceptó

#### Scenario: Comando sobre una incidencia cerrada

- **GIVEN** una incidencia con `IncidentResolved`
- **WHEN** llega un comando de ejecutar un paso
- **THEN** el command handler lo rechaza con el motivo `incident_closed`
- **AND** no se escribe ningún evento

### Requirement: Catálogo de comandos

Los comandos SHALL ser peticiones explícitas con destinatario y nombre en imperativo, distintas de los eventos (hechos ya ocurridos). El command handler MUST aceptar al menos: `RegisterCarrierNotice`, `OpenIncident`, `CreatePlan`, `RevisePlan`, `RequestStep`, `RecordStepResult`, `CloseIncident` y `EscalateIncident`. Cada comando SHALL llevar un `command_id`, que será el `event_id` del evento que lo originó.

#### Scenario: Comando derivado de un evento

- **GIVEN** un `ShipmentDelayed` con `event_id` E1 consumido por el planificador
- **WHEN** el planificador envía el comando `CreatePlan`
- **THEN** el comando lleva `command_id` E1
- **AND** el evento resultante `PlanCreated` lleva `causation_id` E1

### Requirement: Concurrencia optimista en el lado de escritura

Cada incidencia SHALL tener un número de `version` en el estado de escritura. Todo comando MUST indicar la `expected_version` y el command handler SHALL rechazarlo con el motivo `concurrency_conflict` si no coincide, sin escribir eventos. El componente que lo envió reintentará con el estado actualizado.

#### Scenario: Dos comandos simultáneos sobre la misma incidencia

- **GIVEN** una incidencia en la versión 4 y dos comandos con `expected_version` 4 enviados a la vez
- **WHEN** el command handler los procesa
- **THEN** acepta uno y la versión pasa a 5
- **AND** rechaza el otro con `concurrency_conflict` sin escribir eventos

### Requirement: Separación entre el modelo de escritura y el de lectura

El estado de escritura (`incidents`, `plan_steps`) y las vistas (`view_*`) SHALL ser tablas distintas. Los componentes de lectura MUST NOT consultar las tablas de estado de escritura ni hacer joins con ellas. El estado de escritura y los eventos se escriben en la misma transacción.

#### Scenario: Lectura del panel

- **GIVEN** un panel que muestra incidencias abiertas
- **WHEN** consulta los datos
- **THEN** la consulta solo lee tablas `view_*`

### Requirement: Lectura solo desde las vistas

Las consultas SHALL leer únicamente de las vistas. Los agentes y el panel MUST NOT leer del log ni del estado de escritura para decidir. La consulta nunca escribe.

#### Scenario: Consulta del estado

- **GIVEN** una incidencia en curso
- **WHEN** el panel consulta su estado
- **THEN** la respuesta sale de la vista de estado
- **AND** no se genera ningún evento

### Requirement: Vistas mínimas

El sistema SHALL mantener las vistas `incident_status` (estado y paso actual por incidencia), `cost_per_incident` (tokens y coste de efectos), `audit_trail` (quién decidió qué y con qué versión del plan) y `carrier_dashboard` (retrasos y reclamaciones por transportista).

#### Scenario: Incidencia con plan en curso

- **GIVEN** una incidencia con tres pasos, uno completado
- **WHEN** se consulta `incident_status`
- **THEN** muestra la versión del plan vigente y el estado de cada paso

#### Scenario: El panel cuenta incidencias, retrasos, reclamaciones y reembolsos por transportista

- **GIVEN** incidencias de dos transportistas, con retrasos, pérdidas, reclamaciones y reembolsos
- **WHEN** se consulta `carrier_dashboard`
- **THEN** cada transportista muestra sus incidencias, sus retrasos (causa `delay`), sus reclamaciones y sus reembolsos

#### Scenario: Una incidencia sin aviso del transportista cuenta como transportista unknown

- **GIVEN** una incidencia sin `CarrierNoticeReceived`
- **WHEN** se proyectan sus eventos
- **THEN** cuentan para el transportista `unknown`

### Requirement: Proyectores idempotentes

Los proyectores MUST ser idempotentes: procesar el mismo evento varias veces SHALL dejar la vista en el mismo estado.

#### Scenario: Evento duplicado en un proyector

- **GIVEN** una vista actualizada por un `StepCompleted`
- **WHEN** el proyector recibe de nuevo el mismo evento
- **THEN** la vista no cambia

### Requirement: Vistas en PostgreSQL con posición del proyector

Las vistas SHALL residir en tablas de PostgreSQL. Cada proyector MUST registrar en `projector_offsets` el último `seq` aplicado, actualizado en la misma transacción que la vista.

#### Scenario: Reinicio de un proyector

- **GIVEN** un proyector con `last_seq` 120 registrado
- **WHEN** se reinicia
- **THEN** continúa desde el evento con `seq` 121
- **AND** no aplica dos veces los eventos anteriores

### Requirement: Reconstrucción de vistas

El sistema SHALL poder borrar cualquier vista y reconstruirla reproduciendo la tabla `events` desde `seq` 1, sin depender de la retención de Kafka. La vista reconstruida MUST ser idéntica a la que se mantenía de forma incremental.

#### Scenario: Reconstrucción de incident_status

- **GIVEN** una vista `incident_status` con su huella calculada
- **WHEN** se borra y se reproduce el log
- **THEN** la nueva vista tiene la misma huella
- **AND** el sistema registra el tiempo de reconstrucción

#### Scenario: La huella cambia si la vista cambia: detecta una vista reconstruida distinta

- **GIVEN** una vista modificada sin que haya eventos que lo justifiquen
- **WHEN** se reconstruye
- **THEN** la huella anterior y la nueva difieren

### Requirement: Consistencia eventual declarada

Las vistas SHALL ser eventualmente consistentes. Cada respuesta de consulta MUST incluir el último `seq` aplicado por el proyector que alimenta la vista (`last_applied_seq`), para que el cliente sepa cuán actual es.

#### Scenario: Lectura justo después de escribir

- **GIVEN** un comando aceptado que escribió el evento E1
- **WHEN** el cliente consulta la vista antes de que se aplique E1
- **THEN** la respuesta indica un `last_applied_seq` anterior al `seq` de E1

### Requirement: Nueva vista sin tocar la escritura

Añadir una vista nueva MUST NOT requerir cambios en el command handler ni en los eventos existentes.

#### Scenario: Vista de reclamaciones por transportista

- **GIVEN** un log con eventos `ClaimFiled` históricos
- **WHEN** se despliega un proyector nuevo
- **THEN** reproduce el log y construye la vista completa sin modificar el resto del sistema
