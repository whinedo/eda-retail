# CQRS views Specification

## Purpose

Separar quien acepta cambios (command handler, que añade eventos al log) de quien responde consultas (vistas construidas por proyectores). Aplica CQRS (9.4): el log es la fuente de verdad y las vistas se reconstruyen reproduciéndolo.

## Requirements

### Requirement: Escritura solo mediante comandos

Todo cambio de estado SHALL pasar por el command handler del componente dueño de ese estado. Cada componente que recibe comandos tiene el suyo, que valida las reglas de negocio contra su estado y sus réplicas y, si se cumplen, escribe su estado y los eventos en su outbox en la misma transacción. Un comando rechazado MUST NOT escribir eventos. No existe un command handler común a varios componentes.

#### Scenario: Comando válido

- **GIVEN** una incidencia abierta en el orquestador
- **WHEN** el command handler del orquestador recibe el resultado de un paso (de un `StepCompleted`)
- **THEN** actualiza `orchestrator.plan_steps` y escribe los eventos siguientes en `orchestrator.outbox`
- **AND** el comando queda aceptado

#### Scenario: Comando sobre una incidencia cerrada

- **GIVEN** una incidencia con `IncidentResolved` en el orquestador
- **WHEN** llega un comando de ejecutar un paso
- **THEN** el command handler del orquestador lo rechaza con el motivo `incident_closed`
- **AND** no se escribe ningún evento

### Requirement: Catálogo de comandos

Los comandos SHALL ser peticiones explícitas con destinatario y nombre en imperativo, distintas de los eventos (hechos ya ocurridos), y cada uno lo atiende el command handler de un único componente: `intake` (`RegisterCarrierNotice`), `inventory` (`SeedSampleData` y la ejecución de `reserve_stock` y `release_stock`), `interpreter` (`OpenIncident`), `planner` (`CreatePlan`), `replanner` (`RevisePlan`), `orchestrator` (`ApplyRevision`, `AdvancePlan`, `RecordStepResult`, `CloseIncident`, `EscalateIncident`) y cada ejecutor (`ExecuteStep`). Cada comando derivado de un evento SHALL llevar un `command_id`, que será el `event_id` de ese evento.

#### Scenario: Comando derivado de un evento

- **GIVEN** un `ShipmentDelayed` con `event_id` E1 consumido por el planificador
- **WHEN** el planificador envía el comando `CreatePlan` a su propio command handler
- **THEN** el comando lleva `command_id` E1
- **AND** el evento resultante `PlanCreated` lleva `causation_id` E1

### Requirement: Un único dueño por estado

Cada estado SHALL tener un único componente dueño, el único que lo escribe. El estado de la incidencia (`incidents`, `plan_steps`) es del orquestador, que procesa en orden los eventos de cada incidencia (misma partición de Kafka), así que no hay escrituras concurrentes de varios procesos sobre él. Las propuestas de otros componentes (planes, revisiones, resultados de pasos) se deciden al consumirlas: una que llega tarde o se basa en una versión ya superada MUST descartarse sin cambiar el estado.

#### Scenario: Dos revisiones sobre la misma incidencia

- **GIVEN** una incidencia en la versión de plan 2 y dos `PlanRevised` del replanificador basados en la versión 2
- **WHEN** el orquestador los consume
- **THEN** aplica el primero y la versión pasa a 3
- **AND** descarta el segundo como obsoleto, sin cambiar el estado

### Requirement: Separación entre el modelo de escritura y el de lectura

El estado de escritura de cada componente y las vistas (`views.view_*`) SHALL ser tablas distintas. Las vistas solo las escriben los proyectores, a partir del log de `event_log`. Los componentes de lectura MUST NOT consultar el estado de escritura de ningún componente ni hacer joins con él.

#### Scenario: Lectura del panel

- **GIVEN** un panel que muestra incidencias abiertas
- **WHEN** consulta los datos
- **THEN** la consulta solo lee tablas `views.view_*`

### Requirement: Lectura solo desde las vistas

Las consultas de las personas (front, `eda-notice interactive`, experimentos) SHALL leer únicamente de las vistas. Los componentes MUST NOT decidir leyendo vistas, el log ni tablas de otros componentes: deciden con su estado y sus réplicas. La consulta nunca escribe.

#### Scenario: Consulta del estado

- **GIVEN** una incidencia en curso
- **WHEN** el panel consulta su estado
- **THEN** la respuesta sale de la vista de estado
- **AND** no se genera ningún evento

#### Scenario: Decisión del replanificador

- **GIVEN** un `StepFailed` que obliga a revisar el plan
- **WHEN** el replanificador decide si quedan revisiones
- **THEN** usa su réplica `replanner.incidents`, alimentada por `PlanCreated` y `PlanRevised`
- **AND** no consulta la vista `incident_status`

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

### Requirement: Vistas en PostgreSQL con posición en Kafka

Las vistas SHALL residir en tablas de PostgreSQL del esquema de su componente de consulta. Cada componente de consulta (proyectores y `event_log`) MUST consumir los eventos de Kafka y registrar en su tabla `offsets` el último offset aplicado de cada partición, actualizado en la misma transacción que la vista. No lee tablas de ningún otro componente.

#### Scenario: Reinicio de un proyector

- **GIVEN** un proyector que aplicó hasta el offset 120 de la partición 1
- **WHEN** se reinicia
- **THEN** continúa desde el offset 121 de esa partición
- **AND** no aplica dos veces los mensajes anteriores aunque Kafka los reentregue

### Requirement: Reconstrucción de vistas

El sistema SHALL poder borrar cualquier vista y reconstruirla volviendo a consumir el topic `eda.events` desde el principio (retención infinita). La vista reconstruida MUST ser idéntica a la que se mantenía de forma incremental.

#### Scenario: Reconstrucción de incident_status

- **GIVEN** una vista `incident_status` con su huella calculada
- **WHEN** se borra y se vuelve a consumir el topic desde el principio
- **THEN** la nueva vista tiene la misma huella
- **AND** el sistema registra el tiempo de reconstrucción

#### Scenario: La huella cambia si la vista cambia: detecta una vista reconstruida distinta

- **GIVEN** una vista modificada sin que haya eventos que lo justifiquen
- **WHEN** se reconstruye
- **THEN** la huella anterior y la nueva difieren

### Requirement: Vista del log de eventos

El componente de consulta `event_log` SHALL mantener la vista `event_log.events` con todos los eventos consumidos de Kafka, y el front MUST leer los eventos solo de ella. Cada evento SHALL recibir al guardarse un `seq` creciente en el orden de consumo; como los eventos de una incidencia viajan por la misma partición, el `seq` respeta su orden causal dentro de la incidencia. La vista es solo de añadir (la base de datos rechaza `UPDATE`, `DELETE` y `TRUNCATE`) y un `event_id` repetido MUST NOT guardarse dos veces. Guarda también la partición, el offset y el contexto de traza de cada mensaje.

#### Scenario: Orden de una incidencia

- **GIVEN** una incidencia con `ShipmentDelayed`, `PlanCreated` y `StepRequested`, publicados por tres componentes distintos en ese orden causal
- **WHEN** `event_log` los consume
- **THEN** sus `seq` son crecientes en ese mismo orden

#### Scenario: Mensaje entregado dos veces

- **GIVEN** un evento ya guardado en `event_log.events`
- **WHEN** Kafka lo entrega otra vez
- **THEN** la vista sigue teniendo una sola fila con ese `event_id`

### Requirement: Consistencia eventual declarada

Las vistas y las réplicas locales de cada componente SHALL ser eventualmente consistentes: van por detrás de los dueños del estado, y cada componente trata ese retraso de forma explícita (design.md §4.3). Cada respuesta de consulta MUST incluir la posición aplicada de la vista (`applied`: offset por partición), para que el cliente sepa cuán actual es.

#### Scenario: Lectura justo después de escribir

- **GIVEN** un comando aceptado que escribió el evento E1
- **WHEN** el cliente consulta la vista antes de que se aplique E1
- **THEN** la posición `applied` de la partición de E1 es anterior al offset de E1

#### Scenario: Réplica atrasada

- **GIVEN** un aviso sobre el pedido P-1007 que llega al intérprete antes que el `OrderRegistered` de P-1007
- **WHEN** el intérprete valida los pedidos con su réplica
- **THEN** no confirma el evento y lo reintenta con espera exponencial, sin leer las tablas de `inventory`
- **AND** si agota los reintentos sin el pedido, publica `IncidentEscalated` con `no_valid_orders`

### Requirement: Nueva vista sin tocar la escritura

Añadir una vista nueva MUST NOT requerir cambios en el command handler ni en los eventos existentes.

#### Scenario: Vista de reclamaciones por transportista

- **GIVEN** un log con eventos `ClaimFiled` históricos
- **WHEN** se despliega un proyector nuevo
- **THEN** reproduce el log y construye la vista completa sin modificar el resto del sistema
