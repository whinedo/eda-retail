# CQRS views Specification

## Purpose

Separar quien acepta cambios (el command handler de cada sistema, que escribe su estado y sus eventos en su outbox) de quien responde consultas (proyecciones que construyen los componentes de consulta con los eventos de Kafka). Aplica CQRS (9.4): las proyecciones se reconstruyen volviendo a consumir los eventos.

## Requirements

### Requirement: Escritura solo mediante comandos

Todo cambio de estado de un sistema SHALL pasar por su command handler. Cada sistema (un Aggregate en términos de DDD) que recibe comandos tiene el suyo, que valida sus invariantes contra su estado, lo que trae el comando y lo que consulta en las proyecciones y, si se cumplen, escribe su estado y los eventos en su outbox en la misma transacción. Un comando rechazado MUST NOT escribir el evento de éxito. No existe un command handler común a varios sistemas. El orquestador no recibe comandos: es el process manager de la saga y cambia su estado al consumir eventos (spec `incident-planning`, "Saga reactiva").

#### Scenario: Comando válido

- **GIVEN** un comando `ExecuteStep` de `reserve_stock` invocado por la saga y existencias en el almacén
- **WHEN** el command handler de `inventory` lo atiende
- **THEN** escribe la reserva en `inventory.reservations` y `StockReserved` y `StepCompleted` en `inventory.outbox`, en la misma transacción
- **AND** el comando queda aceptado

#### Scenario: Comando rechazado

- **GIVEN** un comando `CreatePlan` cuyo plan propuesto por el LLM incluye la acción `delete_order`
- **WHEN** el command handler del planificador lo valida
- **THEN** lo rechaza
- **AND** no escribe `PlanCreated`

### Requirement: Catálogo de comandos

Los comandos SHALL ser peticiones explícitas con destinatario y nombre en imperativo, distintas de los eventos (hechos ya ocurridos), y cada uno lo atiende el command handler de un único sistema: `intake` (`RegisterCarrierNotice`), `inventory` (`SeedSampleData` y `ExecuteStep` de `reserve_stock` y `release_stock`), `interpreter` (`OpenIncident`, que deriva de `CarrierNoticeReceived`), `planner` (`CreatePlan`), `replanner` (`RevisePlan`) y cada ejecutor (`ExecuteStep` de su acción). Los comandos entre sistemas los invoca la saga del orquestador por REST (spec `event-backbone`, "Invocación de comandos") y nunca viajan por Kafka; el orquestador MUST NOT atender ningún comando. Cada comando SHALL llevar un `command_id` único y como `causation_id` el `event_id` del evento que lo originó, y el evento que produce su destinatario lleva como `causation_id` ese `command_id`.

#### Scenario: Comando derivado de un evento

- **GIVEN** un `ShipmentDelayed` con `event_id` E1 consumido por el orquestador
- **WHEN** la saga invoca `POST /commands/CreatePlan` del planificador
- **THEN** el comando lleva un `command_id` C1 y `causation_id` E1
- **AND** el `PlanCreated` que publica el planificador lleva `causation_id` C1

#### Scenario: El orquestador no recibe comandos

- **GIVEN** el catálogo de comandos
- **WHEN** se revisan sus destinatarios
- **THEN** ninguno es `orchestrator`

#### Scenario: Aviso registrado

- **GIVEN** un comando `RegisterCarrierNotice` del aviso N-1 con `command_id` C1
- **WHEN** el command handler de `intake` lo atiende
- **THEN** escribe el aviso en `intake.carrier_notices` y `CarrierNoticeReceived` con `causation_id` C1 en `intake.outbox`, en la misma transacción

#### Scenario: Aviso repetido

- **GIVEN** el aviso N-1 ya registrado
- **WHEN** llega otro `RegisterCarrierNotice` de N-1 con un `command_id` distinto
- **THEN** el handler de `intake` lo rechaza con `duplicate_notice`
- **AND** no escribe otro `CarrierNoticeReceived`

### Requirement: Un único dueño por estado

Cada estado SHALL tener un único componente dueño, el único que lo escribe. El estado de la incidencia (`incidents`, `plan_steps`) es del orquestador, que procesa en orden los eventos de cada incidencia (misma partición de Kafka), así que no hay escrituras concurrentes de varios procesos sobre él. Las propuestas de otros componentes (planes, revisiones, resultados de pasos) se deciden al consumirlas: una que llega tarde o se basa en una versión ya superada MUST descartarse sin cambiar el estado.

#### Scenario: Dos revisiones sobre la misma incidencia

- **GIVEN** una incidencia en la versión de plan 2 y dos `PlanRevised` del replanificador basados en la versión 2
- **WHEN** el orquestador los consume
- **THEN** aplica el primero y la versión pasa a 3
- **AND** descarta el segundo como obsoleto, sin cambiar el estado

### Requirement: Separación entre el modelo de escritura y el de lectura

El estado de escritura de cada componente y las vistas (`views.view_*`) SHALL ser tablas distintas. Las vistas solo las escriben los proyectores, a partir de los eventos que consumen de Kafka. Ningún componente, de lectura o de escritura, MUST consultar el estado de escritura de otro ni hacer joins con él.

#### Scenario: Lectura del panel

- **GIVEN** un panel que muestra incidencias abiertas
- **WHEN** consulta los datos
- **THEN** la consulta solo lee tablas `views.view_*`

### Requirement: Consultas al lado de lectura

Las consultas de las personas (front, `eda-notice interactive`, experimentos) y las de los sistemas que necesitan información de otro dominio SHALL leer únicamente de las proyecciones, con el módulo `queries`. Un sistema MUST NOT leer las tablas de escritura de otro sistema. La consulta nunca escribe.

#### Scenario: Consulta del estado

- **GIVEN** una incidencia en curso
- **WHEN** el panel consulta su estado
- **THEN** la respuesta sale de la vista de estado
- **AND** no se genera ningún evento

#### Scenario: Decisión del replanificador

- **GIVEN** un `StepFailed` que obliga a revisar el plan
- **WHEN** la saga invoca el comando `RevisePlan` del replanificador
- **THEN** el comando lleva el plan vigente, el paso fallido y las revisiones hechas
- **AND** el replanificador no lee las tablas del orquestador

#### Scenario: Dato de otro dominio

- **GIVEN** un planificador que necesita el importe del pedido P-1002 para validar un reembolso
- **WHEN** valida el plan
- **THEN** lo consulta en la proyección `view_orders`
- **AND** no lee ninguna tabla de `inventory`

### Requirement: Vistas mínimas

El sistema SHALL mantener las vistas `orders` (pedido, importe y `customer_ref`, de `OrderRegistered`), `incident_status` (estado y paso actual por incidencia), `cost_per_incident` (tokens y coste de efectos), `audit_trail` (quién decidió qué y con qué versión del plan) y `carrier_dashboard` (retrasos y reclamaciones por transportista).

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

El componente de consulta `event_log` SHALL mantener la vista `event_log.events` con todos los eventos consumidos de Kafka, y el front MUST leer los eventos solo de ella, haciendo polling sobre esa vista. Cada evento SHALL recibir al guardarse un `seq` creciente en el orden de consumo; como los eventos de una incidencia viajan por la misma partición, el `seq` respeta su orden causal dentro de la incidencia. La vista es solo de añadir (la base de datos rechaza `UPDATE`, `DELETE` y `TRUNCATE`) y un `event_id` repetido MUST NOT guardarse dos veces. Guarda también la partición, el offset y el contexto de traza de cada mensaje.

#### Scenario: Orden de una incidencia

- **GIVEN** una incidencia con `CarrierNoticeReceived`, `ShipmentDelayed` y `PlanCreated`, publicados por tres componentes distintos (`intake`, `interpreter`, `planner`) en ese orden causal
- **WHEN** `event_log` los consume
- **THEN** sus `seq` son crecientes en ese mismo orden

#### Scenario: Mensaje entregado dos veces

- **GIVEN** un evento ya guardado en `event_log.events`
- **WHEN** Kafka lo entrega otra vez
- **THEN** la vista sigue teniendo una sola fila con ese `event_id`

### Requirement: Consistencia eventual declarada

Las proyecciones SHALL ser eventualmente consistentes: van por detrás de los dueños del estado, y cada sistema trata ese retraso de forma explícita (design.md §4.3). Cada respuesta de consulta MUST incluir la posición aplicada de la vista (`applied`: offset por partición), para que el cliente sepa cuán actual es. Un sistema que no encuentra en una proyección el dato que necesita MUST NOT sondearla ni esperar: aparca el trabajo en una tabla suya con un plazo, confirma el offset y lo reanuda al consumir el evento que trae el dato; si vence el plazo, escala.

#### Scenario: Lectura justo después de escribir

- **GIVEN** un comando aceptado que escribió el evento E1
- **WHEN** el cliente consulta la vista antes de que se aplique E1
- **THEN** la posición `applied` de la partición de E1 es anterior al offset de E1

#### Scenario: Proyección atrasada

- **GIVEN** un aviso sobre el pedido P-1007 que llega al intérprete cuando `view_orders` aún no tiene P-1007
- **WHEN** el intérprete consulta los pedidos
- **THEN** aparca el aviso en `interpreter.pending_notices` con un plazo y confirma el offset, sin sondear la vista ni leer las tablas de `inventory`
- **AND** al consumir el `OrderRegistered` de P-1007 reanuda el aviso con los datos de ese evento

#### Scenario: Plazo vencido de un aviso aparcado

- **GIVEN** un aviso aparcado por el pedido P-1007
- **WHEN** vence su plazo sin que llegue el `OrderRegistered` de P-1007
- **THEN** el intérprete publica `IncidentEscalated` con `no_valid_orders`

### Requirement: Nueva vista sin tocar la escritura

Añadir una vista nueva MUST NOT requerir cambios en el command handler ni en los eventos existentes.

#### Scenario: Vista de reclamaciones por transportista

- **GIVEN** un log con eventos `ClaimFiled` históricos
- **WHEN** se despliega un proyector nuevo
- **THEN** reproduce el log y construye la vista completa sin modificar el resto del sistema
