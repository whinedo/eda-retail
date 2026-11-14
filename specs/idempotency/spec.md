# Idempotency Specification

## Purpose

Garantizar que ningún efecto se ejecuta más de una vez aunque el bus duplique eventos, un ejecutor se reinicie o el replanner vuelva a pedir un paso. Aplica Idempotency key (6.2).

## Requirements

### Requirement: Inbox por consumidor

Cada consumidor SHALL registrar los eventos y comandos procesados en su propia tabla `<componente>.inbox` con clave única su `event_id` o `command_id`. El registro MUST escribirse en la misma transacción que sus cambios de estado y los eventos que deja en su outbox. Un evento ya registrado SHALL descartarse sin repetir su procesamiento. Todo comando MUST ser idempotente: un `command_id` ya registrado SHALL devolver la respuesta guardada sin repetir estado, eventos ni efectos.

#### Scenario: Reentrega de Kafka

- **GIVEN** un evento ya procesado y registrado en el `inbox` del consumidor
- **WHEN** Kafka lo entrega otra vez
- **THEN** el insert en el `inbox` no inserta ninguna fila
- **AND** el consumidor confirma el offset sin producir eventos nuevos

#### Scenario: Caída antes de confirmar el offset

- **GIVEN** un consumidor que ha confirmado su transacción pero cae antes de confirmar el offset en Kafka
- **WHEN** se reinicia y recibe de nuevo el mismo evento
- **THEN** el `inbox` lo detecta como ya procesado y lo descarta

#### Scenario: Comando repetido

- **GIVEN** un comando `CreatePlan` con `command_id` C1 ya registrado en `planner.inbox`
- **WHEN** la saga lo invoca de nuevo con C1
- **THEN** el handler devuelve la respuesta guardada
- **AND** no publica otro `PlanCreated`

### Requirement: Inbox y claves de efectos son mecanismos distintos

El `inbox` deduplica eventos por consumidor; la tabla `effect_keys` deduplica efectos por `idempotency_key`. Ninguno de los dos SHALL sustituir al otro: un evento nuevo puede pedir un efecto ya ejecutado, y un evento reentregado no debe repetirlo.

#### Scenario: Evento nuevo que pide un efecto ya ejecutado

- **GIVEN** un `ExecuteStep` distinto (otro `command_id`) con la misma `idempotency_key` que un paso ya completado
- **WHEN** el ejecutor lo procesa
- **THEN** el `inbox` lo registra como nuevo
- **AND** `effect_keys` impide repetir el efecto y devuelve el resultado guardado

### Requirement: Clave de idempotencia por paso

Todo `ExecuteStep` MUST llevar una `idempotency_key` calculada como `hash(incident_id, step_id, plan_version_de_origen)`, donde la versión de origen es aquella en la que se creó el paso. La clave SHALL ser determinista: el mismo paso produce siempre la misma clave.

#### Scenario: Misma clave para el mismo paso

- **GIVEN** un paso con `step_id` S1 creado en la versión 1 del plan de la incidencia I1
- **WHEN** el sistema calcula su clave dos veces
- **THEN** obtiene el mismo valor

#### Scenario: Paso mantenido en una revisión

- **GIVEN** un paso S1 creado en la versión 1 y mantenido en la versión 2
- **WHEN** se vuelve a emitir su `ExecuteStep`
- **THEN** la clave coincide con la de la versión 1

### Requirement: Consulta del almacén de claves antes de actuar

Todo ejecutor SHALL consultar la tabla `effect_keys` antes de ejecutar un efecto. Si la clave ya está registrada como completada, MUST NOT ejecutar el efecto y SHALL devolver el resultado guardado.

#### Scenario: Evento duplicado

- **GIVEN** un `ExecuteStep` ya ejecutado con éxito
- **WHEN** el bus lo entrega por segunda vez
- **THEN** el ejecutor no repite el efecto
- **AND** devuelve el resultado guardado sin publicar un efecto nuevo

### Requirement: Registro atómico de la clave

El ejecutor MUST registrar la clave con un `INSERT … ON CONFLICT DO NOTHING` y los estados `in_progress` y `completed`. Una clave `in_progress` SHALL bloquear a otros ejecutores hasta que se complete o venza su `locked_until`. Cuando el efecto sea simulado y resida en PostgreSQL, el efecto y el paso a `completed` MUST ir en la misma transacción.

#### Scenario: Dos ejecutores a la vez

- **GIVEN** dos instancias del mismo ejecutor que reciben el mismo `ExecuteStep` a la vez
- **WHEN** ambas intentan registrar la clave
- **THEN** solo una la obtiene y ejecuta el efecto
- **AND** la otra espera o descarta el evento

### Requirement: Caída entre el efecto y el registro

Cuando el ejecutor pueda caer entre el efecto y el registro de la clave, el efecto MUST usar además la clave del paso contra el sistema externo, de modo que el servicio externo también rechace el duplicado.

#### Scenario: Caída tras enviar el correo

- **GIVEN** un ejecutor que envía un aviso y cae antes de registrar la clave
- **WHEN** se reentrega el `ExecuteStep`
- **THEN** el reintento usa la misma clave hacia el servicio de correo
- **AND** el cliente recibe un solo aviso

### Requirement: Un aviso por cliente y pedido

El sistema SHALL enviar como máximo un aviso de la misma versión del plan por pedido, aunque el plan se revise o el evento se duplique.

#### Scenario: Revisión del plan tras un aviso enviado

- **GIVEN** un `CustomerNotified` ya publicado para el pedido P1
- **WHEN** el replanner publica una versión nueva que mantiene `notify_customer`
- **THEN** no se envía otro aviso a ese cliente por ese paso

### Requirement: Una reclamación por incidencia y pedido

El sistema MUST NOT crear más de una reclamación por par (incidencia, pedido), aunque el replanner vuelva a solicitar `file_claim`.

#### Scenario: Reclamación solicitada dos veces

- **GIVEN** una reclamación ya creada para el pedido P1 en la incidencia I1
- **WHEN** llega otro `ExecuteStep` de `file_claim` para P1
- **THEN** el ejecutor devuelve el `claim_id` existente
- **AND** no crea otra reclamación

### Requirement: Reservas, reenvíos y reembolsos sin duplicar

El sistema SHALL garantizar como máximo una reserva activa, un reenvío y un reembolso por pedido y paso. La idempotencia de `reserve_stock` y `issue_refund` MUST aplicarse incluso tras la reproducción del log.

#### Scenario: Reproducción del log con ejecutores activos

- **GIVEN** un log reproducido por error ante los ejecutores
- **WHEN** reciben de nuevo `ExecuteStep` ya ejecutados
- **THEN** no se crea ninguna reserva ni reembolso nuevo

### Requirement: Conservación de las claves

El almacén de claves SHALL conservar las claves al menos mientras la incidencia esté abierta y un plazo de seguridad configurable tras cerrarse.

#### Scenario: Duplicado tardío

- **GIVEN** una incidencia cerrada dentro del plazo de seguridad
- **WHEN** llega un `ExecuteStep` duplicado
- **THEN** el ejecutor lo descarta sin repetir el efecto
