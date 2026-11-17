# Resilience Specification

## Purpose

Asegurar que el sistema sobrevive a fallos parciales: reintentos acotados, dead letter para lo irrecuperable, recuperación tras la caída de un consumidor y control de la carga. Aplica Retry con backoff y dead letter (6.6), Backpressure (5.7) y Durable execution (6.1).

## Requirements

### Requirement: Reintentos con espera exponencial

Los errores transitorios SHALL reintentarse con espera exponencial y jitter, hasta un máximo de intentos configurable. Los errores no reintentables MUST NOT reintentarse.

#### Scenario: Error transitorio resuelto en el segundo intento

- **GIVEN** un ejecutor con un error transitorio en el primer intento
- **WHEN** reintenta tras la espera configurada
- **THEN** el segundo intento tiene éxito
- **AND** el efecto se ejecuta una sola vez

### Requirement: Confirmación del offset tras procesar

Los consumidores SHALL confirmar el offset de Kafka manualmente y solo después de haber confirmado la transacción de PostgreSQL. MUST NOT usar la confirmación automática.

#### Scenario: Fallo durante el procesamiento

- **GIVEN** un consumidor que recibe un evento y falla antes de confirmar su transacción
- **WHEN** se reinicia
- **THEN** Kafka le entrega de nuevo el evento porque su offset no se confirmó

### Requirement: Dead letter queue

Un evento que agote sus reintentos o sea irrecuperable (esquema inválido, plan imposible) SHALL moverse al topic `eda.dlq` y a la tabla `dead_letters`, con el motivo y el `correlation_id`. El sistema MUST NOT descartarlo en silencio.

Cada consumidor responde solo de los tipos que atiende: un evento inválido va a la dead letter de cada consumidor que atiende su `event_type`, y los demás consumidores lo ignoran y confirman el offset. Un mensaje del que no se puede leer el `event_type` (no es JSON, no es un objeto o le falta el campo) lo atienden todos. El lado de lectura también tiene dead letters: `event_log` atiende todos los tipos y cada proyector, los que proyecta; los guardan en `event_log.dead_letters` y `views.dead_letters`, en la misma transacción que su posición, y un relay que solo publica dead letters los lleva a `eda.dlq`. Un evento con la estructura común válida y un `event_type` que no está en el catálogo no es inválido (tolerant reader): los consumidores lo ignoran sin dead letter y `event_log` lo guarda como cualquier otro evento, de modo que no se descarta en silencio.

#### Scenario: Evento con esquema inválido

- **GIVEN** un evento que no cumple su esquema
- **WHEN** el consumidor lo rechaza
- **THEN** el evento pasa a la dead letter queue con el motivo `invalid_schema`
- **AND** se publica `IncidentEscalated` si pertenece a una incidencia abierta

#### Scenario: Evento inválido de un tipo que el consumidor no atiende

- **GIVEN** un evento inválido de un `event_type` sin handler en el consumidor
- **WHEN** lo recibe
- **THEN** lo ignora, confirma el offset y no escribe en su `dead_letters`

#### Scenario: Evento inválido en el lado de lectura

- **GIVEN** un evento inválido de un tipo que proyecta un proyector
- **WHEN** lo reciben `event_log` y ese proyector
- **THEN** cada uno lo guarda en su `dead_letters` con el motivo `invalid_schema` y avanza su posición en la misma transacción
- **AND** el evento no se escribe en `event_log.events` ni en las vistas

#### Scenario: Evento de un tipo desconocido

- **GIVEN** un evento con la estructura común válida y un `event_type` que no está en el catálogo
- **WHEN** lo reciben los consumidores
- **THEN** lo ignoran sin dead letter y confirman el offset
- **AND** `event_log` lo guarda en `event_log.events`

### Requirement: Recuperación tras la caída de un consumidor

Si un consumidor cae, SHALL retomar desde su último evento confirmado al reiniciarse. Ninguna incidencia MUST quedarse sin procesar, y ningún paso ya completado SHALL repetirse.

#### Scenario: Caída de un ejecutor a mitad de un plan

- **GIVEN** un plan con cuatro pasos, dos completados
- **WHEN** el ejecutor cae y se reinicia, y la saga le vuelve a invocar los pasos con el mismo `command_id`
- **THEN** procesa los dos pasos pendientes
- **AND** no repite los dos completados

### Requirement: Estado del plan reconstruible

El estado de cada saga SHALL estar en las tablas de su dueño, el orquestador, incluidos sus plazos. Un componente que se reinicia MUST NOT depender de memoria local para saber en qué paso va una incidencia.

#### Scenario: Reinicio del orquestador

- **GIVEN** un orquestador reiniciado con incidencias abiertas
- **WHEN** vuelve a consumir
- **THEN** recupera la versión vigente, los pasos pendientes y los plazos de cada incidencia de `orchestrator.plan_steps` y `orchestrator.deadlines`

### Requirement: Límite de concurrencia

Cada ejecutor SHALL limitar sus operaciones simultáneas con un valor configurable. Cuando se alcance, los eventos MUST esperar en el bus sin perderse.

#### Scenario: Pico de incidencias

- **GIVEN** un ejecutor con límite de 5 operaciones simultáneas y 50 pasos pendientes
- **WHEN** se procesan
- **THEN** nunca hay más de 5 en curso
- **AND** los 50 terminan sin pérdida

### Requirement: Aislamiento entre ejecutores

Un ejecutor lento o caído SHALL NOT impedir que los demás procesen sus pasos.

#### Scenario: Servicio de reclamaciones caído

- **GIVEN** el servicio de reclamaciones sin respuesta
- **WHEN** hay pasos de aviso y de reserva pendientes
- **THEN** los avisos y las reservas se procesan con normalidad
