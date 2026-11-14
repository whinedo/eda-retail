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

#### Scenario: Evento con esquema inválido

- **GIVEN** un evento que no cumple su esquema
- **WHEN** el consumidor lo rechaza
- **THEN** el evento pasa a la dead letter queue con el motivo `invalid_schema`
- **AND** se publica `IncidentEscalated` si pertenece a una incidencia abierta

### Requirement: Recuperación tras la caída de un consumidor

Si un consumidor cae, SHALL retomar desde su último evento confirmado al reiniciarse. Ninguna incidencia MUST quedarse sin procesar, y ningún paso ya completado SHALL repetirse.

#### Scenario: Caída de un ejecutor a mitad de un plan

- **GIVEN** un plan con cuatro pasos, dos completados
- **WHEN** el ejecutor cae y se reinicia
- **THEN** procesa los dos pasos pendientes
- **AND** no repite los dos completados

### Requirement: Estado del plan reconstruible

El estado de cada plan SHALL estar en las tablas de su dueño, el orquestador, y poder reconstruirse a partir del log de `event_log`. Un componente que se reinicia MUST NOT depender de memoria local para saber en qué paso va una incidencia.

#### Scenario: Reinicio del orquestador

- **GIVEN** un orquestador reiniciado con incidencias abiertas
- **WHEN** vuelve a consumir
- **THEN** recupera la versión vigente y los pasos pendientes de cada incidencia de `orchestrator.plan_steps`

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
