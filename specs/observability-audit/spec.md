# Observability and audit Specification

## Purpose

Poder seguir y explicar cada decisión del sistema: trazas por incidencia, coste de las llamadas al LLM y de los efectos, y un registro de auditoría derivado del log. Aplica Tracing distribuido (6.10).

## Requirements

### Requirement: Traza por incidencia

El sistema SHALL generar una traza por incidencia identificada por su `incident_id`, con un span por cada llamada al LLM, evento consumido y efecto ejecutado. Los spans MUST enlazarse mediante `causation_id`.

#### Scenario: Seguimiento de una incidencia

- **GIVEN** una incidencia resuelta
- **WHEN** se consulta su traza
- **THEN** muestra la secuencia aviso, interpretación, plan, pasos y cierre
- **AND** cada span indica su duración y el componente que lo produjo

### Requirement: Registro de las llamadas al LLM

Cada llamada al LLM SHALL registrarse con el agente, el modelo, los tokens de entrada y salida, la latencia y si la salida pasó la validación. El registro MUST NOT incluir datos personales en claro. Cada llamada, aceptada o no, SHALL quedar en el log como un evento `LlmCallRecorded` y sumarse al presupuesto de la incidencia.

#### Scenario: Llamada del planificador

- **GIVEN** una llamada del planificador a un modelo
- **WHEN** termina
- **THEN** se registran los tokens, la latencia y el resultado de la validación

### Requirement: Coste por incidencia

La vista `cost_per_incident` SHALL sumar los tokens, el coste estimado de las llamadas al LLM y el coste de los efectos (reclamaciones, reembolsos, reenvíos) de cada incidencia.

#### Scenario: Incidencia con una revisión del plan

- **GIVEN** una incidencia con un plan y una revisión
- **WHEN** se consulta `cost_per_incident`
- **THEN** incluye las llamadas del planificador y del replanner
- **AND** el importe de los efectos ejecutados

### Requirement: Auditoría de decisiones

La vista `audit_trail` SHALL permitir explicar cada decisión: qué agente propuso qué, con qué versión del plan, qué validación aplicó el código y por qué se aceptó o rechazó.

#### Scenario: Reembolso en lugar de reenvío

- **GIVEN** una incidencia en la que el replanner sustituyó el reenvío por un reembolso
- **WHEN** se consulta su auditoría
- **THEN** aparece el `StepFailed` que lo motivó, el `PlanRevised` y el reembolso resultante

### Requirement: Auditoría reconstruible

El registro de auditoría MUST derivarse solo del log. Si se pierde, SHALL poder reconstruirse reproduciéndolo.

#### Scenario: Pérdida de la vista de auditoría

- **GIVEN** una vista `audit_trail` borrada
- **WHEN** se reproduce el log
- **THEN** la vista reconstruida contiene las mismas decisiones
