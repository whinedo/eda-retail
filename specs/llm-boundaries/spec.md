# LLM boundaries Specification

## Purpose

Fijar la frontera entre lo que decide el LLM y lo que dispone el código. Las salidas del LLM son propuestas que el código valida; ningún LLM escribe en el log ni ejecuta efectos. Aplica Guardrails (3.9) y Bounded autonomy (3.10).

## Requirements

### Requirement: Salida con esquema validado

Toda salida de un agente con LLM (`ShipmentDelayed`, plan, borrador) MUST validarse contra un esquema antes de convertirse en evento. Una salida que no cumpla SHALL descartarse y no escribirse en el log.

#### Scenario: Plan sin campo obligatorio

- **GIVEN** un plan generado sin `depends_on` en uno de sus pasos
- **WHEN** el validador lo comprueba
- **THEN** lo rechaza
- **AND** no se publica `PlanCreated`

### Requirement: El LLM no ejecuta efectos

Los agentes con LLM MUST NOT tener acceso a herramientas con efectos. Solo el código determinista, mediante los ejecutores, SHALL actuar sobre sistemas externos.

#### Scenario: Agente que intenta actuar

- **GIVEN** un agente planificador
- **WHEN** inspecciona sus herramientas disponibles
- **THEN** no hay ninguna que reserve stock, envíe correos ni emita reembolsos

### Requirement: Reglas de negocio fuera del LLM

El importe máximo de reembolso, las acciones permitidas y los umbrales de gravedad SHALL comprobarse en código. Una propuesta del LLM que las incumpla MUST rechazarse aunque el LLM la justifique.

#### Scenario: Reembolso por encima del máximo

- **GIVEN** un plan que propone reembolsar un importe superior al máximo configurado
- **WHEN** el command handler lo valida
- **THEN** rechaza el plan
- **AND** registra el motivo `refund_limit`

### Requirement: Reintentos de generación acotados

Cuando una salida del LLM sea rechazada, el sistema SHALL pedir una nueva como máximo un número configurable de veces. Agotado el límite, MUST publicar `IncidentEscalated` con el motivo `llm_output_invalid`.

#### Scenario: Tres planes inválidos seguidos

- **GIVEN** un límite de tres generaciones y tres planes rechazados
- **WHEN** se agota el límite
- **THEN** se publica `IncidentEscalated` con el motivo `llm_output_invalid`

### Requirement: Presupuesto por incidencia

Cada incidencia SHALL tener un presupuesto máximo de tokens, de llamadas al LLM y de revisiones del plan. Al agotarse cualquiera de ellos, el sistema MUST detener las llamadas y publicar `IncidentEscalated`.

#### Scenario: Presupuesto de tokens agotado

- **GIVEN** una incidencia que ha consumido su presupuesto de tokens
- **WHEN** se solicita otra llamada al LLM
- **THEN** no se realiza
- **AND** se publica `IncidentEscalated` con el motivo `budget_exhausted`

### Requirement: Escalado a una persona

Toda incidencia escalada SHALL quedar visible en `incident_status` con el motivo, para que una persona la gestione. El sistema MUST NOT reintentarla automáticamente tras escalarla.

#### Scenario: Incidencia escalada

- **GIVEN** una incidencia con `IncidentEscalated`
- **WHEN** se consulta `incident_status`
- **THEN** aparece como `escalated` con su motivo
- **AND** no se emiten más `StepRequested`
