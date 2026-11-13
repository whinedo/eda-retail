# Step execution Specification

## Purpose

Ejecutar los efectos reales de cada paso (avisar al cliente, reclamar, reservar stock, reenviar, reembolsar) con código determinista y publicar su resultado. Ningún LLM ejecuta efectos.

## Requirements

### Requirement: Un ejecutor por tipo de acción

Cada acción permitida SHALL tener un ejecutor que consume solo los `StepRequested` de esa acción. Los ejecutores MUST NOT invocar a un LLM para decidir si ejecutan o no.

#### Scenario: Enrutado por acción

- **GIVEN** un `StepRequested` con la acción `file_claim`
- **WHEN** se publica
- **THEN** solo lo procesa el ejecutor de reclamaciones

### Requirement: Resultado explícito

Todo `StepRequested` SHALL terminar en un `StepCompleted` o en un `StepFailed`. Además del evento genérico, el ejecutor publicará el evento de efecto correspondiente: `CustomerNotified`, `ClaimFiled`, `StockReserved`, `StockReleased`, `ShipmentReissued` o `RefundIssued`.

#### Scenario: Reclamación creada

- **GIVEN** un `StepRequested` de `file_claim` válido
- **WHEN** el ejecutor lo procesa
- **THEN** publica `ClaimFiled` con `claim_id`
- **AND** publica `StepCompleted` con el mismo `step_id`

### Requirement: Fallos clasificados

Un `StepFailed` MUST indicar `error` y `retryable`. Los errores transitorios (tiempo de espera, límite de cuota) SHALL marcarse como reintentables; los de negocio (`out_of_stock`, `order_closed`) no.

#### Scenario: Error de negocio

- **GIVEN** un `StepRequested` de `reserve_stock` y un almacén sin existencias
- **WHEN** el ejecutor lo procesa
- **THEN** publica `StepFailed` con `error` `out_of_stock` y `retryable` falso

#### Scenario: Error transitorio

- **GIVEN** un ejecutor cuyo servicio externo no responde a tiempo
- **WHEN** vence el tiempo de espera
- **THEN** publica `StepFailed` con `retryable` verdadero

### Requirement: Límite de reembolso

El ejecutor de reembolsos SHALL aplicar un importe máximo configurable por incidencia. Los reembolsos que lo superen MUST NOT ejecutarse y se resolverán con `IncidentEscalated`.

#### Scenario: Importe por encima del máximo

- **GIVEN** un `StepRequested` de `issue_refund` con un importe superior al máximo configurado
- **WHEN** el ejecutor lo procesa
- **THEN** no emite `RefundIssued`
- **AND** el sistema publica `IncidentEscalated` con el motivo `refund_limit`

### Requirement: Contenido redactado por el LLM

El texto del aviso al cliente y de la reclamación, redactado por el agente redactor, MUST pasar una validación determinista (longitud, idioma, ausencia de datos de otros pedidos, ausencia de importes no autorizados) antes de enviarse.

#### Scenario: Borrador con datos de otro pedido

- **GIVEN** un borrador de aviso que menciona el pedido de otro cliente
- **WHEN** la validación lo revisa
- **THEN** el ejecutor no lo envía
- **AND** publica `StepFailed` con `error` `invalid_draft` y `retryable` verdadero
