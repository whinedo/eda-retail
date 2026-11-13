# Incident planning Specification

## Purpose

Decidir y gestionar qué pasos resuelven una incidencia. Un agente planificador genera un plan versionado, los pasos independientes se emiten en paralelo y un replanner lo revisa cuando algo falla, sin rehacer lo ya hecho. Aplica Planning orientado a eventos (9.2) y Saga (6.3).

## Requirements

### Requirement: Plan según la gravedad

El planificador SHALL publicar `PlanCreated` para toda incidencia con `severity` igual o superior a `medium`. Para `low` SHALL publicar un plan con un solo paso de aviso al cliente.

#### Scenario: Retraso leve

- **GIVEN** un `ShipmentDelayed` con `severity` `low` y `cause` `delay`
- **WHEN** el planificador lo procesa
- **THEN** publica `PlanCreated` con `plan_version` 1 y un único paso `notify_customer`

#### Scenario: Pérdida del paquete

- **GIVEN** un `ShipmentDelayed` con `cause` `loss` y `severity` `high`
- **WHEN** el planificador lo procesa
- **THEN** publica `PlanCreated` con los pasos `notify_customer`, `file_claim`, `reserve_stock` y `reissue_shipment`

### Requirement: Plan estructurado y versionado

Todo plan MUST incluir `plan_version` y una lista de pasos, cada uno con `step_id`, `action`, `params` y `depends_on`. Las acciones permitidas SHALL ser `notify_customer`, `file_claim`, `reserve_stock`, `reissue_shipment`, `release_stock` y `issue_refund`.

#### Scenario: Plan con una acción no permitida

- **GIVEN** un plan generado con la acción `delete_order`
- **WHEN** el sistema lo valida
- **THEN** lo rechaza y no emite ningún `StepRequested`
- **AND** pide al planificador un plan nuevo, dentro de los límites de `llm-boundaries`

### Requirement: Pasos independientes en paralelo

El sistema SHALL emitir a la vez los `StepRequested` de los pasos cuyas dependencias estén cumplidas. MUST NOT emitir un paso con dependencias pendientes.

#### Scenario: Pasos sin dependencias

- **GIVEN** un plan con `notify_customer` y `file_claim` sin dependencias, y `reserve_stock` sin dependencias
- **WHEN** se publica `PlanCreated`
- **THEN** se emiten los tres `StepRequested` sin esperar entre ellos

#### Scenario: Paso con dependencia

- **GIVEN** un plan donde `reissue_shipment` depende de `reserve_stock`
- **WHEN** `reserve_stock` aún no ha emitido `StepCompleted`
- **THEN** no se emite el `StepRequested` de `reissue_shipment`
- **AND** se emite cuando llega el `StepCompleted` de `reserve_stock`

### Requirement: Revisión del plan ante un fallo

Cuando un paso termine en `StepFailed` sin posibilidad de reintento, el replanner SHALL publicar `PlanRevised` con `plan_version` incrementada y los pasos pendientes. MUST NOT incluir de nuevo los pasos ya completados.

#### Scenario: Sin stock en el almacén

- **GIVEN** un plan en el que `reserve_stock` termina en `StepFailed` con el error `out_of_stock` y `retryable` falso
- **WHEN** el replanner lo procesa
- **THEN** publica `PlanRevised` con `plan_version` 2
- **AND** el plan sustituye `reissue_shipment` por `issue_refund`
- **AND** no vuelve a incluir `notify_customer`, que ya estaba completado

### Requirement: Estabilidad de los step_id

El replanner MUST reutilizar el `step_id` de los pasos que mantiene entre versiones. Solo los pasos nuevos recibirán un `step_id` nuevo.

#### Scenario: Paso mantenido entre versiones

- **GIVEN** un paso `file_claim` con `step_id` S2 pendiente en la versión 1
- **WHEN** el replanner publica la versión 2 manteniendo ese paso
- **THEN** el paso conserva el `step_id` S2

### Requirement: Saga con operaciones y compensaciones

El plan de una incidencia SHALL ejecutarse como una saga. Cada acción pertenece a una de estas categorías, definidas en código y no por el LLM:

| Acción | Categoría | Compensación |
| --- | --- | --- |
| `notify_customer` | No compensable (aviso ya enviado) | Ninguna |
| `file_claim` | No compensable (la reclamación sigue siendo válida) | Ninguna |
| `reserve_stock` | Compensable | `release_stock` |
| `reissue_shipment` | Reversible solo antes de salir del almacén; en la PoC se trata como paso final de su rama | Ninguna |
| `issue_refund` | Paso pivote: punto de no retorno | Ninguna |

El sistema MUST NOT emitir `issue_refund` mientras queden compensaciones pendientes de la misma incidencia.

#### Scenario: Reembolso con una compensación pendiente

- **GIVEN** una incidencia con un `reserve_stock` completado, un plan revisado que cancela el reenvío y un `release_stock` aún sin completar
- **WHEN** la orquestación evalúa los pasos pendientes
- **THEN** no emite el `StepRequested` de `issue_refund`
- **AND** lo emite cuando `release_stock` ha terminado en `StepCompleted`

### Requirement: Compensación determinista

Las compensaciones SHALL derivarse en código. Cuando un `PlanRevised` cancele un paso del que depende otro ya completado y compensable, la orquestación MUST emitir el `StepRequested` de la compensación sin intervención del LLM. El LLM MUST NOT incluir pasos de compensación en sus propuestas de plan.

#### Scenario: Fallo del reenvío tras reservar stock

- **GIVEN** un plan con `reserve_stock` completado y `reissue_shipment` en `StepFailed` no reintentable con `error` `carrier_rejected`
- **WHEN** el replanner propone un plan que sustituye el reenvío por `issue_refund`
- **THEN** la orquestación emite `release_stock` para la reserva existente
- **AND** el `StepRequested` de `release_stock` lo genera el código, no el plan propuesto

#### Scenario: Propuesta del LLM con una compensación incluida

- **GIVEN** un plan propuesto que contiene un paso `release_stock`
- **WHEN** el validador lo comprueba
- **THEN** lo rechaza con el motivo `compensation_not_allowed_in_plan`

### Requirement: Orden inverso y idempotencia de las compensaciones

Cuando haya varias compensaciones, SHALL ejecutarse en orden inverso al de los pasos originales. Cada compensación MUST llevar `step_id` igual al del paso original con el sufijo `-comp`, y su `idempotency_key` se calculará con ese `step_id`, de modo que repetirla no libere dos veces.

#### Scenario: Compensación repetida

- **GIVEN** un `release_stock` ya completado para la reserva R1
- **WHEN** llega de nuevo su `StepRequested`
- **THEN** el ejecutor no libera nada
- **AND** devuelve el resultado guardado

### Requirement: Fallo de una compensación

Si una compensación termina en `StepFailed` y agota sus reintentos, el sistema SHALL publicar `IncidentEscalated` con el motivo `compensation_failed` y MUST NOT emitir `issue_refund`.

#### Scenario: La liberación del stock falla

- **GIVEN** un `release_stock` que agota sus reintentos
- **WHEN** el sistema procesa el último `StepFailed`
- **THEN** publica `IncidentEscalated` con el motivo `compensation_failed`
- **AND** no se emite ningún reembolso

### Requirement: Estado de los pasos de la saga

Cada paso SHALL pasar por los estados `pending`, `requested`, `completed` o `failed`; un paso cuya compensación se haya pedido SHALL marcarse `compensating` y, al terminar, `compensated`. Un paso cancelado por una revisión del plan sin efecto previo SHALL marcarse `cancelled`.

#### Scenario: Estados tras una compensación

- **GIVEN** un `reserve_stock` completado cuya compensación acaba de pedirse
- **WHEN** se consulta el estado de los pasos
- **THEN** el paso aparece `compensating`
- **AND** pasa a `compensated` cuando `release_stock` termina

### Requirement: Cierre de la incidencia

Cuando todos los pasos del plan vigente estén completados, el sistema SHALL publicar `IncidentResolved`. Si el plan agota las revisiones permitidas, SHALL publicar `IncidentEscalated`.

#### Scenario: Plan completado

- **GIVEN** un plan cuyos pasos han emitido todos `StepCompleted`
- **WHEN** se procesa el último
- **THEN** se publica `IncidentResolved` con `outcome` `resolved`, o `refunded` si el plan vigente terminó con un reembolso

#### Scenario: Revisiones agotadas

- **GIVEN** una incidencia con el máximo de revisiones del plan ya publicado
- **WHEN** vuelve a fallar un paso sin reintento
- **THEN** se publica `IncidentEscalated` con el motivo `max_revisions`

### Requirement: Eventos obsoletos

El sistema MUST ignorar los `StepCompleted` y `StepFailed` de una `plan_version` anterior a la vigente, salvo para auditoría.

#### Scenario: Resultado de una versión antigua

- **GIVEN** un plan en la versión 2
- **WHEN** llega un `StepFailed` generado por la versión 1
- **THEN** el sistema no lo usa para decidir
- **AND** queda registrado como obsoleto
