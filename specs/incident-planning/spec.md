# Incident planning Specification

## Purpose

Decidir y gestionar qué pasos resuelven una incidencia. Un agente planificador genera un plan versionado, los pasos independientes se emiten en paralelo y un replanner lo revisa cuando algo falla, sin rehacer lo ya hecho. Aplica Planning orientado a eventos (9.2) y Saga (6.3).

## Requirements

### Requirement: Plan según la gravedad

El planificador SHALL publicar `PlanCreated` para toda incidencia con `severity` igual o superior a `medium`. Para `low` SHALL publicar un plan con un solo paso de aviso al cliente.

#### Scenario: Retraso leve

- **GIVEN** un comando `CreatePlan` de una incidencia con `severity` `low` y `cause` `delay`
- **WHEN** el planificador lo procesa
- **THEN** publica `PlanCreated` con `plan_version` 1 y un único paso `notify_customer`

#### Scenario: Pérdida del paquete

- **GIVEN** un comando `CreatePlan` de una incidencia con `cause` `loss` y `severity` `high`
- **WHEN** el planificador lo procesa
- **THEN** publica `PlanCreated` con los pasos `notify_customer`, `file_claim`, `reserve_stock` y `reissue_shipment`

### Requirement: Plan estructurado y versionado

Todo plan MUST incluir `plan_version` y una lista de pasos, cada uno con `step_id`, `action`, `params` y `depends_on`. Las acciones permitidas SHALL ser `notify_customer`, `file_claim`, `reserve_stock`, `reissue_shipment`, `release_stock` y `issue_refund`.

#### Scenario: Plan con una acción no permitida

- **GIVEN** un plan generado con la acción `delete_order`
- **WHEN** el sistema lo valida
- **THEN** lo rechaza y no publica `PlanCreated`
- **AND** pide al planificador un plan nuevo, dentro de los límites de `llm-boundaries`

### Requirement: Reembolso y reenvío excluyentes

Un plan MUST NOT incluir a la vez `issue_refund` y `reissue_shipment` del mismo pedido, ni proponer uno de ellos cuando el otro ya está completado para ese pedido: el cliente recibiría el producto y su importe. El command handler SHALL rechazar el plan con el motivo `refund_and_reissue_conflict`, aunque lo proponga el LLM.

#### Scenario: Reembolso y reenvío del mismo pedido

- **GIVEN** un plan propuesto con `reissue_shipment` e `issue_refund` para el pedido P1
- **WHEN** el command handler lo valida
- **THEN** lo rechaza con el motivo `refund_and_reissue_conflict`
- **AND** no publica `PlanCreated`

#### Scenario: Reembolso tras un reenvío completado

- **GIVEN** una incidencia con `reissue_shipment` completado para el pedido P1
- **WHEN** el replanner propone un plan con `issue_refund` para P1
- **THEN** el command handler lo rechaza con el motivo `refund_and_reissue_conflict`

### Requirement: Pasos independientes en paralelo

La saga SHALL invocar a la vez los comandos `ExecuteStep` de los pasos cuyas dependencias estén cumplidas. MUST NOT invocar un paso con dependencias pendientes.

#### Scenario: Pasos sin dependencias

- **GIVEN** un plan con `notify_customer` y `file_claim` sin dependencias, y `reserve_stock` sin dependencias
- **WHEN** se publica `PlanCreated`
- **THEN** se emiten los tres `ExecuteStep` sin esperar entre ellos

#### Scenario: Paso con dependencia

- **GIVEN** un plan donde `reissue_shipment` depende de `reserve_stock`
- **WHEN** `reserve_stock` aún no ha emitido `StepCompleted`
- **THEN** no se emite el `ExecuteStep` de `reissue_shipment`
- **AND** se emite cuando llega el `StepCompleted` de `reserve_stock`

### Requirement: Revisión del plan ante un fallo

Cuando un paso termine en `StepFailed` sin posibilidad de reintento, la saga SHALL invocar el comando `RevisePlan` al replanner, con el plan vigente, el paso fallido y las revisiones hechas, y el replanner SHALL publicar `PlanRevised` con `plan_version` incrementada y los pasos pendientes. MUST NOT incluir de nuevo los pasos ya completados.

#### Scenario: Sin stock en el almacén

- **GIVEN** un plan en el que `reserve_stock` termina en `StepFailed` con el error `out_of_stock` y `retryable` falso
- **WHEN** el replanner procesa el `RevisePlan` que invoca la saga
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
- **WHEN** la orquestación consume un evento de la incidencia
- **THEN** no emite el `ExecuteStep` de `issue_refund`
- **AND** lo emite cuando `release_stock` ha terminado en `StepCompleted`

### Requirement: Saga reactiva

La saga SHALL avanzar solo por eventos y por el vencimiento de sus propios plazos: cada transición ocurre cuando el orquestador, único dueño de `orchestrator.plan_steps`, consume un evento (`ShipmentDelayed`, `PlanCreated`, `PlanRevised`, `StepCompleted`, `StepFailed`, `IncidentEscalated`) o vence uno de sus plazos, y deja, en la misma transacción, los comandos que invocará por REST (`CreatePlan`, `RevisePlan`, `ExecuteStep`) en su `command_outbox` y los eventos siguientes en su outbox. El orquestador no recibe comandos; un comando aceptado no cambia el estado del paso, que solo cambia al consumir el evento de resultado (spec `event-backbone`, "Invocación de comandos"). Ningún otro componente MUST escribir el estado de la saga, y el orquestador MUST NOT consultar ni esperar el estado de otros componentes: los resultados le llegan como eventos.

#### Scenario: Resultado de un paso

- **GIVEN** un `reserve_stock` en estado `requested` del que depende un `reissue_shipment`
- **WHEN** el orquestador consume el `StepCompleted` de `reserve_stock` publicado por `inventory`
- **THEN** marca el paso `completed` y deja en su `command_outbox` el comando `ExecuteStep` de `reissue_shipment`, en la misma transacción
- **AND** `inventory` no ha escrito ninguna tabla del orquestador

#### Scenario: Compensación terminada

- **GIVEN** un `reserve_stock` en `compensating` y un `issue_refund` retenido
- **WHEN** el orquestador consume el `StepCompleted` de `reserve_stock-comp`
- **THEN** marca el original `compensated`
- **AND** invoca el comando `ExecuteStep` de `issue_refund`

#### Scenario: Inicio de la saga

- **GIVEN** un `ShipmentDelayed` de la incidencia I1
- **WHEN** el orquestador lo consume
- **THEN** crea la incidencia `open` y invoca el comando `CreatePlan` al planificador, con un plazo

### Requirement: Plazos de la saga

Cada comando que invoca la saga SHALL llevar un plazo durable, guardado en `orchestrator.deadlines` en la misma transacción que el comando. Si el plazo vence antes de su resultado, la saga MUST tratar un `ExecuteStep` como un `StepFailed` no reintentable con `error` `timeout` y, si el paso es compensable, pedir además su compensación; un `ExecuteStep` de `issue_refund` (pivote), un `CreatePlan` o un `RevisePlan` vencidos MUST escalar la incidencia con `step_timeout` o `plan_timeout`. Un resultado que llega después del plazo SHALL registrarse como obsoleto sin cambiar el estado. Revisar los plazos vencidos es leer la tabla del propio orquestador, nunca el estado de otro componente.

#### Scenario: Paso sin respuesta

- **GIVEN** un `ExecuteStep` de `file_claim` invocado con un plazo y el ejecutor de reclamaciones caído
- **WHEN** vence el plazo
- **THEN** el paso pasa a `failed` con `error` `timeout`
- **AND** la saga invoca `RevisePlan` al replanner

#### Scenario: Reserva sin respuesta

- **GIVEN** un `ExecuteStep` de `reserve_stock` cuyo plazo vence sin resultado
- **WHEN** la saga lo trata
- **THEN** invoca además el `ExecuteStep` de `reserve_stock-comp`, que no libera nada si la reserva nunca se hizo

#### Scenario: Planificador sin respuesta

- **GIVEN** un `CreatePlan` invocado con un plazo
- **WHEN** vence el plazo sin `PlanCreated`
- **THEN** la saga escala la incidencia con `plan_timeout`

#### Scenario: Resultado después del plazo

- **GIVEN** un paso `failed` por `timeout`
- **WHEN** llega su `StepCompleted`
- **THEN** se registra como obsoleto y no cambia el estado de la saga

#### Scenario: Plazos tras un reinicio

- **GIVEN** un orquestador caído mientras vencía el plazo de un `ExecuteStep`
- **WHEN** se reinicia
- **THEN** trata ese plazo vencido sin perderlo

### Requirement: Escalado de la incidencia

Cuando la saga escale una incidencia (revisiones agotadas, plazo vencido, fallo de una compensación o `IncidentEscalated` de otro componente), SHALL marcarla `escalated`, cancelar los pasos pendientes con `StepCancelled`, pedir la compensación de cada `reserve_stock` completado cuyo dependiente no se haya completado y publicar `IncidentEscalated` con el motivo, si no lo publicó otro componente. MUST NOT invocar `issue_refund`. Un escalado por `compensation_failed` MUST NOT volver a pedir compensaciones.

#### Scenario: Escalado con una reserva hecha

- **GIVEN** una incidencia con `reserve_stock` completado y `reissue_shipment` pendiente
- **WHEN** el replanner publica `IncidentEscalated` con `llm_output_invalid`
- **THEN** la incidencia pasa a `escalated` y `reissue_shipment` a `cancelled`
- **AND** la saga invoca el `ExecuteStep` de `reserve_stock-comp`

### Requirement: Compensación determinista

Las compensaciones SHALL derivarse en código. Cuando un `PlanRevised` cancele un paso del que depende otro ya completado y compensable, la saga MUST invocar el comando `ExecuteStep` de la compensación sin intervención del LLM. El LLM MUST NOT incluir pasos de compensación en sus propuestas de plan.

#### Scenario: Fallo del reenvío tras reservar stock

- **GIVEN** un plan con `reserve_stock` completado y `reissue_shipment` en `StepFailed` no reintentable con `error` `carrier_rejected`
- **WHEN** el replanner propone un plan que sustituye el reenvío por `issue_refund`
- **THEN** la orquestación emite `release_stock` para la reserva existente
- **AND** el `ExecuteStep` de `release_stock` lo genera el código, no el plan propuesto

#### Scenario: Propuesta del LLM con una compensación incluida

- **GIVEN** un plan propuesto que contiene un paso `release_stock`
- **WHEN** el validador lo comprueba
- **THEN** lo rechaza con el motivo `compensation_not_allowed_in_plan`

### Requirement: Orden inverso y idempotencia de las compensaciones

Cuando haya varias compensaciones, SHALL ejecutarse en orden inverso al de los pasos originales. Cada compensación MUST llevar `step_id` igual al del paso original con el sufijo `-comp`, y su `idempotency_key` se calculará con ese `step_id`, de modo que repetirla no libere dos veces.

#### Scenario: Compensación repetida

- **GIVEN** un `release_stock` ya completado para la reserva R1
- **WHEN** llega de nuevo su `ExecuteStep`
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
