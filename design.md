# Diseño técnico de la PoC EDA (incidencias logísticas)

Complementa las specs de `specs/`. Aquí se concreta cómo materializa la pila cada concepto de las specs. Si una decisión de este documento contradice una spec, manda la spec y hay que corregir uno de los dos.

## 1. Pila

| Pieza | Decisión | Motivo |
| --- | --- | --- |
| Lenguaje | Python 3.12 | Decidido |
| Broker | Apache Kafka (imagen `apache/kafka`, KRaft, un solo broker) en Docker | Decidido |
| Base de datos | PostgreSQL 16 en Docker, con volumen persistente | Decidido |
| Cliente Kafka | `confluent-kafka` | Estable, usa librdkafka, permite commit manual de offsets |
| Cliente PostgreSQL | `psycopg` 3 | Transacciones explícitas y `SKIP LOCKED` |
| Migraciones | Alembic; un esquema de PostgreSQL por componente | Cada componente evoluciona sus tablas sin tocar las de otro |
| Esquemas de eventos | `pydantic` | Validación de las salidas del LLM y de los eventos |
| LLM | SDK de Anthropic, detrás de una interfaz `LlmClient` con una implementación simulada para tests | Permite ejecutar los experimentos sin coste y con datos deterministas |
| Tests | `pytest`, con PostgreSQL y Kafka reales en Docker para los de integración | Las garantías (inbox, outbox) solo se prueban con la base real |

## 2. Propiedad de los datos: cada componente, sus tablas

Principio de la PoC: **cada sistema es un Aggregate (DDD): solo escribe en sus propias tablas y solo se comunica por Kafka**. Todo el modelo es reactivo: nada avanza sondeando ni esperando el estado de otro sistema. Hay una única base de datos de PostgreSQL (vale para una PoC), pero cada componente tiene su propio esquema (`intake.*`, `planner.*`…) y sus tablas no se relacionan con las de otro dominio: ni claves foráneas, ni joins, ni lecturas de las tablas de escritura de otro. Lo que un sistema necesita saber de otro dominio lo consulta en el lado de lectura (CQRS, sección 8): las proyecciones de los componentes de consulta, que son modelos de datos independientes y van con retraso (sección 4.3).

Cada sistema que recibe comandos tiene su **propio command handler**, expuesto como API REST: valida el comando contra su estado (los invariantes de su Aggregate), escribe ese estado y los eventos en **su outbox** en la misma transacción (*transactional outbox*), y un relay publica ese outbox en Kafka. El orquestador no recibe comandos: es el *process manager* de la saga, reacciona a eventos e **invoca** de forma síncrona los comandos de los sistemas (secciones 4.2 y 4.4); el resultado le llega como el evento que emite cada sistema.

| Componente | Esquema | Escribe (solo) | Consulta (lado de lectura) |
| --- | --- | --- | --- |
| `intake` (entrada de avisos: `eda-notice`, front) | `intake` | `carrier_notices`, `outbox` | — |
| `interpreter` (agente) | `interpreter` | `interpretations`, `pending_notices`, `llm_calls`, `inbox`, `outbox` | `view_orders`, `view_cost_per_incident` (presupuesto) |
| `planner` (agente) | `planner` | `plans`, `llm_calls`, `inbox`, `outbox` | `view_orders`, `view_cost_per_incident` |
| `replanner` (agente) | `replanner` | `revisions`, `llm_calls`, `inbox`, `outbox` | `view_cost_per_incident` |
| `orchestrator` (saga) | `orchestrator` | `incidents`, `plan_steps`, `deadlines`, `command_outbox`, `inbox`, `outbox` | — |
| `executor-<acción>` | `executor_<acción>` | su tabla de efectos (`customer_messages`, `claims`, `shipments`, `refunds`), `effect_keys`, `llm_calls` (redactor), `inbox`, `outbox` | `view_cost_per_incident` (redactor) |
| `inventory` (pedidos y stock; ejecuta `reserve_stock` y `release_stock`) | `inventory` | `orders`, `stock`, `reservations`, `effect_keys`, `inbox`, `outbox` | — |
| `event_log` (consulta) | `event_log` | `events` (vista del log), `offsets` | — |
| `projectors` (consulta) | `views` | `view_*`, `offsets` | — |
| `baseline_sync` (base síncrona de E6) | `baseline` | sus tablas | — |

Cada esquema tiene también su `dead_letters`. Las consultas al lado de lectura pasan por el módulo `queries` (solo lectura de `views.*`) y se hacen antes de abrir la transacción, como la llamada al LLM. Lo que un comando necesita para ejecutarse viaja en el propio comando (sección 3).

## 3. Componentes, comandos, eventos y grupos de consumidores

Por Kafka **solo viajan eventos**: el topic `eda.events`, con clave `incident_id` y el `event_type` en una cabecera. Cada componente que consume es un proceso con su propio `group.id` e ignora los eventos que no le interesan. Los comandos no se publican: cada sistema los expone como API REST (`POST /commands/<Comando>`) y la saga los invoca de forma síncrona (sección 4.4). Si los comandos fueran mensajes en el bus, la saga sería una coreografía.

| Componente | Comandos que atiende (REST) | Eventos que consume | Eventos que produce | Tipo |
| --- | --- | --- | --- | --- |
| `intake` | `RegisterCarrierNotice` (de `eda-notice` y del front) | — | `CarrierNoticeReceived` | Código |
| `inventory` | `SeedSampleData`; `ExecuteStep` de `reserve_stock` y `release_stock` | — | `OrderRegistered`, `StockReserved`, `StockReleased`, `StepCompleted`, `StepFailed` | Código |
| `interpreter` | — (su handler atiende `OpenIncident`, que deriva de `CarrierNoticeReceived`) | `CarrierNoticeReceived`; `OrderRegistered` (solo para reanudar avisos aparcados) | `ShipmentDelayed` o `IncidentEscalated`, `LlmCallRecorded` | LLM + su command handler |
| `planner` | `CreatePlan` | — | `PlanCreated` o `IncidentEscalated`, `LlmCallRecorded` | LLM + su command handler |
| `orchestrator` (saga) | Ninguno; invoca `CreatePlan`, `RevisePlan` y `ExecuteStep` | `ShipmentDelayed`, `PlanCreated`, `PlanRevised`, `StepCompleted`, `StepFailed`, `IncidentEscalated` | `StepCancelled`, `IncidentResolved`, `IncidentEscalated` | Código |
| `replanner` | `RevisePlan` | — | `PlanRevised` o `IncidentEscalated`, `LlmCallRecorded` | LLM + su command handler |
| `executor-<acción>` | `ExecuteStep` de su acción | — | `StepCompleted`, `StepFailed`, el evento de efecto, `LlmCallRecorded` | Código (el redactor se invoca desde aquí) |
| `event_log` (consulta) | — | todos | — (mantiene la vista del log para el front) | Código |
| `projectors` (consulta) | — | los que necesita cada proyección | — (mantienen sus proyecciones) | Código |

Cada comando lleva lo que su destinatario necesita para ejecutarlo: `CreatePlan`, la incidencia (pedidos, causa, gravedad); `RevisePlan`, el plan vigente, el paso fallido y las revisiones hechas; `ExecuteStep`, la acción, el pedido, el importe y la `idempotency_key`. Así nadie consulta el estado del orquestador.

## 4. Flujo de escritura: un command handler por componente

Cada escritura de un componente es una transacción de PostgreSQL sobre **su** esquema:

```
BEGIN
  1. INSERT INTO <componente>.inbox (message_id) ON CONFLICT DO NOTHING
       -> message_id = event_id del evento o command_id del comando
       -> si no inserta fila: COMMIT y terminar (duplicado; un comando devuelve la respuesta guardada)
  2. el command handler del sistema valida el comando contra su estado (el LLM y las
     consultas al lado de lectura se hacen antes de la transacción)
  3. actualizar su estado (p. ej. inventory.reservations)
  4. INSERT INTO <componente>.outbox (event_id, envelope, trace_context, ...)
COMMIT
confirmar el offset en Kafka (evento) o responder a la petición REST (comando)
```

La llamada al LLM y las consultas a las proyecciones se hacen **antes** de la transacción. Si el proceso cae entre la llamada y el `COMMIT`, el evento se reentrega y se repite la llamada: cuesta tokens, pero no duplica efectos.

### 4.1 CQRS: comandos, consistencia y consultas

- **Command handlers:** uno por sistema que recibe comandos: `intake` (`RegisterCarrierNotice`), `inventory` (`SeedSampleData` y `ExecuteStep` de `reserve_stock` y `release_stock`), `interpreter` (`OpenIncident`, que deriva de `CarrierNoticeReceived`), `planner` (`CreatePlan`), `replanner` (`RevisePlan`) y cada ejecutor (`ExecuteStep` de su acción). Cada uno valida contra su estado, lo que trae el comando y lo que consulta en las proyecciones: el del planificador valida el plan (acciones, ciclos, tope de reembolso con el importe de `view_orders`). No hay un handler común ni una tabla que escriban varios.
- **Saga:** el orquestador no tiene command handler. Es el *process manager*: consume eventos, actualiza su estado (`incidents`, `plan_steps`, `deadlines`) e invoca comandos por REST desde su `command_outbox` (secciones 4.2 y 4.4).
- **Consistencia:** el estado de la incidencia tiene un único dueño, el orquestador, que procesa los mensajes de una incidencia en orden (misma partición), así que no necesita concurrencia optimista entre procesos. Lo que llega tarde (de una versión de plan anterior o tras vencer su plazo) lo descarta comparando `plan_version` (spec `incident-planning`, "Eventos obsoletos").
- **Presupuesto del LLM:** cada llamada se registra en el `llm_calls` del componente y se publica como `LlmCallRecorded`. El proyector de `cost_per_incident` las suma y cada componente consulta esa proyección antes de llamar al LLM.
- **Consultas:** las proyecciones (`event_log.events` y `views.view_*`, sección 8) las consultan las personas (el front, `eda-notice interactive` y los experimentos) y los sistemas que necesitan información de otro dominio (pedidos, presupuesto). Cada respuesta incluye la posición aplicada. Nadie lee las tablas de escritura de otro sistema.

Diagrama: «CQRS» en `docs/README.md` (en vector, `docs/diagrama-cqrs.pdf`).

### 4.3 Consistencia eventual

Cada sistema ve el estado de los demás a través de eventos y proyecciones, así que lo ve **con retraso**: las proyecciones van por detrás de los dueños. No es un fallo que haya que evitar, es la condición normal de una EDA, y cada componente la trata de forma explícita:

| Situación | Ejemplo | Cómo se trata |
| --- | --- | --- |
| La proyección aún no tiene un dato | El intérprete recibe un aviso de `P-1007` y `view_orders` aún no tiene ese pedido | Aparca el aviso en `interpreter.pending_notices` con un plazo y confirma el offset. Al consumir el `OrderRegistered` de `P-1007` lo reanuda con los datos de ese evento. Si vence el plazo, publica `IncidentEscalated` con `no_valid_orders`. Nunca sondea la proyección ni lee las tablas de `inventory` |
| Eventos de productores distintos llegan desordenados | `StepCompleted` de un paso de la versión 1 llega después de `PlanRevised` a la versión 2 | El orquestador compara `plan_version` y aplica la regla de "Eventos obsoletos"; el evento se registra en el inbox y no cambia el estado |
| Duplicados | El relay republica tras una caída | Inbox por componente y `effect_keys` en los ejecutores |
| Presupuesto atrasado | Dos agentes llaman al LLM a la vez para la misma incidencia | El límite se comprueba con la proyección `cost_per_incident`; el exceso queda acotado a las llamadas en curso, se registra en `LlmCallRecorded` y la siguiente comprobación lo ve. Es una cota blanda documentada, no una garantía fuerte |
| Vista atrasada respecto a una acción del usuario | El front envía un aviso y la vista `incident_status` aún no lo refleja | Cada consulta devuelve la posición aplicada de la vista; el front y `eda-notice interactive` muestran «actualizando» hasta que la posición incluye el evento esperado, y nunca leen el estado del dueño para «saltarse» el retraso |
| Decisión con información vieja | El replanificador revisa un plan que el orquestador ya ha cerrado | El dueño (orquestador) valida al consumir `PlanRevised`: si la incidencia está cerrada o la versión no encaja, lo descarta y lo deja en el log como obsoleto |
| Un sistema no responde a un comando | `inventory` está caído cuando la saga invoca `ExecuteStep` de `reserve_stock` | El despachador reintenta la invocación con el mismo `command_id` (sección 4.4), y la saga guarda un plazo por cada comando. Si vence antes del evento de resultado, sigue la ruta de fallo de la saga (sección 4.2): revisión del plan o escalado, con las compensaciones que correspondan. Lo que llegue después es obsoleto |

Regla general: **quien decide sobre un estado es su dueño**, al consumir el evento. Los demás proponen con lo que saben (lo que trae el comando y lo que consultan en las proyecciones) y aceptan que su propuesta pueda llegar tarde. Ningún sistema espera: aparca lo que no puede hacer aún, lo reanuda al consumir el evento que le faltaba y, si vence su plazo, escala.

### 4.2 Saga: operaciones y compensaciones

La orquestación gobierna la saga y es la única que escribe `orchestrator.plan_steps`. La categoría de cada acción y su compensación están en código (tabla en `specs/incident-planning`):

| Acción | Compensación | Notas |
| --- | --- | --- |
| `notify_customer` | Ninguna | |
| `file_claim` | Ninguna | La reclamación sigue siendo válida aunque se reembolse |
| `reserve_stock` | `release_stock` | Única acción compensable; la ejecuta `inventory` |
| `reissue_shipment` | Ninguna | |
| `issue_refund` | Ninguna | Paso pivote: se emite el último y solo sin compensaciones pendientes |

La saga es **reactiva y orquestada**: es un *process manager* dentro del orquestador. Cada transición ocurre al consumir un evento o al vencer uno de sus plazos, en una transacción que actualiza `orchestrator.plan_steps` y `orchestrator.deadlines`, deja en su `command_outbox` los comandos que invocará y en su outbox los eventos siguientes. El orquestador no recibe comandos ni consulta a nadie: invoca por REST el command handler de cada sistema (sección 4.4), que solo responde si acepta el comando, y reacciona al evento que ese sistema emite. Nadie más escribe el estado de la saga.

| Lo que consume | Transición de la saga | Comandos que invoca y eventos que emite |
| --- | --- | --- |
| `ShipmentDelayed` | Crea la incidencia `open` | Comando `CreatePlan` al planificador |
| `PlanCreated` | Guarda los pasos `pending` de la versión 1 | `ExecuteStep` de los pasos sin dependencias |
| `StepCompleted` (versión vigente) | Paso `completed`; desbloquea sus dependientes | `ExecuteStep` de los desbloqueados; `IncidentResolved` si ya no quedan pasos |
| `StepCompleted` de una compensación | El original pasa a `compensated` | Si no quedan `compensating`, el `ExecuteStep` de `issue_refund` que estaba retenido |
| `StepFailed` no reintentable (versión vigente) | Paso `failed` | Comando `RevisePlan` al replanificador; si ya no quedan revisiones, escalado con `max_revisions` |
| `StepFailed` de una compensación | — | Escalado con `compensation_failed`; nunca emite el reembolso |
| `PlanRevised` (versión vigente + 1) | Cancela los pendientes que el plan nuevo no mantiene y añade los nuevos pasos; para cada paso compensable completado cuyo dependiente se cancela, añade `<original>-comp` y marca el original `compensating` | `StepCancelled`, `ExecuteStep` de compensaciones y de pasos desbloqueados; `issue_refund` queda retenido mientras haya `compensating` |
| `IncidentEscalated` (de otro componente) | Escalado | Ver «Escalado» |
| Vence el plazo de un `ExecuteStep` | Paso `failed` con `error` `timeout`; si es compensable, pide además su compensación | Como un `StepFailed` no reintentable; para `issue_refund` (pivote), escalado con `step_timeout` |
| Vence el plazo de `CreatePlan` o `RevisePlan` | — | Escalado con `plan_timeout` |
| El handler rechaza un comando (respuesta 4xx) | Paso `failed` con `error` `rejected` | Como un `StepFailed` no reintentable; para `CreatePlan` o `RevisePlan`, escalado con `command_rejected` |
| Resultado o revisión de una versión ya superada, de una incidencia cerrada o tras vencer su plazo | Ninguna: se registra en el inbox como obsoleto | — |

**Escalado.** La incidencia pasa a `escalated`, la saga cancela los pasos pendientes (`StepCancelled`), pide la compensación de cada `reserve_stock` completado cuyo dependiente no se ha completado y publica `IncidentEscalated` con el motivo. Nunca emite `issue_refund`. Un escalado por `compensation_failed` no vuelve a compensar.

**Plazos.** Cada comando que invoca la saga lleva un plazo, guardado en `orchestrator.deadlines` en la misma transacción. Un hilo del orquestador revisa sus propios plazos vencidos (es su tabla, como el relay con su outbox) y trata cada uno como un mensaje más. El plazo de un `ExecuteStep` cubre los reintentos del despachador y los del ejecutor. Configuración: `SAGA_STEP_TIMEOUT_S`, `SAGA_PLAN_TIMEOUT_S` y, para los avisos aparcados del intérprete, `PENDING_NOTICE_TIMEOUT_S`. Una compensación pedida por un plazo vencido es idempotente: si el efecto original nunca ocurrió, no hace nada.

Los reintentos de un paso con fallo transitorio los hace el propio ejecutor (spec `resilience`); a la saga solo le llega el resultado final.

### 4.4 Invocación de comandos (REST)

Los comandos se invocan de forma síncrona por HTTP: cada sistema expone su command handler como `POST /commands/<Comando>`. La respuesta solo dice si el Aggregate acepta el comando; el resultado de negocio llega después como evento por Kafka, y la saga reacciona a él.

```
saga, al consumir un evento o vencer un plazo:
BEGIN
  inbox; transición en orchestrator.plan_steps
  INSERT INTO orchestrator.command_outbox (command_id, command_type, target, payload, trace_context)
  INSERT INTO orchestrator.deadlines (command_id, expires_at)
  INSERT INTO orchestrator.outbox (eventos de la saga, si los hay)
COMMIT

despachador (hilo del orquestador), sobre su propia tabla:
loop:
  leer command_outbox pendientes (FOR UPDATE SKIP LOCKED)
  POST <URL del target>/commands/<command_type>   (mismo command_id en cada intento)
    202 o 200 -> marcar entregado (aceptado, o ya aceptado antes)
    422       -> marcar rechazado; la saga lo trata como fallo no reintentable
    red o 5xx -> reintentar con backoff y jitter
  si no hay filas: dormir un intervalo corto

command handler del sistema:
BEGIN
  INSERT INTO <sistema>.inbox (command_id) ON CONFLICT DO NOTHING
    -> si ya estaba: COMMIT y devolver la respuesta guardada
  validar invariantes; escribir su estado; INSERT INTO <sistema>.outbox (eventos)
  guardar la respuesta en el inbox
COMMIT
responder 202 (aceptado) o 422 (rechazado)
```

- **Todo comando es idempotente.** El despachador reutiliza el `command_id` en cada reintento y el handler lo deduplica con su inbox en la misma transacción que su estado y sus eventos: un comando repetido devuelve la misma respuesta sin repetir estado, eventos ni efectos.
- **Ni perdidos ni duplicados.** El `command_outbox` de la saga garantiza que cada comando se invoca al menos una vez aunque el orquestador caiga tras el `COMMIT`; el inbox del sistema garantiza que se aplica una sola vez. Es el mismo par outbox/inbox que en los eventos, con transporte síncrono.
- **Un comando aceptado no cambia el estado del paso.** Lo cambia el evento de resultado (`StepCompleted`, `PlanCreated`…). Si el handler llama al LLM, lo hace antes de su transacción; una petición repetida mientras la primera está en curso puede repetir la llamada al LLM (cuesta tokens), pero el inbox impide aplicarla dos veces.
- Configuración: la URL base de cada sistema (`<SISTEMA>_URL`, p. ej. `PLANNER_URL`) y `COMMAND_HTTP_TIMEOUT_S`.

Las compensaciones se ejecutan en orden inverso a los pasos originales y usan la misma idempotencia (`effect_keys` del ejecutor) que cualquier otro paso. Si una compensación agota sus reintentos, la incidencia se escala con `compensation_failed`.

Diagrama: «Secuencia de la saga» en `docs/README.md` (en vector, `docs/diagrama-saga.pdf`).

## 5. Relay del outbox

Cada componente publica **su** outbox: el relay es parte del proceso (un hilo que hace polling de `<componente>.outbox`), sin Debezium:

```
loop:
  BEGIN
    SELECT * FROM <componente>.outbox WHERE published_at IS NULL
    ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED
    producir a eda.events (clave = correlation_id) y esperar la confirmación
    UPDATE <componente>.outbox SET published_at = now(), kafka_partition, kafka_offset
  COMMIT
  si no hay filas: dormir un intervalo corto (p. ej. 100 ms)
```

- **Al menos una vez:** si el relay cae tras producir y antes del `COMMIT`, republica; los duplicados los absorbe el inbox de cada consumidor.
- **Orden:** cada componente publica su outbox en orden y Kafka conserva el orden por partición (`incident_id`). Entre componentes el orden es causal: un evento se produce después de consumir el que lo causa.
- **Republicación:** si se pierde el topic, cada componente republica su outbox (`--replay-from`); la vista del log de `event_log` conserva el histórico para consultarlo.

## 6. Consumidores: inbox y confirmación del offset

```
poll mensaje de Kafka (enable.auto.commit = false)
validar esquema; si es inválido -> eda.dlq + <componente>.dead_letters, confirmar offset
procesar en una transacción (sección 4)
confirmar el offset solo tras el COMMIT
```

- Si el consumidor cae después del `COMMIT` y antes de confirmar el offset, Kafka reentrega y el inbox lo descarta.
- Si falla antes del `COMMIT`, no se confirma el offset y se reintenta con backoff exponencial y jitter. Agotados los reintentos, el mensaje va a la dead letter queue y, si pertenece a una incidencia abierta, el componente publica `IncidentEscalated`.

## 7. Ejecutores: inbox y `effect_keys`

El inbox evita procesar dos veces un evento. `effect_keys` (del propio ejecutor) evita ejecutar dos veces un efecto: un `ExecuteStep` nuevo puede pedir un efecto ya ejecutado, y el segundo mecanismo lo para.

```
BEGIN
  inbox (como en la sección 4)
  INSERT INTO <ejecutor>.effect_keys (key, ..., status='in_progress', locked_until = now() + plazo)
     ON CONFLICT DO NOTHING
  si no inserta fila:
     leer la clave existente
       completed    -> reutilizar el resultado, sin repetir el efecto
       in_progress y locked_until > now() -> reintentar más tarde (no confirmar offset)
       in_progress y locked_until <= now() -> tomar el relevo y ejecutar
  ejecutar el efecto
     en sus propias tablas -> en esta misma transacción (garantía fuerte)
     externo simulado (correo, transportista) -> con la misma clave hacia el servicio simulado
  UPDATE <ejecutor>.effect_keys SET status='completed', result=...
  INSERT INTO <ejecutor>.outbox (StepCompleted + evento de efecto)
COMMIT
```

Restricciones de unicidad que refuerzan la idempotencia incluso sin `effect_keys`, cada una en la tabla de su ejecutor: `claims (incident_id, order_id)`, `refunds (incident_id, step_id)`, `customer_messages (incident_id, step_id)`. Para la variante «sin idempotencia» del experimento E1 se desactivan `INBOX_ENABLED` y `EFFECT_KEYS_ENABLED` y, en la base de pruebas, se eliminan también estas restricciones.

## 8. Lado de consultas: vista del log y proyectores

El lado de lectura (CQRS) son componentes que consumen Kafka y mantienen **sus propias** vistas. No leen tablas de otros componentes, ni siquiera entre ellos.

| Componente | Esquema | Mantiene | Lo usan |
| --- | --- | --- | --- |
| `event_log` | `event_log` | `events`: la vista del log de eventos completo, ordenada, con la partición, el offset y el contexto de traza de cada mensaje | El front (panel de eventos, mapa, detalle de nodo), que hace polling sobre ella, y la auditoría |
| `projectors` (uno por vista) | `views` | `view_orders`, `view_incident_status`, `view_cost_per_incident`, `view_audit_trail`, `view_carrier_dashboard` | El front, `eda-notice interactive`, los experimentos y los sistemas que necesitan datos de otro dominio (`view_orders`, `view_cost_per_incident`) |

Cada uno guarda su posición en Kafka (`offsets`: partición y offset por consumidor) en la misma transacción que su vista, así que aplica cada mensaje exactamente una vez a la vista aunque Kafka lo reentregue.

**Orden del log.** Todos los eventos de una incidencia viajan por la misma partición (clave `incident_id`), y cada uno se publica después de consumir el que lo causa: el orden de la partición es el orden causal de la incidencia. `event_log` asigna un `seq` creciente al guardar cada evento en el orden en que lo consume, así que dentro de una incidencia el `seq` respeta ese orden causal. Entre incidencias distintas el orden es el de llegada, y no importa. La tabla es solo de añadir (triggers que rechazan `UPDATE`, `DELETE` y `TRUNCATE`) y un `event_id` repetido no se guarda dos veces.

```
BEGIN
  INSERT INTO event_log.events (seq, event_id, ..., kafka_partition, kafka_offset, trace_context)
     ON CONFLICT (event_id) DO NOTHING
  UPDATE event_log.offsets SET "offset" = <offset del mensaje> WHERE "partition" = <partición>
COMMIT
```

**Posición de las vistas.** Cada respuesta de una consulta incluye la posición aplicada de su vista (`applied`: offset por partición). El cliente que espera ver el efecto de un evento conoce su partición y su offset (los da la vista del log) y sabe si la vista ya lo incluye.

**Reconstrucción.** Kafka guarda los eventos con retención infinita (sección 9). Reconstruir una vista = vaciar sus tablas, poner sus offsets al principio y volver a consumir el topic. La vista reconstruida debe ser idéntica (misma huella).

## 9. Kafka: configuración

| Parámetro | Valor | Motivo |
| --- | --- | --- |
| Topic `eda.events` | 3 particiones, retención infinita; solo eventos | Orden por `incident_id`, reproducción posible |
| Comandos | No viajan por Kafka: REST síncrono (sección 4.4) | La saga invoca y reacciona al evento |
| Topic `eda.dlq` | 1 partición, retención infinita | Revisión manual |
| Productor (relay de cada componente) | `enable.idempotence=true`, `acks=all` | Sin duplicados por reintentos del productor |
| Consumidor | `enable.auto.commit=false`, `auto.offset.reset=earliest` | Commit tras el `COMMIT` de PostgreSQL |
| Garantía de entrega | Al menos una vez, sin transacciones de Kafka | Los duplicados los resuelven inbox y `effect_keys` |

## 10. Estructura propuesta del repositorio

```
eda-retail/
  specs/                  specs OpenSpec y diagrama
  design.md               este documento
  docker-compose.yml      Kafka y PostgreSQL
  db/init/001_schema.sql  esquema
  .env.example
  pyproject.toml
  src/eda_retail/
    config.py
    events/               modelos pydantic y catálogo de eventos
    infra/                db.py (psycopg), kafka.py y piezas reutilizables por cada componente: outbox y relay, inbox,
                          effect_keys, dead letters (cada componente las usa sobre su propio esquema)
    llm/                  cliente real y simulado, validación de salidas
    agents/               interpreter.py, planner.py, replanner.py, writer.py
    <componente>/         cada sistema con su command handler y sus tablas
    orchestrator/         orquestación del plan
    executors/            un módulo por acción
    event_log/            consumidor que guarda el log de eventos
    inventory/            pedidos, stock y reservas
    projectors/           una vista por módulo
    synthetic/            generador de avisos e inyección de fallos
    baseline_sync/        base síncrona para E6
  experiments/            E1 a E7 con informe
  tests/                  unitarios e integración
```

## 11. Arranque local

```
cp .env.example .env
docker compose up -d      # PostgreSQL con el esquema, Kafka y los topics
docker compose ps         # postgres y kafka deben estar healthy
```

El proyecto de compose se llama siempre `eda-retail` (`name:` en `docker-compose.yml`): los contenedores tienen nombre fijo (`eda-kafka`, `eda-postgres`…), así que un git worktree en otra carpeta ve y usa los mismos contenedores en lugar de intentar crear otros con el mismo nombre. `docker compose down` conserva los datos de ambos volúmenes; `docker compose down -v` los borra y vuelve a crear el esquema en el siguiente arranque.

## 12. Estado de la verificación

- El esquema SQL se ha ejecutado contra PostgreSQL 16 (incluida la columna `version` y los estados de compensación): las tablas se crean, `UPDATE`, `DELETE` y `TRUNCATE` sobre `events` se rechazan, y el inbox y `effect_keys` ignoran las inserciones duplicadas.
- `docker-compose.yml` se ha ejecutado (2026-11-13): `kafka` y `postgres` quedan `healthy` y `kafka-init` crea `eda.events` (3 particiones) y `eda.dlq` (1 partición) con retención infinita. En el primer arranque falló la creación de topics porque el broker solo anunciaba `localhost:9092`, que dentro de otro contenedor apunta a sí mismo; ahora hay dos listeners: `INTERNAL` (`kafka:19092`, para contenedores) y `EXTERNAL` (`localhost:9092`, para los procesos del host).

## 13. Observabilidad: OpenTelemetry y Jaeger

Trazas distribuidas con OpenTelemetry (OpenTracing está archivado y se fusionó en OpenTelemetry), exportadas por OTLP/HTTP a Jaeger (`jaeger` en `docker-compose.yml`, interfaz en http://localhost:16686, almacenamiento en memoria).

| Tipo | Qué se traza | Cómo |
| --- | --- | --- |
| Asíncrono | Escritura del evento → relay → Kafka → consumidor → eventos derivados, en una sola traza por incidencia | Span `<event_type> create` al escribir; su contexto W3C (`traceparent`) se guarda en el outbox del componente (`trace_context`); el relay abre `<event_type> publish` como hijo y lo pone en las cabeceras del mensaje; el consumidor abre `<event_type> process` como hijo de esas cabeceras |
| Síncrono | Cada sentencia SQL y las operaciones en proceso | Instrumentación de psycopg en `infra/db.connect` y decorador `@traced` |

- En los comandos, el despachador pone `traceparent` en la cabecera HTTP y el handler abre `<command_type> handle` como hijo.
- El contexto va en el outbox de cada componente y en las cabeceras del mensaje; `event_log` lo guarda junto al evento, y la republicación (`--replay-from`) conserva la traza original.
- Atributos de cada span: `eda.incident_id` (= `correlation_id`), `eda.event_id`, `eda.causation_id`, `eda.event_type`, `eda.producer`; los de consumo añaden `eda.outcome` (`processed`, `duplicate`) y los datos de Kafka. En Jaeger se busca una incidencia con el tag `eda.incident_id`.
- Las sentencias SQL sin operación en curso (el polling del relay) no se trazan: el sampler descarta los spans CLIENT sin padre.
- Configuración: `TRACING_ENABLED` (por defecto `true`) y `OTEL_EXPORTER_OTLP_ENDPOINT` (por defecto `http://localhost:4318`). Sin Jaeger el sistema funciona; solo se pierden las trazas.
- Cada proceso llama a `setup_tracing("<componente>", settings)` al arrancar; el componente es el `service.name` en Jaeger.
- Esquema: cada outbox tiene su columna `trace_context`. Tras `docker compose up -d`, aplicar las migraciones con `uv run alembic upgrade head`.
- Demo: `uv run python scripts/trace_demo.py` lanza aviso → intérprete → planificador en procesos separados (base `eda_demo`, topic `eda.trace-demo`) e imprime la URL de la traza.

## 14. Front web

Interfaz local para operar y observar la PoC (spec `web-front`): un backend FastAPI (`src/eda_retail/web/`, comando `eda-web`) y un front React + Vite (`web/`). Se arranca con `scripts/run-web.sh`.

| Parte | Cómo |
| --- | --- |
| Acceso | `eda-web` escucha solo en `127.0.0.1`; `LocalOriginGuard` rechaza Host y Origin que no sean de localhost (DNS rebinding). Lee PostgreSQL con `default_transaction_read_only=on` y solo el log de `event_log` y las vistas: nunca escribe ni lee las tablas de otros componentes. Los comandos los envía por `intake` (`scripts/notice.sh`) |
| Eventos | `GET /api/events` pagina `event_log.events` por `seq`, con el contexto de traza y la partición y el offset de Kafka que `event_log` guarda al consumir cada mensaje. `GET /api/events/stream` es SSE: sondea cada 0,5 s tras el último `seq` enviado y reanuda con `Last-Event-ID` |
| Emisor y destino | Emisor = `producer`. Destino = los componentes que declaran ese tipo de evento (`agents.cli.AGENTS`, `orchestrator.runner.EVENT_TYPES`), con los proyectores siempre al final |
| Mapa | Topología estática (`web/topology.py`, con las invocaciones REST de la saga como aristas) más el estado de cada proceso (`pgrep -f`, mismos patrones que `run-agents.sh`) y de PostgreSQL, Kafka y Ollama, con 2 s como máximo por comprobación |
| Comandos | `POST /api/commands` lanza `scripts/notice.sh` con una lista blanca de argumentos y sin shell; una ejecución a la vez, 15 minutos como máximo, salida por SSE. `interactive` corre en una pseudoterminal y recibe líneas por `POST /api/commands/{run_id}/input`. Al parar el backend se termina el grupo de procesos en curso |
| Trazas | Enlace a Jaeger en la cabecera y por evento (`<JAEGER_UI_URL>/trace/<trace_id>`, con `trace_id` del contexto de traza que guarda `event_log`) |

- Configuración: `WEB_PORT` (backend; Vite reenvía `/api` al mismo puerto), `WEB_FREE_TEXT` y `WEB_INTERACTIVE` (`true` para ofrecer `send --text` e `interactive`; por defecto deshabilitados, porque la PoC es un ejemplo académico) y `JAEGER_UI_URL` (por defecto `http://localhost:16686`; si no es http(s) se usa el valor por defecto).
- Tests: pytest para el backend (los de `tests/integration/test_web_events.py`, contra PostgreSQL) y Vitest + Testing Library para el front, con un `EventSource` falso. El hook `web` de pre-commit lanza `npm test` y `npm run typecheck`.
