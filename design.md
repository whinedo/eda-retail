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

Principio de la PoC: **un componente solo escribe en sus propias tablas, y los componentes solo se comunican por Kafka**. Hay una única base de datos de PostgreSQL (vale para una PoC), pero cada componente tiene su propio esquema (`intake.*`, `planner.*`…) y sus tablas no se relacionan con las de otro: ni claves foráneas, ni joins, ni lecturas de tablas ajenas para decidir. Lo que un componente necesita saber de otro le llega en un evento y lo guarda en una tabla suya (réplica local, *event-carried state transfer*).

Cada componente que recibe comandos tiene su **propio command handler**: valida el comando contra su estado, escribe ese estado y los eventos en **su outbox** en la misma transacción (*transactional outbox*), y un relay publica ese outbox en Kafka. Las lecturas para personas (front, CLI) van por las vistas (CQRS); los componentes no deciden leyendo vistas.

| Componente | Esquema | Escribe (solo) | Réplicas locales (de eventos) |
| --- | --- | --- | --- |
| `intake` (entrada de avisos: `eda-notice`, front) | `intake` | `carrier_notices`, `outbox` | — |
| `interpreter` (agente) | `interpreter` | `interpretations`, `llm_calls`, `inbox`, `outbox` | `orders`, `budgets` |
| `planner` (agente) | `planner` | `plans`, `llm_calls`, `inbox`, `outbox` | `orders`, `incidents` (causa, gravedad), `budgets` |
| `replanner` (agente) | `replanner` | `revisions`, `llm_calls`, `inbox`, `outbox` | `incidents` (plan vigente, revisiones), `budgets` |
| `orchestrator` | `orchestrator` | `incidents`, `plan_steps`, `inbox`, `outbox` | — |
| `executor-<acción>` | `executor_<acción>` | su tabla de efectos (`customer_messages`, `claims`, `shipments`, `refunds`), `effect_keys`, `llm_calls` (redactor), `inbox`, `outbox` | `orders`, `budgets` |
| `inventory` (pedidos y stock; ejecuta `reserve_stock` y `release_stock`) | `inventory` | `orders`, `stock`, `reservations`, `effect_keys`, `inbox`, `outbox` | — |
| `event_log` (consulta) | `event_log` | `events` (vista del log), `offsets` | — |
| `projectors` (consulta) | `views` | `view_*`, `offsets` | — |
| `baseline_sync` (base síncrona de E6) | `baseline` | sus tablas | — |

Cada esquema tiene también su `dead_letters`. Las réplicas locales solo guardan lo que el componente necesita (p. ej. el importe de un pedido para validar un reembolso) y se actualizan en la misma transacción que el inbox.

## 3. Componentes, eventos y grupos de consumidores

Cada componente es un proceso con su propio `group.id` en Kafka. Todos los eventos viajan por el topic `eda.events` con el `event_type` en una cabecera, y cada consumidor ignora los que no le interesan.

| Componente | Consume | Produce | Tipo |
| --- | --- | --- | --- |
| `intake` | — (recibe el comando `RegisterCarrierNotice`) | `CarrierNoticeReceived` | Código |
| `inventory` | `StepRequested` de `reserve_stock` y `release_stock` (y el comando `SeedSampleData`) | `OrderRegistered`, `StockReserved`, `StockReleased`, `StepCompleted`, `StepFailed` | Código |
| `interpreter` | `CarrierNoticeReceived`, `OrderRegistered`, `LlmCallRecorded` | `ShipmentDelayed` o `IncidentEscalated`, `LlmCallRecorded` | LLM + su command handler |
| `planner` | `ShipmentDelayed`, `OrderRegistered`, `LlmCallRecorded` | `PlanCreated` o `IncidentEscalated`, `LlmCallRecorded` | LLM + su command handler |
| `orchestrator` | `ShipmentDelayed`, `PlanCreated`, `PlanRevised`, `StepCompleted`, `StepFailed`, `IncidentEscalated` | `StepRequested`, `StepCancelled`, `IncidentResolved`, `IncidentEscalated` | Código |
| `replanner` | `StepFailed`, `PlanCreated`, `PlanRevised`, `LlmCallRecorded` | `PlanRevised` o `IncidentEscalated`, `LlmCallRecorded` | LLM + su command handler |
| `executor-<acción>` | `StepRequested` de su acción, `OrderRegistered`, `LlmCallRecorded` | `StepCompleted`, `StepFailed`, el evento de efecto, `LlmCallRecorded` | Código (el redactor se invoca desde aquí) |
| `event_log` (consulta) | todos | — (mantiene la vista del log para el front) | Código |
| `projectors` (consulta) | los que necesita cada vista | — (mantienen sus vistas) | Código |

`StepRequested` lleva en su carga lo que el ejecutor necesita del plan (pedido, causa, importe), para que no tenga que consultar el estado del orquestador.

## 4. Flujo de escritura: un command handler por componente

Cada escritura de un componente es una transacción de PostgreSQL sobre **su** esquema:

```
BEGIN
  1. INSERT INTO <componente>.inbox (event_id) ON CONFLICT DO NOTHING
       -> si no inserta fila: COMMIT, confirmar offset, terminar (duplicado)
  2. actualizar las réplicas locales que alimenta el evento
  3. si hay decisión (LLM ya llamado antes de la transacción): el command handler del
     componente valida el comando contra su estado y sus réplicas
  4. actualizar su estado (p. ej. orchestrator.plan_steps)
  5. INSERT INTO <componente>.outbox (event_id, envelope, trace_context, ...)
COMMIT
confirmar el offset en Kafka
```

La llamada al LLM se hace **antes** de la transacción. Si el proceso cae entre la llamada y el `COMMIT`, el evento se reentrega y se repite la llamada: cuesta tokens, pero no duplica efectos.

### 4.1 CQRS: comandos, consistencia y consultas

- **Command handlers:** uno por componente que recibe comandos (`intake`, `interpreter`, `planner`, `replanner`, `orchestrator`, `inventory` y cada ejecutor). Cada uno valida solo contra su estado y sus réplicas: el del planificador valida el plan (acciones, ciclos, tope de reembolso con el importe de su réplica de `orders`); el del orquestador aplica la máquina de estados de la saga. No hay un handler común ni una tabla que escriban varios.
- **Consistencia:** el estado de la incidencia (`incidents`, `plan_steps`) tiene un único dueño, el orquestador, que procesa los eventos de una incidencia en orden (misma partición), así que no necesita concurrencia optimista entre procesos. Los eventos que llegan tarde (de una versión de plan anterior) los descarta el orquestador comparando `plan_version` (spec `incident-planning`, "Eventos obsoletos").
- **Presupuesto del LLM:** cada llamada se registra en el `llm_calls` del componente y se publica como `LlmCallRecorded`. Los componentes que llaman al LLM mantienen su réplica `budgets` (tokens y llamadas por incidencia) consumiendo esos eventos y comprueban el límite antes de llamar.
- **Consultas:** las vistas (`event_log.events` y `views.view_*`, sección 8) son para las personas: el front, `eda-notice interactive` y los experimentos. Cada respuesta incluye la posición aplicada de la vista. Ningún componente decide leyendo una vista: lo que necesita lo replica de los eventos.

![CQRS](docs/diagrama-cqrs.png)

### 4.3 Consistencia eventual

Cada componente ve el estado de los demás a través de eventos, así que lo ve **con retraso**: sus réplicas locales y las vistas van por detrás de los dueños. No es un fallo que haya que evitar, es la condición normal de una EDA, y cada componente la trata de forma explícita:

| Situación | Ejemplo | Cómo se trata |
| --- | --- | --- |
| La réplica aún no tiene un dato | El intérprete recibe un aviso de `P-1007` antes que su `OrderRegistered` | El evento no se confirma y se reintenta con backoff (el dato llegará); agotados los reintentos, `IncidentEscalated` con `no_valid_orders`. Nunca se lee la tabla del dueño |
| Eventos de productores distintos llegan desordenados | `StepCompleted` de un paso de la versión 1 llega después de `PlanRevised` a la versión 2 | El orquestador compara `plan_version` y aplica la regla de "Eventos obsoletos"; el evento se registra en el inbox y no cambia el estado |
| Duplicados | El relay republica tras una caída | Inbox por componente y `effect_keys` en los ejecutores |
| Réplica de presupuesto atrasada | Dos agentes llaman al LLM a la vez para la misma incidencia | El límite se comprueba con la réplica `budgets`; el exceso queda acotado a las llamadas en curso, se registra en `LlmCallRecorded` y la siguiente comprobación lo ve. Es una cota blanda documentada, no una garantía fuerte |
| Vista atrasada respecto a una acción del usuario | El front envía un aviso y la vista `incident_status` aún no lo refleja | Cada consulta devuelve la posición aplicada de la vista; el front y `eda-notice interactive` muestran «actualizando» hasta que la posición incluye el evento esperado, y nunca leen el estado del dueño para «saltarse» el retraso |
| Decisión con información vieja | El replanificador revisa un plan que el orquestador ya ha cerrado | El dueño (orquestador) valida al consumir `PlanRevised`: si la incidencia está cerrada o la versión no encaja, lo descarta y lo deja en el log como obsoleto |

Regla general: **quien decide sobre un estado es su dueño**, al consumir el evento. Los demás proponen con lo que saben (sus réplicas) y aceptan que su propuesta pueda llegar tarde.

### 4.2 Saga: operaciones y compensaciones

La orquestación gobierna la saga y es la única que escribe `orchestrator.plan_steps`. La categoría de cada acción y su compensación están en código (tabla en `specs/incident-planning`):

| Acción | Compensación | Notas |
| --- | --- | --- |
| `notify_customer` | Ninguna | |
| `file_claim` | Ninguna | La reclamación sigue siendo válida aunque se reembolse |
| `reserve_stock` | `release_stock` | Única acción compensable; la ejecuta `inventory` |
| `reissue_shipment` | Ninguna | |
| `issue_refund` | Ninguna | Paso pivote: se emite el último y solo sin compensaciones pendientes |

La saga es **reactiva**: es un *process manager* dentro del orquestador. Cada transición ocurre al consumir un evento, en una transacción que actualiza `orchestrator.plan_steps` y deja los eventos siguientes en su outbox. Nadie más escribe el estado de la saga, y el orquestador no consulta ni espera a nadie: los ejecutores, el replanificador y los agentes le informan con eventos.

| Evento consumido | Transición de la saga | Eventos que emite |
| --- | --- | --- |
| `ShipmentDelayed` | Crea la incidencia `open` | — |
| `PlanCreated` | Guarda los pasos `pending` de la versión 1 | `StepRequested` de los pasos sin dependencias |
| `StepCompleted` (versión vigente) | Paso `completed`; desbloquea sus dependientes | `StepRequested` de los desbloqueados; `IncidentResolved` si ya no quedan pasos |
| `StepCompleted` de una compensación | El original pasa a `compensated` | Si no quedan `compensating`, el `StepRequested` de `issue_refund` que estaba retenido |
| `StepFailed` no reintentable (versión vigente) | Paso `failed`; la saga espera una revisión (el replanificador reacciona al mismo evento) | `IncidentEscalated` con `max_revisions` si ya no quedan revisiones |
| `StepFailed` de una compensación | — | `IncidentEscalated` con `compensation_failed`; nunca emite el reembolso |
| `PlanRevised` (versión vigente + 1) | Cancela los pendientes que el plan nuevo no mantiene y añade los nuevos pasos; para cada paso compensable completado cuyo dependiente se cancela, añade `<original>-comp` y marca el original `compensating` | `StepCancelled`, `StepRequested` de compensaciones y de pasos desbloqueados; `issue_refund` queda retenido mientras haya `compensating` |
| `IncidentEscalated` (de otro componente) | Incidencia `escalated`; cancela los pasos pendientes | `StepCancelled` |
| Resultado o revisión de una versión ya superada, o de una incidencia cerrada | Ninguna: se registra en el inbox como obsoleto | — |

Los reintentos de un paso con fallo transitorio los hace el propio ejecutor (spec `resilience`); a la saga solo le llega el resultado final.

Las compensaciones se ejecutan en orden inverso a los pasos originales y usan la misma idempotencia (`effect_keys` del ejecutor) que cualquier otro paso. Si una compensación agota sus reintentos, la incidencia se escala con `compensation_failed`.

![Saga](docs/diagrama-saga.png)

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

El inbox evita procesar dos veces un evento. `effect_keys` (del propio ejecutor) evita ejecutar dos veces un efecto: un `StepRequested` nuevo puede pedir un efecto ya ejecutado, y el segundo mecanismo lo para.

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
| `event_log` | `event_log` | `events`: la vista del log completo, ordenada, con la partición, el offset y el contexto de traza de cada mensaje | El front (panel de eventos, mapa, detalle de nodo) y la auditoría |
| `projectors` (uno por vista) | `views` | `view_incident_status`, `view_cost_per_incident`, `view_audit_trail`, `view_carrier_dashboard` | El front, `eda-notice interactive` y los experimentos |

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
| Topic `eda.events` | 3 particiones, retención infinita | Orden por `incident_id`, reproducción posible |
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
    <componente>/         cada componente con su command handler, sus tablas y sus réplicas
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
| Emisor y destino | Emisor = `producer`. Destino = los componentes que declaran ese tipo de evento (`agents.cli.AGENTS`, `orchestrator.runner.EVENT_TYPES`, `executor-<acción>` para `StepRequested`), con los proyectores siempre al final |
| Mapa | Topología estática (`web/topology.py`) más el estado de cada proceso (`pgrep -f`, mismos patrones que `run-agents.sh`) y de PostgreSQL, Kafka y Ollama, con 2 s como máximo por comprobación |
| Comandos | `POST /api/commands` lanza `scripts/notice.sh` con una lista blanca de argumentos y sin shell; una ejecución a la vez, 15 minutos como máximo, salida por SSE. `interactive` corre en una pseudoterminal y recibe líneas por `POST /api/commands/{run_id}/input`. Al parar el backend se termina el grupo de procesos en curso |
| Trazas | Enlace a Jaeger en la cabecera y por evento (`<JAEGER_UI_URL>/trace/<trace_id>`, con `trace_id` del contexto de traza que guarda `event_log`) |

- Configuración: `WEB_PORT` (backend; Vite reenvía `/api` al mismo puerto), `WEB_FREE_TEXT` y `WEB_INTERACTIVE` (`true` para ofrecer `send --text` e `interactive`; por defecto deshabilitados, porque la PoC es un ejemplo académico) y `JAEGER_UI_URL` (por defecto `http://localhost:16686`; si no es http(s) se usa el valor por defecto).
- Tests: pytest para el backend (los de `tests/integration/test_web_events.py`, contra PostgreSQL) y Vitest + Testing Library para el front, con un `EventSource` falso. El hook `web` de pre-commit lanza `npm test` y `npm run typecheck`.
