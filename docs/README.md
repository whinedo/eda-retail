# PoC académica de EDA con agentes: incidencias logísticas

Specs en estilo OpenSpec. Cada capacidad vive en `specs/<capacidad>/spec.md` con un `## Purpose`, los requisitos (`### Requirement:`, redactados con SHALL/MUST) y al menos un escenario GIVEN/WHEN/THEN (`#### Scenario:`) por requisito.

## Contexto

Una tienda online recibe avisos de retraso de los transportistas, en texto libre o por API. Ante cada incidencia, un sistema de agentes decide qué hacer (avisar al cliente, reclamar al transportista, reenviar, reponer o reembolsar) y lo ejecuta mediante eventos. La PoC compara esta arquitectura con una base síncrona y mide fiabilidad, recuperación, calidad del plan, coste y latencia.

Regla de diseño: **el LLM propone y el código dispone**. Ningún LLM ejecuta efectos ni escribe directamente en el log.

## Diagrama de agentes

```mermaid
flowchart LR
  IN["Aviso del transportista<br/>eda-notice o front"]
  INK["intake<br/>(agregado)"]
  BUS[("Kafka eda.events<br/>solo eventos")]
  INT["Intérprete<br/>(LLM)"]
  ORQ["Saga<br/>orquestador"]
  PLN["Planificador<br/>(LLM)"]
  RPL["Replanificador<br/>(LLM)"]
  EXE["Ejecutores e inventory<br/>(agregados)"]
  RED["Agente redactor<br/>aviso y reclamación"]
  IDS[("effect_keys<br/>de cada ejecutor")]
  PRJ["Proyectores<br/>(consulta CQRS)"]
  VW[("Proyecciones<br/>view_orders, cost_per_incident…")]
  ELOG["event_log<br/>(consulta CQRS)"]
  WEB["Front web"]

  IN == "(1) RegisterCarrierNotice" ==> INK
  INK -- "(2) CarrierNoticeReceived" --> BUS
  BUS -- "(2)" --> INT
  INT -- "(3) ShipmentDelayed" --> BUS
  BUS -- "eventos (3) (5) (8) (10)" --> ORQ
  ORQ == "(4) CreatePlan" ==> PLN
  PLN -- "(5) PlanCreated" --> BUS
  ORQ == "(6) ExecuteStep" ==> EXE
  EXE <-- "(7) borrador validado" --> RED
  EXE <-- "clave" --> IDS
  EXE -- "(8) StepCompleted o StepFailed<br/>y el evento de efecto" --> BUS
  ORQ == "(9) RevisePlan" ==> RPL
  RPL -- "(10) PlanRevised, vuelve a 6" --> BUS
  ORQ -- "(11) IncidentResolved<br/>o IncidentEscalated" --> BUS
  BUS -- "(a)" --> PRJ
  PRJ -- "(a)" --> VW
  INT -. "(b) consulta" .-> VW
  PLN -. "(b) consulta" .-> VW
  BUS -- "(c)" --> ELOG
  WEB -. "(c) polling" .-> ELOG

  classDef llm fill:#E3F0FF,stroke:#2B7DE0,color:#1d1d1f
  classDef code fill:#E6F6EC,stroke:#2E9E5B,color:#1d1d1f
  classDef data fill:#F1F1EE,stroke:#8A8A80,color:#1d1d1f
  classDef ext fill:#FFF3E0,stroke:#E08A2B,color:#1d1d1f
  class INT,PLN,RED,RPL llm
  class INK,ORQ,EXE,PRJ,ELOG code
  class BUS,IDS,VW data
  class IN,WEB ext
```

Azul: agentes con LLM (proponen). Verde: código determinista (dispone). Gris: datos. Naranja: entrada y front. Flecha gruesa: comando REST síncrono e idempotente (`command_id`), que el sistema que lo recibe acepta o rechaza. Flecha fina: evento por Kafka. Punteada: consulta de solo lectura a una proyección. Los pasos (1) a (11) siguen una incidencia: la saga invoca un comando, el sistema escribe su estado y su evento en su outbox en una transacción, y la saga reacciona al evento cuando le llega. (a) a (c) son el lado de lectura. Cada sistema solo escribe en su esquema; los agentes con LLM nunca escriben eventos ni ejecutan efectos.

### Secuencia de la saga

```mermaid
sequenceDiagram
  participant Orq as Saga<br/>(orquestador)
  participant Pl as Planificador<br/>(LLM)
  participant Av as Ejecutor<br/>aviso
  participant Rc as Ejecutor<br/>reclamación
  participant St as inventory<br/>(stock)
  participant En as Ejecutor<br/>envío
  participant Rf as Ejecutor<br/>reembolso
  participant Rp as Replanificador<br/>(LLM)
  Note over Orq,Rp: Flecha continua: comando REST síncrono con command_id (idempotente). La respuesta 202 solo confirma que el sistema lo acepta.<br/>Flecha abierta: evento por Kafka. La saga solo cambia de estado al consumir un evento, y cada comando tiene un plazo.
  Orq->>Pl: (1) CreatePlan
  Pl-->>Orq: 202
  Pl-)Orq: (2) PlanCreated v1 (validado por código):<br/>notify_customer, file_claim, reserve_stock, reissue_shipment
  par pasos independientes en paralelo
    Orq->>Av: (3.a) ExecuteStep notify_customer
  and
    Orq->>Rc: (3.b) ExecuteStep file_claim
  and
    Orq->>St: (3.c) ExecuteStep reserve_stock
  end
  Av-)Orq: (4.a) StepCompleted (CustomerNotified)
  Rc-)Orq: (4.b) StepCompleted (ClaimFiled)
  St-)Orq: (4.c) StepCompleted (StockReserved)
  Orq->>En: (5) ExecuteStep reissue_shipment<br/>(depende de reserve_stock)
  alt Camino feliz
    En-)Orq: (6) StepCompleted (ShipmentReissued)
    Orq->>Orq: (7) IncidentResolved, outcome resolved
  else El reenvío falla (carrier_rejected, no reintentable) o vence su plazo
    En-)Orq: (6) StepFailed
    Orq->>Rp: (7) RevisePlan (plan vigente, paso fallido, revisiones)
    Rp-->>Orq: 202
    Rp-)Orq: (8) PlanRevised v2: reembolso en lugar de reenvío<br/>(validado por código: sin compensaciones, importe dentro del máximo)
    Note over Orq,St: (9) El código de la saga deriva la compensación de reserve_stock<br/>y retiene issue_refund hasta que termine
    Orq->>St: (10) ExecuteStep release_stock<br/>(step_id reserve_stock-comp)
    alt La compensación termina bien
      St-)Orq: (11) StepCompleted (StockReleased)
      Orq->>Rf: (12) ExecuteStep issue_refund<br/>(paso pivote, sin compensación)
      alt El reembolso termina bien
        Rf-)Orq: (13) StepCompleted (RefundIssued)
        Orq->>Orq: (14) IncidentResolved, outcome refunded
      else El reembolso falla
        Rf-)Orq: (13) StepFailed
        Orq->>Orq: (14) nueva revisión del plan o escalado<br/>si se agotan las revisiones
      end
    else La compensación agota sus reintentos
      St-)Orq: (11) StepFailed
      Orq->>Orq: (12) IncidentEscalated, compensation_failed<br/>(no se invoca el reembolso)
    end
  end
```

Operaciones y compensaciones de una pérdida de paquete, con los caminos de éxito, de reembolso tras compensar y de escalado. Si vence el plazo de un comando, la saga lo trata como un fallo no reintentable del paso (y pide su compensación si es compensable); si vence el de `CreatePlan`, `RevisePlan` o `issue_refund`, escala. Al escalar cancela los pasos pendientes y compensa las reservas cuyo dependiente no se completó.

### CQRS

```mermaid
flowchart LR
  subgraph CMD["Lado de comandos (escritura)"]
    SG["Saga<br/>(orquestador)"]
    AG["Agregado<br/>POST /commands/#lt;Comando#gt;"]
    TX[("PostgreSQL, esquema del sistema<br/>inbox, estado y outbox<br/>en una transacción")]
  end
  K["Kafka<br/>eda.events<br/>solo eventos"]
  subgraph QRY["Lado de consultas (lectura)"]
    P["Proyectores<br/>idempotentes"]
    V[("views.view_*<br/>y offsets")]
    EL["event_log"]
    EV[("event_log.events<br/>y offsets")]
    Q["Consultas<br/>front, CLI, experimentos<br/>y otros sistemas"]
  end
  SG -- "(1) comando con command_id<br/>(idempotente, con reintentos)" --> AG
  AG -- "(2) inbox, estado y eventos" --> TX
  AG -. "202 aceptado<br/>o 422 rechazado" .-> SG
  TX -- "(3) relay de su outbox" --> K
  K -- "(4) evento de resultado" --> SG
  K -- "(5) consumen eventos,<br/>offset por partición" --> P
  K -- "(5)" --> EL
  P -- "(6) vista y offset<br/>en una transacción" --> V
  EL -- "(6)" --> EV
  Q -- "(7) solo lectura,<br/>responde con applied" --> V
  Q -- "(7) polling del front" --> EV
  classDef code fill:#E6F6EC,stroke:#2E9E5B,color:#1d1d1f
  classDef data fill:#F1F1EE,stroke:#8A8A80,color:#1d1d1f
  classDef ext fill:#FFF3E0,stroke:#E08A2B,color:#1d1d1f
  class SG,AG,P,EL code
  class TX,V,K,EV data
  class Q ext
```

Lado de comandos (escritura) y lado de consultas (lectura). Los pasos (1) a (4) son el camino de escritura: la saga invoca el comando, el sistema lo acepta o lo rechaza, y el resultado vuelve como evento. Los pasos (5) a (7) son el de lectura: los componentes de consulta consumen Kafka, y las personas y los sistemas consultan sus proyecciones, que van con retraso (consistencia eventual).

## Patrones de la guía que se ejercitan

| Patrón (guía «Patrones aplicados al desarrollo de agentes») | Capacidad |
| --- | --- |
| Planning orientado a eventos (9.2), Plan-and-Execute (3.3), Saga (6.3) | `incident-planning` |
| CQRS (9.4), Outbox e Inbox (6.4) | `event-backbone`, `cqrs-views`, `idempotency` |
| Idempotency key (6.2) | `idempotency` |
| Retry con backoff y dead letter (6.6), Backpressure (5.7), Durable execution (6.1) | `resilience` |
| Guardrails (3.9), Bounded autonomy (3.10) | `llm-boundaries` |
| Tracing distribuido (6.10) | `observability-audit` |
| Evals como quality gates (8.1) | `experiments` |

## Capacidades

| Capacidad | Resumen |
| --- | --- |
| `event-backbone` | Formato del evento, bus, orden y entrega, invocación de comandos y propiedad de los datos |
| `incident-interpretation` | Agente intérprete: del aviso en texto libre a `ShipmentDelayed` |
| `incident-planning` | Planner, replanner, versionado del plan, dependencias, compensación |
| `step-execution` | Ejecutores de pasos y sus efectos |
| `idempotency` | Claves, almacén de claves y reglas por efecto |
| `cqrs-views` | Command handlers de cada sistema, proyecciones, consultas, consistencia eventual y reconstrucción |
| `resilience` | Reintentos, dead letter, recuperación y backpressure |
| `llm-boundaries` | Validación de las salidas del LLM y límites de autonomía |
| `observability-audit` | Trazas, coste por incidencia y auditoría |
| `synthetic-data` | Generador de avisos e inyección de fallos |
| `experiments` | Hipótesis, experimentos, métricas y base síncrona |

## Catálogo de eventos

| Evento | Productor | Campos propios del payload |
| --- | --- | --- |
| `CarrierNoticeReceived` | Entrada | `carrier_notice_id`, `carrier`, `raw_text` |
| `ShipmentDelayed` | Agente intérprete | `incident_id`, `order_ids`, `cause`, `estimated_delay_hours`, `severity` |
| `PlanCreated` | Agente planificador | `incident_id`, `plan_version`, `steps[]` |
| `PlanRevised` | Replanner | `incident_id`, `plan_version`, `steps[]`, `reason` |
| `OrderRegistered` | `inventory` | `order_id`, `customer_ref`, `amount` |
| `StepCompleted` (v2) | Ejecutor o `inventory` | `step_id`, `plan_version`, `result` |
| `StepFailed` (v2) | Ejecutor o `inventory` | `step_id`, `plan_version`, `error`, `retryable`, `attempts` |
| `StepCancelled` | Saga (orquestador) | `incident_id`, `plan_version`, `step_id` |
| `CustomerNotified` | Ejecutor | `order_id`, `message_id` |
| `ClaimFiled` | Ejecutor | `order_id`, `claim_id` |
| `StockReserved` | `inventory` | `order_id`, `warehouse`, `reservation_id` |
| `StockReleased` | `inventory` | `reservation_id` |
| `ShipmentReissued` | Ejecutor | `order_id`, `new_shipment_id` |
| `RefundIssued` | Ejecutor | `order_id`, `amount`, `refund_id` |
| `IncidentResolved` | Saga (orquestador) | `incident_id`, `outcome` |
| `IncidentEscalated` | Saga y agentes | `incident_id`, `reason` |
| `LlmCallRecorded` | Agentes con LLM | `incident_id`, `agent`, `model`, `tokens_in`, `tokens_out`, `latency_ms`, `validation_passed`, `attempt` |

## Catálogo de comandos

| Comando | Lo invoca | Lo atiende | Eventos que puede producir |
| --- | --- | --- | --- |
| `RegisterCarrierNotice` | `eda-notice`, front | `intake` | `CarrierNoticeReceived` |
| `SeedSampleData` | `eda-notice` | `inventory` | `OrderRegistered` |
| `OpenIncident` | El propio intérprete, al consumir `CarrierNoticeReceived` | `interpreter` | `ShipmentDelayed` o `IncidentEscalated` |
| `CreatePlan` | La saga (orquestador), al consumir `ShipmentDelayed` | `planner` | `PlanCreated` o `IncidentEscalated` |
| `RevisePlan` | La saga, al consumir un `StepFailed` no reintentable o vencer el plazo de un paso | `replanner` | `PlanRevised` o `IncidentEscalated` |
| `ExecuteStep` (`incident_id`, `plan_version`, `step_id`, `action`, `params`, `idempotency_key`) | La saga | `executor-<acción>`, o `inventory` para `reserve_stock` y `release_stock` | `StepCompleted` o `StepFailed` y el evento de efecto |
| `RecordLlmCall` | Cada componente con LLM, dentro de su propio handler | El mismo componente | `LlmCallRecorded` |

El orquestador no recibe comandos: es el process manager de la saga, consume eventos e invoca `CreatePlan`, `RevisePlan` y `ExecuteStep`. Cada sistema expone sus comandos por REST (`POST /commands/<Comando>`) y la saga los invoca de forma síncrona desde su `command_outbox`; los comandos nunca viajan por Kafka y su resultado llega como evento. Todo comando es idempotente: el handler lo deduplica con su inbox por `command_id`. Todos llevan `command_id` y como `causation_id` el evento que los originó; los de la saga llevan la `plan_version` vigente, y un plazo guardado en `orchestrator.deadlines`.

## Procesos

> Cada componente tiene su esquema de PostgreSQL, su API de comandos (si atiende comandos), su outbox con su propio relay y su proceso (`odd/tasks/eda-component-ownership.md`). El esquema público ya no tiene tablas del sistema (migración 0017).

### Puesta en marcha

Con Docker y Ollama instalados, y `.env` creado a partir de `.env.example` (fija `LLM_MODEL` a un modelo descargado, p. ej. `qwen2.5:7b-instruct`):

```bash
scripts/start-all.sh           # terminal 1: contenedores, migraciones, todos los procesos y el front web
scripts/notice.sh              # terminal 2: CLI interactivo de avisos (o el menú del front)
tail -f logs/*.log             # terminal 3: seguir el flujo
scripts/stop-all.sh            # al terminar: para los procesos, el front y los contenedores
```

`scripts/start-all.sh` equivale a `docker compose up -d` más `scripts/run-agents.sh` y `scripts/run-web.sh`, que también se pueden lanzar por separado. El front queda en `http://localhost:5173`.

En el CLI interactivo, `s` carga los datos de ejemplo (pedidos `P-1001`…`P-1005` y su stock) y `1`…`4` envían un aviso de ejemplo; después se muestra cómo avanza la incidencia hasta que se resuelve o se escala. Las trazas completas están en Jaeger: `http://localhost:16686`.

El front web (`scripts/run-web.sh`, spec `web-front`) hace lo mismo desde el navegador: el menú de la izquierda lanza los comandos de `scripts/notice.sh`, el mapa muestra los agentes, servicios e integraciones con su estado y resalta el camino de cada evento nuevo, y el panel inferior sigue los eventos en vivo (emisor, destino, partición/offset de Kafka y enlace a su traza en Jaeger). Al pulsar un nodo se ven su detalle y, en los agentes, sus métricas del LLM. El enlace «Analítica» de la cabecera abre la vista de negocio: cuántos pedidos afectados por incidencias hay en cada estado (envío retrasado, en proceso de solución, solucionada, reembolsada, escalada) y la tabla de pedidos, en vivo. Solo escucha en localhost y lee la base de datos con una conexión de solo lectura. El front no ofrece `interactive` (`WEB_INTERACTIVE=true` en `.env` lo vuelve a ofrecer) ni texto libre, ni `send --text` ni la opción `t` de `interactive`, que se lanza con `--no-free-text` (`WEB_FREE_TEXT=true` lo vuelve a ofrecer).

### Scripts

| Script | Qué hace |
| --- | --- |
| `scripts/start-all.sh [--logs DIR] [--no-web]` | Arranca todo: `docker compose up -d`, espera a que postgres y kafka estén healthy, aplica las migraciones y lanza `run-agents.sh` y `run-web.sh` (sin el front con `--no-web`). Ctrl+C para los procesos y el front y deja los contenedores; si uno de los dos scripts termina, para el otro |
| `scripts/run-agents.sh [--logs DIR] [--no-migrate]` | Comprueba que kafka y postgres están en marcha y que Ollama tiene `LLM_MODEL`, aplica las migraciones y arranca todos los procesos cargando `.env`; deja un log por proceso en `logs/` y los para todos con Ctrl+C o si alguno termina |
| `scripts/notice.sh [argumentos de eda-notice]` | Abre `eda-notice` (por defecto, el modo interactivo) con la misma configuración (`.env`) |
| `scripts/run-web.sh [--logs DIR]` | Arranca el front web: `eda-web` en `127.0.0.1:WEB_PORT` (8000 por defecto) y Vite en `http://localhost:5173`; la primera vez instala las dependencias de `web/` (`npm ci`, requiere Node 24). Carga `.env` (`WEB_PORT`, `JAEGER_UI_URL`) y para los dos con Ctrl+C o si uno termina |
| `scripts/stop-all.sh [--dry-run] [--keep-docker]` | Para todos los procesos de la PoC (también el front, su backend y Vite) y después los contenedores (`docker compose stop`, sin borrar datos); `--keep-docker` deja los contenedores y `--dry-run` solo muestra qué pararía |

### Comandos

Cada componente es un proceso independiente; los scripts anteriores los lanzan, pero también se pueden arrancar uno a uno. Todos los comandos `eda-*` cargan solos el `.env` del directorio de trabajo, sin pisar las variables ya definidas (no hace falta `--env-file`):

| Comando | Qué hace |
| --- | --- |
| `uv run eda-event-log` | Consulta de CQRS `event_log`: consume `eda.events` y guarda cada evento en `event_log.events` con un `seq` creciente, su partición, su offset y su contexto de traza; su posición va en `event_log.offsets`. El front lee el log de ahí |
| `uv run eda-intake` | Sistema `intake`: atiende `RegisterCarrierNotice` en `POST /commands/RegisterCarrierNotice` (escucha en `INTAKE_URL`, por defecto `http://127.0.0.1:8101`), guarda el aviso en `intake.carrier_notices` y publica `CarrierNoticeReceived` con el relay de `intake.outbox`. `eda-notice` y el front registran los avisos aquí |
| `uv run eda-inventory` | Sistema `inventory`: atiende `SeedSampleData` y `ExecuteStep` de `reserve_stock` y `release_stock` (escucha en `INVENTORY_URL`, por defecto `http://127.0.0.1:8102`) sobre `inventory.orders`, `stock` y `reservations`, y publica `OrderRegistered`, `StockReserved`, `StockReleased`, `StepCompleted` y `StepFailed` con el relay de `inventory.outbox`. Lo invoca la saga |
| `uv run eda-projector` | Mantiene las vistas `views.view_*` (`orders`, `incident_status`, `cost_per_incident`, `audit_trail`, `carrier_dashboard`, `order_status`) consumiendo `eda.events` de Kafka, cada proyector con su offset por partición en `views.offsets` |
| `uv run eda-projector rebuild <vista>` | Vacía una vista y su posición, vuelve a consumir `eda.events` desde el principio e imprime la huella antes y después, si son idénticas y el tiempo de reconstrucción (sale con código 1 si difieren). Se lanza con `eda-projector` parado |
| `uv run eda-interpreter` | Sistema `interpreter` (LLM + su command handler): consume `CarrierNoticeReceived` y publica `ShipmentDelayed` o `IncidentEscalated` y un `LlmCallRecorded` por llamada desde `interpreter.outbox`. Consulta el presupuesto en `view_cost_per_incident` y los pedidos en `view_orders`; si la vista aún no tiene ninguno, aparca el aviso en `interpreter.pending_notices` hasta su `OrderRegistered` o hasta `PENDING_NOTICE_TIMEOUT_S` (300 s por defecto). |
| `uv run eda-planner` | Sistema `planner` (LLM + su command handler): atiende `CreatePlan` en `POST /commands/CreatePlan` (escucha en `PLANNER_URL`, por defecto `http://127.0.0.1:8103`) con la incidencia (pedidos, causa, retraso y gravedad). Consulta el presupuesto en `view_cost_per_incident` y los importes en `view_orders`, valida el plan en código (acciones, compensaciones, dependencias, tope de reembolso e importe del pedido) y publica `PlanCreated` o `IncidentEscalated` y un `LlmCallRecorded` por llamada desde `planner.outbox`; una incidencia con plan rechaza otro `CreatePlan` con `plan_exists`. Lo invoca la saga. |
| `uv run eda-replanner` | Sistema `replanner` (LLM + su command handler): atiende `RevisePlan` en `POST /commands/RevisePlan` (escucha en `REPLANNER_URL`, por defecto `http://127.0.0.1:8104`) con el plan vigente (cada paso con su estado), el paso fallido, su error y las revisiones hechas; no lee el estado del orquestador. Consulta el presupuesto en `view_cost_per_incident` y los importes en `view_orders`, valida la revisión en código (las reglas del plan, pasos completados, acción de los pasos mantenidos y estabilidad de los `step_id`) y publica `PlanRevised` o `IncidentEscalated` (`max_revisions` sin LLM cuando no quedan revisiones) y un `LlmCallRecorded` por llamada desde `replanner.outbox`; una versión ya revisada rechaza otro `RevisePlan` con `revision_exists`. |
| `uv run eda-orchestrator` | Sistema `orchestrator`, la saga reactiva (sin command handler): consume `ShipmentDelayed`, `PlanCreated`, `PlanRevised`, `StepCompleted`, `StepFailed` e `IncidentEscalated`, guarda su estado en `orchestrator.incidents` y `plan_steps`, e invoca por REST desde su `command_outbox` `CreatePlan` (`PLANNER_URL`), `RevisePlan` (`REPLANNER_URL`, por defecto `http://127.0.0.1:8104`) y `ExecuteStep` (`INVENTORY_URL` para `reserve_stock` y `release_stock`; `EXECUTOR_<ACCIÓN>_URL`, por defecto `8111`–`8114`, para el resto), reintentando con el mismo `command_id` si el sistema no responde. Cada comando lleva un plazo (`SAGA_PLAN_TIMEOUT_S`, 300 s; `SAGA_STEP_TIMEOUT_S`, 60 s). Publica `StepRequested` (notificación para la auditoría y el front), `StepCancelled`, `IncidentResolved` e `IncidentEscalated` desde `orchestrator.outbox`. Los ejecutores de `notify_customer`, `file_claim`, `reissue_shipment` e `issue_refund` son `eda-step-executor <acción>` |
| `uv run eda-notice [interactive --timeout N --no-free-text]` | Modo interactivo: menú para enviar avisos (de ejemplo o texto libre) y seguir cada incidencia en la vista `incident_status` hasta que se cierra; `--no-free-text` quita el texto libre del menú |
| `uv run eda-notice seed` | Envía `SeedSampleData` a `inventory` (`INVENTORY_URL`) con los pedidos (`P-1001`…`P-1005`) y el stock de ejemplo; repetirlo no duplica nada |
| `uv run eda-notice send [--example delay\|loss\|out_of_stock\|injection] [--text "…"]` | Envía `RegisterCarrierNotice` a `intake` (`INTAKE_URL`): la entrada que pone en marcha el flujo. Si `intake` o `inventory` no responden en `COMMAND_HTTP_TIMEOUT_S`, termina con error |
| `uv run eda-web [--port N]` | Backend del front web (API y streams SSE) en `127.0.0.1`, puerto `WEB_PORT` (8000 por defecto); nunca escucha en otra interfaz |
| `uv run eda-step-executor <acción>` | Ejecutor de `notify_customer`, `file_claim`, `reissue_shipment` o `issue_refund`, un proceso por acción: atiende el `ExecuteStep` de la saga en `POST /commands/ExecuteStep` (escucha en `EXECUTOR_<ACCIÓN>_URL`, por defecto `8111`–`8114`). Comprueba el pedido en `view_orders`, redacta el aviso o la reclamación con el redactor (la causa y el retraso llegan en el comando; presupuesto de `view_cost_per_incident`), llama al servicio simulado con la `idempotency_key` reintentando los errores transitorios (`STEP_MAX_ATTEMPTS`) y, en una transacción, registra la clave en `executor_<acción>.effect_keys`, el efecto en su tabla y publica el evento del efecto y `StepCompleted`, o `StepFailed` (con `IncidentEscalated` `refund_limit` antes si el reembolso supera el máximo). Una clave ya completada devuelve el resultado guardado sin repetir el efecto |

Cada proceso escribe en stdout una línea por evento recibido, por comando (aceptado o rechazado, con los eventos que produce) y por reintento (`LOG_LEVEL`, por defecto `INFO`); el detalle completo está en las trazas de Jaeger (`http://localhost:16686`).

Los ejecutores reintentan los errores transitorios hasta `STEP_MAX_ATTEMPTS` (por defecto 3) con espera exponencial y jitter (`STEP_BACKOFF_BASE_SECONDS`, por defecto 0,5; `STEP_BACKOFF_MAX_SECONDS`, por defecto 10) y procesan como mucho `EXECUTOR_MAX_CONCURRENCY` pasos a la vez (por defecto 5). Un evento con esquema inválido o que agota sus reintentos va a la tabla `dead_letters`, y el relay lo publica en `eda.dlq` (`DLQ_TOPIC`).

Los agentes llaman a un modelo local de Ollama (`OLLAMA_HOST`, por defecto `http://localhost:11434`; `LLM_MODEL`, por defecto `llama3.1`; `LLM_SEED`, por defecto 42; `LLM_TIMEOUT_SECONDS`, por defecto 120) con `temperature` 0 para que las ejecuciones sean repetibles. Los tests usan siempre el cliente simulado.

La vista `cost_per_incident` guarda solo hechos del log (tokens, reclamaciones, reenvíos e importe reembolsado); la consulta calcula el coste en dinero con `LLM_PRICE_IN_PER_1K` y `LLM_PRICE_OUT_PER_1K` (precio por cada 1000 tokens de entrada y de salida) y `EFFECT_COST_CLAIM` y `EFFECT_COST_REISSUE` (coste de cada reclamación y de cada reenvío); un reembolso cuesta su importe. Todos valen 0 por defecto, porque Ollama es local. Así una vista reconstruida es idéntica aunque cambien los precios.

Las consultas (`queries/`) solo leen las tablas `views.view_*` y las posiciones de los proyectores en `views.offsets`, nunca escriben y devuelven `applied` (el offset aplicado por partición): `get_incident_status`, `get_incident_cost`, `get_audit_trail`, `get_carrier_dashboard` y `get_order_status`.

## Decisiones asumidas

Anotadas aquí porque no estaban cerradas en la conversación; se pueden cambiar sin tocar el resto.

- CQRS sin event sourcing puro: cada sistema guarda su estado de escritura en sus tablas y publica sus eventos desde su outbox en la misma transacción. Las proyecciones (y la vista del log de `event_log`) se construyen consumiendo Kafka y se reconstruyen volviendo a consumir el topic.
- Saga: solo `reserve_stock` tiene compensación (`release_stock`). `issue_refund` es el paso pivote y se ejecuta el último.
- Los umbrales numéricos de las hipótesis (capacidad `experiments`) son valores iniciales.
- Pila fijada: Python, Apache Kafka (Docker, KRaft, un broker) como bus de eventos y PostgreSQL (Docker, volumen persistente) como almacén de cada componente, con un esquema por componente y sin relaciones entre ellos: estado, outbox, inbox, claves de idempotencia y proyecciones. Los detalles están en `../design.md`; el esquema SQL, en `../db/init/001_schema.sql`, y la infraestructura local, en `../docker-compose.yml`.
- Los requisitos hablan de «el bus», «el log» y «el almacén de claves» como conceptos; `design.md` indica cómo los materializa la pila.
- El envío de correos, el transportista, el almacén y los pagos son simulados.
- La `idempotency_key` se calcula con la versión del plan **en la que se creó el paso**, no con la vigente, para que un paso mantenido en una revisión conserve su clave y no se duplique. Esto afina la fórmula `hash(incident_id, plan_version, step_id)` del documento de casos de negocio.
- La saga es un process manager reactivo en el orquestador: decide al consumir cada evento con el estado de `orchestrator.plan_steps`, invoca comandos por REST y no atiende ninguno. Varias compensaciones se encadenan en orden inverso al de los pasos originales.
- Un paso que el plan revisado no mantiene y que no llegó a tener efecto (`pending`, `requested` o `failed`) pasa a `cancelled`. Un resultado de una versión anterior del plan queda en el log, pero no se usa para decidir.
- `StepCompleted` y `StepFailed` v2 llevan la `plan_version` del comando `ExecuteStep` que los pidió, para reconocer los resultados obsoletos. Los v1 se leen como de la versión 1 del plan (y `StepFailed` con un intento).

## Experimentos

| Experimento | Hipótesis | Capacidades implicadas |
| --- | --- | --- |
| E1 Duplicados en el bus | H1 | `idempotency`, `event-backbone` |
| E2 Caída de un ejecutor | H2 | `resilience`, `step-execution` |
| E3 Fallo de un paso y replanificación | H3 | `incident-planning`, `step-execution` |
| E4 Reconstrucción de vistas | H4 | `cqrs-views` |
| E5 Calidad del plan | H5 | `incident-planning`, `llm-boundaries` |
| E6 Eventos frente a síncrono | H6 | todas |
| E7 Auditabilidad | H7 | `observability-audit` |

### Cómo se ejecutan

Cada experimento se lanza con un solo comando y escribe su informe en `experiments/results/<exp>-<fecha>/` (`report.json` y `report.md`, con la semilla, la configuración, el modelo, la versión de los prompts y las métricas). Necesita `docker compose` en marcha; no hace falta arrancar los procesos:

```bash
uv run eda-experiment e1 --seed 42 --incidents 50
```

El arnés (`experiments/harness.py`) monta el sistema completo en un solo proceso, con los componentes de `scripts/run-agents.sh` (los command handlers se ejecutan en el proceso, sin HTTP; el intérprete y la saga consumen Kafka; cada componente publica su outbox; `event_log` y los proyectores mantienen el lado de lectura), contra una base de datos y un topic propios de cada ejecución que se borran al terminar. Los avisos salen del generador con semilla (`synthetic/generator.py`) y los fallos se inyectan con `synthetic/faults.py`. E1–E4 y E7 usan un LLM oráculo que responde con las etiquetas del generador, así miden la arquitectura y no el modelo; E5 y E6 usan Ollama.

E1 compara tres modos de idempotencia: `full` (todo activo), `no_effect_keys` (los ejecutores sin inbox, sin `effect_keys` y con servicios que no reconocen la clave) y `none` (todo desactivado). H1 se evalúa con los dos primeros; `none` se informa como observación: sin inbox, un evento repetido vuelve a llegar al planificador y la incidencia se escala antes de ejecutar ningún paso. Sin la idempotencia de los efectos, la segunda ejecución de un paso choca con la restricción UNIQUE de su tabla, el ejecutor cae y Kafka vuelve a entregar el mensaje: el efecto externo se repite hasta que el arnés salta el mensaje tras tres caídas.

| Comando | Qué hace |
| --- | --- |
| `uv run eda-experiment e1` | E1: cada evento y cada comando de la saga entregados 2 y 3 veces, en los tres modos de idempotencia |
| `uv run eda-experiment e2` | E2: el ejecutor de `notify_customer` cae tras la mitad de sus pasos, después de confirmar y antes de responder; la saga lo reinvoca con el mismo `command_id` |
| `uv run eda-experiment e3` | E3: 30 incidencias de pérdida con los dos almacenes sin stock; el replanner sustituye la reserva y el reenvío por un reembolso |
| `uv run eda-experiment e4` | E4: 200 incidencias; borra cada vista, la reconstruye volviendo a consumir el topic de la ejecución y compara las huellas |
| `uv run eda-experiment e5` | E5: el planificador aislado, con la interpretación etiquetada, sobre 40 incidencias y 3 repeticiones con Ollama; acierto global, por causa y planes que no coinciden. `--llm oracle` lo ejecuta sin coste y `--threshold` cambia el umbral de H5 (0,8) |
| `uv run eda-experiment e6` | E6: las dos versiones con los mismos avisos y el mismo LLM, sin fallos y con fallos (cada evento 2 veces y un 30 % de fallos en `reserve_stock`), 50 incidencias y 3 repeticiones con Ollama; latencia (media, mediana y p95), tokens, coste, incidencias resueltas y efectos duplicados. `--llm oracle` lo ejecuta sin coste |
| `uv run eda-experiment e7` | E7: 50 incidencias con un 50 % de fallos en `reserve_stock`; explica 10 elegidas al azar con `audit_trail` y la cadena de `causation_id` |

Sin `--incidents`, cada experimento usa el tamaño de su escenario en la spec (E1 y E2: 50; E3: 30; E4: 200; E7: 50).

La base síncrona de referencia (`baseline_sync/runner.py`) atiende cada aviso de principio a fin en una sola llamada, con los mismos prompts y comprobaciones de los agentes, las mismas reglas del plan, las mismas acciones de los ejecutores y las mismas compensaciones, pero sin `events`, outbox, inbox, vistas ni Kafka. Guarda el estado de negocio en su esquema `baseline_sync` (migración 0016) de una base de datos PostgreSQL propia de cada ejecución, así E6 compara las dos versiones con la misma base de datos. Recibe los mismos avisos y los mismos fallos de negocio que la versión de eventos; un aviso repetido devuelve la incidencia ya creada.

E5 y E6 repiten 3 veces con Ollama. Cada repetición usa su propia semilla (`--seed` + número de repetición) y temperatura 0,7 (`--temperature`): con temperatura 0 y la misma semilla las repeticiones salen idénticas y no muestran ninguna variación. El sistema sigue con temperatura 0 por defecto (`LLM_TEMPERATURE`).

La latencia de E6 se lee con cuidado: el arnés atiende los consumidores por turnos en un solo proceso, con esperas cortas en cada consumidor, así que la latencia de la versión de eventos incluye ese reparto de turnos y no es la de un despliegue con un proceso por componente. Con el oráculo la diferencia la marca el arnés; con Ollama la domina el LLM.

## Segunda versión (fuera de alcance)

No se desarrolla por el momento. La segunda versión de la PoC añadiría los dos modos de comunicación que faltan, MCP y A2A, para cubrir todos los de un sistema distribuido con agentes y mostrar cuándo conviene cada uno:

| Modo | Cuándo usarlo | En la PoC |
| --- | --- | --- |
| Evento (Kafka) | Un hecho ya ocurrido que interesa a varios, sin esperar respuesta | `ShipmentDelayed`, `StepCompleted`… (versión actual) |
| Comando REST | Una orden a un sistema dueño de su estado, que la acepta o la rechaza al momento; idempotente (`command_id`) | `CreatePlan`, `ExecuteStep`… (versión actual) |
| MCP | Usar una herramienta o un recurso: la herramienta no razona, solo expone operaciones | Servidor de correo simulado para las notificaciones al cliente |
| A2A | Hablar con otro agente autónomo, a menudo de otra organización y opaco por dentro, en una tarea larga que se negocia | Agente del transportista, consultado por el planificador |

- **Servidor de correo simulado (MCP).** Un servidor MCP expone la herramienta de envío de correo; el ejecutor de `notify_customer` se conecta como cliente MCP y la invoca con el texto que redactó el agente redactor. El cliente MCP es el ejecutor (código), no el agente: el LLM sigue sin herramientas con efectos (regla 1). El servidor resuelve el destinatario a partir de `customer_ref`, así que ni el log ni el plan llevan correos (regla 10).
- **Agente del transportista (A2A).** Un agente externo, de la empresa de transporte, con su propio LLM y sus propias reglas. Antes de proponer el plan, el planificador le pide por A2A las opciones que ofrece: nueva fecha de entrega, si puede reenviar y si aceptaría una reclamación. La tarea A2A puede pasar por `input-required`, por ejemplo si el transportista pide el número de seguimiento. La consulta no tiene efectos y su respuesta es contenido no confiable, como el texto de los avisos (regla 11): el planificador solo la usa como contexto y el plan se valida igual que hoy. La reclamación real la sigue presentando el ejecutor de `file_claim` con un comando REST. Así se ve el contraste con el mismo interlocutor: con su agente se negocia por A2A y los efectos van por comandos.
- **Guardrails en MCP y A2A.** Los dos se usarían con guardrails, porque abren el sistema a un servidor de herramientas y a un agente que no controlamos:
  - MCP: lista blanca de herramientas (solo la de envío), validación con esquema de la entrada y la salida, límite de envíos por incidencia y ningún dato personal en lo que el servidor devuelve.
  - A2A: autenticación del agente remoto y comprobación de su Agent Card, validación con esquema de la respuesta, plazo y número máximo de rondas (`input-required`), solo referencias en lo que se le envía (`order_id`, nunca nombres, correos ni direcciones) y su respuesta tratada como posible prompt injection.
