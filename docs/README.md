# PoC académica de EDA con agentes: incidencias logísticas

Specs en estilo OpenSpec. Cada capacidad vive en `specs/<capacidad>/spec.md` con un `## Purpose`, los requisitos (`### Requirement:`, redactados con SHALL/MUST) y al menos un escenario GIVEN/WHEN/THEN (`#### Scenario:`) por requisito.

## Contexto

Una tienda online recibe avisos de retraso de los transportistas, en texto libre o por API. Ante cada incidencia, un sistema de agentes decide qué hacer (avisar al cliente, reclamar al transportista, reenviar, reponer o reembolsar) y lo ejecuta mediante eventos. La PoC compara esta arquitectura con una base síncrona y mide fiabilidad, recuperación, calidad del plan, coste y latencia.

Regla de diseño: **el LLM propone y el código dispone**. Ningún LLM ejecuta efectos ni escribe directamente en el log.

## Diagrama de agentes

```mermaid
flowchart LR
  IN["Aviso del transportista<br/>texto libre o API"]
  BUS[("Bus de eventos<br/>y log de eventos")]
  INT["Agente intérprete"]
  PLN["Agente planificador"]
  RPL["Replanner"]
  CH["Command handler<br/>y validador"]
  ORQ["Orquestación del plan"]
  EXE["Ejecutores<br/>aviso, reclamación, stock,<br/>reenvío, reembolso"]
  RED["Agente redactor<br/>aviso y reclamación"]
  IDS[("Almacén de claves<br/>de idempotencia")]
  PRJ["Proyectores"]
  VW[("Vistas CQRS")]
  FIN["Cierre de la incidencia<br/>IncidentResolved o escalado"]

  IN -- "(1) CarrierNoticeReceived" --> BUS
  BUS --> INT
  INT -- "(2) propone ShipmentDelayed" --> CH
  BUS --> PLN
  PLN -- "(3) propone PlanCreated" --> CH
  CH -- "(a) escribe solo eventos válidos" --> BUS
  BUS --> ORQ
  ORQ -- "(4) StepRequested" --> BUS
  BUS --> EXE
  EXE <-- "(5) texto / borrador validado" --> RED
  EXE <-- "(6) clave" --> IDS
  EXE -- "(7) StepCompleted o StepFailed" --> BUS
  BUS -- "(8) StepFailed" --> RPL
  RPL -- "(9) propone PlanRevised, vuelve a 4" --> CH
  ORQ -- "(10)" --> FIN
  BUS -- "(b)" --> PRJ
  PRJ -- "(c)" --> VW

  classDef llm fill:#E3F0FF,stroke:#2B7DE0,color:#1d1d1f
  classDef code fill:#E6F6EC,stroke:#2E9E5B,color:#1d1d1f
  classDef data fill:#F1F1EE,stroke:#8A8A80,color:#1d1d1f
  classDef ext fill:#FFF3E0,stroke:#E08A2B,color:#1d1d1f
  class INT,PLN,RED,RPL llm
  class CH,ORQ,EXE,PRJ code
  class BUS,IDS,VW data
  class IN,FIN ext
```

Azul: agentes con LLM (proponen). Verde: código determinista (dispone). Gris: datos. Naranja: entrada y salida. Los pasos (1) a (10) siguen el flujo de una incidencia, con el bus entre cada productor y su consumidor; (a) a (c) son la escritura validada y la proyección de las vistas. Los agentes con LLM nunca escriben en el log ni ejecutan efectos.

### Secuencia de la saga

```mermaid
sequenceDiagram
  participant Orq as Orquestación
  participant Av as Ejecutor<br/>aviso
  participant Rc as Ejecutor<br/>reclamación
  participant St as Ejecutor<br/>stock
  participant En as Ejecutor<br/>envío
  participant Rf as Ejecutor<br/>reembolso
  participant Rp as Replanner<br/>(LLM)
  participant CH as Command<br/>handler
  Note over Orq,CH: Plan v1 de una pérdida de paquete: notify_customer, file_claim, reserve_stock y reissue_shipment.<br/>Todos los mensajes viajan por el bus y cada StepRequested lleva su idempotency_key.
  par pasos independientes en paralelo
    Orq->>Av: (1.a) StepRequested notify_customer
  and
    Orq->>Rc: (1.b) StepRequested file_claim
  and
    Orq->>St: (1.c) StepRequested reserve_stock
  end
  Av-->>Orq: (2.a) StepCompleted (CustomerNotified)
  Rc-->>Orq: (2.b) StepCompleted (ClaimFiled)
  St-->>Orq: (2.c) StepCompleted (StockReserved)
  Orq->>En: (3) StepRequested reissue_shipment<br/>(depende de reserve_stock)
  alt Camino feliz
    En-->>Orq: (4) StepCompleted (ShipmentReissued)
    Orq->>Orq: (5) IncidentResolved, outcome resolved
  else El reenvío falla (carrier_rejected, no reintentable)
    En-->>Orq: (4) StepFailed
    Orq->>Rp: (5) StepFailed
    Rp->>CH: (6) propone el plan v2:<br/>reembolso en lugar de reenvío
    CH-->>Orq: (7) PlanRevised v2<br/>(validado: sin compensaciones en el plan, importe dentro del máximo)
    Note over Orq,St: (8) El código deriva la compensación de reserve_stock<br/>y bloquea issue_refund hasta que termine
    Orq->>St: (9) StepRequested release_stock<br/>(step_id reserve_stock-comp)
    alt La compensación termina bien
      St-->>Orq: (10) StepCompleted (StockReleased)
      Orq->>Rf: (11) StepRequested issue_refund<br/>(paso pivote, sin compensación)
      alt El reembolso termina bien
        Rf-->>Orq: (12) StepCompleted (RefundIssued)
        Orq->>Orq: (13) IncidentResolved, outcome refunded
      else El reembolso falla
        Rf-->>Orq: (12) StepFailed
        Orq->>Orq: (13) nueva revisión del plan o<br/>IncidentEscalated si se agotan las revisiones
      end
    else La compensación agota sus reintentos
      St-->>Orq: (10) StepFailed
      Orq->>Orq: (11) IncidentEscalated, compensation_failed<br/>(no se emite el reembolso)
    end
  end
```

Operaciones y compensaciones de una pérdida de paquete, con los caminos de éxito, de reembolso tras compensar y de escalado.

### CQRS

```mermaid
flowchart LR
  subgraph CMD["Lado de comandos (escritura)"]
    A["Agentes, orquestación<br/>y ejecutores"]
    CH["Command handler<br/>valida reglas, esquema<br/>y expected_version"]
    TX[("PostgreSQL, una transacción<br/>incidents, plan_steps,<br/>events, outbox, inbox")]
  end
  K["Kafka<br/>eda.events"]
  subgraph QRY["Lado de consultas (lectura)"]
    P["Proyectores<br/>idempotentes"]
    V[("view_* y<br/>projector_offsets")]
    Q["Consultas<br/>panel y agentes"]
  end
  A -- "(1) comando con<br/>command_id y expected_version" --> CH
  CH -- "(2) estado y eventos<br/>en la misma transacción" --> TX
  CH -. "rechaza:<br/>concurrency_conflict,<br/>incident_closed" .-> A
  TX -- "(3) relay del outbox" --> K
  K -- "(4) eventos a los<br/>consumidores" --> A
  TX -- "(5) lee events por seq" --> P
  P -- "(6) actualiza la vista<br/>y last_seq" --> V
  Q -- "(7) solo lectura,<br/>responde con last_applied_seq" --> V
  classDef code fill:#E6F6EC,stroke:#2E9E5B,color:#1d1d1f
  classDef data fill:#F1F1EE,stroke:#8A8A80,color:#1d1d1f
  classDef llm fill:#E3F0FF,stroke:#2B7DE0,color:#1d1d1f
  class CH,P code
  class TX,V,K data
  class A,Q llm
```

Lado de comandos (escritura) y lado de consultas (lectura). Los pasos (1) a (4) son el camino de escritura y (5) a (7), el de lectura.

## Patrones de la guía que se ejercitan

| Patrón (guía «Patrones aplicados al desarrollo de agentes») | Capacidad |
| --- | --- |
| Planning orientado a eventos (9.2), Plan-and-Execute (3.3), Saga (6.3) | `incident-planning` |
| Control optimista de concurrencia (5.6) | `cqrs-views` |
| CQRS (9.4), Outbox (6.4) | `event-backbone`, `cqrs-views` |
| Idempotency key (6.2) | `idempotency` |
| Retry con backoff y dead letter (6.6), Backpressure (5.7), Durable execution (6.1) | `resilience` |
| Guardrails (3.9), Bounded autonomy (3.10) | `llm-boundaries` |
| Tracing distribuido (6.10) | `observability-audit` |
| Evals como quality gates (8.1) | `experiments` |

## Capacidades

| Capacidad | Resumen |
| --- | --- |
| `event-backbone` | Formato del evento, log de eventos, bus, orden y entrega |
| `incident-interpretation` | Agente intérprete: del aviso en texto libre a `ShipmentDelayed` |
| `incident-planning` | Planner, replanner, versionado del plan, dependencias, compensación |
| `step-execution` | Ejecutores de pasos y sus efectos |
| `idempotency` | Claves, almacén de claves y reglas por efecto |
| `cqrs-views` | Command handler, proyectores, vistas y reconstrucción |
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
| `StepRequested` | Orquestación del plan | `incident_id`, `plan_version`, `step_id`, `action`, `params`, `idempotency_key` |
| `StepCompleted` (v2) | Ejecutor | `step_id`, `plan_version`, `result` |
| `StepFailed` (v2) | Ejecutor | `step_id`, `plan_version`, `error`, `retryable`, `attempts` |
| `StepCancelled` | Orquestación | `incident_id`, `plan_version`, `step_id` |
| `CustomerNotified` | Ejecutor | `order_id`, `message_id` |
| `ClaimFiled` | Ejecutor | `order_id`, `claim_id` |
| `StockReserved` | Ejecutor | `order_id`, `warehouse`, `reservation_id` |
| `StockReleased` | Ejecutor | `reservation_id` |
| `ShipmentReissued` | Ejecutor | `order_id`, `new_shipment_id` |
| `RefundIssued` | Ejecutor | `order_id`, `amount`, `refund_id` |
| `IncidentResolved` | Orquestación | `incident_id`, `outcome` |
| `IncidentEscalated` | Orquestación | `incident_id`, `reason` |
| `LlmCallRecorded` | Agentes con LLM | `incident_id`, `agent`, `model`, `tokens_in`, `tokens_out`, `latency_ms`, `validation_passed`, `attempt` |

## Catálogo de comandos

| Comando | Lo envía | Efecto (eventos que puede producir) |
| --- | --- | --- |
| `RegisterCarrierNotice` | Entrada | `CarrierNoticeReceived` |
| `OpenIncident` | Intérprete | `ShipmentDelayed` |
| `CreatePlan` | Planificador | `PlanCreated` |
| `RevisePlan` | Replanner | `PlanRevised` |
| `ApplyRevision` | Orquestación, al recibir `PlanRevised` | `StepCancelled` de los pasos no mantenidos; deriva en código los pasos `-comp` |
| `AdvancePlan` | Orquestación | Los `StepRequested` de todos los pasos listos, o `IncidentResolved` si el plan terminó |
| `RequestStep` | Orquestación | `StepRequested` |
| `RecordStepResult` | Ejecutores | `StepCompleted` o `StepFailed` y el evento de efecto (con la `plan_version` del `StepRequested`; uno obsoleto queda en el log sin cambiar el plan) |
| `CloseIncident` | Orquestación | `IncidentResolved` |
| `EscalateIncident` | Cualquiera | `IncidentEscalated` (si la incidencia aún no existe, la crea escalada a partir de su aviso) |
| `RecordLlmCall` | Agentes con LLM | `LlmCallRecorded` (no usa `expected_version` y admite incidencias cerradas) |

Todos llevan `command_id` y `expected_version` (`RecordLlmCall` lo ignora: es un hecho de solo añadir). Los comandos solo existen dentro del command handler; lo que se publica y se guarda son los eventos.

## Procesos

Cada componente es un proceso independiente (con `docker compose up -d` levantado):

| Comando | Qué hace |
| --- | --- |
| `uv run eda-relay` | Publica en Kafka las filas pendientes del outbox, en orden de `seq` |
| `uv run eda-projector` | Mantiene las vistas `view_*` leyendo `events` por `seq` |
| `uv run eda-agent interpreter` | Interpreta `CarrierNoticeReceived` y abre la incidencia |
| `uv run eda-agent planner` | Propone el plan a partir de `ShipmentDelayed` |
| `uv run eda-agent replanner` | Revisa el plan ante un `StepFailed` no reintentable |

Los agentes llaman a un modelo local de Ollama (`OLLAMA_HOST`, por defecto `http://localhost:11434`; `LLM_MODEL`, por defecto `llama3.1`; `LLM_SEED`, por defecto 42; `LLM_TIMEOUT_SECONDS`, por defecto 120) con `temperature` 0 para que las ejecuciones sean repetibles. Los tests usan siempre el cliente simulado.

## Decisiones asumidas

Anotadas aquí porque no estaban cerradas en la conversación; se pueden cambiar sin tocar el resto.

- CQRS sin event sourcing puro: el estado de escritura (`incidents`, `plan_steps`) se guarda en tablas y el log registra los hechos. Ambos se escriben en la misma transacción, y las vistas se reconstruyen desde el log.
- Saga: solo `reserve_stock` tiene compensación (`release_stock`). `issue_refund` es el paso pivote y se ejecuta el último.
- Los umbrales numéricos de las hipótesis (capacidad `experiments`) son valores iniciales.
- Pila fijada: Python, Apache Kafka (Docker, KRaft, un broker) como bus y PostgreSQL (Docker, volumen persistente) como log de eventos, outbox, inbox y almacén de claves. Los detalles están en `../design.md`; el esquema SQL, en `../db/init/001_schema.sql`, y la infraestructura local, en `../docker-compose.yml`.
- Los requisitos siguen hablando de «el bus», «el log» y «el almacén de claves» como conceptos; `design.md` indica cómo los materializa la pila.
- El envío de correos, el transportista, el almacén y los pagos son simulados.
- La `idempotency_key` se calcula con la versión del plan **en la que se creó el paso**, no con la vigente, para que un paso mantenido en una revisión conserve su clave y no se duplique. Esto afina la fórmula `hash(incident_id, plan_version, step_id)` del documento de casos de negocio.
- `StepCompleted` y `StepFailed` v2 llevan la `plan_version` del `StepRequested` que los pidió, para reconocer los resultados obsoletos. Los v1 se leen como de la versión 1 del plan (y `StepFailed` con un intento).

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
