# Web front Specification

## Purpose

Ofrecer una interfaz web local para operar y observar la PoC: lanzar los comandos de `scripts/notice.sh`, ver el mapa de sistemas (agentes, servicios, integraciones y comunicaciones) y seguir los eventos que se intercambian. El front es una vista de la PoC, no parte de su lógica: no escribe en el log de eventos ni salta el command handler (cqrs-views).

Mockups validados: `docs/front/1-vista-general.png`, `2-ejecutando-send-loss.png`, `3-detalle-nodo-filtro.png`.

## Requirements

### Requirement: Distribución de la pantalla

La pantalla SHALL tener una cabecera con el estado de la infraestructura, un menú de comandos a la izquierda, el mapa de sistemas en el área principal y el panel de eventos intercambiados en la parte inferior.

#### Scenario: Vista en reposo

- **GIVEN** la PoC en marcha y ningún comando ejecutado
- **WHEN** se abre el front
- **THEN** se muestran el menú, el mapa y el panel de eventos con los últimos eventos
- **AND** el área de salida del menú indica que no hay ejecuciones

### Requirement: Menú de comandos

El menú SHALL ofrecer los comandos de `scripts/notice.sh`: `seed`, `send`, `send --example` (delay, loss, out_of_stock, injection), `send --text` (texto libre) e `interactive --timeout N`. El backend MUST ejecutarlos invocando `scripts/notice.sh` con una lista blanca de argumentos y SHALL mostrar su salida en el menú a medida que se produce.

#### Scenario: Ejecutar un aviso de ejemplo

- **GIVEN** PostgreSQL y los agentes en marcha
- **WHEN** el usuario pulsa `loss` en `send --example`
- **THEN** el backend ejecuta `scripts/notice.sh send --example loss`
- **AND** la salida aparece en el área de salida con el código de salida al terminar

#### Scenario: Argumento fuera de la lista blanca

- **GIVEN** una petición de ejecución con un subcomando o ejemplo no previsto
- **WHEN** el backend la recibe
- **THEN** la rechaza con 422 y no lanza ningún proceso

#### Scenario: Texto libre

- **GIVEN** un texto escrito en `send --text`
- **WHEN** se envía
- **THEN** el texto se pasa como un único argumento, sin interpretación del shell

#### Scenario: Agentes parados

- **GIVEN** que `scripts/notice.sh` avisa de que los agentes no están en marcha
- **WHEN** el usuario ejecuta `send`
- **THEN** la salida muestra ese aviso tal cual

### Requirement: Mapa de sistemas

El mapa SHALL mostrar como nodos: `eda-notice`, PostgreSQL, relay, Kafka (`eda.events`), `eda.dlq`, proyector, agentes con LLM (intérprete, planificador, replanificador), orquestador, ejecutores (`notify_customer`, `file_claim`, `reissue_shipment`, `reserve_stock`, `release_stock`, `issue_refund`), Ollama, servicios simulados (correo, transportista, almacén, pagos) y Jaeger. Las aristas SHALL representar las comunicaciones de design.md §3 y se distinguirán por tipo de nodo (LLM, código, datos y bus, externo, simulado).

#### Scenario: Estado de un nodo

- **GIVEN** el proceso de un agente o ejecutor
- **WHEN** el backend consulta el estado de procesos
- **THEN** el nodo se pinta como activo si el proceso existe y como caído si no
- **AND** la cabecera muestra «Procesos: N de 12»

#### Scenario: Salud de la infraestructura

- **GIVEN** Kafka, PostgreSQL y Ollama
- **WHEN** el front consulta `GET /api/health`
- **THEN** cada uno se muestra como healthy o down en la cabecera

#### Scenario: Comunicación en curso

- **GIVEN** un comando ejecutándose que genera eventos
- **WHEN** llega un evento nuevo
- **THEN** la arista entre su emisor y su destino se resalta unos segundos

### Requirement: Detalle de un nodo

Al seleccionar un nodo el front SHALL abrir un panel con su descripción, el estado del proceso, los eventos que emite y consume y, para los agentes con LLM, las métricas derivadas de los eventos `LlmCallRecorded` (llamadas, tokens de entrada y salida, latencia, validaciones superadas).

#### Scenario: Filtrar eventos por nodo

- **GIVEN** el panel de detalle del planificador abierto
- **WHEN** el usuario pulsa «Filtrar eventos»
- **THEN** el panel inferior muestra solo los eventos emitidos o recibidos por el planificador

### Requirement: Panel de eventos intercambiados

El panel SHALL mostrar los eventos más recientes primero, con las columnas emisor, destino, tipo de evento y timestamp, y además seq, incident_id, plan, partición/offset, event_id, causation_id y payload. La tabla MUST ser más ancha que el panel y tener desplazamiento horizontal a la derecha, con la cabecera fija en el desplazamiento vertical. SHALL permitir filtrar por incidencia y pausar la actualización en vivo.

#### Scenario: Desplazamiento lateral

- **GIVEN** el panel con todas las columnas
- **WHEN** el usuario se desplaza a la derecha
- **THEN** ve las columnas restantes sin que se mueva el resto de la pantalla

#### Scenario: Eventos en vivo

- **GIVEN** el panel en estado «en vivo»
- **WHEN** se añade un evento al log
- **THEN** aparece en el panel en menos de 2 segundos, ordenado por `seq`
- **AND** si el usuario pulsa «Pausar», no se añaden más filas hasta reanudar

#### Scenario: Evento sin partición registrada

- **GIVEN** que `events` no guarda partición ni offset de Kafka
- **WHEN** se muestra la columna partición/offset
- **THEN** muestra «—» para esos eventos

### Requirement: Origen de emisor y destino

El emisor de un evento SHALL derivarse del componente que lo escribió (campo `producer` del evento, o el catálogo en `eda_retail.events.catalog` si no existe) y el destino de los consumidores que el catálogo asigna a ese tipo de evento (design.md §3). Un evento con varios consumidores SHALL mostrarse con el destino principal y el resto en el detalle.

#### Scenario: Evento con consumidor único

- **GIVEN** un `PlanCreated`
- **WHEN** se muestra en el panel
- **THEN** emisor `planner` y destino `orchestrator`

### Requirement: Acceso de solo lectura y local

El backend SHALL leer los eventos de la tabla `events` y el estado de las incidencias de las vistas `view_*`, sin escribir en ninguna tabla. SHALL escuchar solo en `127.0.0.1`, no admitir orígenes distintos de `localhost` y ejecutar un único comando a la vez.

#### Scenario: Escritura directa rechazada

- **GIVEN** cualquier endpoint de la API
- **WHEN** se inspecciona su acceso a datos
- **THEN** solo emite consultas de lectura; los cambios de estado ocurren únicamente por `scripts/notice.sh`

#### Scenario: Segunda ejecución simultánea

- **GIVEN** un comando en ejecución
- **WHEN** llega otra petición de ejecución
- **THEN** se rechaza con 409

### Requirement: Comando interactivo

`interactive --timeout N` SHALL ejecutarse con una pseudoterminal y el front SHALL ofrecer una entrada de texto para enviar líneas a su entrada estándar mientras dure la sesión. N MUST estar entre 1 y 600.

#### Scenario: Responder al menú

- **GIVEN** una sesión interactiva con una pregunta pendiente
- **WHEN** el usuario escribe una opción y envía
- **THEN** el backend la entrega al proceso y la salida continúa en el área de salida

### Requirement: Actualización en vivo

El backend SHALL exponer los eventos nuevos por Server-Sent Events, con `seq` como identificador, de modo que el cliente retome desde `Last-Event-ID` tras una reconexión sin perder ni duplicar filas.

#### Scenario: Reconexión

- **GIVEN** un cliente que recibió hasta `seq` 1290 y perdió la conexión
- **WHEN** reconecta con `Last-Event-ID: 1290`
- **THEN** recibe desde 1291 en adelante

## API prevista

| Método y ruta | Uso |
| --- | --- |
| `GET /api/events?limit=&before_seq=&incident_id=&node=` | Página de eventos con emisor y destino |
| `GET /api/events/stream` | SSE de eventos nuevos |
| `GET /api/topology` | Nodos y aristas del mapa |
| `GET /api/nodes/{id}` | Detalle y métricas de un nodo |
| `GET /api/health` | Kafka, PostgreSQL, Ollama y procesos |
| `POST /api/commands` | Ejecuta un comando de la lista blanca; devuelve `run_id` |
| `GET /api/commands/{run_id}/stream` | SSE con la salida |
| `POST /api/commands/{run_id}/input` | Línea para `interactive` |

## Decisiones abiertas

- Partición y offset: requieren que el relay los registre al publicar (cambio en `outbox`/`events` con migración Alembic). Hasta entonces la columna muestra «—».
- Estado de procesos: se deduce con `pgrep` como `scripts/notice.sh`; Kafka y PostgreSQL por conexión, Ollama por `GET /api/tags`.
