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

El menú SHALL ofrecer los comandos de `scripts/notice.sh`: `seed`, `send`, `send --example` (delay, loss, out_of_stock, injection), `send --text` (texto libre) e `interactive --timeout N`. El backend MUST ejecutarlos invocando `scripts/notice.sh` con una lista blanca de argumentos, sin shell, y SHALL mostrar su salida (estándar y de error) en el menú a medida que se produce. Los ejemplos admitidos son los de `eda_retail.synthetic.sample.EXAMPLES`; el texto libre MUST tener entre 1 y 2000 caracteres. Una ejecución MUST terminar a los 15 minutos como mucho, para que un proceso colgado no bloquee el menú.

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

#### Scenario: Ejecución que no termina

- **GIVEN** un comando en ejecución que supera el tiempo máximo (15 minutos)
- **WHEN** se cumple ese tiempo
- **THEN** el backend termina el proceso y su grupo, y la salida acaba con un código de salida distinto de 0

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

El emisor de un evento SHALL ser el campo `producer` del evento. El destino SHALL derivarse de los tipos de evento que declara cada componente (design.md §3): los agentes en `eda_retail.agents.cli.AGENTS`, el orquestador en `eda_retail.orchestrator.runner.EVENT_TYPES` y, para `StepRequested`, el ejecutor de la acción del paso (`executor-<acción>`). Los proyectores leen todos los eventos y SHALL figurar siempre como último consumidor. El destino principal es el primer consumidor de esa lista y el resto se muestra en el detalle; un evento que solo leen los proyectores tiene `projectors` como destino.

#### Scenario: Evento con consumidor único

- **GIVEN** un `PlanCreated`
- **WHEN** se muestra en el panel
- **THEN** emisor `planner` y destino `orchestrator`

#### Scenario: Paso pedido a un ejecutor

- **GIVEN** un `StepRequested` con acción `notify_customer`
- **WHEN** se muestra en el panel
- **THEN** su destino es `executor-notify_customer`

#### Scenario: Evento con varios consumidores

- **GIVEN** un `StepFailed`
- **WHEN** se muestra en el panel
- **THEN** su destino principal es `orchestrator` y el detalle añade `replanner` y `projectors`

#### Scenario: Evento sin consumidor de Kafka

- **GIVEN** un `LlmCallRecorded`
- **WHEN** se muestra en el panel
- **THEN** su destino es `projectors`

### Requirement: Página de eventos

`GET /api/events` SHALL devolver los eventos de `events` del más reciente al más antiguo, con `limit` entre 1 y 500 (100 por defecto) y `before_seq` para pedir la página anterior. SHALL admitir los filtros `incident_id` (por `correlation_id` o por el `incident_id` del payload) y `node` (eventos que ese nodo emite o consume). La respuesta SHALL incluir `next_before_seq`, el `seq` desde el que pedir la página siguiente, o `null` si no hay más.

#### Scenario: Paginar hacia atrás

- **GIVEN** 5 eventos y `limit=2`
- **WHEN** el cliente pide la primera página y después la siguiente con `next_before_seq`
- **THEN** recibe los `seq` más altos primero, sin repetir ni saltar ninguno

#### Scenario: Filtrar por nodo

- **GIVEN** un `StepRequested` de `notify_customer` y otro de `file_claim`
- **WHEN** se pide `node=executor-notify_customer`
- **THEN** solo aparece el de `notify_customer`

#### Scenario: Límite fuera de rango

- **GIVEN** cualquier estado del log
- **WHEN** se pide `limit=0` o `limit=501`
- **THEN** la API responde 422

### Requirement: Acceso de solo lectura y local

El backend SHALL leer los eventos de la tabla `events` y el estado de las incidencias de las vistas `view_*`, sin escribir en ninguna tabla. SHALL escuchar solo en `127.0.0.1`, no admitir orígenes distintos de `localhost` y ejecutar un único comando a la vez. SHALL rechazar con 403 las peticiones cuya cabecera `Host` no sea `localhost`, `127.0.0.1` o `[::1]` (con o sin puerto), para que una web externa no pueda llegar al backend redirigiendo su dominio a 127.0.0.1 (DNS rebinding).

#### Scenario: Escritura directa rechazada

- **GIVEN** cualquier endpoint de la API
- **WHEN** se inspecciona su acceso a datos
- **THEN** solo emite consultas de lectura; los cambios de estado ocurren únicamente por `scripts/notice.sh`

#### Scenario: Segunda ejecución simultánea

- **GIVEN** un comando en ejecución
- **WHEN** llega otra petición de ejecución
- **THEN** se rechaza con 409

#### Scenario: Host ajeno

- **GIVEN** una petición con la cabecera `Host: evil.example`
- **WHEN** llega a cualquier endpoint
- **THEN** se rechaza con 403

### Requirement: Comando interactivo

`interactive --timeout N` SHALL ejecutarse con una pseudoterminal y el front SHALL ofrecer una entrada de texto para enviar líneas a su entrada estándar mientras dure la sesión. N MUST estar entre 1 y 600. Cada línea MUST tener como mucho 200 caracteres y ningún salto de línea; enviar una línea a una ejecución que no es interactiva o que ya terminó se rechaza con 409.

#### Scenario: Responder al menú

- **GIVEN** una sesión interactiva con una pregunta pendiente
- **WHEN** el usuario escribe una opción y envía
- **THEN** el backend la entrega al proceso y la salida continúa en el área de salida

### Requirement: Actualización en vivo

El backend SHALL exponer los eventos nuevos en `GET /api/events/stream` por Server-Sent Events, con `seq` como identificador y cada evento con las mismas columnas que la página de eventos, de modo que el cliente retome desde `Last-Event-ID` tras una reconexión sin perder ni duplicar filas. En la primera conexión el navegador no puede enviar `Last-Event-ID`, así que el stream SHALL admitir `after_seq` (el `seq` más alto que el panel ya muestra); si llegan los dos, manda `Last-Event-ID`. Sin ninguno, el stream empieza tras el último `seq` del log. El backend SHALL consultar el log al menos cada 0,5 s para cumplir el plazo de 2 s del panel.

#### Scenario: Reconexión

- **GIVEN** un cliente que recibió hasta `seq` 1290 y perdió la conexión
- **WHEN** reconecta con `Last-Event-ID: 1290`
- **THEN** recibe desde 1291 en adelante

#### Scenario: Primera conexión tras cargar el panel

- **GIVEN** un panel que cargó la página de eventos hasta `seq` 1290
- **WHEN** abre el stream con `after_seq=1290`
- **THEN** recibe desde 1291 en adelante, en orden de `seq`

#### Scenario: Conexión sin posición

- **GIVEN** un log con eventos hasta `seq` 1290
- **WHEN** se abre el stream sin `Last-Event-ID` ni `after_seq`
- **THEN** solo recibe los eventos añadidos después, desde 1291

### Requirement: Acceso a las trazas en Jaeger

La cabecera SHALL ofrecer un enlace a la interfaz web de Jaeger, que se abre en una pestaña nueva. Cada fila del panel de eventos SHALL ofrecer un enlace «ver traza» a la traza de ese evento en Jaeger (`<JAEGER_UI_URL>/trace/<trace_id>`). El backend MUST obtener el `trace_id` del `traceparent` guardado en `outbox.trace_context` para ese `seq`, con una consulta de solo lectura. La URL base sale de `JAEGER_UI_URL` (por defecto `http://localhost:16686`, el puerto publicado en `docker-compose.yml`). El front MUST NOT consultar la API de Jaeger: solo enlaza a su interfaz.

#### Scenario: Abrir Jaeger desde la cabecera

- **GIVEN** el front abierto
- **WHEN** el usuario pulsa el enlace a Jaeger de la cabecera
- **THEN** se abre `JAEGER_UI_URL` en una pestaña nueva

#### Scenario: Ver la traza de un evento

- **GIVEN** un evento cuyo `outbox.trace_context` tiene `traceparent` `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
- **WHEN** se muestra en el panel de eventos
- **THEN** su enlace «ver traza» apunta a `http://localhost:16686/trace/4bf92f3577b34da6a3ce929d0e0e4736`

#### Scenario: Evento sin traza

- **GIVEN** un evento escrito con las trazas desactivadas (`TRACING_ENABLED=false`), sin `trace_context` o con un `traceparent` mal formado
- **WHEN** se muestra en el panel de eventos
- **THEN** la columna de traza muestra «—» y no hay enlace

## API prevista

| Método y ruta | Uso |
| --- | --- |
| `GET /api/events?limit=&before_seq=&incident_id=&node=` | Página de eventos con emisor, destino y `trace_id` |
| `GET /api/events/stream?after_seq=` | SSE de eventos nuevos (`Last-Event-ID` al reconectar) |
| `GET /api/topology` | Nodos y aristas del mapa |
| `GET /api/nodes/{id}` | Detalle y métricas de un nodo |
| `GET /api/health` | Kafka, PostgreSQL, Ollama y procesos |
| `POST /api/commands` | Ejecuta un comando de la lista blanca; devuelve `run_id` |
| `GET /api/commands/{run_id}/stream` | SSE con la salida y, al final, el código de salida |
| `POST /api/commands/{run_id}/input` | Línea para `interactive` |

## Decisiones abiertas

- Partición y offset: requieren que el relay los registre al publicar (cambio en `outbox`/`events` con migración Alembic). Hasta entonces la columna muestra «—».
- Estado de procesos: se deduce con `pgrep` como `scripts/notice.sh`; Kafka y PostgreSQL por conexión, Ollama por `GET /api/tags`.
