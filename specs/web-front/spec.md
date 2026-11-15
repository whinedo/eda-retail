# Web front Specification

## Purpose

Ofrecer una interfaz web local para operar y observar la PoC: lanzar los comandos de `scripts/notice.sh`, ver el mapa de sistemas (agentes, servicios, integraciones y comunicaciones), seguir los eventos que se intercambian (auditoría) y ver el estado de los pedidos afectados (analítica de negocio). El front es una vista de la PoC, no parte de su lógica: no escribe en el log de eventos ni salta el command handler (cqrs-views).

Mockups validados: `docs/front/1-vista-general.png`, `2-ejecutando-send-loss.png`, `3-detalle-nodo-filtro.png`.

## Requirements

### Requirement: Distribución de la pantalla

La pantalla SHALL tener una cabecera con el estado de la infraestructura, un menú de comandos a la izquierda, el mapa de sistemas en el área principal y el panel de eventos intercambiados en la parte inferior. El menú de comandos SHALL ser una columna estrecha (240 px) para dejar al mapa el mayor espacio posible.

#### Scenario: Vista en reposo

- **GIVEN** la PoC en marcha y ningún comando ejecutado
- **WHEN** se abre el front
- **THEN** se muestran el menú, el mapa y el panel de eventos con los últimos eventos
- **AND** el área de salida del menú indica que no hay ejecuciones

### Requirement: Reinicio de la vista

La cabecera SHALL ofrecer el botón «Resetear simulación», que devuelve la pantalla al punto de partida para lanzar un comando: vacía el panel de eventos, deja el área de salida del menú sin ejecuciones, quita los filtros de incidencia y nodo, cierra el detalle del nodo, quita el camino resaltado del mapa y reanuda el panel si estaba en pausa. Después, el panel SHALL mostrar solo los eventos añadidos al log tras el reinicio. El reinicio es solo de la vista: MUST NOT escribir en la base de datos (el log es inmutable y el front es de solo lectura). El botón SHALL estar desactivado mientras se ejecuta un comando. Al recargar la página se vuelve a ver la página de eventos completa.

#### Scenario: Resetear la vista

- **GIVEN** el panel con eventos, el detalle de un nodo abierto, un filtro de incidencia y la salida de un comando terminado
- **WHEN** el usuario pulsa «Resetear simulación»
- **THEN** el panel de eventos queda vacío y sin filtros, el detalle del nodo se cierra y el mapa no resalta ningún camino
- **AND** el área de salida del menú indica que no hay ejecuciones
- **AND** no se hace ninguna petición que escriba en el backend

#### Scenario: Eventos tras el reinicio

- **GIVEN** la vista reseteada cuando el log llegaba hasta `seq` 1290
- **WHEN** se añade al log el evento 1291
- **THEN** el panel muestra solo el 1291
- **AND** no ofrece «Cargar más» con los eventos anteriores al reinicio

#### Scenario: Reinicio con un comando en ejecución

- **GIVEN** un comando en ejecución
- **WHEN** el usuario mira la cabecera
- **THEN** «Resetear simulación» está desactivado

### Requirement: Menú de comandos

El menú SHALL ofrecer los comandos de `scripts/notice.sh`: `seed`, `send`, `send --example` (delay, loss, out_of_stock, injection), `send --text` (texto libre) e `interactive --timeout N`. Como la PoC es un ejemplo académico, `interactive` está deshabilitado por defecto: solo se ofrece con `WEB_INTERACTIVE=true`; deshabilitado, el menú no lo muestra y el backend MUST rechazar con 422 cualquier petición `interactive`, sin lanzar ningún proceso. Por lo mismo, el texto libre está deshabilitado por defecto: solo se ofrece con `WEB_FREE_TEXT=true`. Deshabilitado, el menú no muestra `send --text` y el backend MUST rechazar con 422 cualquier petición con `text`, sin lanzar ningún proceso. Tampoco se ofrece dentro de `interactive`: el backend MUST lanzarlo con `--no-free-text`, que quita la opción `t) texto libre` de su menú. El backend MUST ejecutarlos invocando `scripts/notice.sh` con una lista blanca de argumentos, sin shell, y SHALL mostrar su salida (estándar y de error) en el menú a medida que se produce. Los ejemplos admitidos son los de `eda_retail.synthetic.sample.EXAMPLES`, y cada uno SHALL llevar en el menú una descripción corta de lo que provoca, porque los avisos de `loss` y `out_of_stock` se parecen y lo que cambia es el camino del plan; el texto libre MUST tener entre 1 y 2000 caracteres. Una ejecución MUST terminar a los 15 minutos como mucho, para que un proceso colgado no bloquee el menú. El menú SHALL agrupar los comandos en dos secciones: «Preparación», solo con `seed`, que avisa de que no genera eventos (carga datos de referencia en `orders` y `stock`, no hechos del dominio), y «Avisos», con `send`, sus variantes e `interactive`, que ponen en marcha el flujo de eventos.

#### Scenario: Preparar los datos de ejemplo

- **GIVEN** el menú de comandos
- **WHEN** el usuario mira la sección «Preparación»
- **THEN** solo contiene `seed`, con el aviso «No genera eventos»
- **AND** los comandos `send` e `interactive` están en la sección «Avisos»

#### Scenario: Descripción de los ejemplos

- **GIVEN** la tarjeta `send --example`
- **WHEN** el usuario la mira
- **THEN** cada ejemplo tiene su descripción: `delay` «Retraso de 24 h en P-1001: el plan avisa al cliente.», `loss` «Pérdida de P-1002: reclamar, reservar stock y reenviar.», `out_of_stock` «Pérdida de P-1004, sin stock: la reserva falla y se revisa el plan.» e `injection` «Retraso de P-1003 con una orden inyectada (reembolsar 1000 EUR): se trata como dato y los reembolsos tienen un tope.»

#### Scenario: Ejecutar un aviso de ejemplo

- **GIVEN** PostgreSQL y los agentes en marcha
- **WHEN** el usuario pulsa `loss` en `send --example`
- **THEN** el backend ejecuta `scripts/notice.sh send --example loss`
- **AND** la salida aparece en el área de salida con el código de salida al terminar

#### Scenario: Argumento fuera de la lista blanca

- **GIVEN** una petición de ejecución con un subcomando o ejemplo no previsto
- **WHEN** el backend la recibe
- **THEN** la rechaza con 422 y no lanza ningún proceso

#### Scenario: Interactive deshabilitado

- **GIVEN** el backend sin `WEB_INTERACTIVE=true`
- **WHEN** se abre el menú o llega `POST /api/commands` con `{"command": "interactive", "timeout": 60}`
- **THEN** el menú no muestra `interactive`
- **AND** el backend responde 422 con «el comando interactive está deshabilitado (WEB_INTERACTIVE)» y no lanza ningún proceso

#### Scenario: Texto libre deshabilitado

- **GIVEN** el backend sin `WEB_FREE_TEXT=true`
- **WHEN** se abre el menú o llega `POST /api/commands` con `{"command": "send", "text": "…"}`
- **THEN** el menú no muestra `send --text`
- **AND** el backend responde 422 con «el texto libre está deshabilitado (WEB_FREE_TEXT)» y no lanza ningún proceso
- **AND** `interactive` se lanza como `scripts/notice.sh interactive --timeout N --no-free-text` y su menú no ofrece `t) texto libre`

#### Scenario: Texto libre

- **GIVEN** `WEB_FREE_TEXT=true` y un texto escrito en `send --text`
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

#### Scenario: Script que no se puede lanzar

- **GIVEN** `scripts/notice.sh` ausente o sin permiso de ejecución
- **WHEN** llega una petición de ejecución
- **THEN** el backend responde 500 con el motivo, no deja recursos abiertos y el menú admite la siguiente ejecución

#### Scenario: Parada del backend

- **GIVEN** un comando en ejecución
- **WHEN** se para el backend
- **THEN** el backend termina el proceso y su grupo antes de salir

### Requirement: Mapa de sistemas

El mapa SHALL mostrar como nodos: `eda-notice`, PostgreSQL, relay, Kafka (`eda.events`), `eda.dlq`, proyector, agentes con LLM (intérprete, planificador, replanificador), orquestador, ejecutores (`notify_customer`, `file_claim`, `reissue_shipment`, `reserve_stock`, `release_stock`, `issue_refund`), Ollama, servicios simulados (correo, aseguradora, transportista, almacén y pagos, los de `eda_retail.executors.services` más las tablas de stock) y Jaeger. Las aristas SHALL representar las comunicaciones de design.md §3, incluidas las invocaciones REST de comandos de la saga (`orchestrator` → `planner`, `replanner`, `inventory` y ejecutores), y se distinguirán por tipo de nodo (LLM, código, datos y bus, externo, simulado). El id de cada nodo SHALL coincidir con el emisor o destino que usan los eventos (`intake` para `eda-notice`, `interpreter`, `orchestrator`, `executor-<acción>`, `projectors`…), para poder resaltar la arista de cada evento. `GET /api/topology` SHALL devolver esos nodos (id, etiqueta, tipo, descripción y si se vigila su proceso) y aristas (origen, destino).

#### Scenario: Estado de un nodo

- **GIVEN** el proceso de un agente o ejecutor
- **WHEN** el backend consulta el estado de procesos
- **THEN** el nodo se pinta como activo si el proceso existe y como caído si no
- **AND** la cabecera muestra «Procesos: N de 12»

#### Scenario: Salud de la infraestructura

- **GIVEN** Kafka, PostgreSQL y Ollama
- **WHEN** el front consulta `GET /api/health`
- **THEN** cada uno se muestra como healthy o down en la cabecera

#### Scenario: Servicio caído

- **GIVEN** Ollama parado o que no responde en 2 segundos
- **WHEN** el front consulta `GET /api/health`
- **THEN** Ollama aparece como down y el resto de comprobaciones responde igual

#### Scenario: Comunicación en curso

- **GIVEN** un comando ejecutándose que genera eventos
- **WHEN** llega un evento nuevo
- **THEN** se resaltan unos segundos (3 s) su emisor, su destino y las aristas del camino más corto entre ellos en el mapa (ningún par emisor y destino de un evento es una arista directa: un `StepCompleted` va de `executor-<acción>` a `orchestrator` pasando por PostgreSQL, el relay y Kafka)
- **AND** un id que no está en el mapa no resalta nada

### Requirement: Detalle de un nodo

Al seleccionar un nodo el front SHALL abrir un panel con su descripción, el estado del proceso, los eventos que emite y consume y, para los agentes con LLM, las métricas derivadas de los eventos `LlmCallRecorded` (llamadas, tokens de entrada y salida, latencia, validaciones superadas). `GET /api/nodes/{id}` SHALL devolver esos datos: los tipos que consume salen de las mismas declaraciones que el destino de los eventos, y los que emite, de los eventos con ese `producer` en el log. Los ejecutores `notify_customer` y `file_claim` muestran las métricas del redactor (`writer`), que se invoca desde ellos. Un id desconocido responde 404.

#### Scenario: Filtrar eventos por nodo

- **GIVEN** el panel de detalle del planificador abierto
- **WHEN** el usuario pulsa «Filtrar eventos»
- **THEN** el panel inferior muestra solo los eventos emitidos o recibidos por el planificador

#### Scenario: Métricas de un agente con LLM

- **GIVEN** dos `LlmCallRecorded` del planificador, con 100 y 300 tokens de entrada, 20 y 40 de salida, 1000 y 3000 ms y una sola validación superada
- **WHEN** se pide `GET /api/nodes/planner`
- **THEN** las métricas son 2 llamadas, 400 tokens de entrada, 60 de salida, 2000 ms de latencia media y 1 validación superada

### Requirement: Panel de eventos intercambiados

El panel SHALL mostrar los eventos más recientes primero, con las columnas emisor, destino, tipo de evento y timestamp, y además seq, incident_id, plan, partición/offset, event_id, causation_id y payload. La tabla MUST ser más ancha que el panel y tener desplazamiento horizontal a la derecha, con la cabecera fija en el desplazamiento vertical. SHALL permitir filtrar por incidencia y pausar la actualización en vivo. La altura del panel SHALL poder cambiarse, solo en vertical, con un separador entre el mapa y el panel: arrastrándolo o, con el separador enfocado, con las flechas arriba y abajo (20 px por pulsación) e Inicio y Fin. La altura MUST quedar entre 96 px (la barra y la cabecera de la tabla) y la que deja al mapa al menos 160 px; el mapa ocupa el resto. La altura elegida se recuerda en ese navegador.

#### Scenario: Reducir el panel para ver el mapa

- **GIVEN** el panel de eventos a 340 px de alto
- **WHEN** el usuario arrastra el separador 200 px hacia abajo
- **THEN** el panel mide 140 px y el mapa gana esos 200 px
- **AND** el ancho de las columnas no cambia
- **AND** si sigue arrastrando hacia abajo, el panel no baja de 96 px

#### Scenario: Desplazamiento lateral

- **GIVEN** el panel con todas las columnas
- **WHEN** el usuario se desplaza a la derecha
- **THEN** ve las columnas restantes sin que se mueva el resto de la pantalla

#### Scenario: Eventos en vivo

- **GIVEN** el panel en estado «en vivo»
- **WHEN** se añade un evento al log
- **THEN** aparece en el panel en menos de 2 segundos, ordenado por `seq`
- **AND** si el usuario pulsa «Pausar», no se añaden más filas hasta reanudar

#### Scenario: Partición y offset de un evento publicado

- **GIVEN** un evento que `event_log` consumió de la partición 2 con offset 1290 (spec event-backbone, "Kafka como bus de entrega")
- **WHEN** se muestra la columna partición/offset
- **THEN** muestra `p2 / 1290`

#### Scenario: Evento aún no consumido

- **GIVEN** un evento que todavía no está en `event_log.events` (aún no se ha publicado o `event_log` no lo ha consumido)
- **WHEN** se pide la página de eventos
- **THEN** no aparece
- **AND** aparece, con su partición y su offset, en cuanto `event_log` lo guarda

### Requirement: Vista de analítica de pedidos

Además de la vista principal, que es de auditoría (mapa y eventos), el front SHALL tener una vista de negocio con el estado de los pedidos afectados por incidencias, accesible desde la principal con un enlace «Analítica» en la cabecera, siempre visible, y con un enlace de vuelta. Comparte la cabecera con el estado de la infraestructura.

La vista SHALL mostrar:

- un contador por estado de la vista `order_status` (spec `cqrs-views`, «Estado de los pedidos»): Envío retrasado (`ShipmentDelayed`), En proceso de solución (`PlanCreated`/`PlanRevised`), Solucionada (`IncidentResolved` `resolved`), Reembolsada (`IncidentResolved` `refunded`) y Escalada (`IncidentEscalated`); pulsar un contador filtra la tabla por ese estado;
- una tabla de pedidos, los cambiados más recientemente primero, con las columnas pedido, estado, incidencia, último evento, timestamp y `seq`.

La vista SHALL seguir las consideraciones de UX de la vista principal: la tabla MUST ser más ancha que su panel cuando no quepa, con desplazamiento horizontal a la derecha sin mover el resto de la pantalla y la cabecera fija en el desplazamiento vertical; SHALL actualizarse en vivo (un cambio aparece en menos de 2 segundos) con un control «Pausar»/«Reanudar»; y SHALL poder filtrarse por incidencia, como el panel de eventos. Solo lee la vista `order_status` (CQRS): no consulta el log ni las tablas de ningún componente.

#### Scenario: Acceso desde la vista principal

- **GIVEN** el front abierto en la vista principal
- **WHEN** el usuario pulsa «Analítica» en la cabecera
- **THEN** ve los contadores por estado y la tabla de pedidos
- **AND** vuelve a la vista principal con el enlace de vuelta, sin perder el filtro ni la altura del panel de eventos

#### Scenario: Pedido reembolsado en vivo

- **GIVEN** la vista de analítica en vivo con P-1001 en «En proceso de solución»
- **WHEN** se proyecta el `IncidentResolved` de su incidencia con `outcome` `refunded`
- **THEN** en menos de 2 segundos P-1001 aparece como «Reembolsada» y los contadores cambian

#### Scenario: Filtro por estado

- **GIVEN** pedidos en varios estados
- **WHEN** el usuario pulsa el contador «Envío retrasado»
- **THEN** la tabla solo muestra los pedidos en `delayed`

#### Scenario: Desplazamiento lateral en la analítica

- **GIVEN** la tabla de pedidos más ancha que su panel
- **WHEN** el usuario se desplaza a la derecha
- **THEN** ve las columnas restantes sin que se mueva el resto de la pantalla
- **AND** la cabecera de la tabla sigue visible al desplazarse en vertical

### Requirement: Origen de emisor y destino

El emisor de un evento SHALL ser su campo `producer`. El destino SHALL derivarse de los tipos de evento que declara cada componente (design.md §3): los agentes en `eda_retail.agents.cli.AGENTS` (salvo los que ya solo atienden comandos REST, como `planner`) y el orquestador en `eda_retail.orchestrator.consumer.EVENT_TYPES`. Los proyectores leen todos los eventos y SHALL figurar siempre como último consumidor. El destino principal es el primer consumidor de esa lista y el resto se muestra en el detalle; un evento que solo leen los proyectores tiene `projectors` como destino.

#### Scenario: Evento con consumidor único

- **GIVEN** un `PlanCreated`
- **WHEN** se muestra en el panel
- **THEN** emisor `planner` y destino `orchestrator`

#### Scenario: Resultado de un ejecutor

- **GIVEN** un `StepCompleted` publicado por `executor-notify_customer`
- **WHEN** se muestra en el panel
- **THEN** su emisor es `executor-notify_customer` y su destino `orchestrator`

#### Scenario: Evento con varios consumidores

- **GIVEN** un `StepFailed`
- **WHEN** se muestra en el panel
- **THEN** su destino principal es `orchestrator` y el detalle añade `projectors`

#### Scenario: Evento sin consumidor de Kafka

- **GIVEN** un `LlmCallRecorded`
- **WHEN** se muestra en el panel
- **THEN** su destino es `projectors`

### Requirement: Página de eventos

`GET /api/events` SHALL devolver los eventos de la vista del log (`event_log.events`, spec `cqrs-views`) del más reciente al más antiguo, con `limit` entre 1 y 500 (100 por defecto) y `before_seq` para pedir la página anterior. SHALL admitir los filtros `incident_id` (por `correlation_id` o por el `incident_id` del payload) y `node` (eventos que ese nodo emite o consume). La respuesta SHALL incluir `next_before_seq`, el `seq` desde el que pedir la página siguiente, o `null` si no hay más.

#### Scenario: Paginar hacia atrás

- **GIVEN** 5 eventos y `limit=2`
- **WHEN** el cliente pide la primera página y después la siguiente con `next_before_seq`
- **THEN** recibe los `seq` más altos primero, sin repetir ni saltar ninguno

#### Scenario: Filtrar por nodo

- **GIVEN** un `CustomerNotified` de `executor-notify_customer` y un `ClaimFiled` de `executor-file_claim`
- **WHEN** se pide `node=executor-notify_customer`
- **THEN** solo aparece el `CustomerNotified`

#### Scenario: Límite fuera de rango

- **GIVEN** cualquier estado del log
- **WHEN** se pide `limit=0` o `limit=501`
- **THEN** la API responde 422

### Requirement: Acceso de solo lectura y local

El backend SHALL leer los eventos de la vista del log `event_log.events` y el estado de las incidencias de las vistas `view_*`, sin escribir en ninguna tabla ni leer las de otros componentes. SHALL escuchar solo en `127.0.0.1`, no admitir orígenes distintos de `localhost` y ejecutar un único comando a la vez. SHALL rechazar con 403 las peticiones cuya cabecera `Host` no sea `localhost`, `127.0.0.1` o `[::1]` (con o sin puerto), para que una web externa no pueda llegar al backend redirigiendo su dominio a 127.0.0.1 (DNS rebinding).

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

Con `WEB_INTERACTIVE=true`, `interactive --timeout N` SHALL ejecutarse con una pseudoterminal y el front SHALL ofrecer una entrada de texto para enviar líneas a su entrada estándar mientras dure la sesión. N MUST estar entre 1 y 600. Cada línea MUST tener como mucho 200 caracteres y ningún salto de línea; enviar una línea a una ejecución que no es interactiva o que ya terminó se rechaza con 409. Enviar una línea MUST NOT bloquear la salida del proceso: si su entrada está llena porque no la lee, la línea se rechaza con 409 y la salida sigue llegando. Si el seguimiento de la salida falla, la ejecución MUST terminar con un código de salida para no dejar el menú ocupado.

#### Scenario: Responder al menú

- **GIVEN** una sesión interactiva con una pregunta pendiente
- **WHEN** el usuario escribe una opción y envía
- **THEN** el backend la entrega al proceso y la salida continúa en el área de salida

#### Scenario: Proceso que no lee su entrada

- **GIVEN** una sesión interactiva cuyo proceso no lee la entrada y sigue escribiendo
- **WHEN** el usuario envía líneas hasta llenar su entrada
- **THEN** las líneas que no caben se rechazan con 409 y la salida del proceso sigue llegando hasta su código de salida

### Requirement: Actualización en vivo

El backend SHALL exponer los eventos nuevos en `GET /api/events/stream` por Server-Sent Events, con `seq` como identificador y cada evento con las mismas columnas que la página de eventos, de modo que el cliente retome desde `Last-Event-ID` tras una reconexión sin perder ni duplicar filas. Un `Last-Event-ID` que no sea un número ASCII se ignora, en este stream y en el de la salida de los comandos. En la primera conexión el navegador no puede enviar `Last-Event-ID`, así que el stream SHALL admitir `after_seq` (el `seq` más alto que el panel ya muestra); si llegan los dos, manda `Last-Event-ID`. Sin ninguno, el stream empieza tras el último `seq` del log. El backend SHALL consultar el log al menos cada 0,5 s para cumplir el plazo de 2 s del panel.

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

La cabecera SHALL ofrecer un enlace a la interfaz web de Jaeger, que se abre en una pestaña nueva. Cada fila del panel de eventos SHALL ofrecer un enlace «ver traza» a la traza de ese evento en Jaeger (`<JAEGER_UI_URL>/trace/<trace_id>`). El backend MUST obtener el `trace_id` del `traceparent` que `event_log` guarda en `event_log.events` para ese `seq`, con una consulta de solo lectura. La URL base sale de `JAEGER_UI_URL` (por defecto `http://localhost:16686`, el puerto publicado en `docker-compose.yml`); el backend la entrega al front en `GET /api/config` y MUST ignorar un valor que no sea una URL `http` o `https`, usando entonces el valor por defecto. Cada evento de la página y del stream lleva su `trace_id` (o `null`). El front MUST NOT consultar la API de Jaeger: solo enlaza a su interfaz.

#### Scenario: Abrir Jaeger desde la cabecera

- **GIVEN** el front abierto
- **WHEN** el usuario pulsa el enlace a Jaeger de la cabecera
- **THEN** se abre `JAEGER_UI_URL` en una pestaña nueva

#### Scenario: Ver la traza de un evento

- **GIVEN** un evento cuyo `trace_context` en `event_log.events` tiene `traceparent` `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
- **WHEN** se muestra en el panel de eventos
- **THEN** su enlace «ver traza» apunta a `http://localhost:16686/trace/4bf92f3577b34da6a3ce929d0e0e4736`

#### Scenario: Evento sin traza

- **GIVEN** un evento escrito con las trazas desactivadas (`TRACING_ENABLED=false`), sin `trace_context` o con un `traceparent` mal formado
- **WHEN** se muestra en el panel de eventos
- **THEN** la columna de traza muestra «—» y no hay enlace

#### Scenario: URL de Jaeger no válida

- **GIVEN** `JAEGER_UI_URL=javascript:alert(1)`
- **WHEN** el front pide `GET /api/config`
- **THEN** recibe `http://localhost:16686`

## API prevista

| Método y ruta | Uso |
| --- | --- |
| `GET /api/events?limit=&before_seq=&incident_id=&node=` | Página de eventos con emisor, destino y `trace_id` |
| `GET /api/events/stream?after_seq=` | SSE de eventos nuevos (`Last-Event-ID` al reconectar) |
| `GET /api/topology` | Nodos y aristas del mapa |
| `GET /api/nodes/{id}` | Detalle y métricas de un nodo |
| `GET /api/health` | Kafka, PostgreSQL, Ollama y procesos |
| `GET /api/analytics/orders?status=&incident_id=` | Contadores por estado y pedidos de `order_status`, con `applied` |
| `GET /api/config` | Configuración para el front (`jaeger_ui_url`, `free_text_enabled`, `interactive_enabled`) |
| `POST /api/commands` | Ejecuta un comando de la lista blanca; devuelve `run_id` |
| `GET /api/commands/{run_id}/stream` | SSE con la salida y, al final, el código de salida |
| `POST /api/commands/{run_id}/input` | Línea para `interactive` |

## Decisiones

- Estado de procesos: se deduce con `pgrep -f` y los mismos patrones que `scripts/run-agents.sh`; PostgreSQL con `SELECT 1` en una conexión de solo lectura, Kafka listando los topics y Ollama con `GET /api/tags` (`OLLAMA_HOST`). Cada comprobación tiene un tiempo máximo de 2 s.
- Arranque: `scripts/run-web.sh` lanza `eda-web` (127.0.0.1:`WEB_PORT`, 8000 por defecto) y el servidor de Vite en http://localhost:5173, que reenvía `/api` al mismo `WEB_PORT`; los dos leen `.env`. `scripts/start-all.sh` lo arranca junto con los contenedores y los procesos de la PoC, y `scripts/stop-all.sh` lo para todo, Vite incluido.
- Partición y offset: `event_log` los guarda en `event_log.events` al consumir cada mensaje; la API los lee de ahí. La vista es solo de añadir.
