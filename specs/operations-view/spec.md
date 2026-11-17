# Operations view Specification

## Purpose

Ofrecer, dentro de la analítica del front, una segunda pantalla de operaciones que responde a «¿cómo funciona el sistema?»: cuántas incidencias se resuelven y cuántas se escalan y por qué, cuánto cuestan, cuánto tardan, cuánto replanifican, qué calidad tiene el LLM y cuán al día están las vistas. La vista de pedidos (spec `web-front`, «Vista de analítica de pedidos») responde a «¿cómo está cada pedido?» y sigue siendo la pantalla por defecto de la analítica. Como el resto del front, es una vista de la PoC y no parte de su lógica: solo lee proyecciones (specs `cqrs-views` e `incident-metrics`) y no escribe en el log ni salta el command handler.

Mockups: pendientes de validar. Esta spec no los sustituye; se parte de los de `docs/front/` y de la vista de analítica de pedidos, de la que hereda cabecera, estilo y controles.

## Requirements

### Requirement: Acceso desde la analítica de pedidos

La vista de operaciones SHALL ser una pantalla nueva dentro de la analítica y MUST NOT añadir ningún enlace nuevo a la cabecera. El enlace «Analítica» de la cabecera sigue abriendo, por defecto, la vista actual del estado de los pedidos (`#/analytics`). Esa vista SHALL tener, junto a su título, un enlace «Operaciones» que abre la vista de operaciones (`#/analytics/operations`). La vista de operaciones SHALL tener un enlace de vuelta «← Estado de los pedidos» y, como la de pedidos, el de «← Vista principal». Comparte la cabecera con el estado de la infraestructura y el aviso de avisos aparcados. Abrir la dirección de operaciones directamente MUST mostrar la vista de operaciones.

#### Scenario: Entrada por la cabecera

- **GIVEN** el front abierto en la vista principal
- **WHEN** el usuario pulsa «Analítica» en la cabecera
- **THEN** ve la vista del estado de los pedidos, con el enlace «Operaciones» junto a su título
- **AND** no hay un enlace «Operaciones» en la cabecera

#### Scenario: De pedidos a operaciones y vuelta

- **GIVEN** la vista de pedidos con un filtro por incidencia
- **WHEN** el usuario pulsa «Operaciones» y después «← Estado de los pedidos»
- **THEN** vuelve a la vista de pedidos sin perder ese filtro

#### Scenario: Volver a la vista principal

- **GIVEN** la vista de operaciones
- **WHEN** el usuario pulsa «← Vista principal»
- **THEN** ve el mapa y el panel de eventos, sin perder la altura que tenía el panel

#### Scenario: Dirección directa

- **GIVEN** el front cargado desde `http://localhost:5173/#/analytics/operations`
- **WHEN** termina de cargar
- **THEN** muestra la vista de operaciones

### Requirement: Resumen de estado y motivos de escalado

La vista SHALL mostrar, arriba, un contador por estado de incidencia de `incident_metrics`: Abierta (`open`), Solucionada (`resolved`), Reembolsada (`refunded`) y Escalada (`escalated`), con las mismas etiquetas y colores que la vista de pedidos (Escalada en rojo). Debajo, el desglose de las escaladas por motivo (`escalation_reason`): una barra por motivo con su recuento y su porcentaje sobre el total de escaladas. Pulsar un contador o un motivo SHALL filtrar la tabla de incidencias (requirement «Tabla de incidencias»); pulsarlo de nuevo quita el filtro. Un motivo que no esté en el glosario de eventos del manual (`no_valid_orders`, `llm_output_invalid`, `budget_exhausted`, `max_revisions`, `refund_limit`, `compensation_failed`, `step_timeout`, `plan_timeout`, `command_rejected`) se muestra con el texto recibido, sin error. Los contadores y los motivos se calculan sobre la incidencia filtrada cuando hay filtro por incidencia, no sobre el filtro de estado ni el de motivo.

#### Scenario: Escaladas por motivo

- **GIVEN** 23 incidencias solucionadas y 7 escaladas, todas por `llm_output_invalid`
- **WHEN** el usuario abre la vista de operaciones
- **THEN** los contadores muestran 23 en «Solucionada» y 7 en «Escalada»
- **AND** el desglose muestra una barra `llm_output_invalid` con 7 (100 %)

#### Scenario: Filtrar por motivo

- **GIVEN** escaladas por `llm_output_invalid` y por `step_timeout`
- **WHEN** el usuario pulsa la barra `step_timeout`
- **THEN** la tabla solo muestra las incidencias escaladas por ese motivo

#### Scenario: Sin escaladas

- **GIVEN** ninguna incidencia escalada
- **WHEN** se muestra el resumen
- **THEN** el contador «Escalada» vale 0 y el desglose dice «Sin incidencias escaladas»

#### Scenario: Motivo desconocido

- **GIVEN** una incidencia escalada con un motivo que no está en el glosario
- **WHEN** se muestra el desglose
- **THEN** aparece una barra con el texto de ese motivo y la vista no falla

### Requirement: Tabla de incidencias

Debajo del resumen, una tabla con una fila por incidencia, las cambiadas más recientemente primero, con las columnas: incidencia, estado (Escalada en rojo), motivo de escalado, transportista, causa y gravedad, versión del plan, pasos fallidos, compensaciones, duración, tokens, coste y último evento con su partición y su offset. La duración es `closed_at` − `opened_at` y queda vacía mientras la incidencia está abierta; el coste sale de `cost_per_incident` con los precios de la configuración, y con los precios por defecto muestra 0 salvo los reembolsos. Cada fila SHALL tener un enlace «Ver eventos» que abre la vista principal con el filtro de incidencia ya puesto. La tabla SHALL poder filtrarse por incidencia, como el panel de eventos y la vista de pedidos, y por los contadores y motivos del resumen. Como las demás tablas, MUST ser más ancha que su panel cuando no quepa, con desplazamiento horizontal sin mover el resto de la pantalla y la cabecera fija en el desplazamiento vertical, y se pagina con «Cargar más» (100 filas cada vez).

#### Scenario: Incidencia con una revisión

- **GIVEN** una incidencia `loss` de gravedad `high` que pasó por un `PlanRevised` y terminó con reembolso
- **WHEN** el usuario mira la tabla
- **THEN** su fila muestra estado «Reembolsada», versión del plan 2, 1 paso fallido y su duración

#### Scenario: Incidencia abierta

- **GIVEN** una incidencia con plan y sin cierre
- **WHEN** el usuario mira la tabla
- **THEN** su estado es «Abierta» y su duración está vacía

#### Scenario: Ver los eventos de una incidencia

- **GIVEN** una fila de la tabla
- **WHEN** el usuario pulsa «Ver eventos»
- **THEN** se abre la vista principal con el `incident_id` de esa fila en el filtro **Incidencia** del panel de eventos

#### Scenario: Filtro por incidencia

- **GIVEN** incidencias distintas
- **WHEN** el usuario escribe un `incident_id` completo y pulsa Intro
- **THEN** la tabla, los contadores y los desgloses se limitan a esa incidencia

#### Scenario: Desplazamiento lateral en operaciones

- **GIVEN** la tabla más ancha que su panel
- **WHEN** el usuario se desplaza a la derecha
- **THEN** ve las columnas restantes sin que se mueva el resto de la pantalla
- **AND** la cabecera de la tabla sigue visible al desplazarse en vertical

### Requirement: Coste por escenario

La vista SHALL mostrar una tabla de coste por pareja de causa y gravedad (`delay`/`low`, `loss`/`high`…) con: incidencias, media de tokens, coste del LLM, coste de los efectos e importe reembolsado, con los datos de la consulta de resumen. Cuando todos los precios valgan 0 (configuración por defecto con Ollama local), SHALL destacar los tokens y avisar de que el coste en dinero es 0 porque no hay precios configurados, con el nombre de las variables (`LLM_PRICE_IN_PER_1K`, `LLM_PRICE_OUT_PER_1K`, `EFFECT_COST_CLAIM`, `EFFECT_COST_REISSUE`).

#### Scenario: Una pérdida cuesta más que un retraso leve

- **GIVEN** incidencias `delay`/`low`, planificadas por código sin LLM del planificador, y `loss`/`high`, con LLM y cuatro pasos
- **WHEN** el usuario mira el coste por escenario
- **THEN** `loss`/`high` muestra más tokens de media que `delay`/`low`

#### Scenario: Precios sin configurar

- **GIVEN** ninguna variable de precios definida
- **WHEN** se muestra la tabla
- **THEN** los costes en dinero valen 0, salvo los reembolsos, y la vista indica que no hay precios configurados

#### Scenario: Incidencias sin causa

- **GIVEN** incidencias escaladas con `no_valid_orders`, sin causa ni gravedad
- **WHEN** se muestra la tabla
- **THEN** aparecen agrupadas en una fila «Sin interpretar»

### Requirement: Llamadas al LLM por agente

La vista SHALL mostrar, por agente con LLM (intérprete, planificador, replanificador y redactor), las llamadas, los tokens de entrada y de salida, la latencia media y el p95, y el porcentaje de validaciones superadas, con los datos de `get_llm_stats`. El p95 SHALL tener un texto de ayuda que diga que se calcula por rango más próximo.

#### Scenario: Latencia y validaciones de un agente

- **GIVEN** 20 llamadas del planificador con latencias de 100 a 2000 ms, de las que 15 superaron la validación
- **WHEN** el usuario mira la tabla de agentes
- **THEN** el planificador muestra 20 llamadas, latencia media 1050 ms, p95 1900 ms y 75 % de validaciones superadas

#### Scenario: Agente sin llamadas

- **GIVEN** un agente sin ningún `LlmCallRecorded`
- **WHEN** se muestra la tabla
- **THEN** su fila aparece con 0 llamadas y sin latencia ni porcentaje, en lugar de ocultarse

### Requirement: Replanificación y pasos fallidos

La vista SHALL mostrar la distribución de incidencias por versión del plan (v1, v2, v3…) y una tabla de pasos fallidos por acción y código de error (`reserve_stock` con `out_of_stock`, etc.), de `incident_metrics` y `view_step_failures`.

#### Scenario: Distribución por versión del plan

- **GIVEN** 40 incidencias con plan v1 y 10 con plan v2
- **WHEN** el usuario mira la distribución
- **THEN** ve 40 en v1 y 10 en v2, y las de v2 son las que se replanificaron

#### Scenario: Fallos por acción

- **GIVEN** 10 `StepFailed` de `reserve_stock` con error `out_of_stock` y 2 de `file_claim` con `timeout`
- **WHEN** el usuario mira la tabla de pasos fallidos
- **THEN** aparece `reserve_stock` / `out_of_stock` con 10 y `file_claim` / `timeout` con 2

### Requirement: Transportistas y consultas A2A

La vista SHALL mostrar, por transportista, los retrasos, las reclamaciones y los reembolsos de `carrier_dashboard`. Si existen consultas al agente del transportista (versión 2, spec `a2a-carrier`), SHALL mostrar además, por resultado (`answered`, `timeout`, `failed`, `rejected_by_guardrail`), el número de consultas, las rondas medias y la latencia media. Con la versión 1, esa parte no se muestra o dice que las consultas solo existen con `POC_VERSION=2`; no es un error.

#### Scenario: Consultas A2A en la versión 2

- **GIVEN** 8 `CarrierConsulted` `answered` y 2 `timeout`
- **WHEN** el usuario mira la sección
- **THEN** ve 8 `answered` y 2 `timeout` con sus rondas y latencias medias

#### Scenario: Versión 1

- **GIVEN** un log sin `CarrierConsulted`
- **WHEN** se muestra la sección
- **THEN** no hay tabla de consultas, y los datos de `carrier_dashboard` se ven igual

### Requirement: Frescura de las vistas

La vista SHALL mostrar, por proyector y por partición, la posición aplicada, el *end offset* y el `lag`, y el `lag` total por proyector, con los datos de la consulta de retraso (spec `incident-metrics`, «Retraso de las vistas»). Un `lag` mayor que 0 se destaca; un `lag` nulo (Kafka sin respuesta) se muestra como «sin datos», no como 0. El texto de ayuda MUST explicar que las cifras de esta vista van por detrás de los eventos (consistencia eventual) y que el `lag` mide cuánto.

#### Scenario: Proyector al día

- **GIVEN** un `lag` total de 0 en todos los proyectores
- **WHEN** el usuario mira la sección
- **THEN** ve las posiciones y `lag` 0, sin destacar nada

#### Scenario: Proyector atrasado

- **GIVEN** el proyector `operations` con 3 mensajes sin aplicar en la partición 1
- **WHEN** el usuario mira la sección
- **THEN** ve `lag` 3 destacado en esa partición y en el total de `operations`

#### Scenario: Kafka sin respuesta

- **GIVEN** Kafka parado
- **WHEN** se muestra la sección
- **THEN** las posiciones aplicadas se ven y el `lag` dice «sin datos»

### Requirement: Actualización en vivo

La vista SHALL actualizarse en vivo con los mismos criterios que la de pedidos: un cambio aparece en menos de 2 segundos, con un control «Pausar» / «Reanudar» que congela la pantalla sin perder los cambios, e indicador «en vivo» / «en pausa». La vista MUST consultar las proyecciones, no el log, y cada respuesta incluye `applied`, para que la pantalla indique hasta qué posición ha leído. El botón «Resetear simulación» de la cabecera MUST NOT afectar a esta vista, como no afecta a la de pedidos: la analítica cubre todo el histórico.

#### Scenario: Incidencia que se cierra en vivo

- **GIVEN** la vista en vivo con una incidencia «Abierta»
- **WHEN** se proyecta su `IncidentEscalated` con motivo `step_timeout`
- **THEN** en menos de 2 segundos pasa a «Escalada», los contadores cambian y aparece la barra `step_timeout`

#### Scenario: Vista en pausa

- **GIVEN** la vista en pausa
- **WHEN** se cierran incidencias
- **THEN** la pantalla no cambia
- **AND** al pulsar «Reanudar» aparecen todos los cambios

#### Scenario: Resetear simulación

- **GIVEN** la vista de operaciones con datos
- **WHEN** el usuario pulsa «Resetear simulación» en la cabecera
- **THEN** la vista de operaciones conserva sus datos

### Requirement: Estados vacíos y errores

Con un log vacío, o sin ninguna incidencia, la vista SHALL mostrar los contadores en 0 y las tablas vacías con un texto («Sin incidencias todavía»), sin error. Si falla la carga, SHALL mostrar un error rojo encima de las tablas, como la vista de pedidos, y reintentar en el siguiente refresco. Si el backend no responde, la cabecera muestra «Backend: sin conexión».

#### Scenario: Primer arranque

- **GIVEN** un sistema recién arrancado, sin eventos
- **WHEN** el usuario abre la vista de operaciones
- **THEN** ve los cuatro contadores en 0, el desglose de escaladas vacío y la tabla con «Sin incidencias todavía»

#### Scenario: Fallo de carga

- **GIVEN** una consulta que falla
- **WHEN** el front la pide
- **THEN** aparece un error en rojo y la vista se recupera sola cuando la consulta vuelve a responder

### Requirement: Acceso de solo lectura y local

La vista de operaciones MUST NOT escribir nada: sus endpoints son `GET`, solo leen las vistas de `incident-metrics`, `cost_per_incident`, `carrier_dashboard`, las posiciones aplicadas (`views.offsets` y `event_log.offsets`) y los metadatos de Kafka de la consulta de retraso, y el backend sigue escuchando solo en `127.0.0.1` (spec `web-front`, «Acceso de solo lectura y local»).

#### Scenario: Escritura rechazada

- **GIVEN** el backend en marcha
- **WHEN** se envía un `POST` a cualquier ruta `/api/operations/*`
- **THEN** responde 405 y no cambia nada

## API prevista

| Método y ruta | Uso |
| --- | --- |
| `GET /api/operations/summary?incident_id=` | Contadores por estado, escaladas por motivo, distribución por versión del plan, pasos fallidos y coste por escenario; con `applied` |
| `GET /api/operations/incidents?state=&reason=&incident_id=&limit=&before=` | Página de la tabla de incidencias, con duración y coste, con `applied` y `next_before` (el `before` de la página siguiente) |
| `GET /api/operations/llm` | Llamadas, tokens, latencia media y p95 y validaciones por agente, con `applied` |
| `GET /api/operations/carriers` | `carrier_dashboard` y, con la versión 2, las consultas A2A por resultado |
| `GET /api/operations/freshness` | Posición aplicada, *end offset* y `lag` por proyector y partición |

## Decisiones

- **Navegación.** La analítica sigue teniendo una única entrada en la cabecera, y la vista de pedidos sigue siendo su pantalla por defecto; operaciones cuelga de ella con un enlace. Así no cambia la cabecera ni el recorrido que describe el manual.
- **Rutas.** `#/analytics` (pedidos) y `#/analytics/operations` (operaciones).
- **«Ver eventos».** Abrir la vista principal con el filtro de incidencia puesto es un comportamiento nuevo del front: hoy el filtro se escribe a mano. Hay que añadirlo a la spec `web-front` al implementarlo.
- **Reseteo.** «Resetear simulación» no afecta a la analítica ni a esta vista, porque no borra datos y la analítica lee las proyecciones completas.
- **Fuera de alcance.** El panel de resultados de experimentos (`experiments/results`), alarmas o umbrales, exportación a CSV y un historial por intervalos de tiempo.

## Cambios a aplicar en otras specs al implementarla

Esta spec no modifica las existentes. Para dejarlas coherentes hay que añadir, al implementar:

- `web-front`: en «Vista de analítica de pedidos», el enlace «Operaciones» junto al título; el filtro de incidencia por enlace en la vista principal; y las rutas de «API prevista».
- `cqrs-views`: la nueva vista en «Vistas mínimas», con referencia a `incident-metrics`.
- `experiments`: las vistas nuevas en la huella de reconstrucción del E4.
- `README.md` y `docs/manual-usuario-eda-retail.md`: la lista de specs y la sección 9 del manual.
