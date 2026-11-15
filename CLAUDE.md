# CLAUDE.md: PoC académica de EDA con agentes (incidencias logísticas)

Sistema de agentes orientado a eventos que gestiona incidencias de transporte (retrasos, pérdidas, averías) de una tienda online. Es una PoC académica: el objetivo es medir fiabilidad, recuperación, calidad del plan, coste y latencia frente a una base síncrona, no construir un producto.

## Fuentes de verdad

Antes de escribir código, lee lo que afecte a la tarea:

- `docs/README.md`: contexto, catálogo de eventos, catálogo de comandos y decisiones asumidas.
- `specs/<capacidad>/spec.md`: requisitos en estilo OpenSpec (SHALL/MUST y escenarios GIVEN/WHEN/THEN). **Cada escenario debe tener un test.**
- `design.md`: cómo materializa la pila cada concepto (tablas, topics, flujos).
- `db/init/001_schema.sql`: esquema de PostgreSQL.
- `docs/diagrama-agentes.*`, `diagrama-saga.*`, `diagrama-cqrs.*`: diagramas.

Si el código y una spec se contradicen, manda la spec. Si la spec está mal, se corrige la spec en el mismo cambio y se avisa; no se esquiva en el código.

## Pila

Python 3.12, Apache Kafka (Docker, KRaft, un broker), PostgreSQL 16 (Docker, volumen persistente), `confluent-kafka`, `psycopg` 3, `pydantic`, `pytest`. Infraestructura local: `docker compose up -d` (el `docker-compose.yml` aún no se ha ejecutado nunca: comprueba que el broker queda `healthy` y que se crean los topics).

## Reglas de diseño que no se negocian

1. **El LLM propone y el código dispone.** Ningún agente con LLM escribe en el log, ejecuta efectos ni tiene herramientas con efectos. Toda salida del LLM se valida con un esquema (pydantic) y con reglas de negocio en código antes de convertirse en evento.
2. **El log es solo de añadir.** Kafka (`eda.events`, retención infinita) es el log; `event_log.events`, su vista consultable, nunca recibe `UPDATE`, `DELETE` ni `TRUNCATE` (hay triggers que lo impiden). Un error se corrige con un evento posterior.
3. **Cada componente es un Aggregate con su esquema.** Escribe solo en sus tablas (`<esquema>.*`); su estado, su `outbox` y su `inbox` van en la misma transacción de PostgreSQL. Nunca se publica en Kafka desde el código de negocio: solo el relay del componente publica, leyendo de su `outbox`. Ningún componente lee las tablas de otro: lo que necesita le llega en el comando o lo consulta en las vistas.
4. **Un relay por componente.** Publica su outbox en orden. No lo escales a varias instancias.
5. **Entrega al menos una vez.** Todos los consumidores son idempotentes: `inbox (consumer, event_id)` para eventos y `effect_keys` para efectos. Los dos mecanismos son distintos y ninguno sustituye al otro.
6. **Commit del offset de Kafka solo después del `COMMIT` de PostgreSQL.** `enable.auto.commit=false`. La llamada al LLM se hace antes de abrir la transacción.
7. **`idempotency_key` = hash(`incident_id`, `step_id`, versión del plan en la que se creó el paso).** Un paso mantenido en una revisión conserva su clave.
8. **Las compensaciones las deriva el código**, nunca el LLM. Solo `reserve_stock` es compensable (`release_stock`, con `step_id` terminado en `-comp`). `issue_refund` es el paso pivote: no se emite mientras haya compensaciones pendientes.
9. **Comandos por REST, eventos por Kafka (CQRS).** Los comandos son peticiones REST idempotentes (`command_id`, inbox del destinatario) que invoca la saga; por Kafka solo viajan eventos, y la saga reacciona a ellos. Las consultas solo leen `views.view_*` y responden con `applied` (offset por partición). Los proyectores consumen Kafka y guardan su posición en `views.offsets` en la misma transacción que la vista.
10. **Sin datos personales en el log.** Los eventos llevan referencias (`order_id`, `customer_ref`), nunca nombres, correos ni direcciones.
11. **El texto de los avisos del transportista es contenido no confiable.** El intérprete no tiene herramientas con efectos y ignora las instrucciones incrustadas.

## Estructura

```
src/eda_retail/
  config.py, rules.py, plan_validation.py
  events/        modelos pydantic y catálogo de eventos
  infra/         db, kafka, outbox y relay por esquema, inbox, effect_keys, comandos REST, consumidores
  llm/           cliente real y simulado, validación de salidas, prompts (llm/prompts)
  intake/ inventory/ interpreter/ planner/ replanner/   un componente por paquete (su esquema)
  orchestrator/  la saga: reglas, transiciones, dispatcher de comandos y plazos
  step_executors/ executor_<acción>/   runtime común y un paquete por ejecutor
  event_log/ projectors/ queries/      lado de lectura
  synthetic/     generador de avisos, datos de ejemplo e inyección de fallos
  baseline_sync/ base síncrona para E6 (esquema propio)
  experiments/   E1 a E7 con informe
tests/           unitarios e integración
```

## Orden de trabajo sugerido

Para un componente nuevo o un cambio en uno existente:

1. Spec y catálogo: escenarios en `specs/`, eventos en `events/catalog.py` y comandos en el catálogo de comandos.
2. Esquema propio: una migración nueva con sus tablas, su `outbox`, `inbox` y `dead_letters` (y `effect_keys` si ejecuta efectos).
3. Command handler (`infra/commands.py`) o consumidor (`infra/consumer.py`) con la lógica del Aggregate, con tests de integración contra PostgreSQL y Kafka reales. Los componentes con LLM se prueban con el cliente simulado.
4. Lo que lo usa: transiciones de la saga (`orchestrator/`), proyecciones (`projectors/`, `queries/`) y front.
5. `experiments/` si cambia algo que miden E1 a E7.

## Convenciones

- **Tests:** un test por escenario de las specs, con el nombre del escenario en el docstring. Los de integración usan Postgres y Kafka reales en Docker (no mocks de la base de datos). Los componentes con LLM se prueban con el cliente simulado.
- **Determinismo:** el generador de datos y la inyección de fallos usan semilla. Los experimentos deben ser repetibles.
- **Idioma:** documentación y mensajes de error en español. Nombres de código, eventos y comandos en inglés. Términos técnicos en inglés cuando sean de uso general (inbox, outbox, consumer, retry).
- **Configuración** por variables de entorno (`.env.example`). Nada de credenciales en el repositorio.
- **Interruptores para experimentos:** `INBOX_ENABLED` y `EFFECT_KEYS_ENABLED` permiten medir el sistema sin idempotencia (E1). Por defecto están activos.
- **Migraciones:** `db/init/001_schema.sql` solo se ejecuta al crear el volumen. Si el esquema cambia, añade Alembic y migraciones; no edites el volumen a mano.

## Qué no hacer

- No uses transacciones de Kafka ni exactly-once: la garantía viene de inbox y `effect_keys`.
- No leas del log ni del estado de escritura para responder consultas.
- No permitas que un plan propuesto por el LLM incluya pasos de compensación (se rechaza con `compensation_not_allowed_in_plan`).
- No añadas acciones fuera de la lista permitida: `notify_customer`, `file_claim`, `reserve_stock`, `reissue_shipment`, `release_stock`, `issue_refund`.
- No cambies el nombre ni la forma de un evento del catálogo sin actualizar la spec, el esquema y `event_version`.
