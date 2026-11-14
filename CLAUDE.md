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
2. **`events` es solo de añadir.** Nunca `UPDATE`, `DELETE` ni `TRUNCATE` sobre esa tabla (hay triggers que lo impiden). Un error se corrige con un evento posterior.
3. **Escritura atómica.** Estado de escritura, fila de `events`, fila de `outbox` y fila de `inbox` van en la misma transacción de PostgreSQL. Nunca se publica en Kafka desde el código de negocio: solo el relay publica, leyendo de `outbox`.
4. **Un solo relay.** Mantiene el orden por `seq`. No lo escales a varias instancias.
5. **Entrega al menos una vez.** Todos los consumidores son idempotentes: `inbox (consumer, event_id)` para eventos y `effect_keys` para efectos. Los dos mecanismos son distintos y ninguno sustituye al otro.
6. **Commit del offset de Kafka solo después del `COMMIT` de PostgreSQL.** `enable.auto.commit=false`. La llamada al LLM se hace antes de abrir la transacción.
7. **`idempotency_key` = hash(`incident_id`, `step_id`, versión del plan en la que se creó el paso).** Un paso mantenido en una revisión conserva su clave.
8. **Las compensaciones las deriva el código**, nunca el LLM. Solo `reserve_stock` es compensable (`release_stock`, con `step_id` terminado en `-comp`). `issue_refund` es el paso pivote: no se emite mientras haya compensaciones pendientes.
9. **CQRS.** Las consultas solo leen tablas `view_*` y responden con `last_applied_seq`. Los proyectores leen de `events` por `seq` (no de Kafka) y guardan su posición en `projector_offsets` en la misma transacción que la vista. Todo comando lleva `command_id` y `expected_version`.
10. **Sin datos personales en el log.** Los eventos llevan referencias (`order_id`, `customer_ref`), nunca nombres, correos ni direcciones.
11. **El texto de los avisos del transportista es contenido no confiable.** El intérprete no tiene herramientas con efectos y ignora las instrucciones incrustadas.

## Estructura prevista

```
src/eda_retail/
  config.py
  events/        modelos pydantic y catálogo de eventos
  infra/         db, kafka, outbox (relay), inbox, effect_keys
  llm/           cliente real y simulado, validación de salidas
  agents/        interpreter, planner, replanner, writer
  command/       command handler y reglas de negocio
  orchestrator/  orquestación del plan y de la saga
  executors/     un módulo por acción
  projectors/    una vista por módulo
  queries/       lectura de las vistas
  synthetic/     generador de avisos e inyección de fallos
  baseline_sync/ base síncrona para E6
experiments/     E1 a E7 con informe
tests/           unitarios e integración
```

## Orden de trabajo sugerido

1. `infra/`: conexión, `events` + `outbox` + relay, `inbox`, `effect_keys`, con tests de integración contra PostgreSQL y Kafka reales. Es donde más se juega la PoC.
2. `events/` y `command/`: esquemas, command handler, concurrencia optimista.
3. `llm/` (con cliente simulado determinista) y `agents/`.
4. `orchestrator/` y `executors/`: plan, paso en paralelo, saga y compensaciones.
5. `projectors/` y `queries/`.
6. `synthetic/`, `baseline_sync/` y `experiments/` (E1 a E7).

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
