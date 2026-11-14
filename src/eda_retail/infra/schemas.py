"""Un esquema de PostgreSQL por componente (design.md §2, spec event-backbone "Propiedad de los datos").

Hay una sola base de datos para la PoC, pero cada componente escribe solo en su esquema y sus
tablas no se relacionan con las de otro: sin claves foráneas ni referencias entre esquemas.
Aquí se definen las tablas de infraestructura que reutiliza cada componente:

- `outbox`: eventos pendientes de publicar por el relay del componente, con su contexto de
  traza y la posición de Kafka que confirmó el broker.
- `inbox`: eventos y comandos ya procesados (`message_id` = `event_id` o `command_id`). En un
  comando guarda además su respuesta, para devolver la misma si se repite (comandos idempotentes).
- `dead_letters`: mensajes irrecuperables, que el relay publica en `eda.dlq`.
- `effect_keys`: claves de idempotencia de los efectos, solo en quien ejecuta efectos.

Las tablas propias de cada dominio (estado y proyecciones) las añade su tarea.

La migración 0008 aplica `ddl()` a su lista congelada de componentes: cambiar estas tablas o
añadir un componente exige una migración nueva, nunca editar `ddl()`, porque una base nueva y una
ya migrada acabarían distintas.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class Component:
    publishes: bool = True  # tiene outbox, inbox y dead letters
    effect_keys: bool = False


EXECUTOR_ACTIONS = ("notify_customer", "file_claim", "reissue_shipment", "issue_refund")

COMPONENTS: dict[str, Component] = {
    "intake": Component(),
    "interpreter": Component(),
    "planner": Component(),
    "replanner": Component(),
    "orchestrator": Component(),
    **{f"executor_{action}": Component(effect_keys=True) for action in EXECUTOR_ACTIONS},
    "inventory": Component(effect_keys=True),
    # Lado de lectura: consumen Kafka y no publican; sus tablas llegan con T3.
    "event_log": Component(publishes=False),
    "views": Component(publishes=False),
}


def qualified(schema: str, table: str) -> str:
    """`<schema>.<table>`, con el nombre del esquema validado (se interpola en el SQL)."""
    return f"{check_schema(schema)}.{table}"


def check_schema(schema: str) -> str:
    if not _IDENTIFIER.match(schema):
        raise ValueError(f"nombre de esquema no válido: {schema!r}")
    return schema


def ddl(name: str) -> list[str]:
    """Sentencias idempotentes que crean el esquema del componente y sus tablas de infraestructura."""
    check_schema(name)
    if name not in COMPONENTS:
        raise ValueError(f"componente desconocido: {name!r}")
    component = COMPONENTS[name]
    statements = [f"CREATE SCHEMA IF NOT EXISTS {name}"]
    if not component.publishes:
        return statements
    statements += [
        f"""CREATE TABLE IF NOT EXISTS {name}.outbox (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID        NOT NULL UNIQUE,
    event_type      TEXT        NOT NULL,
    event_version   INTEGER     NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    correlation_id  TEXT        NOT NULL,
    causation_id    UUID,
    producer        TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    trace_context   JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    kafka_partition INTEGER,
    kafka_offset    BIGINT
)""",
        f"CREATE INDEX IF NOT EXISTS outbox_pending_idx ON {name}.outbox (id) WHERE published_at IS NULL",
        f"""CREATE TABLE IF NOT EXISTS {name}.inbox (
    message_id   UUID PRIMARY KEY,
    kind         TEXT NOT NULL CHECK (kind IN ('event', 'command')),
    status_code  INTEGER,
    response     JSONB,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
        f"""CREATE TABLE IF NOT EXISTS {name}.dead_letters (
    id             BIGSERIAL PRIMARY KEY,
    consumer       TEXT        NOT NULL,
    event_id       UUID,
    correlation_id TEXT,
    reason         TEXT        NOT NULL,
    raw_message    JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
)""",
    ]
    if component.effect_keys:
        statements += [
            f"""CREATE TABLE IF NOT EXISTS {name}.effect_keys (
    idempotency_key TEXT PRIMARY KEY,
    incident_id     TEXT        NOT NULL,
    step_id         TEXT        NOT NULL,
    action          TEXT        NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('in_progress', 'completed')),
    result          JSONB,
    locked_until    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
)""",
            f"CREATE INDEX IF NOT EXISTS effect_keys_incident_idx ON {name}.effect_keys (incident_id)",
        ]
    return statements
