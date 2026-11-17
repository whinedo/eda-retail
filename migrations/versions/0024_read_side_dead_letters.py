"""Dead letters del lado de lectura: `event_log.dead_letters` y `views.dead_letters` (spec resilience,
«Dead letter queue»; experimento E18).

`event_log` y los proyectores dejan de saltarse en silencio los mensajes inválidos: los guardan en la
dead letter de su esquema en la misma transacción que su posición, y un relay que solo publica dead
letters los lleva a `eda.dlq`. Son consumidores posicionados sin inbox, y su posición se puede
rebobinar (reconstrucción de vistas, E21), así que la dead letter se identifica por su posición en
Kafka: un mensaje reaplicado no crea una segunda fila.

Revision ID: 0024
Revises: 0023
"""

from alembic import op

revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None

SCHEMAS = ("event_log", "views")


def upgrade() -> None:
    for schema in SCHEMAS:
        op.execute(
            f"""
            CREATE TABLE {schema}.dead_letters (
                id              BIGSERIAL PRIMARY KEY,
                consumer        TEXT        NOT NULL,
                event_id        UUID,
                correlation_id  TEXT,
                reason          TEXT        NOT NULL,
                raw_message     JSONB       NOT NULL,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                published_at    TIMESTAMPTZ,
                kafka_topic     TEXT        NOT NULL,
                kafka_partition INTEGER     NOT NULL,
                kafka_offset    BIGINT      NOT NULL,
                UNIQUE (consumer, kafka_topic, kafka_partition, kafka_offset)
            )
            """
        )
        op.execute(f"CREATE INDEX dead_letters_pending_idx ON {schema}.dead_letters (id) WHERE published_at IS NULL")


def downgrade() -> None:
    for schema in SCHEMAS:
        op.execute(f"DROP TABLE {schema}.dead_letters")
