"""Versión y huella del prompt en cada `llm_calls` (spec agent-harness, «Registro común de las llamadas con la versión
del prompt»): las filas anteriores quedan con `prompt_version` `unknown` y `prompt_hash` nulo, como sus
`LlmCallRecorded` v1.

Revision ID: 0029
Revises: 0028
"""

from alembic import op

revision = "0029"
down_revision = "0028"
branch_labels = None
depends_on = None

SCHEMAS = ("interpreter", "planner", "replanner", "executor_notify_customer", "executor_file_claim")


def upgrade() -> None:
    for schema in SCHEMAS:
        op.execute(f"ALTER TABLE {schema}.llm_calls ADD COLUMN prompt_version TEXT NOT NULL DEFAULT 'unknown',"
                   " ADD COLUMN prompt_hash TEXT")


def downgrade() -> None:
    for schema in SCHEMAS:
        op.execute(f"ALTER TABLE {schema}.llm_calls DROP COLUMN prompt_hash, DROP COLUMN prompt_version")
