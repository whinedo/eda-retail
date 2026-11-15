"""Trabajos de los comandos largos (odd/tasks/eda-async-commands.md T2; design.md §4.4).

`<componente>.command_jobs` en cada componente cuyos comandos llaman al LLM o a un servicio externo
(`planner`, `replanner` y los cuatro ejecutores): la petición acepta el comando y deja aquí su trabajo en la
misma transacción que su inbox; el worker del componente lo hace después (`infra/jobs.py`).

- `job_id` es la clave: sin unicidad por `command_id`, porque sin inbox (E1, modo `none`) un comando repetido
  debe repetir el trabajo.
- `status`: `pending` (espera su intento), `running` (tomado, con `lease_until`), `done` o `rejected`.
- `trace_context`: el contexto W3C de la petición, para que el trabajo continúe su traza.

La lista de componentes está congelada aquí, como en la 0008: añadir uno exige otra migración.

También cambia `orchestrator.deadlines` (T3): el plazo empieza a contar cuando el dispatcher envía el comando
por primera vez, no cuando la saga lo encola. La saga guarda `timeout_s` y deja `expires_at` en NULL hasta el
primer envío; un plazo sin `expires_at` no vence. El downgrade da a esos plazos su vencimiento desde ahora.

Revision ID: 0019
Revises: 0018
"""

from alembic import op

revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None

COMPONENTS = ("planner", "replanner", "executor_notify_customer", "executor_file_claim",
              "executor_reissue_shipment", "executor_issue_refund")

_JOBS = """
CREATE TABLE {schema}.command_jobs (
    job_id          BIGSERIAL PRIMARY KEY,
    command_id      UUID        NOT NULL,
    command_type    TEXT        NOT NULL,
    correlation_id  TEXT        NOT NULL,
    causation_id    UUID,
    payload         JSONB       NOT NULL,
    trace_context   JSONB,
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'running', 'done', 'rejected')),
    attempts        INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    outcome         JSONB
)"""

STATEMENTS = tuple(
    statement
    for schema in COMPONENTS
    for statement in (
        _JOBS.format(schema=schema),
        (f"CREATE INDEX command_jobs_claim_idx ON {schema}.command_jobs (next_attempt_at, job_id)"
         " WHERE status IN ('pending', 'running')"),
        (f"CREATE INDEX command_jobs_open_idx ON {schema}.command_jobs (correlation_id)"
         " WHERE status IN ('pending', 'running')"),
    )
)


DEADLINES = (
    "ALTER TABLE orchestrator.deadlines ADD COLUMN timeout_s DOUBLE PRECISION",
    "ALTER TABLE orchestrator.deadlines ALTER COLUMN expires_at DROP NOT NULL",
)


def upgrade() -> None:
    for statement in (*STATEMENTS, *DEADLINES):
        op.execute(statement)


def downgrade() -> None:
    op.execute("UPDATE orchestrator.deadlines SET expires_at = now() + make_interval(secs => coalesce(timeout_s, 0))"
               " WHERE expires_at IS NULL")
    op.execute("ALTER TABLE orchestrator.deadlines ALTER COLUMN expires_at SET NOT NULL")
    op.execute("ALTER TABLE orchestrator.deadlines DROP COLUMN timeout_s")
    for schema in reversed(COMPONENTS):
        op.execute(f"DROP TABLE {schema}.command_jobs")
