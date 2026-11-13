"""`plan_steps`: el `step_id` es único por incidencia, no global.

El planificador asigna `step_id` como S1, S2…, que se repiten entre incidencias. La
clave primaria pasa a (`incident_id`, `step_id`) y `compensates` referencia un paso de la
misma incidencia.

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE plan_steps DROP CONSTRAINT IF EXISTS plan_steps_compensates_fkey")
    op.execute("ALTER TABLE plan_steps DROP CONSTRAINT plan_steps_pkey")
    op.execute("ALTER TABLE plan_steps ADD CONSTRAINT plan_steps_pkey PRIMARY KEY (incident_id, step_id)")
    op.execute(
        "ALTER TABLE plan_steps ADD CONSTRAINT plan_steps_compensates_fkey "
        "FOREIGN KEY (incident_id, compensates) REFERENCES plan_steps (incident_id, step_id)"
    )


def downgrade() -> None:
    # Falla si dos incidencias comparten step_id: es la restricción que se quita al subir.
    op.execute("ALTER TABLE plan_steps DROP CONSTRAINT plan_steps_compensates_fkey")
    op.execute("ALTER TABLE plan_steps DROP CONSTRAINT plan_steps_pkey")
    op.execute("ALTER TABLE plan_steps ADD CONSTRAINT plan_steps_pkey PRIMARY KEY (step_id)")
    op.execute(
        "ALTER TABLE plan_steps ADD CONSTRAINT plan_steps_compensates_fkey "
        "FOREIGN KEY (compensates) REFERENCES plan_steps (step_id)"
    )
