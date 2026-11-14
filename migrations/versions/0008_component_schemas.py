"""Un esquema por componente con sus tablas de infraestructura (T2, odd/tasks/eda-component-ownership.md).

Crea el esquema de cada componente (`infra.schemas.COMPONENTS`) con su `outbox`, `inbox` y
`dead_letters`, y `effect_keys` en los ejecutores e `inventory`. Es aditiva: las tablas
compartidas del modelo anterior (`events`, `outbox`, `inbox`…) siguen hasta T10.

Revision ID: 0008
Revises: 0007
"""

from alembic import op

from eda_retail.infra.schemas import COMPONENTS, ddl

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name in COMPONENTS:
        for statement in ddl(name):
            op.execute(statement)


def downgrade() -> None:
    for name in reversed(list(COMPONENTS)):
        op.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
