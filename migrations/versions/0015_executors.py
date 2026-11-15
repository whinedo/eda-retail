"""Tablas de efectos de los ejecutores (T9, odd/tasks/eda-component-ownership.md).

Cada ejecutor escribe solo en su esquema (`effect_keys`, `outbox`, `inbox` y `dead_letters` ya existen
desde la 0008):
- `executor_notify_customer.customer_messages` y `llm_calls` (redactor);
- `executor_file_claim.claims` y `llm_calls` (redactor);
- `executor_reissue_shipment.shipments`;
- `executor_issue_refund.refunds`.

Sin claves foráneas hacia otros esquemas. Es aditiva: las tablas públicas de efectos y los ejecutores
anteriores se quedan hasta T10 y T11.

Revision ID: 0015
Revises: 0014
"""

from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None

_LLM_CALLS = """
CREATE TABLE {schema}.llm_calls (
    id                BIGSERIAL PRIMARY KEY,
    incident_id       TEXT        NOT NULL,
    command_id        UUID        NOT NULL,
    model             TEXT        NOT NULL,
    tokens_in         INTEGER     NOT NULL,
    tokens_out        INTEGER     NOT NULL,
    latency_ms        INTEGER     NOT NULL,
    validation_passed BOOLEAN     NOT NULL,
    attempt           INTEGER     NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
)"""

STATEMENTS = (
    """
CREATE TABLE executor_notify_customer.customer_messages (
    message_id  TEXT PRIMARY KEY,
    incident_id TEXT        NOT NULL,
    order_id    TEXT        NOT NULL,
    step_id     TEXT        NOT NULL,
    text        TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    _LLM_CALLS.format(schema="executor_notify_customer"),
    """
CREATE TABLE executor_file_claim.claims (
    claim_id    TEXT PRIMARY KEY,
    incident_id TEXT        NOT NULL,
    order_id    TEXT        NOT NULL,
    step_id     TEXT        NOT NULL,
    text        TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    _LLM_CALLS.format(schema="executor_file_claim"),
    """
CREATE TABLE executor_reissue_shipment.shipments (
    shipment_id TEXT PRIMARY KEY,
    incident_id TEXT        NOT NULL,
    order_id    TEXT        NOT NULL,
    step_id     TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    """
CREATE TABLE executor_issue_refund.refunds (
    refund_id   TEXT PRIMARY KEY,
    incident_id TEXT          NOT NULL,
    order_id    TEXT          NOT NULL,
    step_id     TEXT          NOT NULL,
    amount      NUMERIC(10,2) NOT NULL CHECK (amount > 0),
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
)""",
)

TABLES = (
    "executor_notify_customer.customer_messages", "executor_notify_customer.llm_calls",
    "executor_file_claim.claims", "executor_file_claim.llm_calls",
    "executor_reissue_shipment.shipments", "executor_issue_refund.refunds",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for table in reversed(TABLES):
        op.execute(f"DROP TABLE {table}")
