"""spec event-backbone, «Pérdida del topic»: orden de la republicación, sin Kafka ni PostgreSQL. La republicación
contra Kafka real está en tests/integration/test_component_outbox.py."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from eda_retail.infra.outbox import republish_order

T0 = datetime(2026, 11, 18, tzinfo=UTC)


def _row(schema: str, id_: int, partition, offset, minute: int = 0) -> dict:
    return {"schema": schema, "id": id_, "kafka_partition": partition, "kafka_offset": offset,
            "created_at": T0 + timedelta(minutes=minute)}


def test_republication_order():
    """Orden de la republicación"""
    rows = [
        _row("orchestrator", 1, 0, 5), _row("planner", 1, 0, 4), _row("executor_file_claim", 1, 0, 6),
        _row("intake", 1, 1, 0), _row("orchestrator", 2, None, None, minute=2),
        _row("planner", 2, None, None, minute=1),
    ]
    ordered = [(r["schema"], r["id"]) for r in republish_order(rows)]
    assert ordered == [("planner", 1), ("orchestrator", 1), ("executor_file_claim", 1), ("intake", 1),
                       ("planner", 2), ("orchestrator", 2)]
