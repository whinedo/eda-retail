"""Vista audit_trail sin base de datos: qué eventos audita y con qué motivo."""

from __future__ import annotations

from eda_retail.events.catalog import build_event
from eda_retail.infra.positions import Position
from eda_retail.projectors import audit_trail


class Capture:
    def __init__(self) -> None:
        self.params: list[tuple] = []

    def execute(self, sql, params=None):
        self.params.append(params)


def test_carrier_consulted_is_audited_with_its_outcome_as_reason():
    """CarrierConsulted entra en la auditoría (spec a2a-carrier): actor planner, motivo = outcome"""
    event = build_event("CarrierConsulted", {"incident_id": "I-1", "outcome": "timeout", "rounds": 0,
                                             "latency_ms": 5000}, correlation_id="I-1", causation_id=None,
                        producer="planner")
    conn = Capture()

    audit_trail.HANDLERS["CarrierConsulted"](conn, Position("eda.events", 0, 7), event)

    [params] = conn.params
    assert params[3:8] == ("I-1", None, "planner", "CarrierConsulted", "timeout")
