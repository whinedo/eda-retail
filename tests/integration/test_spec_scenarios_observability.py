"""Escenarios de la spec observability-audit sin test propio (T3, grupo C).

La traza se lee del exportador en memoria que instala `tests/conftest.py` (en lugar de Jaeger): el arnés ejecuta
una incidencia completa con los mismos componentes que el sistema desplegado, en un solo proceso.
"""

from __future__ import annotations

import pytest

from eda_retail.events.ids import incident_id_for_notice
from eda_retail.experiments.harness import RunConfig, started
from eda_retail.synthetic.generator import generate

pytestmark = pytest.mark.slow

SEED = 1  # un retraso con un solo paso (`notify_customer`) que se resuelve

# Atributos que nombran el componente que produjo el span, del más específico al más general.
COMPONENT_ATTRIBUTES = ("eda.component", "messaging.consumer.group.name", "eda.agent", "eda.projector", "eda.producer")


def _component(span) -> str | None:
    return next((span.attributes[a] for a in COMPONENT_ATTRIBUTES if span.attributes.get(a)), None)


def _first_start(trace: list, name: str) -> int:
    starts = [s.start_time for s in trace if s.name == name]
    assert starts, f"la traza no tiene el span {name}"
    return min(starts)


def test_follow_an_incident_trace(base_settings, spans):
    """Seguimiento de una incidencia"""
    data = generate(SEED, 1)
    incident = incident_id_for_notice(data.notices[0].carrier_notice_id)

    # GIVEN una incidencia resuelta
    with started(base_settings, RunConfig(seed=SEED, incidents=1), data=data) as h:
        result = h.result(h.elapsed)
    assert result.resolved == 1

    # WHEN se consulta su traza (la del aviso, como en Jaeger con el tag eda.incident_id)
    finished = spans.get_finished_spans()
    notice = [s for s in finished if s.name == "CarrierNoticeReceived create"
              and s.attributes.get("eda.incident_id") == incident]
    assert len(notice) == 1
    trace = [s for s in finished if s.context.trace_id == notice[0].context.trace_id]

    # THEN muestra la secuencia aviso, interpretación, plan, pasos y cierre
    sequence = ["CarrierNoticeReceived create", "llm interpreter", "ShipmentDelayed create", "PlanCreated create",
                "StepRequested create", "StepCompleted create", "IncidentResolved create"]
    starts = [_first_start(trace, name) for name in sequence]
    assert starts == sorted(starts)

    # AND cada span indica su duración y el componente que lo produjo
    for span in trace:
        assert span.start_time is not None and span.end_time is not None and span.end_time >= span.start_time
        assert span.resource.attributes.get("service.name")
    tagged = [s for s in trace if s.attributes.get("eda.incident_id") == incident]
    assert tagged and all(_component(s) for s in tagged), [s.name for s in tagged if not _component(s)]
