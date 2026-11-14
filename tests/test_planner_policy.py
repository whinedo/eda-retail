"""La política de planes del prompt del planificador coincide con el plan esperado (supuesto A1)."""

from __future__ import annotations

import pytest

from eda_retail.agents import planner
from eda_retail.synthetic.generator import VARIANTS, expected_plan


@pytest.mark.parametrize("cause", VARIANTS)
@pytest.mark.parametrize("severity", ["low", "medium", "high"])
def test_policy_matches_expected_plan(cause, severity):
    """El prompt pide, para cada causa y gravedad, las acciones del plan esperado"""
    expected = [s["action"] for s in expected_plan(cause, severity, "P1")]
    assert list(planner.policy_actions(cause, severity)) == expected


def test_policy_is_written_in_the_prompt():
    """Cada fila de la política aparece en el prompt de sistema del planificador"""
    for when, actions in planner.POLICY:
        assert f"- {when}: {', '.join(actions)}." in planner.SYSTEM
