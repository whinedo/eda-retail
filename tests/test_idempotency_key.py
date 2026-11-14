"""idempotency: clave de idempotencia por paso (sin base de datos)."""

from __future__ import annotations

from eda_retail.infra.effect_keys import compute_idempotency_key


def test_same_key_for_same_step():
    """Misma clave para el mismo paso"""
    first = compute_idempotency_key("I1", "S1", origin_plan_version=1)
    second = compute_idempotency_key("I1", "S1", origin_plan_version=1)
    assert first == second


def test_step_kept_in_revision():
    """Paso mantenido en una revisión"""
    created_in_v1 = compute_idempotency_key("I1", "S1", origin_plan_version=1)
    # En la versión 2 se vuelve a emitir con la versión de origen, no con la vigente.
    reissued_in_v2 = compute_idempotency_key("I1", "S1", origin_plan_version=1)
    assert reissued_in_v2 == created_in_v1
    assert compute_idempotency_key("I1", "S1", origin_plan_version=2) != created_in_v1


def test_key_has_no_ambiguous_concatenation():
    """La clave distingue identificadores que concatenados serían iguales"""
    assert compute_idempotency_key("I1", "2S", origin_plan_version=1) != compute_idempotency_key(
        "I12", "S", origin_plan_version=1
    )
