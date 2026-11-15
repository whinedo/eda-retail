"""Los proyectores de las vistas CQRS, con las tablas `views.view_*` que mantiene cada uno."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from eda_retail.projectors import (
    audit_trail,
    carrier_dashboard,
    cost_per_incident,
    incident_status,
    order_status,
    orders,
)
from eda_retail.projectors.base import Projector


@dataclass(frozen=True)
class ProjectorSpec:
    build: Callable[..., Projector]
    tables: tuple[str, ...]
    # Columnas que no salen del log (p. ej. la hora de la última escritura): no entran en la huella.
    volatile: frozenset[str] = frozenset()


PROJECTORS: dict[str, ProjectorSpec] = {
    orders.PROJECTOR_NAME: ProjectorSpec(orders.build_projector, ("views.view_orders",)),
    incident_status.PROJECTOR_NAME: ProjectorSpec(
        incident_status.build_projector, ("views.view_incident_status",), frozenset({"updated_at"})
    ),
    cost_per_incident.PROJECTOR_NAME: ProjectorSpec(
        cost_per_incident.build_projector, ("views.view_cost_per_incident",)
    ),
    audit_trail.PROJECTOR_NAME: ProjectorSpec(audit_trail.build_projector, ("views.view_audit_trail",)),
    carrier_dashboard.PROJECTOR_NAME: ProjectorSpec(
        carrier_dashboard.build_projector, ("views.view_carrier_dashboard", "views.view_carrier_incidents")
    ),
    order_status.PROJECTOR_NAME: ProjectorSpec(order_status.build_projector, ("views.view_order_status",)),
}


def build_all(**kwargs: Any) -> list[Projector]:
    return [spec.build(**kwargs) for spec in PROJECTORS.values()]
