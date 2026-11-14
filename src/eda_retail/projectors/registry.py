"""Los cuatro proyectores de las vistas CQRS, con las tablas `view_*` que mantiene cada uno."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from eda_retail.projectors import audit_trail, carrier_dashboard, cost_per_incident, incident_status
from eda_retail.projectors.base import Projector


@dataclass(frozen=True)
class ProjectorSpec:
    build: Callable[..., Projector]
    tables: tuple[str, ...]
    # Columnas que no salen del log (p. ej. la hora de la última escritura): no entran en la huella.
    volatile: frozenset[str] = frozenset()


PROJECTORS: dict[str, ProjectorSpec] = {
    incident_status.PROJECTOR_NAME: ProjectorSpec(
        incident_status.build_projector, ("view_incident_status",), frozenset({"updated_at"})
    ),
    cost_per_incident.PROJECTOR_NAME: ProjectorSpec(cost_per_incident.build_projector, ("view_cost_per_incident",)),
    audit_trail.PROJECTOR_NAME: ProjectorSpec(audit_trail.build_projector, ("view_audit_trail",)),
    carrier_dashboard.PROJECTOR_NAME: ProjectorSpec(
        carrier_dashboard.build_projector, ("view_carrier_dashboard", "view_carrier_incidents")
    ),
}


def build_all(**kwargs: Any) -> list[Projector]:
    return [spec.build(**kwargs) for spec in PROJECTORS.values()]
