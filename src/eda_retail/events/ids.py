"""Identificadores derivados que comparten varios componentes."""

from __future__ import annotations

import uuid

_NOTICE_NAMESPACE = uuid.UUID("6f1d7c0e-2b8a-4c55-9d3e-7a1b2c3d4e5f")


def incident_id_for_notice(carrier_notice_id: str) -> str:
    """Un aviso da lugar como mucho a una incidencia: su id se deriva del aviso."""
    return f"I-{uuid.uuid5(_NOTICE_NAMESPACE, carrier_notice_id)}"
