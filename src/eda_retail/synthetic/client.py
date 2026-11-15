"""Cliente de `eda-notice` (y del front, que lo lanza): invoca por REST los comandos de `intake`
(`RegisterCarrierNotice`) y de `inventory` (`SeedSampleData`) (design.md §3 y §4.4).

`eda-notice` no escribe en ninguna tabla: solo envía comandos a los sistemas que los atienden.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass

from eda_retail.config import Settings
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.infra import command_client
from eda_retail.infra.command_client import CommandReply
from eda_retail.inventory.handler import SAMPLE_DATA_CORRELATION
from eda_retail.synthetic.sample_data import CARRIER, ORDERS, STOCK


class SeedRejected(RuntimeError):
    """`inventory` rechazó los datos de ejemplo."""


@dataclass(frozen=True)
class Registered:
    accepted: bool
    carrier_notice_id: str
    incident_id: str
    reason: str | None = None


class NoticeClient:
    def __init__(self, settings: Settings, *, invoke: Callable[..., CommandReply] = command_client.invoke) -> None:
        self._settings = settings
        self._invoke = invoke

    def register(self, text: str, *, carrier: str = CARRIER, carrier_notice_id: str | None = None) -> Registered:
        """Registra un aviso del transportista. Lanza `CommandUnavailable` si `intake` no responde."""
        notice = carrier_notice_id or f"N-{uuid.uuid4().hex[:8]}"
        incident_id = incident_id_for_notice(notice)
        reply = self._invoke(
            self._settings.intake_url, "RegisterCarrierNotice", command_id=uuid.uuid4(), correlation_id=incident_id,
            causation_id=None, payload={"carrier_notice_id": notice, "carrier": carrier, "raw_text": text},
            timeout=self._settings.command_http_timeout_s,
        )
        return Registered(reply.accepted, notice, incident_id, reply.reason)

    def seed(self) -> tuple[int, int]:
        """Carga los pedidos y el stock de ejemplo en `inventory`. Devuelve (pedidos, stock) nuevos."""
        payload = {
            "orders": [
                {"order_id": o, "customer_ref": c, "sku": s, "quantity": q, "amount": str(a), "carrier": CARRIER}
                for o, c, s, q, a in ORDERS
            ],
            "stock": [{"warehouse": w, "sku": s, "available": a} for w, s, a in STOCK],
        }
        reply = self._invoke(
            self._settings.inventory_url, "SeedSampleData", command_id=uuid.uuid4(),
            correlation_id=SAMPLE_DATA_CORRELATION, causation_id=None, payload=payload,
            timeout=self._settings.command_http_timeout_s,
        )
        if not reply.accepted:
            raise SeedRejected(f"SeedSampleData rechazado: {reply.reason}")
        return reply.body["orders"], reply.body["stock"]
