"""Invariantes comunes de resiliencia I1 a I5 (spec experiments, «Invariantes comunes de resiliencia»).

Todo experimento de resiliencia (E11 en adelante) los comprueba al terminar, con el sistema parado, y los
guarda en el informe con su recuento de violaciones. Su hipótesis no se da por cumplida si I1 o I2 tienen
alguna violación (`gate`).

- I1: incidencias del log (`CarrierNoticeReceived`) sin `IncidentResolved` ni `IncidentEscalated`.
- I2: efectos externos repetidos (`Harness.duplicates`, los mismos que mide E1).
- I3: más de un reembolso por incidencia y pedido, y `issue_refund` pedidos (`StepRequested`) con alguna
  compensación de la incidencia pedida y aún sin completar en el log.
- I4: filas de `outbox` sin publicar o cuyo evento no llegó a `event_log.events` (perdidas).
- I5: vistas cuya huella reconstruida desde el topic no coincide con la mantenida (`projectors.rebuild`).
  Reconstruye las vistas en su sitio, así que va lo último.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from eda_retail.experiments.harness import PUBLISHERS, Harness
from eda_retail.infra.schemas import qualified
from eda_retail.orchestrator.rules import COMPENSATION_SUFFIX
from eda_retail.projectors.rebuild import rebuild
from eda_retail.projectors.registry import PROJECTORS

GATING = ("I1", "I2")


def refunds_with_pending_compensations(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """`issue_refund` pedidos mientras su incidencia tenía una compensación pedida y sin completar (I3).

    `events`: `StepRequested` y `StepCompleted` del log en orden de `seq`, con `correlation_id` y `payload`."""
    pending: dict[str, set[str]] = {}
    out = []
    for e in events:
        incident, p = e["correlation_id"], e["payload"]
        if e["event_type"] == "StepRequested":
            if p["step_id"].endswith(COMPENSATION_SUFFIX):
                pending.setdefault(incident, set()).add(p["step_id"])
            elif p["action"] == "issue_refund" and pending.get(incident):
                out.append({"incident_id": incident, "step_id": p["step_id"],
                            "pending": sorted(pending[incident])})
        elif e["event_type"] == "StepCompleted":
            pending.get(incident, set()).discard(p["step_id"])
    return out


def _rows(h: Harness, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    rows = [dict(r) for r in h.conn.execute(sql, params).fetchall()]
    h.conn.commit()
    return rows


def check(h: Harness, *, views: bool = True, volatile: frozenset[str] = frozenset()) -> dict[str, dict[str, Any]]:
    """I1 a I5 de la ejecución del arnés `h`: `{"I1": {"violations": n, ...}, ...}`. Con `views=False` no
    reconstruye las vistas (I5 queda sin comprobar, `None`). `volatile`: columnas que I5 no compara (E19: la
    posición en Kafka, que cambia al republicar)."""
    log_caught_up = h.catch_up_log()  # I1, I3 e I4 leen event_log
    open_ = _rows(h, """
        SELECT DISTINCT correlation_id FROM event_log.events WHERE event_type = 'CarrierNoticeReceived'
        EXCEPT SELECT correlation_id FROM event_log.events WHERE event_type IN ('IncidentResolved', 'IncidentEscalated')
    """)
    duplicates = h.duplicates()
    refunds = _rows(h, """
        SELECT incident_id, order_id, count(*) AS n FROM executor_issue_refund.refunds
        GROUP BY incident_id, order_id HAVING count(*) > 1
    """)
    steps = _rows(h, """
        SELECT correlation_id, event_type, payload FROM event_log.events
        WHERE event_type IN ('StepRequested', 'StepCompleted') ORDER BY seq
    """)
    early = refunds_with_pending_compensations(steps)
    lost = _rows(h, " UNION ALL ".join(
        f"SELECT '{schema}' AS component, o.event_id, o.published_at IS NOT NULL AS published"
        f" FROM {qualified(schema, 'outbox')} o"
        f" WHERE o.published_at IS NULL OR NOT EXISTS (SELECT 1 FROM event_log.events e WHERE e.event_id = o.event_id)"
        for schema in PUBLISHERS))
    out: dict[str, dict[str, Any]] = {
        "I1": {"violations": len(open_), "open": [r["correlation_id"] for r in open_][:20]},
        "I2": {"violations": sum(duplicates.values()), "repeated": duplicates},
        "I3": {"violations": len(refunds) + len(early), "repeated_refunds": refunds[:20],
               "refunds_with_pending_compensations": early[:20]},
        "I4": {"violations": len(lost), "lost": [{**r, "event_id": str(r["event_id"])} for r in lost][:20],
               "log_caught_up": log_caught_up},
    }
    if views:
        differ = [view for view in sorted(PROJECTORS) if not rebuild(h.settings, h.conn, view, volatile=volatile).identical]
        out["I5"] = {"violations": len(differ), "views": differ}
    else:
        out["I5"] = {"violations": None, "views": []}
    return out


def gate(invariants: Mapping[str, Mapping[str, Any]]) -> bool:
    """False si I1 o I2 tienen alguna violación: la hipótesis no se da por cumplida."""
    return all(invariants[name]["violations"] == 0 for name in GATING)


def summary(invariants: Mapping[str, Mapping[str, Any]]) -> dict[str, int | None]:
    """Recuento de violaciones por invariante."""
    return {name: inv["violations"] for name, inv in invariants.items()}
