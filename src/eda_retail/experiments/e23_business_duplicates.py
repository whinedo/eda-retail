"""E23: duplicados de negocio (spec experiments, «E23 Duplicados de negocio: avisos, reclamaciones, avisos al
cliente y reembolsos (H23)»; spec synthetic-data, «Reenvío de avisos del transportista» y «Pedido compartido entre
avisos»).

Cuatro ejecuciones de 50 incidencias con un 50 % de fallos en `reserve_stock`, que fuerzan replanes:

- (a) `resent_notices`: cada aviso se reenvía de 1 a 5 veces con el mismo `carrier_notice_id`, unos en orden y
  otros a la vez;
- (b) `rerequested_steps`: el replanner vuelve a pedir `file_claim` y `notify_customer` ya completados con
  `step_id` nuevos (`RerequestOracle`);
- (c) `refund_again`: el primer replan mantiene, tras el reembolso, la reserva fallida, que vuelve a fallar (misma
  clave), y el replan siguiente vuelve a proponer `issue_refund` (`RefundAgainOracle`);
- (d) `shared_order`: parte de los pedidos reciben un segundo aviso con otro `carrier_notice_id` y las mismas
  etiquetas, así que el oráculo (que indexa por pedido) responde igual a las dos incidencias.

Los recuentos se hacen por incidencia: los avisos al cliente por (incidencia, pedido, versión del plan) y las
reclamaciones, reembolsos y reservas activas por (incidencia, pedido), los máximos de la spec idempotency.
"""

from __future__ import annotations

import dataclasses
import json
import random
from collections import Counter
from collections.abc import Iterable, Mapping
from decimal import Decimal
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.oracle import _PLAN_LINE, Oracle, eval_list
from eda_retail.experiments.report import Report
from eda_retail.synthetic.faults import FaultPlan
from eda_retail.synthetic.generator import Dataset, generate

FAIL_RESERVE = 0.5
MAX_RESENDS = 5
SHARED_EVERY = 4  # (d): uno de cada 4 pedidos recibe un segundo aviso
VARIANTS = ("resent_notices", "rerequested_steps", "refund_again", "shared_order")
REREQUESTED = ("file_claim", "notify_customer")
# (b): la reserva tarda en responder para que `file_claim` y `notify_customer` ya estén completados cuando falla
RESERVE_DELAY_S = {"reserve_stock": 2.0}


# -- oráculos de replan (variantes b y c) --------------------------------------------------------------
def _plan(prompt: str) -> list[dict[str, Any]]:
    """Pasos del plan vigente que muestra el prompt del replanner (sin compensaciones)."""
    steps = []
    for line in prompt.splitlines():
        m = _PLAN_LINE.match(line)
        if m and not m["id"].endswith("-comp"):
            steps.append({"step_id": m["id"], "action": m["action"], "deps": eval_list(m["deps"]),
                          "status": m["status"]})
    return steps


def _next_id(*plans: Iterable[Mapping[str, Any]]) -> int:
    return max(int(s["step_id"][1:]) for plan in plans for s in plan) + 1


class RerequestOracle(Oracle):
    """(b): además del plan del oráculo, vuelve a pedir `file_claim` y `notify_customer` ya completados con
    `step_id` nuevos para el mismo pedido (el replanner no acepta un paso completado en el plan:
    `completed_step_in_plan`)."""

    def _replan(self, prompt: str, order: str) -> list[dict[str, Any]]:
        kept = super()._replan(prompt, order)
        done = [s for s in _plan(prompt) if s["status"] == "completed" and s["action"] in REREQUESTED]
        n = _next_id(_plan(prompt), kept)
        return kept + [{"step_id": f"S{n + i}", "action": s["action"], "params": {"order_id": order},
                        "depends_on": []} for i, s in enumerate(done)]


class RefundAgainOracle(Oracle):
    """(c): cuando el oráculo sustituye un reenvío por un reembolso tras fallar `reserve_stock` y aún no hay ningún
    reembolso completado, mantiene la reserva fallida (su `step_id`, para que el replanner no lo rechace con
    `step_id_changed`) dependiendo del reembolso. Conserva su clave y por tanto su fallo, así que vuelve a fallar y
    el replan siguiente propone `issue_refund` otra vez."""

    def _replan(self, prompt: str, order: str) -> list[dict[str, Any]]:
        kept = super()._replan(prompt, order)
        plan = _plan(prompt)
        refunded = any(s["action"] == "issue_refund" and s["status"] == "completed" for s in plan)
        refund = next((s for s in kept if s["action"] == "issue_refund"), None)
        failed = next((s for s in plan if s["status"] == "failed" and s["action"] == "reserve_stock"), None)
        if refund is not None and failed is not None and not refunded:
            kept.append({"step_id": failed["step_id"], "action": "reserve_stock", "params": {"order_id": order},
                         "depends_on": [refund["step_id"]]})
        return kept


# -- datos (variantes a y d) ---------------------------------------------------------------------------
def shared_order_dataset(seed: int, incidents: int) -> Dataset:
    """(d): el conjunto de `generate` y, para uno de cada `SHARED_EVERY` pedidos, un segundo aviso con otro
    `carrier_notice_id` y las mismas etiquetas (spec synthetic-data, «Pedido compartido entre avisos»)."""
    data = generate(seed, incidents)
    twins = []
    for n in data.notices[::SHARED_EVERY]:
        notice_id = f"{n.carrier_notice_id}-B"
        api = json.loads(n.api_text) | {"notice_id": notice_id}
        twins.append(dataclasses.replace(n, carrier_notice_id=notice_id, api_text=json.dumps(api, sort_keys=True)))
    return dataclasses.replace(data, notices=data.notices + tuple(twins))


def resend_plan(seed: int, notices: int) -> list[tuple[int, int, bool]]:
    """(a): (aviso, reenvíos de 1 a `MAX_RESENDS`, a la vez) por aviso; los pares en orden y los impares a la vez
    (spec synthetic-data, «Reenvío de avisos del transportista»)."""
    rng = random.Random(f"{seed}:resend")
    return [(i, rng.randint(1, MAX_RESENDS), i % 2 == 1) for i in range(notices)]


# -- recuentos -----------------------------------------------------------------------------------------
def _max_per(rows: Iterable[Mapping[str, Any]], *keys: str) -> int:
    return max(Counter(tuple(r[k] for k in keys) for r in rows).values(), default=0)


def summarize(events: Iterable[Mapping[str, Any]], effects: Mapping[str, list[Mapping[str, Any]]],
              amounts: Mapping[str, Decimal], *, discards: int, resends: int) -> dict[str, Any]:
    """Recuentos del informe de una variante.

    `events`: filas de `event_log.events` (`event_type`, `correlation_id`, `payload`) en orden de `seq`;
    `effects`: `claims`, `messages` (con `plan_version`), `refunds` (con `amount`) y `reservations` activas, cada
    fila con `incident_id` y `order_id`; `amounts`: importe de cada pedido; `discards`: comandos rechazados con
    `duplicate_notice`; `resends`: reenvíos inyectados."""
    events = list(events)
    received = [e for e in events if e["event_type"] == "CarrierNoticeReceived"]
    incidents_per_notice = Counter()
    for notice, incident in {(e["payload"]["carrier_notice_id"], e["correlation_id"]) for e in received}:
        incidents_per_notice[notice] += 1
    delayed = Counter(e["correlation_id"] for e in events if e["event_type"] == "ShipmentDelayed")
    requested: dict[tuple[str, str, str], set[str]] = {}
    for e in events:
        if e["event_type"] == "StepRequested":
            p = e["payload"]
            requested.setdefault((e["correlation_id"], str(p["params"].get("order_id")), p["action"]),
                                 set()).add(p["step_id"])
    asked_again = Counter(action for (_, _, action), ids in requested.items() if len(ids) > 1)
    incidents_per_order = Counter()
    for incident, order in {(e["correlation_id"], o) for e in events if e["event_type"] == "ShipmentDelayed"
                            for o in e["payload"]["order_ids"]}:
        incidents_per_order[order] += 1
    refunded: Counter[tuple[str, str]] = Counter()
    for r in effects["refunds"]:
        refunded[(r["incident_id"], r["order_id"])] += Decimal(str(r["amount"]))
    return {
        "resends": resends,
        "duplicate_notice_discards": discards,
        "incidents_per_notice": max(incidents_per_notice.values(), default=0),
        "shipment_delayed_per_incident": max(delayed.values(), default=0),
        "claims_per_incident_order": _max_per(effects["claims"], "incident_id", "order_id"),
        "notices_per_incident_order_version": _max_per(effects["messages"], "incident_id", "order_id",
                                                       "plan_version"),
        "refunds_per_incident_order": _max_per(effects["refunds"], "incident_id", "order_id"),
        "over_refunded": sum(1 for (_, order), total in refunded.items() if total > amounts[order]),
        "active_reservations_per_incident_order": _max_per(effects["reservations"], "incident_id", "order_id"),
        "requested_again": {action: asked_again[action] for action in ("file_claim", "notify_customer",
                                                                       "issue_refund")},
        "shared_orders": sum(1 for n in incidents_per_order.values() if n > 1),
    }


EXERCISED = {  # lo que cada variante debe haber provocado para que su resultado cuente
    "resent_notices": lambda m: m["resends"] > 0,
    "rerequested_steps": lambda m: m["requested_again"]["file_claim"] + m["requested_again"]["notify_customer"] > 0,
    "refund_again": lambda m: m["requested_again"]["issue_refund"] > 0,
    "shared_order": lambda m: m["shared_orders"] > 0,
}


def h23(variant: str, m: Mapping[str, Any]) -> bool:
    """Los recuentos de la variante en los máximos de las specs idempotency e incident-interpretation, cada
    repetición descartada con `duplicate_notice`, la variante ejercida e I1 a I5 sin violaciones."""
    return (EXERCISED[variant](m)
            and m["duplicate_notice_discards"] == m["resends"]
            and m["incidents_per_notice"] <= 1 and m["shipment_delayed_per_incident"] <= 1
            and m["claims_per_incident_order"] <= 1 and m["notices_per_incident_order_version"] <= 1
            and m["refunds_per_incident_order"] <= 1 and m["over_refunded"] == 0
            and m["active_reservations_per_incident_order"] <= 1
            and all(inv["violations"] == 0 for inv in m["invariants"].values()))


# -- ejecución -----------------------------------------------------------------------------------------
def _rows(h: Harness, sql: str) -> list[dict[str, Any]]:
    rows = [dict(r) for r in h.conn.execute(sql).fetchall()]
    h.conn.commit()
    return rows


def _measure(h: Harness, resends: int) -> dict[str, Any]:
    events = _rows(h, "SELECT event_type, correlation_id, payload FROM event_log.events ORDER BY seq")
    effects = {
        "claims": _rows(h, "SELECT incident_id, order_id FROM executor_file_claim.claims"),
        "messages": _rows(h, "SELECT incident_id, order_id, plan_version FROM"
                             " executor_notify_customer.customer_messages"),
        "refunds": _rows(h, "SELECT incident_id, order_id, amount FROM executor_issue_refund.refunds"),
        "reservations": _rows(h, "SELECT incident_id, order_id FROM inventory.reservations WHERE status = 'active'"),
    }
    [discards] = _rows(h, "SELECT count(*) AS n FROM intake.inbox WHERE kind = 'command'"
                          " AND response->>'reason' = 'duplicate_notice'")
    amounts = {o.order_id: o.amount for o in h.data.orders}
    return summarize(events, effects, amounts, discards=discards["n"], resends=resends)


def run_variant(base: Settings, variant: str, *, seed: int, incidents: int) -> dict[str, Any]:
    faults = FaultPlan(seed=seed, fail_probability={"reserve_stock": FAIL_RESERVE},
                       step_delay_seconds=RESERVE_DELAY_S if variant == "rerequested_steps" else {})
    data = shared_order_dataset(seed, incidents) if variant == "shared_order" else generate(seed, incidents)
    oracle = {"rerequested_steps": RerequestOracle, "refund_again": RefundAgainOracle}.get(variant, Oracle)
    config = RunConfig(seed=seed, incidents=len(data.notices), faults=faults)
    with Harness(base, config, data=data, llm=oracle(data).client()) as h:
        h.send_notices()
        resends = 0
        if variant == "resent_notices":
            for index, times, concurrent in resend_plan(seed, len(data.notices)):
                if concurrent:
                    h.resend_notice_concurrently(index, times)
                else:
                    for _ in range(times):
                        h.resend_notice(index)
                resends += times
        result = h.result(h.run_until_closed())
        metrics = {"closed": result.closed, "open": result.open, "escalated": result.escalated,
                   "escalation_reasons": result.escalation_reasons, **_measure(h, resends)}
        metrics["invariants"] = invariants.check(h)
    metrics["h23"] = h23(variant, metrics) and invariants.gate(metrics["invariants"])
    return metrics


def run_e23(base: Settings, *, seed: int = 42, incidents: int = 50) -> Report:
    variants = {v: run_variant(base, v, seed=seed, incidents=incidents) for v in VARIANTS}
    return Report(
        experiment="E23", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "fail_probability": {"reserve_stock": FAIL_RESERVE},
                "max_resends": MAX_RESENDS, "shared_every": SHARED_EVERY, "variants": list(VARIANTS),
                "step_delay_seconds": {"rerequested_steps": RESERVE_DELAY_S}},
        metrics={"variants": variants}, hypothesis={"H23": all(v["h23"] for v in variants.values())},
    )
