"""E19: pérdida del topic (spec experiments, «E19 Pérdida del topic (H19)»; spec event-backbone, «Pérdida del topic»;
spec synthetic-data, «Pérdida del topic»).

Cierra 200 incidencias con un 30 % de fallos en `reserve_stock` (revisiones y compensaciones). Con el sistema
parado, pierde el contenido de `eda.events` (`Harness.lose_topic`), republica a la vez el outbox entero de todos los
componentes en el orden que tenía cada partición (`Harness.republish`) y corre hasta que el lag es 0. Después llegan
20 avisos nuevos, que deben cerrarse.

- `truncated`: el topic se vacía con `delete_records`; los offsets siguen creciendo y los consumidores reciben lo
  republicado como duplicados.
- `recreated`: el topic se borra y se crea de nuevo; los offsets vuelven a 0. El procedimiento lleva a 0 los grupos de
  Kafka y pone a -1 la posición guardada del lado de lectura: sin ello, el lado de lectura descartaría los mensajes
  nuevos por tener un offset no posterior al guardado.

Compara la foto de antes de la pérdida con la de después de la republicación (las de E21: ejecuciones de los
servicios, filas de outbox, inbox, `command_outbox`, reservas, reembolsos y log, huella de `plan_steps` y de cada
vista), cuenta los mensajes publicados desde la pérdida frente a las filas republicadas, comprueba en cada outbox que
el offset de sus filas crece en el orden del outbox dentro de cada partición y, entre todos los outbox, que cada
partición conserva el orden que tenía en el topic perdido. I1 a I5 al terminar, sin comparar la posición en Kafka que
guardan algunas vistas.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.e21_replay import changes, snapshot
from eda_retail.experiments.harness import PUBLISHERS, TOPIC_LOSS_MODES, Harness, RunConfig
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report
from eda_retail.infra.schemas import qualified
from eda_retail.orchestrator import consumer as orchestrator
from eda_retail.synthetic.faults import FaultPlan
from eda_retail.synthetic.generator import generate

FAIL_RESERVE = 0.3
NEW_NOTICES = 20  # avisos que llegan tras la recuperación
# I5 no compara la posición en Kafka que guardan algunas vistas: al republicar cambia, y las vistas mantenidas conservan
# la original porque `views.applied_events` descarta el evento repetido.
KAFKA_POSITION = frozenset({"kafka_partition", "kafka_offset"})
VARIANTS = TOPIC_LOSS_MODES


def check_order(rows: Iterable[Mapping[str, Any]], start: Mapping[int, int]) -> dict[str, int]:
    """Filas de un outbox (en orden de `id`, con `kafka_partition` y `kafka_offset`): las que no se republicaron (sin
    posición o con un offset anterior a `start` de su partición) y las desordenadas (su offset no crece respecto a la
    fila anterior de su partición)."""
    last: dict[int, int] = {}
    stale = unordered = 0
    for row in rows:
        partition, offset = row["kafka_partition"], row["kafka_offset"]
        if partition is None or offset is None or offset < start.get(partition, 0):
            stale += 1
            continue
        if partition in last and offset <= last[partition]:
            unordered += 1
        last[partition] = offset
    return {"not_republished": stale, "unordered": unordered}


def outbox_order(h: Harness, start: Mapping[int, int]) -> dict[str, dict[str, int]]:
    out = {}
    for schema in PUBLISHERS:
        rows = h.conn.execute(f"SELECT kafka_partition, kafka_offset FROM {qualified(schema, 'outbox')} ORDER BY id"
                              ).fetchall()
        out[schema] = check_order(rows, start)
    h.conn.commit()
    return out


def first_offsets(h: Harness) -> dict[int, int]:
    """El menor offset de cada partición entre las filas de todos los outbox (la primera publicación tras la pérdida)."""
    union = " UNION ALL ".join(f"SELECT kafka_partition, kafka_offset FROM {qualified(s, 'outbox')}" for s in PUBLISHERS)
    rows = h.conn.execute(f"SELECT kafka_partition AS p, min(kafka_offset) AS first FROM ({union}) o"
                          " WHERE kafka_partition IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall()
    h.conn.commit()
    return {r["p"]: r["first"] for r in rows}


Position = tuple[int | None, int | None]


def outbox_positions(h: Harness) -> dict[tuple[str, int], Position]:
    """(esquema, id) de cada fila de outbox -> su (partición, offset) en el topic."""
    out = {}
    for schema in PUBLISHERS:
        for r in h.conn.execute(f"SELECT id, kafka_partition, kafka_offset FROM {qualified(schema, 'outbox')}"):
            out[(schema, r["id"])] = (r["kafka_partition"], r["kafka_offset"])
    h.conn.commit()
    return out


def reordered(before: Mapping[Any, Position], after: Mapping[Any, Position]) -> int:
    """Filas que cambiaron de partición o cuyo offset nuevo no crece en el orden que tenían en el topic perdido dentro de
    su partición (el orden entre componentes). Las filas sin posición antes de la pérdida no cuentan."""
    old = sorted((p, o, key) for key, (p, o) in before.items() if p is not None and o is not None)
    count, last = 0, {}
    for partition, _, key in old:
        new_partition, new_offset = after.get(key, (None, None))
        if new_partition != partition or new_offset is None:
            count += 1
            continue
        if partition in last and new_offset <= last[partition]:
            count += 1
        last[partition] = new_offset
    return count


def h19_variant(m: Mapping[str, Any]) -> bool:
    order = m["order"].values()
    return (m["lag"] == 0 and m["messages_since_loss"] == sum(m["republished"].values())
            and not any(o["not_republished"] or o["unordered"] for o in order) and m["reordered"] == 0
            and not m["changes"]
            and m["open"] == 0 and all(inv["violations"] == 0 for inv in m["invariants"].values()))


def run_variant(base: Settings, mode: str, *, seed: int, incidents: int, new_notices: int) -> dict[str, Any]:
    data = generate(seed, incidents + new_notices)
    config = RunConfig(seed=seed, incidents=incidents + new_notices,
                       faults=FaultPlan(seed=seed, fail_probability={"reserve_stock": FAIL_RESERVE}))
    with Harness(base, config, data=data) as h:
        h.send_notices(range(incidents))
        elapsed = h.run_until_closed(upto=incidents)
        closed_before = h.closed()
        before = snapshot(h)
        positions = outbox_positions(h)
        highs = h.lose_topic(mode)
        republished = h.republish()
        recovery_s = h.run_until_caught_up()
        lag = h.lag()
        start = highs if mode == "truncated" else dict.fromkeys(highs, 0)
        messages_since = h.messages() - sum(start.values())
        order = outbox_order(h, start)
        first = first_offsets(h)
        moved = reordered(positions, outbox_positions(h))
        diff = changes(before, snapshot(h))
        h.send_notices(range(incidents, incidents + new_notices))
        elapsed += h.run_until_closed()
        result = h.result(elapsed)
        checked = invariants.check(h, volatile=KAFKA_POSITION)
    metrics: dict[str, Any] = {
        "closed_before_loss": closed_before, "closed": result.closed, "open": result.open,
        "resolved": result.resolved, "escalated": result.escalated,
        "messages_before_loss": sum(highs.values()), "republished": republished,
        "messages_since_loss": messages_since, "recovery_s": recovery_s, "lag": lag,
        "start_offsets": start, "first_offsets": first, "order": order, "reordered": moved, "changes": diff, "invariants": checked,
    }
    metrics["h19"] = h19_variant(metrics)
    return metrics


def run_e19(base: Settings, *, seed: int = 42, incidents: int = 200, new_notices: int = NEW_NOTICES) -> Report:
    results = {v: run_variant(base, v, seed=seed, incidents=incidents, new_notices=new_notices) for v in VARIANTS}
    holds = all(m["h19"] and invariants.gate(m["invariants"]) for m in results.values())
    return Report(
        experiment="E19", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "new_notices": new_notices, "mode": "in_process",
                "fail_probability": {"reserve_stock": FAIL_RESERVE}, "variants": list(VARIANTS),
                "orchestrator_group": orchestrator.SCHEMA},
        metrics={"variants": results},
        hypothesis={"H19": holds},
    )
