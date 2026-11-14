"""E4 Reconstrucción de vistas (hipótesis H4, spec experiments).

Ejecuta las incidencias hasta cerrarlas y, con el sistema parado, borra cada una de las
vistas y la reconstruye volviendo a consumir el topic de la ejecución (`projectors.rebuild`). H4: la huella de cada
vista reconstruida coincide con la original; el informe registra el tiempo por vista.
"""

from __future__ import annotations

from eda_retail.config import Settings
from eda_retail.experiments.harness import RunConfig, started
from eda_retail.experiments.report import Report
from eda_retail.projectors.rebuild import rebuild
from eda_retail.projectors.registry import PROJECTORS


def run_e4(base: Settings, *, seed: int = 42, incidents: int = 200) -> Report:
    with started(base, RunConfig(seed=seed, incidents=incidents)) as h:
        model = h.result(h.elapsed).model
        log_size = h.conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"]
        h.conn.commit()
        views = {}
        for view in PROJECTORS:
            r = rebuild(h.settings, h.conn, view)
            views[view] = {"identical": r.identical, "events_applied": r.events_applied, "seconds": r.seconds,
                           "fingerprint": r.fingerprint_after}
    return Report(
        experiment="E4", seed=seed, model=model, config={"incidents": incidents},
        metrics={"events": log_size, "views": views}, hypothesis={"H4": all(v["identical"] for v in views.values())},
    )
