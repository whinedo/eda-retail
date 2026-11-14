"""Proceso de los proyectores: `eda-projector`. Lee `events` por `seq`, no Kafka."""

from __future__ import annotations

import argparse
import time

from eda_retail.config import load_settings
from eda_retail.infra.db import connect
from eda_retail.infra.tracing import setup_tracing
from eda_retail.projectors.incident_status import build_projector


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Proyectores de las vistas CQRS")
    parser.add_argument("--idle-seconds", type=float, default=0.2, help="espera cuando no hay eventos nuevos")
    args = parser.parse_args(argv)
    settings = load_settings()
    provider = setup_tracing("projectors", settings)
    conn = connect(settings.database_url)
    projectors = [build_projector()]
    try:
        while True:
            if not sum(p.run_once(conn) for p in projectors):
                time.sleep(args.idle_seconds)
    finally:
        conn.close()
        if provider is not None:
            provider.shutdown()


if __name__ == "__main__":
    main()
