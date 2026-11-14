"""Proceso de los proyectores: `eda-projector`. Lee `events` por `seq`, no Kafka.

- `eda-projector`: mantiene las cuatro vistas, cada proyector con su posición.
- `eda-projector rebuild <vista>`: reconstruye una vista desde el log e imprime las huellas
  antes y después y el tiempo de reconstrucción (experimento E4).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from eda_retail.config import load_settings
from eda_retail.infra.db import connect
from eda_retail.infra.logs import setup_logging
from eda_retail.infra.tracing import setup_tracing
from eda_retail.projectors.rebuild import rebuild
from eda_retail.projectors.registry import PROJECTORS, build_all


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Proyectores de las vistas CQRS")
    parser.add_argument("--idle-seconds", type=float, default=0.2, help="espera cuando no hay eventos nuevos")
    commands = parser.add_subparsers(dest="command")
    rebuild_cmd = commands.add_parser("rebuild", help="reconstruye una vista reproduciendo el log desde seq 1")
    rebuild_cmd.add_argument("view", choices=sorted(PROJECTORS), help="vista a reconstruir")
    return parser.parse_args(argv)


def _run(conn, idle_seconds: float) -> None:
    log = logging.getLogger("eda_retail.projector")
    projectors = build_all()
    while True:
        advanced = sum(p.run_once(conn) for p in projectors)
        if advanced:
            log.info("vistas al día: %d eventos aplicados", advanced)
        else:
            time.sleep(idle_seconds)


def _rebuild(conn, view: str) -> int:
    result = rebuild(conn, view)
    print(f"vista: {result.view}")
    print(f"huella antes:   {result.fingerprint_before}")
    print(f"huella después: {result.fingerprint_after}")
    print(f"idéntica: {'sí' if result.identical else 'no'}")
    print(f"eventos reproducidos: {result.events_applied}")
    print(f"tiempo: {result.seconds:.3f} s")
    return 0 if result.identical else 1


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    setup_logging("projector")
    provider = setup_tracing("projectors", settings)
    conn = connect(settings.database_url)
    try:
        if args.command == "rebuild":
            sys.exit(_rebuild(conn, args.view))
        _run(conn, args.idle_seconds)
    finally:
        conn.close()
        if provider is not None:
            provider.shutdown()


if __name__ == "__main__":
    main()
