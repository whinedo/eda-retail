"""Logs de los procesos: una línea por evento, comando o lote, en stdout.

Complementan las trazas de Jaeger: permiten seguir el flujo con `tail -f logs/*.log`.
Nivel con `LOG_LEVEL` (por defecto INFO).
"""

from __future__ import annotations

import logging
import os
import sys


def setup_logging(process: str) -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format=f"%(asctime)s %(levelname)s [{process}] %(message)s",
        stream=sys.stdout,
        force=True,
    )
    # Las librerías solo hablan si hay un problema.
    for noisy in ("httpx", "httpcore", "urllib3", "opentelemetry"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
