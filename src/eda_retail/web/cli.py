"""Proceso del front: `eda-web`. Sirve la API en 127.0.0.1 (nunca en otra interfaz).

- `eda-web`: escucha en 127.0.0.1:`WEB_PORT` (8000 por defecto).
- `eda-web --port N`: cambia el puerto; la interfaz no se puede cambiar.
"""

from __future__ import annotations

import argparse
import os

import uvicorn

from eda_retail.web.app import create_app

LOOPBACK = "127.0.0.1"
DEFAULT_PORT = 8000


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backend del front web (solo localhost)")
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("WEB_PORT", DEFAULT_PORT)),
        help="puerto en 127.0.0.1 (por defecto WEB_PORT u 8000)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    uvicorn.run(create_app(), host=LOOPBACK, port=args.port)
