"""`eda-notice`: carga datos de ejemplo y registra avisos del transportista.

    uv run eda-notice                      # modo interactivo: menú y seguimiento de la incidencia
    uv run eda-notice interactive --no-free-text  # el menú solo ofrece los avisos de ejemplo
    uv run eda-notice seed                 # pedidos P-1001…P-1005 y su stock
    uv run eda-notice send                 # aviso de ejemplo (retraso de P-1001)
    uv run eda-notice send --example loss  # delay | loss | out_of_stock | injection
    uv run eda-notice send --text "El pedido P-1003 llega con 2 días de retraso"
"""

from __future__ import annotations

import argparse
import sys

from eda_retail.config import load_settings
from eda_retail.infra.command_client import CommandUnavailable
from eda_retail.infra.db import connect
from eda_retail.synthetic.client import NoticeClient, SeedRejected
from eda_retail.synthetic.interactive import run
from eda_retail.synthetic.sample import CARRIER, EXAMPLES


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="eda-notice", description="Entrada de avisos de la PoC")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("seed", help="carga pedidos y stock de ejemplo")
    live = sub.add_parser("interactive", help="menú para enviar avisos y seguir cada incidencia (por defecto)")
    live.add_argument("--timeout", type=float, default=180.0, help="segundos siguiendo cada incidencia")
    live.add_argument("--no-free-text", action="store_true", help="el menú no ofrece texto libre (solo ejemplos)")
    send = sub.add_parser("send", help="registra un aviso del transportista")
    group = send.add_mutually_exclusive_group()
    group.add_argument("--example", choices=sorted(EXAMPLES), default="delay")
    group.add_argument("--text", help="texto libre del aviso")
    send.add_argument("--carrier", default=CARRIER)
    return parser.parse_args(argv)


def _client() -> NoticeClient:
    return NoticeClient(load_settings())


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    client = _client()
    try:
        if args.command in (None, "interactive"):
            conn = connect(load_settings().database_url)
            try:
                run(
                    conn, input, print, client, timeout=getattr(args, "timeout", 180.0),
                    free_text=not getattr(args, "no_free_text", False),
                )
            except (EOFError, KeyboardInterrupt):
                print()
            finally:
                conn.close()
            return 0
        if args.command == "seed":
            orders, stock = client.seed()
            print(f"datos de ejemplo: {orders} pedidos y {stock} filas de stock nuevos")
            return 0
        sent = client.register(args.text or EXAMPLES[args.example], carrier=args.carrier)
        if not sent.accepted:
            print(f"aviso rechazado: {sent.reason}")
            return 1
        print(f"aviso {sent.carrier_notice_id} registrado; incidencia {sent.incident_id}")
        return 0
    except (CommandUnavailable, SeedRejected) as error:
        print(f"error: {error}; ¿están en marcha intake e inventory? (scripts/run-agents.sh)", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
