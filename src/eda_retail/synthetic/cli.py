"""`eda-notice`: carga datos de ejemplo y registra avisos del transportista.

    uv run eda-notice seed                 # pedidos P-1001…P-1005 y su stock
    uv run eda-notice send                 # aviso de ejemplo (retraso de P-1001)
    uv run eda-notice send --example loss  # delay | loss | out_of_stock | injection
    uv run eda-notice send --text "El pedido P-1003 llega con 2 días de retraso"
"""

from __future__ import annotations

import argparse

from eda_retail.config import load_settings
from eda_retail.infra.db import connect
from eda_retail.synthetic.sample import CARRIER, EXAMPLES, seed, send_notice


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="eda-notice", description="Entrada de avisos de la PoC")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("seed", help="carga pedidos y stock de ejemplo")
    send = sub.add_parser("send", help="registra un aviso del transportista")
    group = send.add_mutually_exclusive_group()
    group.add_argument("--example", choices=sorted(EXAMPLES), default="delay")
    group.add_argument("--text", help="texto libre del aviso")
    send.add_argument("--carrier", default=CARRIER)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    conn = connect(load_settings().database_url)
    try:
        if args.command == "seed":
            orders, stock = seed(conn)
            print(f"datos de ejemplo: {orders} pedidos y {stock} filas de stock nuevos")
            return 0
        sent = send_notice(conn, args.text or EXAMPLES[args.example], carrier=args.carrier)
        if not sent.result.accepted:
            print(f"aviso rechazado: {sent.result.reason} {sent.result.detail or ''}".rstrip())
            return 1
        print(f"aviso {sent.carrier_notice_id} registrado; incidencia {sent.incident_id}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
