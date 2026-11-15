"""Proceso del servidor de correo: `eda-mail-server [--no-seed]` (segunda versión, `POC_VERSION=2`).

Al arrancar siembra su directorio (`mail_server.customers`) con las direcciones sintéticas de todos los
clientes del generador y de los datos de ejemplo (idempotente; `--no-seed` lo omite) y atiende MCP por
Streamable HTTP en `127.0.0.1:MAIL_SERVER_PORT`, ruta `/mcp` (el cliente usa `MAIL_SERVER_URL`).
"""

from __future__ import annotations

import argparse
import logging

from eda_retail.config import load_settings
from eda_retail.infra.db import connect
from eda_retail.infra.logs import setup_logging
from eda_retail.mail_server.directory import synthetic_directory
from eda_retail.mail_server.server import NAME, build_server
from eda_retail.mail_server.store import PgMailStore, seed_customers

HOST = "127.0.0.1"
PATH = "/mcp"
log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Servidor de correo simulado (MCP, herramienta send_email)")
    parser.add_argument("--no-seed", action="store_true", help="no siembra el directorio de clientes al arrancar")
    args = parser.parse_args(argv)
    settings = load_settings()
    setup_logging(NAME)
    if not args.no_seed:
        with connect(settings.database_url) as conn:
            added = seed_customers(conn, synthetic_directory().items())
        log.info("directorio sembrado: %d clientes nuevos", added)
    server = build_server(PgMailStore(lambda: connect(settings.database_url)))
    server.run("streamable-http", host=HOST, port=settings.mail_server_port, streamable_http_path=PATH,
               stateless_http=True)


if __name__ == "__main__":
    main()
