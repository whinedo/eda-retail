"""Proceso `eda-inventory`: API de comandos de `inventory` y el relay de su outbox (`INVENTORY_URL`)."""

from __future__ import annotations

import argparse

from eda_retail.config import load_settings
from eda_retail.infra.component import serve
from eda_retail.inventory.handler import SCHEMA, specs


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Pedidos, stock y reservas (inventory)").parse_args(argv)
    settings = load_settings()
    serve("inventory", SCHEMA, specs(settings), settings.inventory_url)


if __name__ == "__main__":
    main()
