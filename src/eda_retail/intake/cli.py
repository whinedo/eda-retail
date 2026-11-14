"""Proceso `eda-intake`: API de comandos de `intake` y el relay de su outbox (`INTAKE_URL`)."""

from __future__ import annotations

import argparse

from eda_retail.config import load_settings
from eda_retail.infra.component import serve
from eda_retail.intake.handler import SCHEMA, specs


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Entrada de avisos del transportista (intake)").parse_args(argv)
    serve("intake", SCHEMA, specs(), load_settings().intake_url)


if __name__ == "__main__":
    main()
