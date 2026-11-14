"""`eda-experiment <exp>`: ejecuta un experimento con un solo comando y escribe su informe
(spec experiments, «Informe reproducible»).

Usa la configuración de siempre (`.env`): PostgreSQL y Kafka de `docker compose`. Cada
ejecución crea y borra su propia base de datos y su topic.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable

from eda_retail.config import Settings, load_settings
from eda_retail.experiments.e1_duplicates import run_e1
from eda_retail.experiments.e2_crash import run_e2
from eda_retail.experiments.report import Report, write_report
from eda_retail.infra.logs import setup_logging

EXPERIMENTS: dict[str, Callable[..., Report]] = {"e1": run_e1, "e2": run_e2}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Experimentos de la PoC (E1 a E7)")
    parser.add_argument("experiment", choices=sorted(EXPERIMENTS))
    parser.add_argument("--seed", type=int, default=42, help="semilla del generador y de los fallos (42)")
    parser.add_argument("--incidents", type=int, default=50, help="número de incidencias (50)")
    parser.add_argument("--out", default="experiments/results", help="carpeta de los informes")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, settings: Settings | None = None) -> int:
    args = parse_args(argv)
    setup_logging("experiment")
    logging.getLogger("eda_retail").setLevel(logging.WARNING)  # el informe es la salida
    report = EXPERIMENTS[args.experiment](settings or load_settings(), seed=args.seed, incidents=args.incidents)
    path = write_report(report, args.out)
    for name, ok in report.hypothesis.items():
        print(f"{name}: {'cumplida' if ok else 'no cumplida'}")
    print(f"informe: {path / 'report.md'}")
    return 0 if all(report.hypothesis.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
