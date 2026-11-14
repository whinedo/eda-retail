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
from eda_retail.experiments.e3_replan import run_e3
from eda_retail.experiments.e4_rebuild import run_e4
from eda_retail.experiments.e7_audit import run_e7
from eda_retail.experiments.report import Report, write_report
from eda_retail.infra.logs import setup_logging

EXPERIMENTS: dict[str, Callable[..., Report]] = {"e1": run_e1, "e2": run_e2, "e3": run_e3, "e4": run_e4, "e7": run_e7}
# Tamaño del escenario de cada experimento en la spec (E7 elige 10 de ellas al azar).
DEFAULT_INCIDENTS = {"e1": 50, "e2": 50, "e3": 30, "e4": 200, "e7": 50}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Experimentos de la PoC (E1 a E7)")
    parser.add_argument("experiment", choices=sorted(EXPERIMENTS))
    parser.add_argument("--seed", type=int, default=42, help="semilla del generador y de los fallos (42)")
    parser.add_argument("--incidents", type=int, help="número de incidencias (por defecto, el de la spec)")
    parser.add_argument("--out", default="experiments/results", help="carpeta de los informes")
    return parser.parse_args(argv)


def incidents_for(args: argparse.Namespace) -> int:
    return args.incidents if args.incidents is not None else DEFAULT_INCIDENTS[args.experiment]


def main(argv: list[str] | None = None, settings: Settings | None = None) -> int:
    args = parse_args(argv)
    setup_logging("experiment")
    logging.getLogger("eda_retail").setLevel(logging.WARNING)  # el informe es la salida
    report = EXPERIMENTS[args.experiment](settings or load_settings(), seed=args.seed,
                                             incidents=incidents_for(args))
    path = write_report(report, args.out)
    for name, ok in report.hypothesis.items():
        print(f"{name}: {'cumplida' if ok else 'no cumplida'}")
    print(f"informe: {path / 'report.md'}")
    return 0 if all(report.hypothesis.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
