"""`eda-experiment <exp>`: ejecuta un experimento con un solo comando y escribe su informe
(spec experiments, «Informe reproducible»).

Usa la configuración de siempre (el `.env` del directorio de trabajo, que se carga solo): PostgreSQL y Kafka de `docker compose`. Cada
ejecución crea y borra su propia base de datos y su topic. Los experimentos con LLM real (E5 y E6)
usan Ollama por defecto; `--llm oracle` los ejecuta con el oráculo, sin coste (decisión D2).
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from typing import Any

from eda_retail.config import Settings, load_settings
from eda_retail.experiments.e1_duplicates import run_e1
from eda_retail.experiments.e2_crash import run_e2
from eda_retail.experiments.e3_replan import run_e3
from eda_retail.experiments.e4_rebuild import run_e4
from eda_retail.experiments.e5_plan_quality import run_e5
from eda_retail.experiments.e6_compare import run_e6
from eda_retail.experiments.e7_audit import run_e7
from eda_retail.experiments.e17_parked import run_e17
from eda_retail.experiments.e18_poison import run_e18
from eda_retail.experiments.e21_replay import run_e21
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report, write_report
from eda_retail.infra.logs import setup_logging
from eda_retail.llm.ollama_client import OllamaClient

EXPERIMENTS: dict[str, Callable[..., Report]] = {
    "e1": run_e1, "e2": run_e2, "e3": run_e3, "e4": run_e4, "e5": run_e5, "e6": run_e6, "e7": run_e7,
    "e17": run_e17, "e18": run_e18, "e21": run_e21,
}
# Tamaño del escenario de cada experimento en la spec (E7 elige 10 de ellas al azar).
DEFAULT_INCIDENTS = {"e1": 50, "e2": 50, "e3": 30, "e4": 200, "e5": 40, "e6": 50, "e7": 50, "e17": 50, "e18": 100, "e21": 50}
LLM_EXPERIMENTS = {"e5", "e6"}  # con LLM real y repeticiones (spec «Repetición para el componente no determinista»)
LLMS = {"oracle": lambda data, repetition: Oracle(data).client()}
LLM_CHOICES = ("ollama", "oracle")
DEFAULT_TEMPERATURE = 0.7  # con temperatura 0 y la misma semilla, las repeticiones salen idénticas


def ollama_factory(seed: int, temperature: float) -> Callable[..., Any]:
    """Cada repetición con su propia semilla (`seed + n`) y la temperatura indicada."""
    return lambda data, repetition: OllamaClient(seed=seed + repetition, temperature=temperature)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Experimentos de la PoC (E1 a E7, E17, E18 y E21)")
    parser.add_argument("experiment", choices=sorted(EXPERIMENTS))
    parser.add_argument("--seed", type=int, default=42, help="semilla del generador y de los fallos (42)")
    parser.add_argument("--incidents", type=int, help="número de incidencias (por defecto, el de la spec)")
    parser.add_argument("--out", default="experiments/results", help="carpeta de los informes")
    parser.add_argument("--llm", choices=LLM_CHOICES, help="LLM de E5 y E6 (ollama por defecto; oracle sin coste)")
    parser.add_argument("--repetitions", type=int, help="repeticiones de E5 y E6 (3 por defecto, mínimo 3 en la spec)")
    parser.add_argument("--threshold", type=float, help="umbral de acierto de H5 (0.8 por defecto)")
    parser.add_argument("--temperature", type=float,
                        help=f"temperatura de Ollama en E5 y E6 ({DEFAULT_TEMPERATURE} por defecto)")
    args = parser.parse_args(argv)
    if args.experiment not in LLM_EXPERIMENTS and any(
            v is not None for v in (args.llm, args.repetitions, args.threshold, args.temperature)):
        parser.error("--llm, --repetitions, --threshold y --temperature solo se usan en "
                     + ", ".join(sorted(LLM_EXPERIMENTS)))
    if args.threshold is not None and args.experiment != "e5":
        parser.error("--threshold solo se usa en e5 (H6 no tiene umbral)")
    return args


def incidents_for(args: argparse.Namespace) -> int:
    return args.incidents if args.incidents is not None else DEFAULT_INCIDENTS[args.experiment]


def experiment_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"seed": args.seed, "incidents": incidents_for(args)}
    if args.experiment in LLM_EXPERIMENTS:
        temperature = args.temperature if args.temperature is not None else DEFAULT_TEMPERATURE
        kwargs["llm"] = LLMS["oracle"] if args.llm == "oracle" else ollama_factory(args.seed, temperature)
        kwargs["repetitions"] = args.repetitions if args.repetitions is not None else 3
        if args.threshold is not None:
            kwargs["threshold"] = args.threshold
    return kwargs


def main(argv: list[str] | None = None, settings: Settings | None = None) -> int:
    args = parse_args(argv)
    settings = settings or load_settings()  # carga `.env` antes de crear el cliente de Ollama (LLM_MODEL)
    setup_logging("experiment")
    logging.getLogger("eda_retail").setLevel(logging.WARNING)  # el informe es la salida
    logging.getLogger("alembic").setLevel(logging.WARNING)  # las migraciones de cada base de datos aislada
    report = EXPERIMENTS[args.experiment](settings, **experiment_kwargs(args))
    path = write_report(report, args.out)
    for name, ok in report.hypothesis.items():
        print(f"{name}: {'cumplida' if ok else 'no cumplida'}")
    print(f"informe: {path / 'report.md'}")
    return 0 if all(report.hypothesis.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
