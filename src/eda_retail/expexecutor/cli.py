"""CLI del expexecutor (spec expexecutor): `eda-expexecutor run|report|delete`.

- `run <exp>`: ejecuta el experimento con `eda-experiment` (parámetros de la línea de comandos, de
  `experiments/config.py` o por defecto), guarda su salida en `experiment.log` y genera `results.md`.
- `report <carpeta>`: genera `results.md` de una ejecución ya hecha, sin repetirla.
- `delete <carpeta>`: borra el `results.md` de esa ejecución; `delete --all`, todas las carpetas de
  ejecución de la carpeta de resultados.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn, TextIO

from eda_retail.experiments.cli import EXPERIMENTS
from eda_retail.expexecutor import config
from eda_retail.expexecutor.results import LOG, REPORT, RESULTS, ResultsError, regenerate, write_results

DEFAULT_OUT = "experiments/results"
RUN_FOLDER = re.compile(r"^e\d+-\d{8}T\d{6}$")  # `<exp>-<fecha>`, como las escribe experiments/report.py

Runner = Callable[[list[str], TextIO], int]

_SPANISH = (
    ("the following arguments are required", "faltan los argumentos"),
    ("unrecognized arguments", "argumentos desconocidos"),
    ("invalid choice", "opción no válida"),
    ("choose from", "elige entre"),
    ("invalid int value", "valor entero no válido"),
    ("invalid float value", "valor decimal no válido"),
    ("expected one argument", "falta su valor"),
    ("argument", "argumento"),
)


class SpanishParser(argparse.ArgumentParser):
    """Los errores de argparse, en español."""

    def error(self, message: str) -> NoReturn:
        for english, spanish in _SPANISH:
            message = message.replace(english, spanish)
        self.print_usage(sys.stderr)
        self.exit(2, f"error: {message}\n")


def eda_experiment(argv: list[str], log: TextIO) -> int:
    """Ejecuta `eda-experiment` con su salida (stdout y stderr) en el log."""
    log.flush()
    return subprocess.run([sys.executable, "-m", "eda_retail.experiments.cli", *argv],
                          stdout=log, stderr=subprocess.STDOUT, check=False).returncode


def _parser() -> argparse.ArgumentParser:
    parser = SpanishParser(prog="eda-expexecutor",
                           description="Ejecuta los experimentos y genera su informe de resultados (results.md).")
    sub = parser.add_subparsers(dest="command", required=True, parser_class=SpanishParser,
                                metavar="{run,report,delete}")
    run = sub.add_parser("run", help="ejecuta un experimento y genera su informe de resultados")
    run.add_argument("experiment", help=f"experimento ({', '.join(sorted(EXPERIMENTS))})")
    run.add_argument("--seed", type=int, help="semilla del generador y de los fallos")
    run.add_argument("--incidents", type=int, help="número de incidencias")
    run.add_argument("--llm", help="LLM de E5 y E6 (ollama u oracle)")
    run.add_argument("--repetitions", type=int, help="repeticiones de E5 y E6")
    run.add_argument("--threshold", type=float, help="umbral de acierto de H5")
    run.add_argument("--temperature", type=float, help="temperatura de Ollama en E5 y E6")
    run.add_argument("--out", default=DEFAULT_OUT, help=f"carpeta de resultados ({DEFAULT_OUT})")
    run.add_argument("--config", help=f"fichero de configuración (por defecto, {config.DEFAULT_PATH})")
    report = sub.add_parser("report", help="genera el informe de resultados de una ejecución ya hecha")
    report.add_argument("folder", help="carpeta de la ejecución (<out>/<exp>-<fecha>)")
    delete = sub.add_parser("delete", help="borra el informe de una ejecución o, con --all, todos los datos")
    delete.add_argument("folder", nargs="?", help="carpeta de la ejecución cuyo results.md se borra")
    delete.add_argument("--all", action="store_true",
                        help="borra todas las carpetas de ejecución de la carpeta de resultados")
    delete.add_argument("--out", default=DEFAULT_OUT, help=f"carpeta de resultados para --all ({DEFAULT_OUT})")
    return parser


def _fail(message: str, code: int = 2) -> int:
    print(f"error: {message}", file=sys.stderr)
    return code


def _run_folders(out: Path, exp: str) -> set[Path]:
    return {p for p in out.iterdir() if p.is_dir() and p.name.startswith(f"{exp}-")} if out.exists() else set()


def run(args: argparse.Namespace, runner: Runner) -> int:
    exp = args.experiment
    if exp not in EXPERIMENTS:
        return _fail(f"experimento desconocido '{exp}' (disponibles: {', '.join(sorted(EXPERIMENTS))})")
    cli = {name: getattr(args, name) for name in config.PARAMS}
    try:
        applied = config.resolve(exp, cli=cli, file=config.load(args.config))
    except config.ConfigError as exc:
        return _fail(str(exc))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    before = _run_folders(out, exp)
    argv = config.argv(exp, applied, out)
    pending = out / f".expexecutor-{exp}.log"
    with pending.open("w") as log:
        log.write(f"expexecutor: eda-experiment {' '.join(argv)}\n")
        for a in applied:
            log.write(f"expexecutor: parámetro {a.name} = {a.value} ({a.origin})\n")
        log.flush()
        code = runner(argv, log)
        log.write(f"expexecutor: eda-experiment terminó con código {code}\n")
    created = sorted(p for p in _run_folders(out, exp) - before if (p / REPORT).exists())
    if created:
        folder = created[-1]
    else:  # ejecución fallida: sin report.json, la carpeta la crea el expexecutor
        folder = out / f"{exp}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"
        folder.mkdir(parents=True, exist_ok=True)
    pending.replace(folder / LOG)
    results = write_results(folder)
    print(f"informe de resultados: {results}")
    return code


def report(args: argparse.Namespace) -> int:
    try:
        path = regenerate(args.folder)
    except ResultsError as exc:
        return _fail(str(exc))
    print(f"informe de resultados: {path}")
    return 0


def delete(args: argparse.Namespace) -> int:
    if args.all and args.folder:
        return _fail("indica una carpeta o --all, no las dos cosas")
    if args.all:
        out = Path(args.out)
        folders = sorted(p for p in out.iterdir() if p.is_dir() and RUN_FOLDER.match(p.name)) if out.is_dir() else []
        for folder in folders:
            shutil.rmtree(folder)
        print(f"borradas {len(folders)} carpetas de ejecución de {out}")
        return 0
    if not args.folder:
        return _fail("indica la carpeta de la ejecución o --all")
    path = Path(args.folder) / RESULTS
    if not path.is_file():
        return _fail(f"no hay {RESULTS} en {args.folder}: no se borra nada")
    path.unlink()
    print(f"borrado {path}")
    return 0


def main(argv: list[str] | None = None, runner: Runner | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "run":
        return run(args, runner or eda_experiment)
    if args.command == "report":
        return report(args)
    return delete(args)


if __name__ == "__main__":
    sys.exit(main())
