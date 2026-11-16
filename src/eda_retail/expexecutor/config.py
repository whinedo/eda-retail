"""Fichero de configuración de los experimentos (spec expexecutor).

`experiments/config.py` define `EXPERIMENTS`, un diccionario con una entrada por experimento y sus
parámetros de `eda-experiment`. Gana la línea de comandos, después el fichero y después el valor por
defecto de `eda-experiment`. Se valida entero antes de ejecutar nada.
"""

from __future__ import annotations

import runpy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from eda_retail.experiments.cli import (
    DEFAULT_INCIDENTS,
    DEFAULT_TEMPERATURE,
    EXPERIMENTS,
    LLM_CHOICES,
    LLM_EXPERIMENTS,
)

DEFAULT_PATH = Path("experiments/config.py")
# Valores por defecto de eda-experiment que no son constantes de su módulo (tests/test_expexecutor.py
# comprueba que coinciden con los suyos).
DEFAULT_SEED = 42
DEFAULT_REPETITIONS = 3
DEFAULT_THRESHOLD = 0.8

PARAMS: dict[str, type] = {"seed": int, "incidents": int, "llm": str, "repetitions": int,
                           "threshold": float, "temperature": float}
ORDER = tuple(PARAMS)

COMMAND_LINE = "línea de comandos"
FILE = "fichero de configuración"
DEFAULT = "valor por defecto"


class ConfigError(ValueError):
    """Fichero de configuración o parámetro no válido; el mensaje está en español."""


@dataclass(frozen=True)
class Applied:
    name: str
    value: Any
    origin: str


def accepted(exp: str) -> tuple[str, ...]:
    """Parámetros que acepta `eda-experiment` para el experimento (las mismas reglas que su CLI)."""
    names = ["seed", "incidents"]
    if exp in LLM_EXPERIMENTS:
        names += ["llm", "repetitions", "temperature"]
    if exp == "e5":
        names.append("threshold")
    return tuple(n for n in ORDER if n in names)


def defaults(exp: str) -> dict[str, Any]:
    out: dict[str, Any] = {"seed": DEFAULT_SEED, "incidents": DEFAULT_INCIDENTS[exp]}
    if exp in LLM_EXPERIMENTS:
        out |= {"llm": "ollama", "repetitions": DEFAULT_REPETITIONS, "temperature": DEFAULT_TEMPERATURE}
    if exp == "e5":
        out["threshold"] = DEFAULT_THRESHOLD
    return out


def check_params(exp: str, params: dict[str, Any], where: str) -> None:
    if exp not in EXPERIMENTS:
        raise ConfigError(f"{where}: experimento desconocido '{exp}' (disponibles: {', '.join(sorted(EXPERIMENTS))})")
    if not isinstance(params, dict):
        raise ConfigError(f"{where}: los parámetros de {exp} deben ser un diccionario")
    for name, value in params.items():
        if name not in PARAMS:
            raise ConfigError(f"{where}: parámetro desconocido '{name}' en {exp}")
        if name not in accepted(exp):
            raise ConfigError(f"{where}: el parámetro '{name}' no se usa en {exp}")
        kind = PARAMS[name]
        valid = (not isinstance(value, bool)
                 and (isinstance(value, (int, float)) if kind is float else isinstance(value, kind)))
        if not valid:
            raise ConfigError(f"{where}: '{name}' de {exp} debe ser de tipo {kind.__name__}, no {value!r}")
        if name == "llm" and value not in LLM_CHOICES:
            raise ConfigError(f"{where}: 'llm' de {exp} debe ser uno de {', '.join(LLM_CHOICES)}, no {value!r}")


def validate(experiments: Any, where: str = "fichero de configuración") -> dict[str, dict[str, Any]]:
    if not isinstance(experiments, dict):
        raise ConfigError(f"{where}: EXPERIMENTS debe ser un diccionario")
    for exp, params in experiments.items():
        check_params(exp, params, where)
    return experiments


def load(path: Path | str | None) -> dict[str, dict[str, Any]]:
    """Lee `EXPERIMENTS` del fichero. Sin `path`, el de por defecto, que puede no existir."""
    explicit = path is not None
    path = Path(path) if explicit else DEFAULT_PATH
    if not path.exists():
        if explicit:
            raise ConfigError(f"no existe el fichero de configuración {path}")
        return {}
    try:
        namespace = runpy.run_path(str(path))
    except Exception as exc:  # un fichero con errores es un error de configuración
        raise ConfigError(f"no se pudo leer {path}: {type(exc).__name__}: {exc}") from exc
    if "EXPERIMENTS" not in namespace:
        raise ConfigError(f"{path} no define EXPERIMENTS")
    return validate(namespace["EXPERIMENTS"], str(path))


def resolve(exp: str, *, cli: dict[str, Any], file: dict[str, dict[str, Any]]) -> list[Applied]:
    """Parámetros aplicados y su origen: línea de comandos, fichero o valor por defecto."""
    given = {k: v for k, v in cli.items() if v is not None}
    check_params(exp, given, COMMAND_LINE)
    from_file = file.get(exp, {})
    base = defaults(exp)
    applied = []
    for name in accepted(exp):
        if name in given:
            applied.append(Applied(name, given[name], COMMAND_LINE))
        elif name in from_file:
            applied.append(Applied(name, from_file[name], FILE))
        else:
            applied.append(Applied(name, base[name], DEFAULT))
    return applied


def argv(exp: str, applied: list[Applied], out: Path | str) -> list[str]:
    """Argumentos de `eda-experiment` para los parámetros aplicados."""
    args = [exp]
    for a in applied:
        args += [f"--{a.name}", str(a.value)]
    return [*args, "--out", str(out)]
