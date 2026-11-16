"""Informe de resultados de una ejecución (spec expexecutor).

`results.md` se genera solo con lo que hay en la carpeta de la ejecución (`report.json` y
`experiment.log`), sin LLM y siempre igual para los mismos ficheros: propósito, resultado (esperado,
obtenido y si coinciden) y evidencias, cada una con su origen.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from eda_retail.expexecutor.catalog import CATALOG, MISSING, Check, Entry

RESULTS = "results.md"
REPORT = "report.json"
LOG = "experiment.log"
FAILED_TAIL = 20  # líneas del final del log que se citan de una ejecución fallida
MAX_VALUE = 300  # caracteres de un valor citado antes de recortarlo
# Líneas del log que respaldan el resultado: avisos, errores, resultado de la hipótesis y lo que anotó
# el propio expexecutor (comando, parámetros y su origen, código de salida).
EVIDENCE_LINE = re.compile(
    r"WARNING|ERROR|CRITICAL|Traceback|Exception|^%[0-4]\||^H\d+: |^informe: |^expexecutor: "
)
EXIT_LINE = re.compile(r"^expexecutor: eda-experiment terminó con código (-?\d+)$")


class ResultsError(ValueError):
    """La carpeta no permite generar el informe; el mensaje está en español."""


def _value(value: Any) -> str:
    if value is MISSING:
        return "sin dato"
    text = json.dumps(value, ensure_ascii=False, sort_keys=True) if not isinstance(value, str) else value
    return text if len(text) <= MAX_VALUE else text[:MAX_VALUE] + "…"


def _entry(folder: Path, report: dict[str, Any] | None) -> tuple[str, Entry | None]:
    key = str(report.get("experiment", "")).lower() if report else ""
    key = key or folder.name.split("-", 1)[0].lower()
    return key, CATALOG.get(key)


def _purpose(key: str, entry: Entry | None) -> list[str]:
    if entry is None:
        return [f"El experimento `{key}` no está en el catálogo del expexecutor.", ""]
    return [entry.purpose, "", f"Hipótesis: {entry.expected}", ""]


def _checks_table(checks: list[Check]) -> list[str]:
    if not checks:
        return []
    rows = ["| Métrica | Esperado | Obtenido | Cumple |", "| --- | --- | --- | --- |"]
    rows += [f"| `{c.metric}` | {c.expected} | `{_value(c.obtained)}` | {'sí' if c.ok else 'no'} |" for c in checks]
    return [*rows, ""]


def _result(entry: Entry | None, report: dict[str, Any]) -> list[str]:
    hypothesis: dict[str, bool] = report.get("hypothesis") or {}
    checks = entry.checks(report) if entry is not None else []
    lines = [f"- **Esperado:** {entry.expected if entry else 'sin definición en el catálogo'}"]
    if not hypothesis:
        lines += [("- **Obtenido:** el experimento no comprueba ninguna hipótesis con umbral; las métricas "
                   "describen el resultado (ver «Evidencias»)."),
                  "- **Coinciden:** no aplica.", ""]
        return lines + _checks_table(checks)
    obtained = ", ".join(f"{name} {'cumplida' if ok else 'no cumplida'}" for name, ok in hypothesis.items())
    met = all(hypothesis.values())
    lines += [f"- **Obtenido:** {obtained}.",
              f"- **Coinciden:** {'sí, el resultado obtenido coincide con el esperado' if met else 'no, el resultado obtenido no coincide con el esperado'}.",
              ""]
    lines += _checks_table(checks)
    failing = [c for c in checks if not c.ok]
    if not met:
        lines += ([f"Difiere en: {', '.join(f'`{c.metric}` (esperado {c.expected}, obtenido `{_value(c.obtained)}`)' for c in failing)}.", ""]
                  if failing else ["Difiere en: ninguna comprobación del catálogo lo explica; ver las métricas en «Evidencias».", ""])
    return lines


def _report_evidence(report: dict[str, Any], checks: list[Check]) -> list[str]:
    items = [("Semilla", "seed"), ("Modelo", "model"), ("Versión de los prompts", "prompts_version"),
             ("Configuración", "config"), ("Hipótesis", "hypothesis")]
    lines = [f"- {label}: `{_value(report.get(key, MISSING))}` — `{REPORT}` › `{key}`" for label, key in items]
    for name, value in (report.get("metrics") or {}).items():
        lines.append(f"- Métrica `{name}`: `{_value(value)}` — `{REPORT}` › `metrics.{name}`")
    for c in checks:
        if not c.metric.startswith("metrics.") or c.metric.count(".") > 1 or " " in c.metric:
            lines.append(f"- Comprobación `{c.metric}`: `{_value(c.obtained)}` — `{REPORT}` › `{c.metric}`")
    return lines


def _log_lines(path: Path) -> list[str] | None:
    return path.read_text(errors="replace").splitlines() if path.exists() else None


def _log_evidence(lines: list[str] | None, *, tail: bool = False) -> list[str]:
    if lines is None:
        return [f"- No hay log de la ejecución (`{LOG}`): las evidencias salen solo de `{REPORT}`."]
    if tail:
        start = max(0, len(lines) - FAILED_TAIL)
        chosen = list(enumerate(lines, 1))[start:]
    else:
        chosen = [(n, line) for n, line in enumerate(lines, 1) if EVIDENCE_LINE.search(line)]
    if not chosen:
        return [f"- El log (`{LOG}`) no tiene avisos, errores ni resultados que citar."]
    return [f"- `{LOG}`, línea {n}: `{_value(line.strip())}`" for n, line in chosen]


def render(folder: Path | str) -> str:
    """Contenido de `results.md` de la carpeta. Sin `report.json`, es una ejecución fallida."""
    folder = Path(folder)
    report_path, log = folder / REPORT, _log_lines(folder / LOG)
    report = json.loads(report_path.read_text()) if report_path.exists() else None
    key, entry = _entry(folder, report)
    title = entry.title if entry else key.upper()
    lines = [f"# Resultados de {title}", "", "## Propósito", "", *_purpose(key, entry), "## Resultado", ""]
    if report is None:
        code = next((m.group(1) for line in log or [] if (m := EXIT_LINE.match(line))), "desconocido")
        lines += [f"- **Esperado:** {entry.expected if entry else 'sin definición en el catálogo'}",
                  f"- **Obtenido:** ejecución fallida (código {code}): el experimento terminó sin escribir `{REPORT}`.",
                  "- **Coinciden:** no.", "", "## Evidencias", "",
                  f"- No hay `{REPORT}`: la ejecución terminó sin informe del experimento.",
                  *_log_evidence(log, tail=True)]
        return "\n".join(lines) + "\n"
    checks = entry.checks(report) if entry is not None else []
    lines += _result(entry, report)
    lines += ["## Evidencias", "", *_report_evidence(report, checks), *_log_evidence(log)]
    return "\n".join(lines) + "\n"


def write_results(folder: Path | str) -> Path:
    path = Path(folder) / RESULTS
    path.write_text(render(folder))
    return path


def regenerate(folder: Path | str) -> Path:
    """`report`: genera el informe de una ejecución hecha; exige su `report.json`."""
    folder = Path(folder)
    if not folder.is_dir():
        raise ResultsError(f"no existe la carpeta de la ejecución {folder}")
    if not (folder / REPORT).exists():
        raise ResultsError(f"la carpeta {folder} no tiene {REPORT}: no hay ejecución de la que informar")
    return write_results(folder)
