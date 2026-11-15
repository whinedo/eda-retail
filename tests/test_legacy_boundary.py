"""Frontera con el código anterior (T10a): los componentes nuevos no importan el modelo compartido.

El handler compartido (`command`), los agentes (`agents`), los ejecutores (`executors`), el runner de la
orquestación y el relay de la tabla `outbox` pública solo los usan los experimentos y la base síncrona
hasta T11; T10b los borra. Cualquier otro módulo de `src/eda_retail` debe importar sus reglas, prompts,
errores y servicios de su sitio nuevo.
"""

from __future__ import annotations

import ast
import pathlib

SRC = pathlib.Path(__file__).parents[1] / "src" / "eda_retail"
LEGACY = ("eda_retail.command", "eda_retail.agents", "eda_retail.executors", "eda_retail.orchestrator.runner",
          "eda_retail.infra.relay", "eda_retail.synthetic.sample")
# Módulos del modelo compartido (los borra T10b); desde T11 los experimentos tampoco los usan.
ALLOWED = ("command/", "agents/", "executors/", "orchestrator/runner.py", "infra/relay.py", "synthetic/sample.py")


def _imports(path: pathlib.Path) -> set[str]:
    found = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
        elif isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
    return found


def test_new_modules_do_not_import_the_legacy_model():
    offenders = {}
    for path in sorted(SRC.rglob("*.py")):
        relative = path.relative_to(SRC).as_posix()
        if relative.startswith(ALLOWED):
            continue
        legacy = sorted(m for m in _imports(path) if any(m == p or m.startswith(p + ".") for p in LEGACY))
        if legacy:
            offenders[relative] = legacy
    assert offenders == {}
