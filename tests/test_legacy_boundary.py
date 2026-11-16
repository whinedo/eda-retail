"""El modelo compartido ya no existe (T10b): ni sus paquetes ni importaciones suyas.

El handler compartido (`command`), los agentes (`agents`), los ejecutores (`executors`), el runner de la
orquestación, el relay de la tabla `outbox` pública y la siembra anterior (`synthetic.sample`) se
borraron; la migración 0017 borra sus tablas.
"""

from __future__ import annotations

import ast
import pathlib

SRC = pathlib.Path(__file__).parents[1] / "src" / "eda_retail"
TESTS = pathlib.Path(__file__).parent
LEGACY = ("eda_retail.command", "eda_retail.agents", "eda_retail.executors", "eda_retail.orchestrator.runner",
          "eda_retail.infra.relay", "eda_retail.synthetic.sample")


def _imports(path: pathlib.Path) -> set[str]:
    found = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
        elif isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
    return found


def test_legacy_modules_are_gone():
    for module in LEGACY:
        relative = module.removeprefix("eda_retail.").replace(".", "/")
        assert not list((SRC / relative).rglob("*.py")) and not (SRC / f"{relative}.py").exists(), module


def test_nothing_imports_the_legacy_model():
    offenders = {}
    for path in sorted([*SRC.rglob("*.py"), *TESTS.rglob("*.py")]):
        legacy = sorted(m for m in _imports(path) if any(m == p or m.startswith(p + ".") for p in LEGACY))
        if legacy:
            offenders[str(path)] = legacy
    assert offenders == {}
