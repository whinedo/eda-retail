"""Registro de las ejecuciones de los servicios simulados de un proceso ejecutor (spec experiments, «Todos los
componentes en procesos propios»).

Con `SERVICE_EXECUTIONS_LOG=<fichero>`, `eda-step-executor <acción>` usa el servicio simulado de su acción y anota
cada ejecución real (no las llamadas repetidas que el servicio reconoce por su clave) como una línea JSON con el
servicio y la `idempotency_key`. El harness lee esos ficheros al terminar (`read_executions`) para contar las
ejecuciones por clave. Sin la variable, el ejecutor no cambia: usa su servicio simulado sin anotar nada.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path

from eda_retail.step_executors.services import SERVICE_OF, FakeService, Services

EXECUTIONS_ENV = "SERVICE_EXECUTIONS_LOG"


class ExecutionLog:
    """Añade una línea por ejecución a `path`; cada línea se escribe y se cierra entera antes de volver."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.Lock()

    def record(self, service: str, key: str, external_id: str) -> None:
        line = json.dumps({"service": service, "idempotency_key": key, "external_id": external_id,
                           "pid": os.getpid(), "at": time.time()})
        with self._lock, self.path.open("a", encoding="utf-8") as out:
            out.write(line + "\n")


def service_from_env(action: str, environ: Mapping[str, str] = os.environ) -> FakeService | None:
    """El servicio simulado de `action` que anota sus ejecuciones en `SERVICE_EXECUTIONS_LOG`; None sin ella."""
    raw = environ.get(EXECUTIONS_ENV, "").strip()
    if not raw:
        return None
    service: FakeService = getattr(Services(), SERVICE_OF[action])
    service.on_execute = ExecutionLog(Path(raw)).record
    return service


def read_executions(paths: Iterable[Path]) -> Counter[tuple[str, str]]:
    """Ejecuciones por (servicio, `idempotency_key`) de los ficheros que existan; ignora una línea cortada (el
    proceso murió mientras la escribía)."""
    out: Counter[tuple[str, str]] = Counter()
    for path in paths:
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            out[(entry["service"], entry["idempotency_key"])] += 1
    return out
