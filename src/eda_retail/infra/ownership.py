"""Comprobación estática: cada componente solo escribe en su esquema (spec event-backbone,
"Propiedad de los datos", escenario "Escritura fuera de su esquema").

Busca en el código fuente las sentencias de escritura (`INSERT INTO`, `UPDATE`, `DELETE FROM`,
`TRUNCATE`) y devuelve las que no van a una tabla cualificada con el esquema del componente.
Una tabla sin esquema también cuenta: es una de las tablas compartidas del modelo anterior.
Las lecturas (`SELECT`) no cuentan: un sistema puede consultar las proyecciones de `views`. Los
bloqueos de fila (`FOR UPDATE`, `FOR NO KEY UPDATE`) y `ON CONFLICT … DO UPDATE` tampoco: actúan
sobre la tabla de la propia sentencia. Un destino interpolado (`{...}` en una f-string) se informa
siempre, porque no se puede comprobar: el código de un componente escribe en tablas con nombre
literal; los destinos interpolados son cosa de `infra/`, que no pertenece a ningún componente.
"""

from __future__ import annotations

import re

# Paquete bajo src/ -> esquema del componente. Cada tarea añade el suyo al migrar el componente.
COMPONENT_PACKAGES: dict[str, str] = {
    "eda_retail/event_log": "event_log",
    "eda_retail/projectors": "views",
}

_WRITE = re.compile(
    r"\b(?P<verb>INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?)\s+"
    r"(?P<table>\{[^}]*\}|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)",
    re.IGNORECASE,
)
# Palabras que, justo antes de UPDATE, indican un bloqueo de fila o un upsert, no una escritura aparte.
_NOT_A_WRITE_BEFORE_UPDATE = {"FOR", "KEY", "DO"}


def foreign_writes(source: str, schema: str) -> list[str]:
    """Tablas en las que escribe `source` fuera de `schema`, en el orden en que aparecen."""
    found = []
    for match in _WRITE.finditer(source):
        if match.group("verb").upper() == "UPDATE":
            previous = source[: match.start()].split()
            if previous and previous[-1].upper() in _NOT_A_WRITE_BEFORE_UPDATE:
                continue
        table = match.group("table")
        owner, _, _ = table.rpartition(".")
        if table.startswith("{") or owner != schema:
            found.append(table)
    return found
