"""Comprobación estática: cada componente solo escribe en su esquema (spec event-backbone,
"Propiedad de los datos", escenario "Escritura fuera de su esquema").

Busca en el código fuente las sentencias de escritura (`INSERT INTO`, `UPDATE`, `DELETE FROM`,
`TRUNCATE`) y devuelve las que no van a una tabla cualificada con el esquema del componente.
Una tabla sin esquema también cuenta: es una de las tablas compartidas del modelo anterior.
Las lecturas (`SELECT`) no cuentan: un sistema puede consultar las proyecciones de `views`.
"""

from __future__ import annotations

import re

# Paquete bajo src/ -> esquema del componente. Cada tarea añade el suyo al migrar el componente.
COMPONENT_PACKAGES: dict[str, str] = {}

_WRITE = re.compile(
    r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?)\s+([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)",
    re.IGNORECASE,
)


def foreign_writes(source: str, schema: str) -> list[str]:
    """Tablas en las que escribe `source` fuera de `schema`, en el orden en que aparecen."""
    found = []
    for match in _WRITE.finditer(source):
        table = match.group(1)
        owner, _, _ = table.rpartition(".")
        if owner != schema:
            found.append(table)
    return found
