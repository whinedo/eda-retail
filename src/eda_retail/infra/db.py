"""Conexión a PostgreSQL con psycopg 3, con cada sentencia trazada (OpenTelemetry)."""

from __future__ import annotations

import psycopg
from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
from opentelemetry.trace import NoOpTracerProvider
from psycopg.rows import dict_row

from eda_retail.infra import tracing


def connect(database_url: str) -> psycopg.Connection:
    """Abre una conexión sin autocommit: cada `with conn.transaction()` es una transacción."""
    conn = psycopg.connect(database_url, row_factory=dict_row, autocommit=False)
    return PsycopgInstrumentor().instrument_connection(
        conn, tracer_provider=tracing.provider() or NoOpTracerProvider()
    )
