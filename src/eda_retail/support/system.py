"""El sistema real que ve y repara `eda-support` (spec support): procesos con `pgrep`, Kafka con su API de
administración y PostgreSQL.

`eda-support` es una herramienta de mantenimiento, no un componente: escribe en tablas de otros esquemas (filas de
`outbox`, posiciones del lado de lectura, las vistas que reconstruye) solo dentro de las acciones de recuperación, con
sus condiciones previas comprobadas. Su registro (`support.runs`) va en una conexión propia, para que una acción que
falla a mitad deje su fila `failed` aunque su transacción se deshaga.
"""

from __future__ import annotations

from typing import Any

import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from psycopg.types.json import Jsonb

from eda_retail.config import Settings
from eda_retail.infra.db import connect
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.outbox import ComponentRelay, republish_in_order
from eda_retail.infra.schemas import JOB_COMPONENTS, qualified
from eda_retail.interpreter.consumer import SCHEMA as INTERPRETER
from eda_retail.projectors.rebuild import rebuild
from eda_retail.projectors.registry import PROJECTORS
from eda_retail.support.actions import GROUPS, PUBLISHERS
from eda_retail.synthetic.faults import group_lag, reset_group_offsets
from eda_retail.web.health import pgrep
from eda_retail.web.topology import monitored

KAFKA_TIMEOUT_S = 5.0
READ_SIDE = ("event_log", "views")  # guardan su posición en `<esquema>.offsets` y tienen dead letters (0024)


class LiveSystem:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._topic = settings.events_topic
        self._conn: psycopg.Connection | None = None
        self._runs_conn: psycopg.Connection | None = None
        self._admin: AdminClient | None = None

    # -- conexiones, abiertas al usarse: el diagnóstico sigue aunque una de las dos no responda
    @property
    def conn(self) -> psycopg.Connection:
        if self._conn is None:
            self._conn = connect(self.settings.database_url)
        return self._conn

    @property
    def admin(self) -> AdminClient:
        if self._admin is None:
            self._admin = AdminClient({"bootstrap.servers": self.settings.kafka_bootstrap_servers,
                                       "socket.timeout.ms": int(KAFKA_TIMEOUT_S * 1000)})
        return self._admin

    def _rows(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        with self.conn.transaction():
            return self.conn.execute(sql, params).fetchall()

    # -- lectura
    def processes(self) -> dict[str, bool]:
        return {node.id: pgrep(node.process) for node in monitored(self.settings.poc_version)}

    def watermarks(self) -> dict[int, tuple[int, int]]:
        """Marcas baja y alta de cada partición; sin el topic, {} (no se pide por nombre para no crearlo)."""
        probe = Consumer({"bootstrap.servers": self.settings.kafka_bootstrap_servers, "group.id": "support.probe",
                          "enable.auto.commit": False, "socket.timeout.ms": int(KAFKA_TIMEOUT_S * 1000)})
        try:
            metadata = probe.list_topics(timeout=KAFKA_TIMEOUT_S)
            if self._topic not in metadata.topics:
                return {}
            return {p: probe.get_watermark_offsets(TopicPartition(self._topic, p), timeout=KAFKA_TIMEOUT_S)
                    for p in sorted(metadata.topics[self._topic].partitions)}
        finally:
            probe.close()

    def group_members(self) -> dict[str, int]:
        futures = self.admin.describe_consumer_groups(list(GROUPS), request_timeout=KAFKA_TIMEOUT_S)
        return {group: len(future.result(timeout=KAFKA_TIMEOUT_S).members) for group, future in futures.items()}

    def group_lags(self, highs: dict[int, int]) -> dict[str, int]:
        return {group: group_lag(self.admin, group, self._topic, highs, timeout=KAFKA_TIMEOUT_S) for group in GROUPS}

    def outbox(self) -> dict[str, dict[str, Any]]:
        result = {}
        for schema in PUBLISHERS:
            [row] = self._rows(f"SELECT count(*) AS rows, count(*) FILTER (WHERE published_at IS NULL) AS pending,"
                               f" max(id) AS max_id FROM {qualified(schema, 'outbox')}")
            result[schema] = dict(row)
        return result

    def outbox_positions(self) -> dict[int, tuple[int, int]]:
        union = " UNION ALL ".join(f"SELECT kafka_partition, kafka_offset FROM {qualified(s, 'outbox')}"
                                   " WHERE kafka_partition IS NOT NULL AND kafka_offset IS NOT NULL" for s in PUBLISHERS)
        rows = self._rows(f"SELECT kafka_partition, min(kafka_offset) AS low, max(kafka_offset) AS high"
                          f" FROM ({union}) AS positioned GROUP BY kafka_partition")
        return {r["kafka_partition"]: (r["low"], r["high"]) for r in rows}

    def dead_letters(self) -> dict[str, dict[str, int]]:
        result = {}
        for schema in (*PUBLISHERS, *READ_SIDE):
            [row] = self._rows(f"SELECT count(*) AS rows, count(*) FILTER (WHERE published_at IS NULL) AS unpublished"
                               f" FROM {qualified(schema, 'dead_letters')}")
            result[schema] = dict(row)
        return result

    def failed_jobs(self) -> dict[str, int]:
        return {schema: self._rows(f"SELECT count(*) AS n FROM {qualified(schema, 'command_jobs')}"
                                   " WHERE status = 'failed'")[0]["n"] for schema in JOB_COMPONENTS}

    def parked_notices(self) -> int:
        return self._rows(f"SELECT count(*) AS n FROM {qualified(INTERPRETER, 'pending_notices')}")[0]["n"]

    def read_side_positions(self) -> list[dict[str, Any]]:
        return self._rows(
            "SELECT consumer, kafka_partition, kafka_offset FROM event_log.offsets WHERE kafka_topic = %s"
            " UNION ALL SELECT consumer, kafka_partition, kafka_offset FROM views.offsets WHERE kafka_topic = %s",
            (self._topic, self._topic))

    def outbox_range(self, schema: str, from_id: int) -> dict[str, Any]:
        table = qualified(schema, "outbox")
        [row] = self._rows(f"SELECT EXISTS (SELECT 1 FROM {table} WHERE id = %s) AS found, count(*) AS rows,"
                           f" min(id) AS first_id, max(id) AS last_id FROM {table} WHERE id >= %s", (from_id, from_id))
        return {"exists": row["found"], "rows": row["rows"], "first_id": row["first_id"], "last_id": row["last_id"]}

    def view_rows(self, view: str) -> dict[str, int]:
        return {table: self._rows(f"SELECT count(*) AS n FROM {table}")[0]["n"] for table in PROJECTORS[view].tables}

    # -- efectos
    def reset_for_recreated(self, partitions: list[int]) -> dict[str, Any]:
        """Topic recreado: los grupos al offset 0 y la posición del lado de lectura a -1 (spec synthetic-data)."""
        reset_group_offsets(self.admin, GROUPS, self._topic, partitions)
        positions = 0
        with self.conn.transaction():
            for schema in READ_SIDE:
                positions += self.conn.execute(f"UPDATE {qualified(schema, 'offsets')} SET kafka_offset = -1"
                                               " WHERE kafka_topic = %s", (self._topic,)).rowcount
        return {"groups": list(GROUPS), "positions": positions}

    def republish(self) -> dict[str, int]:
        return republish_in_order(self.conn, make_producer(self.settings.kafka_bootstrap_servers), PUBLISHERS,
                                  self._topic)

    def replay_from(self, schema: str, outbox_id: int) -> int:
        return ComponentRelay(self.conn, None, schema, self._topic).replay_from(outbox_id)

    def rebuild(self, view: str) -> dict[str, Any]:
        result = rebuild(self.settings, self.conn, view)
        return {"view": result.view, "fingerprint_before": result.fingerprint_before,
                "fingerprint_after": result.fingerprint_after, "identical": result.identical,
                "events_applied": result.events_applied, "seconds": round(result.seconds, 3)}

    # -- registro
    @property
    def runs_conn(self) -> psycopg.Connection:
        if self._runs_conn is None:
            self._runs_conn = connect(self.settings.database_url)
        return self._runs_conn

    def start_run(self, action: str, args: dict, token: str, origin: str) -> int:
        with self.runs_conn.transaction():
            return self.runs_conn.execute(
                "INSERT INTO support.runs (action, args, token, origin) VALUES (%s, %s, %s, %s) RETURNING id",
                (action, Jsonb(args), token, origin)).fetchone()["id"]

    def finish_run(self, run_id: int, outcome: str, summary: dict) -> None:
        with self.runs_conn.transaction():
            self.runs_conn.execute(
                "UPDATE support.runs SET finished_at = now(), outcome = %s, summary = %s WHERE id = %s",
                (outcome, Jsonb(summary), run_id))

    def close(self) -> None:
        for conn in (self._conn, self._runs_conn):
            if conn is not None:
                conn.close()
