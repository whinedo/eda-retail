"""Modo multiproceso del harness (spec experiments, «Modo multiproceso del harness»): instancias reales de
`eda-orchestrator` contra la base de datos y el topic de la ejecución, con el resto de componentes en el harness."""

from __future__ import annotations

import json
import time

import psycopg
import pytest
from confluent_kafka.admin import AdminClient
from psycopg.conninfo import make_conninfo

from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.processes import wait_for_group

pytestmark = pytest.mark.slow


def _database_exists(settings, name: str) -> bool:
    admin_url = make_conninfo(settings.database_url, dbname="postgres")
    with psycopg.connect(admin_url, autocommit=True) as admin:
        return admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone() is not None


def _topics_gone(settings, *topics: str, timeout: float = 15.0) -> bool:
    """Los metadatos del broker pueden tardar un poco en reflejar el borrado."""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    end = time.monotonic() + timeout
    while set(topics) & set(admin.list_topics(timeout=10).topics):
        if time.monotonic() > end:
            return False
        time.sleep(0.2)
    return True


def test_run_isolation(base_settings):
    """Aislamiento de la ejecución: al terminar, sus procesos están parados y su base de datos y su topic se han
    borrado; mientras dura, las incidencias se cierran con el orquestador en procesos propios"""
    config = RunConfig(seed=7, incidents=3, orchestrator_processes=2, session_timeout_ms=6000, idle_seconds=30.0,
                       timeout_seconds=180.0)
    with Harness(base_settings, config) as h:
        assignment = wait_for_group(h.processes, h.partitions(), 60.0, step=h.step)
        h.send_notices()
        h.run_until_closed()
        result = h.result(0.0)
        logs = {p.name: p.log() for p in h.processes}
        name, topic, processes = h.name, h.settings.events_topic, list(h.processes)
        assert all(p.alive() for p in processes)
        assert not any(d.consumer.name == "orchestrator" for d, _ in h._deliveries)
    detail = json.dumps({"assignment": assignment, "closed": result.closed, "open": result.open,
                         "messages": {n: len(log.messages) for n, log in logs.items()}})
    assert result.open == 0, detail
    assert sum(len(log.messages) for log in logs.values()) > 0, detail
    assert all(not p.alive() for p in processes), detail
    assert not _database_exists(base_settings, name), detail
    assert _topics_gone(base_settings, topic, f"{topic}.dlq"), detail


def test_aborted_run_leaves_nothing_behind(base_settings):
    """Aislamiento de la ejecución: una ejecución abortada también para sus procesos y borra su base de datos y su
    topic"""
    config = RunConfig(seed=7, incidents=1, orchestrator_processes=1)
    with pytest.raises(RuntimeError, match="abortada"), Harness(base_settings, config) as h:
        name, topic, processes = h.name, h.settings.events_topic, list(h.processes)
        raise RuntimeError("abortada")
    assert all(not p.alive() for p in processes)
    assert not _database_exists(base_settings, name)
    assert _topics_gone(base_settings, topic, f"{topic}.dlq")
