"""Fixtures de integración: PostgreSQL y Kafka reales de `docker compose`.

El log `event_log.events` no admite TRUNCATE, así que cada sesión de tests crea una base
`eda_test` nueva con las migraciones de Alembic (parten de `db/init/001_schema.sql` y 0017 borra ese modelo). Cada test usa un topic de
Kafka propio para no ver mensajes de otros tests.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import Iterator, MutableMapping
from pathlib import Path

import psycopg
import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from eda_retail import config
from eda_retail.config import Settings, load_settings
from eda_retail.infra import tracing
from eda_retail.infra.db import connect
from eda_retail.infra.migrations import upgrade

TEST_DB = "eda_test"


def _with_db(url: str, dbname: str) -> str:
    base, _, _ = url.rpartition("/")
    return f"{base}/{dbname}"


INTEGRATION_DIR = Path(__file__).resolve().parent / "integration"
REPO_ENV_FILE = Path(__file__).resolve().parents[1] / config.ENV_FILE


@pytest.fixture(autouse=True)
def _no_local_env_file(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Los tests unitarios no leen el `.env` del repositorio ni dejan variables cargadas para el test siguiente.

    `load_settings` (p. ej. desde `create_app`) añade el `.env` a `os.environ`: con el `.env` local (`POC_VERSION=2`,
    etc.), los tests darían otro resultado que en CI, que no tiene `.env`. Un `.env` de otra carpeta (`tmp_path`) se
    sigue cargando. Los de integración sí lo leen: es de donde sacan la base de datos y Kafka."""
    if INTEGRATION_DIR in Path(request.node.path).resolve().parents:
        yield
        return
    load = config.load_env_file

    def without_repo_env(path: Path, environ: MutableMapping[str, str] = os.environ) -> list[str]:
        return [] if Path(path).resolve() == REPO_ENV_FILE else load(path, environ)

    monkeypatch.setattr(config, "load_env_file", without_repo_env)
    saved = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(saved)


@pytest.fixture(scope="session", autouse=True)
def _span_exporter() -> InMemorySpanExporter:
    """Proveedor de trazas en memoria para toda la sesión (sin Jaeger)."""
    exporter = InMemorySpanExporter()
    tracing.install(tracing.build_provider("tests", SimpleSpanProcessor(exporter)))
    return exporter


@pytest.fixture
def spans(_span_exporter: InMemorySpanExporter) -> InMemorySpanExporter:
    _span_exporter.clear()
    return _span_exporter


@pytest.fixture(scope="session")
def base_settings() -> Settings:
    return load_settings()


@pytest.fixture(scope="session")
def database_url(base_settings: Settings) -> str:
    admin_url = _with_db(base_settings.database_url, "postgres")
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        admin.execute(f"CREATE DATABASE {TEST_DB}")
    url = _with_db(base_settings.database_url, TEST_DB)
    upgrade(url)  # esquema base (001_schema.sql) + migraciones de Alembic
    return url


@pytest.fixture
def conn(database_url: str) -> Iterator[psycopg.Connection]:
    c = connect(database_url)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
def connect_db(database_url: str):
    opened: list[psycopg.Connection] = []

    def _open() -> psycopg.Connection:
        c = connect(database_url)
        opened.append(c)
        return c

    yield _open
    for c in opened:
        c.close()


@pytest.fixture
def topic(base_settings: Settings) -> Iterator[str]:
    name = f"test.{uuid.uuid4().hex[:12]}"
    admin = AdminClient({"bootstrap.servers": base_settings.kafka_bootstrap_servers})
    for fut in admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)]).values():
        fut.result(timeout=30)
    yield name
    for fut in admin.delete_topics([name]).values():
        with contextlib.suppress(Exception):  # limpieza best effort
            fut.result(timeout=30)


@pytest.fixture
def settings(base_settings: Settings, database_url: str, topic: str) -> Settings:
    from dataclasses import replace

    return replace(base_settings, database_url=database_url, events_topic=topic)
