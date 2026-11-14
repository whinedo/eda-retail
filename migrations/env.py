"""Entorno de Alembic. Usa `sqlalchemy.url` si se ha fijado; si no, DATABASE_URL / POSTGRES_*."""

from __future__ import annotations

from alembic import context
from sqlalchemy import create_engine, pool

from eda_retail.config import load_settings


def _sqlalchemy_url() -> str:
    url = context.config.get_main_option("sqlalchemy.url") or load_settings().database_url
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url.removeprefix("postgresql://")
    return url


def run_migrations_online() -> None:
    engine = create_engine(_sqlalchemy_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=None)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    raise SystemExit("las migraciones offline no están soportadas")
run_migrations_online()
