"""Configuración de Alembic desde código (sin base de datos)."""

from __future__ import annotations

from eda_retail.infra.migrations import _config


def test_database_url_with_percent_encoded_password():
    """Una URL con contraseña codificada con % (p. ej. p%40ss) llega intacta a Alembic"""
    url = "postgresql://eda:p%40ss@localhost:5432/eda"
    assert _config(url).get_main_option("sqlalchemy.url") == url
