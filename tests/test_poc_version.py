"""`POC_VERSION`: la PoC funciona como versión 1 (por defecto) o versión 2 (MCP y A2A, design.md §15).

Cualquier otro valor (también uno vacío) es un error claro."""

from __future__ import annotations

import pytest

from eda_retail import config
from eda_retail.config import Settings, load_settings, poc_version_from_env

INVALID = ["3", "0", "dos", "v2", ""]


def test_poc_version_defaults_to_1(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # sin .env
    monkeypatch.delenv("POC_VERSION", raising=False)
    assert Settings().poc_version == 1
    assert load_settings().poc_version == 1
    assert poc_version_from_env({}) == 1


def test_poc_version_2_is_read_from_the_environment(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("POC_VERSION", "2")
    assert Settings().poc_version == 2
    assert load_settings().poc_version == 2
    assert poc_version_from_env({"POC_VERSION": "2"}) == 2


def test_poc_version_2_is_read_from_the_env_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("POC_VERSION", "1")
    monkeypatch.delenv("POC_VERSION")  # registrado: al terminar se quita el que carga load_env_file
    (tmp_path / config.ENV_FILE).write_text("POC_VERSION=2\n")
    assert load_settings().poc_version == 2


@pytest.mark.parametrize("raw", INVALID)
def test_any_other_poc_version_is_a_clear_error(monkeypatch, tmp_path, raw):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("POC_VERSION", raw)
    with pytest.raises(ValueError, match="POC_VERSION debe ser 1 o 2"):
        load_settings()
    with pytest.raises(ValueError, match="POC_VERSION debe ser 1 o 2"):
        poc_version_from_env()


@pytest.mark.parametrize("raw", INVALID)
def test_settings_validates_the_poc_version_too(monkeypatch, raw):
    """Settings() sin load_settings también rechaza una versión que no es 1 ni 2"""
    monkeypatch.setenv("POC_VERSION", raw)
    with pytest.raises(ValueError, match="POC_VERSION debe ser 1 o 2"):
        Settings()


def test_poc_version_from_env(monkeypatch):
    monkeypatch.delenv("POC_VERSION", raising=False)
    assert poc_version_from_env() == 1
    monkeypatch.setenv("POC_VERSION", "2")
    assert poc_version_from_env() == 2
    monkeypatch.setenv("POC_VERSION", "7")
    with pytest.raises(ValueError, match="POC_VERSION debe ser 1 o 2"):
        poc_version_from_env()
