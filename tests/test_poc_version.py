"""Versión de la PoC (`POC_VERSION`): 1 por defecto, 1 o 2, cualquier otro valor es un error claro."""

from __future__ import annotations

import pytest

from eda_retail import config


def test_version_one_by_default(monkeypatch):
    monkeypatch.delenv("POC_VERSION", raising=False)
    assert config.Settings().poc_version == 1
    assert config.poc_version_from_env({}) == 1


def test_version_two(monkeypatch):
    monkeypatch.setenv("POC_VERSION", "2")
    assert config.Settings().poc_version == 2
    assert config.poc_version_from_env({"POC_VERSION": "2"}) == 2


@pytest.mark.parametrize("raw", ["3", "0", "dos", "v2"])
def test_another_version_is_a_clear_error(monkeypatch, tmp_path, raw):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("POC_VERSION", raw)
    with pytest.raises(ValueError, match="POC_VERSION debe ser 1 o 2"):
        config.load_settings()
