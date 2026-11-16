"""La configuración carga `.env` sola: cualquier `uv run eda-*` usa el `.env` del proyecto, no los valores por defecto."""

from __future__ import annotations

import os

from eda_retail import config


def test_env_file_fills_missing_variables_without_overriding(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("# comentario\n\nLLM_MODEL=qwen2.5:14b\nexport OLLAMA_HOST='http://gpu:11434'\n"
                        'DATABASE_URL="postgresql://a:b@h:5432/x"\nNOT_A_PAIR\n')
    environ = {"DATABASE_URL": "postgresql://explicit/db"}

    loaded = config.load_env_file(env_file, environ)

    assert environ == {"DATABASE_URL": "postgresql://explicit/db", "LLM_MODEL": "qwen2.5:14b",
                       "OLLAMA_HOST": "http://gpu:11434"}
    assert loaded == ["LLM_MODEL", "OLLAMA_HOST"]


def test_missing_env_file_changes_nothing(tmp_path):
    environ: dict[str, str] = {}
    assert config.load_env_file(tmp_path / ".env", environ) == []
    assert environ == {}


def test_load_settings_reads_the_env_file_of_the_working_directory(tmp_path, monkeypatch):
    """Los experimentos usan el LLM_MODEL de `.env` aunque se lancen sin `--env-file`"""
    (tmp_path / ".env").write_text("LLM_MODEL=modelo-del-env\nEVENTS_TOPIC=eda.from-env\n")
    for name in ("LLM_MODEL", "EVENTS_TOPIC"):
        monkeypatch.setenv(name, "placeholder")  # monkeypatch deshace también lo que cargue `.env`
        monkeypatch.delenv(name)
    monkeypatch.chdir(tmp_path)

    settings = config.load_settings()

    assert settings.events_topic == "eda.from-env"
    assert os.environ["LLM_MODEL"] == "modelo-del-env"
