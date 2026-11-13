"""Configuración leída de variables de entorno (ver `.env.example`)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    user = os.environ.get("POSTGRES_USER", "eda")
    password = os.environ.get("POSTGRES_PASSWORD", "eda")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "eda")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=_database_url)
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    events_topic: str = field(default_factory=lambda: os.environ.get("EVENTS_TOPIC", "eda.events"))
    dlq_topic: str = field(default_factory=lambda: os.environ.get("DLQ_TOPIC", "eda.dlq"))
    inbox_enabled: bool = field(default_factory=lambda: _env_bool("INBOX_ENABLED", True))
    effect_keys_enabled: bool = field(
        default_factory=lambda: _env_bool("EFFECT_KEYS_ENABLED", True)
    )
    effect_lock_seconds: int = field(
        default_factory=lambda: int(os.environ.get("EFFECT_LOCK_SECONDS", "30"))
    )
    # Ejecutores (spec resilience): reintentos con espera exponencial y límite de concurrencia.
    step_max_attempts: int = field(default_factory=lambda: int(os.environ.get("STEP_MAX_ATTEMPTS", "3")))
    step_backoff_base_seconds: float = field(
        default_factory=lambda: float(os.environ.get("STEP_BACKOFF_BASE_SECONDS", "0.5"))
    )
    step_backoff_max_seconds: float = field(
        default_factory=lambda: float(os.environ.get("STEP_BACKOFF_MAX_SECONDS", "10"))
    )
    executor_max_concurrency: int = field(
        default_factory=lambda: int(os.environ.get("EXECUTOR_MAX_CONCURRENCY", "5"))
    )
    tracing_enabled: bool = field(default_factory=lambda: _env_bool("TRACING_ENABLED", True))
    otlp_endpoint: str = field(
        default_factory=lambda: os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    )


def load_settings() -> Settings:
    return Settings()
