"""Configuración leída de variables de entorno (ver `.env.example`)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_decimal(name: str) -> Decimal:
    raw = os.environ.get(name)
    return Decimal(raw) if raw else Decimal(0)


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
    # URL base de cada sistema que atiende comandos (design.md §4.4): su proceso escucha en ese host y
    # puerto, y la saga lo invocará ahí.
    intake_url: str = field(default_factory=lambda: os.environ.get("INTAKE_URL", "http://127.0.0.1:8101"))
    inventory_url: str = field(
        default_factory=lambda: os.environ.get("INVENTORY_URL", "http://127.0.0.1:8102")
    )
    command_http_timeout_s: float = field(
        default_factory=lambda: float(os.environ.get("COMMAND_HTTP_TIMEOUT_S", "5"))
    )
    # Plazo de un aviso aparcado por el intérprete hasta que llegue su pedido (design.md §4.3).
    pending_notice_timeout_s: float = field(
        default_factory=lambda: float(os.environ.get("PENDING_NOTICE_TIMEOUT_S", "300"))
    )
    tracing_enabled: bool = field(default_factory=lambda: _env_bool("TRACING_ENABLED", True))
    otlp_endpoint: str = field(
        default_factory=lambda: os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    )
    # Precios para la consulta de coste (la vista guarda solo hechos del log). Por defecto 0:
    # Ollama es local.
    llm_price_in_per_1k: Decimal = field(default_factory=lambda: _env_decimal("LLM_PRICE_IN_PER_1K"))
    llm_price_out_per_1k: Decimal = field(default_factory=lambda: _env_decimal("LLM_PRICE_OUT_PER_1K"))
    effect_cost_claim: Decimal = field(default_factory=lambda: _env_decimal("EFFECT_COST_CLAIM"))
    effect_cost_reissue: Decimal = field(default_factory=lambda: _env_decimal("EFFECT_COST_REISSUE"))


def load_settings() -> Settings:
    return Settings()
