"""Configuración leída de variables de entorno (ver `.env.example`).

`load_settings` carga antes el `.env` del directorio de trabajo, si existe, sin pisar las variables ya
definidas: cualquier `uv run eda-*` usa el `.env` del proyecto aunque no se pase `--env-file`.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

ENV_FILE = ".env"
POC_VERSIONS = (1, 2)  # 1: la primera versión; 2: añade el servidor de correo MCP y el agente A2A (design.md §15)


def _env_optional_int(name: str) -> int | None:
    """El entero de `name`, o None si no está definida o está vacía."""
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else None


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


def poc_version_from_env(environ: Mapping[str, str] = os.environ) -> int:
    """`POC_VERSION` (1 si no está definida): la versión de la PoC, 1 o 2; cualquier otro valor, también uno
    vacío, es un error de configuración."""
    raw = environ.get("POC_VERSION", "1").strip()
    if raw not in {str(v) for v in POC_VERSIONS}:
        raise ValueError(f"POC_VERSION debe ser 1 o 2, no {raw!r}")
    return int(raw)


@dataclass(frozen=True)
class Settings:
    # Versión de la PoC (POC_VERSION): 1 (la primera, por defecto) o 2 (con MCP y A2A, design.md §15). Se valida
    # al construir: con cualquier otro valor, Settings() lanza ValueError.
    poc_version: int = field(default_factory=poc_version_from_env)
    database_url: str = field(default_factory=_database_url)
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    # session.timeout.ms del consumidor del orquestador (KAFKA_SESSION_TIMEOUT_MS); sin definir, el de librdkafka
    # (45 s). E16 lo acorta para congelar una instancia más que su sesión sin esperar tanto.
    kafka_session_timeout_ms: int | None = field(
        default_factory=lambda: _env_optional_int("KAFKA_SESSION_TIMEOUT_MS")
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
    planner_url: str = field(default_factory=lambda: os.environ.get("PLANNER_URL", "http://127.0.0.1:8103"))
    replanner_url: str = field(default_factory=lambda: os.environ.get("REPLANNER_URL", "http://127.0.0.1:8104"))
    # Ejecutores de las acciones que no son de inventory (T9); reserve_stock y release_stock van a INVENTORY_URL.
    executor_notify_customer_url: str = field(
        default_factory=lambda: os.environ.get("EXECUTOR_NOTIFY_CUSTOMER_URL", "http://127.0.0.1:8111")
    )
    executor_file_claim_url: str = field(
        default_factory=lambda: os.environ.get("EXECUTOR_FILE_CLAIM_URL", "http://127.0.0.1:8112")
    )
    executor_reissue_shipment_url: str = field(
        default_factory=lambda: os.environ.get("EXECUTOR_REISSUE_SHIPMENT_URL", "http://127.0.0.1:8113")
    )
    executor_issue_refund_url: str = field(
        default_factory=lambda: os.environ.get("EXECUTOR_ISSUE_REFUND_URL", "http://127.0.0.1:8114")
    )
    # Plazos durables de la saga (design.md §4.2): un ExecuteStep y un CreatePlan/RevisePlan (LLM, más lento).
    saga_step_timeout_s: float = field(default_factory=lambda: float(os.environ.get("SAGA_STEP_TIMEOUT_S", "60")))
    saga_plan_timeout_s: float = field(default_factory=lambda: float(os.environ.get("SAGA_PLAN_TIMEOUT_S", "300")))
    command_http_timeout_s: float = field(
        default_factory=lambda: float(os.environ.get("COMMAND_HTTP_TIMEOUT_S", "5"))
    )
    # Worker de los comandos con trabajo largo (design.md §4.4): lease de un trabajo tomado (si el worker cae,
    # otro lo retoma al vencer), sondeo cuando no hay trabajo y espera entre reintentos de un trabajo que falla.
    job_lease_seconds: float = field(default_factory=lambda: float(os.environ.get("JOB_LEASE_SECONDS", "600")))
    job_poll_interval_s: float = field(default_factory=lambda: float(os.environ.get("JOB_POLL_INTERVAL_S", "0.2")))
    job_retry_base_seconds: float = field(
        default_factory=lambda: float(os.environ.get("JOB_RETRY_BASE_SECONDS", "1"))
    )
    job_retry_max_seconds: float = field(
        default_factory=lambda: float(os.environ.get("JOB_RETRY_MAX_SECONDS", "30"))
    )
    job_max_attempts: int = field(default_factory=lambda: int(os.environ.get("JOB_MAX_ATTEMPTS", "5")))
    # Plazo de un aviso aparcado por el intérprete hasta que llegue su pedido (design.md §4.3).
    pending_notice_timeout_s: float = field(
        default_factory=lambda: float(os.environ.get("PENDING_NOTICE_TIMEOUT_S", "300"))
    )
    # Segunda versión (spec mcp-mail): el servidor de correo MCP y el límite de correos por incidencia.
    mail_server_url: str = field(
        default_factory=lambda: os.environ.get("MAIL_SERVER_URL", "http://127.0.0.1:8120/mcp")
    )
    mail_server_port: int = field(default_factory=lambda: int(os.environ.get("MAIL_SERVER_PORT", "8120")))
    mcp_max_emails_per_incident: int = field(
        default_factory=lambda: int(os.environ.get("MCP_MAX_EMAILS_PER_INCIDENT", "3"))
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
    # Transporte de cada pedido, en % de su importe (10 por defecto); un pedido reenviado lo paga otra vez.
    shipping_cost_pct: Decimal = field(default_factory=lambda: Decimal(os.environ.get("SHIPPING_COST_PCT") or "10"))
    # Agente del transportista (A2A, spec a2a-carrier); solo se usa con POC_VERSION=2. CARRIER_AGENT_URL es
    # la URL que publica su Agent Card y que consulta el planificador; CARRIER_AGENT_PORT, donde escucha en
    # 127.0.0.1. A2A_TIMEOUT_S es el plazo total de una consulta y A2A_MAX_ROUNDS el máximo de rondas
    # input-required; CARRIER_AGENT_SEED, la semilla de sus reglas deterministas.
    carrier_agent_url: str = field(
        default_factory=lambda: os.environ.get("CARRIER_AGENT_URL", "http://127.0.0.1:8130")
    )
    carrier_agent_port: int = field(default_factory=lambda: int(os.environ.get("CARRIER_AGENT_PORT", "8130")))
    a2a_carrier_token: str = field(default_factory=lambda: os.environ.get("A2A_CARRIER_TOKEN", ""))
    a2a_timeout_s: float = field(default_factory=lambda: float(os.environ.get("A2A_TIMEOUT_S", "5")))
    a2a_max_rounds: int = field(default_factory=lambda: int(os.environ.get("A2A_MAX_ROUNDS", "2")))
    carrier_agent_seed: int = field(default_factory=lambda: int(os.environ.get("CARRIER_AGENT_SEED", "42")))


def load_env_file(path: Path, environ: MutableMapping[str, str] = os.environ) -> list[str]:
    """Añade al entorno las variables de `path` (`CLAVE=valor`) que aún no estén definidas.

    Ignora líneas vacías, comentarios y líneas sin `=`; admite `export ` delante y comillas alrededor del
    valor. Devuelve los nombres que cargó (nunca los valores, que pueden ser credenciales)."""
    if not path.is_file():
        return []
    loaded = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.removeprefix("export ").partition("=")
        name, value = name.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        if name and name not in environ:
            environ[name] = value
            loaded.append(name)
    return loaded


def load_settings() -> Settings:
    """Carga `.env` y construye `Settings`; un POC_VERSION no válido para el proceso con un ValueError claro."""
    load_env_file(Path.cwd() / ENV_FILE)
    return Settings()
