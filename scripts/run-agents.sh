#!/usr/bin/env bash
# Arranca todos los procesos de la PoC (componentes, proyector y log de eventos); cada uno publica su outbox.
# Con la versión 2 de la PoC arranca también el servidor de correo MCP (eda-mail-server) y el agente
# del transportista (eda-carrier-agent, A2A), que exige A2A_CARRIER_TOKEN.
#
# Uso: scripts/run-agents.sh [--version 1|2] [--logs DIR] [--no-migrate]
#
#   --version 1|2  versión de la PoC; gana a POC_VERSION de .env (por defecto, la de .env o 1)
#
# Requiere `docker compose up -d` (kafka y postgres) y, para los agentes con LLM, Ollama en
# OLLAMA_HOST con el modelo LLM_MODEL. Si existe .env, todos los procesos lo cargan. Cada proceso
# escribe en DIR/<nombre>.log (por defecto logs/). Ctrl+C los para todos. Si un proceso termina,
# el script para el resto y sale con error.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() { sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; }

LOGS_DIR="logs"
MIGRATE=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --logs) LOGS_DIR="$2"; shift 2 ;;
    --no-migrate) MIGRATE=0; shift ;;
    --version)
      # Exportada, gana a la de .env: ni `uv run --env-file` ni load_env_file pisan una variable ya definida.
      case "${2:-}" in
        1|2) export POC_VERSION="$2"; shift 2 ;;
        *) echo "error: --version debe ser 1 o 2, no '${2:-}'" >&2; usage >&2; exit 2 ;;
      esac ;;
    -h|--help) usage; exit 0 ;;
    *) echo "opción desconocida: $1" >&2; exit 2 ;;
  esac
done

# Nombre del proceso y comando, en orden de arranque.
PROCESSES=(
  "projector|eda-projector"
  "event-log|eda-event-log"
  "intake|eda-intake"
  "inventory|eda-inventory"
  "orchestrator|eda-orchestrator"
  "interpreter|eda-interpreter"
  "planner|eda-planner"
  "replanner|eda-replanner"
  "executor-notify_customer|eda-step-executor notify_customer"
  "executor-file_claim|eda-step-executor file_claim"
  "executor-reissue_shipment|eda-step-executor reissue_shipment"
  "executor-issue_refund|eda-step-executor issue_refund"
)

for service in kafka postgres; do
  if ! docker compose ps --status running --services 2>/dev/null | grep -qx "$service"; then
    echo "error: el servicio '$service' no está en marcha; ejecuta: docker compose up -d" >&2
    exit 1
  fi
done

UV_ARGS=()
if [[ -f .env ]]; then
  UV_ARGS=(--env-file .env)
fi
uvrun() { uv run ${UV_ARGS[@]+"${UV_ARGS[@]}"} "$@"; }

# OLLAMA_HOST, LLM_MODEL, POC_VERSION (validada con poc_version_from_env: 1 por defecto) y si hay
# A2A_CARRIER_TOKEN, tal como los verán los procesos (entorno + .env). Una versión no válida aborta aquí.
CONFIG=$(uvrun python -c '
import os, sys
from eda_retail.config import poc_version_from_env
e = os.environ.get
try:
    version = poc_version_from_env()
except ValueError as error:
    sys.exit(f"error: {error}")
print(e("OLLAMA_HOST", "http://localhost:11434"), e("LLM_MODEL", "llama3.1"), version,
      "yes" if e("A2A_CARRIER_TOKEN") else "no")') || exit 1
read -r OLLAMA MODEL POC_VERSION A2A_TOKEN_SET <<<"$CONFIG"
# La versión 2 arranca además el servidor de correo MCP y el agente del transportista (A2A).
case "$POC_VERSION" in
  1) ;;
  2)
    if [[ "$A2A_TOKEN_SET" != "yes" ]]; then
      echo "error: POC_VERSION=2 necesita A2A_CARRIER_TOKEN (en .env) para el agente del transportista" >&2
      exit 1
    fi
    PROCESSES=("mail-server|eda-mail-server" "${PROCESSES[@]}")
    PROCESSES+=("carrier-agent|eda-carrier-agent")
    ;;
  *) echo "error: POC_VERSION debe ser 1 o 2, no '$POC_VERSION'" >&2; exit 1 ;;
esac
echo "PoC: versión $POC_VERSION"
if ! TAGS=$(curl -fsS --max-time 3 "$OLLAMA/api/tags" 2>/dev/null); then
  echo "aviso: Ollama no responde en $OLLAMA; los agentes con LLM fallarán" >&2
elif ! grep -q "\"name\":\"$MODEL\"" <<<"$TAGS"; then
  echo "error: Ollama no tiene el modelo '$MODEL'; ejecuta: ollama pull $MODEL (o fija LLM_MODEL en .env)" >&2
  exit 1
fi
echo "LLM: $MODEL en $OLLAMA"

if [[ "$MIGRATE" -eq 1 ]]; then
  uvrun alembic upgrade head
fi

mkdir -p "$LOGS_DIR"
PIDS=()
set -m  # cada proceso en su propio grupo: al pararlo se para también el python que lanza uv

stop_all() {
  trap - INT TERM EXIT
  echo
  echo "parando ${#PIDS[@]} procesos..."
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap stop_all INT TERM EXIT

for entry in "${PROCESSES[@]}"; do
  name="${entry%%|*}"
  cmd="${entry#*|}"
  # shellcheck disable=SC2086  # cmd se divide a propósito en programa y argumentos
  uvrun $cmd >"$LOGS_DIR/$name.log" 2>&1 &
  PIDS+=("$!")
  printf '  %-28s pid %-6s %s\n' "$name" "$!" "$LOGS_DIR/$name.log"
done

echo "en marcha. Logs: tail -f $LOGS_DIR/*.log  ·  Ctrl+C para parar"
echo "para enviar avisos (en otra terminal): scripts/notice.sh  ·  para pararlo todo: scripts/stop-all.sh"

# Si cualquier proceso termina, se para el resto (trap EXIT) y se informa de cuál fue.
while true; do
  for i in "${!PIDS[@]}"; do
    if ! kill -0 "${PIDS[$i]}" 2>/dev/null; then
      entry="${PROCESSES[$i]}"
      echo "error: '${entry%%|*}' ha terminado; mira $LOGS_DIR/${entry%%|*}.log" >&2
      exit 1
    fi
  done
  sleep 1
done
