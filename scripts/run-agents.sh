#!/usr/bin/env bash
# Arranca todos los procesos de la PoC (relay, proyector, agentes, orquestador y ejecutores).
#
# Uso: scripts/run-agents.sh [--logs DIR] [--no-migrate]
#
# Requiere `docker compose up -d` (kafka y postgres) y, para los agentes con LLM, Ollama en
# OLLAMA_HOST con el modelo LLM_MODEL. Si existe .env, todos los procesos lo cargan. Cada proceso escribe en DIR/<nombre>.log (por defecto logs/). Ctrl+C los para
# todos. Si un proceso termina, el script para el resto y sale con error.
set -euo pipefail

cd "$(dirname "$0")/.."

LOGS_DIR="logs"
MIGRATE=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --logs) LOGS_DIR="$2"; shift 2 ;;
    --no-migrate) MIGRATE=0; shift ;;
    -h|--help) sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "opción desconocida: $1" >&2; exit 2 ;;
  esac
done

# Nombre del proceso y comando, en orden de arranque.
PROCESSES=(
  "relay|eda-relay"
  "projector|eda-projector"
  "event-log|eda-event-log"
  "intake|eda-intake"
  "inventory|eda-inventory"
  "orchestrator|eda-orchestrator"
  "interpreter|eda-interpreter"
  "planner|eda-planner"
  "agent-replanner|eda-agent replanner"
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

# OLLAMA_HOST y LLM_MODEL tal como los verán los procesos (entorno + .env).
read -r OLLAMA MODEL < <(uvrun python -c \
  'import os; print(os.environ.get("OLLAMA_HOST", "http://localhost:11434"), os.environ.get("LLM_MODEL", "llama3.1"))')
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
