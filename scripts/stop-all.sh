#!/usr/bin/env bash
# Para todos los procesos de la PoC (los de scripts/start-all.sh, run-agents.sh y run-web.sh, el
# front con su servidor de Vite y cualquier eda-* suelto) y después los contenedores de docker compose.
#
# Uso: scripts/stop-all.sh [--dry-run] [--keep-docker]
#
#   --dry-run      muestra qué pararía, sin parar nada
#   --keep-docker  para solo los procesos; deja kafka, postgres y jaeger en marcha
#
# Los contenedores se paran con `docker compose stop`: no se borran ni los contenedores ni
# el volumen de PostgreSQL, así que `docker compose up -d` vuelve al mismo estado.
set -euo pipefail

cd "$(dirname "$0")/.."

DRY_RUN=0
KEEP_DOCKER=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --keep-docker) KEEP_DOCKER=1; shift ;;
    -h|--help) sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "opción desconocida: $1" >&2; exit 2 ;;
  esac
done

# start-all.sh, run-agents.sh, run-web.sh, el Vite del front y cada proceso de la PoC (también los de la versión 2,
# eda-mail-server y eda-carrier-agent): tanto `uv run eda-…` como el python que lanza.
# Todos reciben SIGTERM a la vez; los que sigan vivos a los 10 s, SIGKILL.
PATTERN='scripts/(start-all|run-agents|run-web)\.sh|eda-retail[^ ]*/web/node_modules/\.bin/vite|(^|/| )eda-(projector|event-log|intake|inventory|interpreter|planner|replanner|orchestrator|step-executor|notice|web|mail-server|carrier-agent)( |$)'

list_pids() {
  pgrep -f "$PATTERN" 2>/dev/null | grep -vx "$$" || true
}

PIDS=$(list_pids)
if [[ -z "$PIDS" ]]; then
  echo "no hay procesos de la PoC en marcha"
else
  echo "procesos de la PoC:"
  # shellcheck disable=SC2086  # lista de pids separada por espacios a propósito
  ps -o pid=,command= -p $(echo $PIDS | tr ' ' ',') | sed 's/^/  /'
  if [[ "$DRY_RUN" -eq 0 ]]; then
    # shellcheck disable=SC2086
    kill -TERM $PIDS 2>/dev/null || true
    for _ in $(seq 1 20); do  # hasta 10 s para que cierren conexiones y consumidores
      [[ -z "$(list_pids)" ]] && break
      sleep 0.5
    done
    if REMAINING=$(list_pids) && [[ -n "$REMAINING" ]]; then
      echo "siguen vivos tras 10 s; se fuerzan: $REMAINING"
      # shellcheck disable=SC2086
      kill -KILL $REMAINING 2>/dev/null || true
    fi
    echo "procesos parados"
  fi
fi

if [[ "$KEEP_DOCKER" -eq 1 ]]; then
  exit 0
fi
RUNNING=$(docker compose ps --status running --services 2>/dev/null || true)
if [[ -z "$RUNNING" ]]; then
  echo "no hay contenedores de docker compose en marcha"
elif [[ "$DRY_RUN" -eq 1 ]]; then
  echo "contenedores que se pararían: $(echo "$RUNNING" | tr '\n' ' ')"
else
  docker compose stop
  echo "contenedores parados (datos conservados; vuelve con: docker compose up -d)"
fi
