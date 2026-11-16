#!/usr/bin/env bash
# Arranca el front web: el backend (eda-web, en 127.0.0.1:WEB_PORT) y el servidor de Vite
# (http://localhost:5173), que reenvía /api al backend.
#
# Uso: scripts/run-web.sh [--version 1|2] [--logs DIR]
#
#   --version 1|2  versión de la PoC que muestra el front (mapa, procesos vigilados e insignia de la
#                  cabecera); gana a POC_VERSION de .env (por defecto, la de .env o 1)
#
# Requiere postgres en marcha (docker compose up -d) y Node 24 con npm; la primera vez instala
# las dependencias de web/ (npm ci). Carga .env si existe (WEB_PORT, JAEGER_UI_URL). Cada proceso
# escribe en DIR/<nombre>.log (por defecto logs/). Ctrl+C para los dos; si uno termina, para el otro.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() { sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; }

LOGS_DIR="logs"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --logs) LOGS_DIR="$2"; shift 2 ;;
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

if ! docker compose ps --status running --services 2>/dev/null | grep -qx postgres; then
  echo "error: postgres no está en marcha; ejecuta: docker compose up -d" >&2
  exit 1
fi
if ! command -v npm >/dev/null 2>&1; then
  echo "error: falta npm; instala Node 24" >&2
  exit 1
fi
if [[ ! -d web/node_modules ]]; then
  npm ci --prefix web
fi

UV_ARGS=()
if [[ -f .env ]]; then
  UV_ARGS=(--env-file .env)
fi
uvrun() { uv run ${UV_ARGS[@]+"${UV_ARGS[@]}"} "$@"; }

mkdir -p "$LOGS_DIR"
NAMES=("web-backend" "web-vite")
PIDS=()
set -m  # cada proceso en su propio grupo: al pararlo se paran también sus hijos

stop_all() {
  trap - INT TERM EXIT
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap stop_all INT TERM EXIT

# eda-web lee WEB_PORT de .env (uv --env-file); Vite lo lee del mismo .env (vite.config.ts).
uvrun eda-web >"$LOGS_DIR/web-backend.log" 2>&1 &
PIDS+=("$!")
npm --prefix web run dev >"$LOGS_DIR/web-vite.log" 2>&1 &
PIDS+=("$!")

echo "front en http://localhost:5173  ·  logs: $LOGS_DIR/web-backend.log, $LOGS_DIR/web-vite.log  ·  Ctrl+C para parar"

while true; do
  for i in "${!PIDS[@]}"; do
    if ! kill -0 "${PIDS[$i]}" 2>/dev/null; then
      echo "error: '${NAMES[$i]}' ha terminado; mira $LOGS_DIR/${NAMES[$i]}.log" >&2
      exit 1
    fi
  done
  sleep 1
done
