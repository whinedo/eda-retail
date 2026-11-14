#!/usr/bin/env bash
# Arranca toda la PoC: los contenedores (docker compose: kafka, postgres y jaeger), las
# migraciones, todos los procesos (scripts/run-agents.sh) y el front web (scripts/run-web.sh).
#
# Uso: scripts/start-all.sh [--logs DIR] [--no-web]
#
#   --logs DIR  carpeta de logs, un fichero por proceso (por defecto logs/)
#   --no-web    no arranca el front ni su backend
#
# Ctrl+C para los procesos y el front, pero deja los contenedores en marcha; para pararlo todo,
# contenedores incluidos: scripts/stop-all.sh. Si run-agents.sh o run-web.sh terminan, para el resto.
set -euo pipefail

cd "$(dirname "$0")/.."

LOGS_DIR="logs"
WEB=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --logs) LOGS_DIR="$2"; shift 2 ;;
    --no-web) WEB=0; shift ;;
    -h|--help) sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "opción desconocida: $1" >&2; exit 2 ;;
  esac
done

docker compose up -d

# Espera a que postgres y kafka pasen su healthcheck (como mucho 120 s).
for container in eda-postgres eda-kafka; do
  for _ in $(seq 120); do
    status=$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null || echo missing)
    [[ "$status" == "healthy" ]] && break
    sleep 1
  done
  if [[ "$status" != "healthy" ]]; then
    echo "error: $container no está healthy ($status); mira: docker compose logs ${container#eda-}" >&2
    exit 1
  fi
done
echo "contenedores: postgres y kafka healthy"

UV_ARGS=()
if [[ -f .env ]]; then
  UV_ARGS=(--env-file .env)
fi
# Antes que el front: su API lee columnas que añaden las migraciones.
uv run ${UV_ARGS[@]+"${UV_ARGS[@]}"} alembic upgrade head

mkdir -p "$LOGS_DIR"
NAMES=("run-agents.sh")
PIDS=()
set -m  # cada script en su propio grupo, para pararlo con todos sus procesos

stop_all() {
  trap - INT TERM EXIT
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  echo "procesos parados; los contenedores siguen en marcha (scripts/stop-all.sh los para)"
}
trap stop_all INT TERM EXIT

scripts/run-agents.sh --logs "$LOGS_DIR" --no-migrate &
PIDS+=("$!")
if [[ "$WEB" -eq 1 ]]; then
  scripts/run-web.sh --logs "$LOGS_DIR" &
  PIDS+=("$!")
  NAMES+=("run-web.sh")
fi

while true; do
  for i in "${!PIDS[@]}"; do
    if ! kill -0 "${PIDS[$i]}" 2>/dev/null; then
      echo "error: '${NAMES[$i]}' ha terminado; mira su salida arriba y $LOGS_DIR/" >&2
      exit 1
    fi
  done
  sleep 1
done
