#!/usr/bin/env bash
# Abre el CLI interactivo de avisos (`eda-notice`) con la misma configuración que
# scripts/run-agents.sh: carga .env si existe.
#
# Uso: scripts/notice.sh [argumentos de eda-notice]
#
#   scripts/notice.sh                         # menú interactivo
#   scripts/notice.sh interactive --timeout 300
#   scripts/notice.sh seed
#   scripts/notice.sh send --example loss
#
# Los avisos solo avanzan si los procesos están en marcha (scripts/run-agents.sh).
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

if ! docker compose ps --status running --services 2>/dev/null | grep -qx postgres; then
  echo "error: postgres no está en marcha; ejecuta: docker compose up -d" >&2
  exit 1
fi
if ! pgrep -f 'eda-agent interpreter' >/dev/null 2>&1; then
  echo "aviso: los agentes no están en marcha (scripts/run-agents.sh); los avisos no avanzarán" >&2
fi

UV_ARGS=()
if [[ -f .env ]]; then
  UV_ARGS=(--env-file .env)
fi
exec uv run ${UV_ARGS[@]+"${UV_ARGS[@]}"} eda-notice "$@"
