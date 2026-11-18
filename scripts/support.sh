#!/usr/bin/env bash
# Abre el CLI de soporte técnico (`eda-support`, spec support) con la misma configuración que
# scripts/run-agents.sh: carga .env si existe.
#
# Uso: scripts/support.sh [argumentos de eda-support]
#
#   scripts/support.sh status
#   scripts/support.sh republish --mode truncated
#   scripts/support.sh replay-from planner 120
#   scripts/support.sh rebuild orders
#
# Las acciones que escriben son una simulación salvo con --confirm <token>.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

UV_ARGS=()
if [[ -f .env ]]; then
  UV_ARGS=(--env-file .env)
fi
exec uv run ${UV_ARGS[@]+"${UV_ARGS[@]}"} eda-support "$@"
