#!/usr/bin/env bash
# Crea el .env de la PoC a partir de .env.example y genera A2A_CARRIER_TOKEN (versión 2).
#
# Uso: scripts/init-env.sh
#   - Sin .env: lo copia de .env.example.
#   - Si A2A_CARRIER_TOKEN falta o está vacío, genera un valor aleatorio (64 caracteres hexadecimales).
#   - Nunca sobrescribe un .env existente ni un token ya definido, y nunca muestra el token.
set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT/.env"
EXAMPLE="$ROOT/.env.example"

if [[ -f "$ENV_FILE" ]]; then
  echo ".env ya existe: no se sobrescribe."
else
  if [[ ! -f "$EXAMPLE" ]]; then
    echo "error: falta .env.example en $ROOT" >&2
    exit 1
  fi
  cp "$EXAMPLE" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  echo ".env creado a partir de .env.example."
fi

if grep -qE '^A2A_CARRIER_TOKEN=.+' "$ENV_FILE"; then
  echo "A2A_CARRIER_TOKEN ya tiene valor: no se cambia."
  exit 0
fi

if command -v openssl >/dev/null 2>&1; then
  token="$(openssl rand -hex 32)"
else
  token="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
fi

if grep -qE '^A2A_CARRIER_TOKEN=' "$ENV_FILE"; then
  tmp="$(mktemp "${ENV_FILE}.XXXXXX")"
  awk -v t="$token" '/^A2A_CARRIER_TOKEN=/{print "A2A_CARRIER_TOKEN=" t; next} {print}' "$ENV_FILE" > "$tmp"
  chmod 600 "$tmp"
  mv "$tmp" "$ENV_FILE"
else
  [[ -s "$ENV_FILE" && "$(tail -c1 "$ENV_FILE")" != "" ]] && echo >> "$ENV_FILE"
  printf 'A2A_CARRIER_TOKEN=%s\n' "$token" >> "$ENV_FILE"
fi
echo "A2A_CARRIER_TOKEN generado en .env (no se muestra)."
echo "Revisa LLM_MODEL en .env: debe ser un modelo descargado en Ollama (p. ej. qwen2.5:7b-instruct)."
