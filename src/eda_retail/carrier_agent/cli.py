"""Proceso `eda-carrier-agent`: el agente del transportista simulado por A2A (spec a2a-carrier).

Escucha en `127.0.0.1:CARRIER_AGENT_PORT` y publica en su Agent Card la URL `CARRIER_AGENT_URL`. Exige
`A2A_CARRIER_TOKEN` y decide con las reglas de `carrier_agent.rules` y `CARRIER_AGENT_SEED`. Solo se
arranca con `POC_VERSION=2` (scripts/run-agents.sh).
"""

from __future__ import annotations

import argparse

import uvicorn

from eda_retail.carrier_agent.server import create_app
from eda_retail.config import load_settings


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Agente del transportista simulado (A2A)").parse_args(argv)
    settings = load_settings()
    if not settings.a2a_carrier_token:
        raise SystemExit("error: falta A2A_CARRIER_TOKEN (ver .env.example)")
    app = create_app(url=settings.carrier_agent_url, token=settings.a2a_carrier_token,
                     seed=settings.carrier_agent_seed)
    uvicorn.run(app, host="127.0.0.1", port=settings.carrier_agent_port)


if __name__ == "__main__":
    main()
