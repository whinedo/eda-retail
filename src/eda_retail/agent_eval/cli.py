"""`eda-agent-eval <directorio>`: el informe por agente de un directorio grabado con `LLM_RECORD_DIR` (spec
agent-harness, «Evaluación por agente»). Lo imprime en JSON; no escribe en la base ni en Kafka."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from eda_retail.agent_eval.report import agent_report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Informe por agente de las llamadas al LLM grabadas")
    parser.add_argument("directory", type=Path, help="directorio grabado con LLM_RECORD_DIR")
    args = parser.parse_args(argv)
    if not args.directory.is_dir():
        print(f"eda-agent-eval: {args.directory} no es un directorio", file=sys.stderr)
        return 2
    print(json.dumps(agent_report(args.directory), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
