"""CLI de soporte técnico: `eda-support` (spec support).

- `eda-support status`: diagnóstico de solo lectura.
- `eda-support republish --mode truncated|recreated`: republica el outbox de todos los componentes tras perder el topic.
- `eda-support replay-from <componente> <outbox_id>`: marca como pendientes las filas del outbox desde `outbox_id`.
- `eda-support rebuild <vista>`: reconstruye una vista.

Las tres acciones que escriben son una simulación salvo con `--confirm <token>`, el token que da la simulación. Con
`--json`, la salida es un único objeto JSON (la vista de soporte del front la lee así). Códigos de salida: 0 bien,
1 la ejecución falló, 2 la acción se negó.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from typing import Any

from eda_retail.projectors.registry import PROJECTORS
from eda_retail.support import actions
from eda_retail.support.actions import FAILED, MODES, OK, REFUSED, DatabaseUnavailable, System


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="salida en un objeto JSON")
    write = argparse.ArgumentParser(add_help=False, parents=[common])
    write.add_argument("--confirm", metavar="TOKEN", help="ejecuta la acción con el token de su simulación")
    write.add_argument("--origin", choices=("cli", "web"), default="cli", help=argparse.SUPPRESS)

    parser = argparse.ArgumentParser(prog="eda-support", description="Diagnóstico y recuperación del sistema",
                                     parents=[common])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", parents=[common], help="diagnóstico de solo lectura")
    republish = commands.add_parser("republish", parents=[write], help="republica todos los outbox tras perder el topic")
    republish.add_argument("--mode", choices=MODES, required=True,
                           help="truncated: vaciado con delete_records; recreated: borrado y creado de nuevo")
    replay = commands.add_parser("replay-from", parents=[write], help="vuelve a publicar un outbox desde una fila")
    replay.add_argument("component", help="componente con outbox")
    replay.add_argument("outbox_id", type=int, help="primera fila que se vuelve a publicar")
    rebuild = commands.add_parser("rebuild", parents=[write], help="reconstruye una vista")
    rebuild.add_argument("view", choices=sorted(PROJECTORS), help="vista a reconstruir")
    args = parser.parse_args(argv)
    args.json = getattr(args, "json", False)
    return args


def _action_args(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "republish":
        return {"mode": args.mode}
    if args.command == "replay-from":
        return {"component": args.component, "outbox_id": args.outbox_id}
    return {"view": args.view}


def _command_line(action: str, args: dict[str, Any], token: str) -> str:
    if action == "republish":
        rest = f"--mode {args['mode']}"
    elif action == "replay-from":
        rest = f"{args['component']} {args['outbox_id']}"
    else:
        rest = args["view"]
    return f"eda-support {action} {rest} --confirm {token}"


def _print_status(report: dict[str, Any]) -> None:
    print("Alertas: " + (", ".join(report["alerts"]) or "ninguna"))
    alive = [n for n, up in report["processes"].items() if up]
    print(f"Procesos vivos: {', '.join(alive) or 'ninguno'}")
    for section in ("topic", "groups", "read_side", "outbox", "dead_letters", "failed_jobs", "parked_notices"):
        value = report[section]
        print(f"{section}: {'sin datos' if value is None else json.dumps(value, sort_keys=True, default=str)}")
    for service, error in report["errors"].items():
        print(f"error de {service}: {error}")


def run(argv: list[str] | None, open_system: Callable[[], System]) -> int:
    args = parse_args(argv)
    system = open_system()
    try:
        return _run(args, system)
    except DatabaseUnavailable as exc:
        print(f"No se ejecuta: {exc}", file=sys.stderr)
        return FAILED
    finally:
        system.close()


def _run(args: argparse.Namespace, system: System) -> int:
    if args.command == "status":
        report = actions.status(system)
        print(json.dumps(report, sort_keys=True, default=str)) if args.json else _print_status(report)
        return OK
    action_args = _action_args(args)
    if args.confirm is None:
        plan = actions.plan(system, args.command, action_args)
        if args.json:
            print(json.dumps({"plan": plan.as_dict()}, sort_keys=True, default=str))
        if plan.refusal is not None:
            print(f"No se puede: {plan.refusal}", file=sys.stderr)
            return REFUSED
        if not args.json:
            print(f"Simulación de {args.command}: no se ha escrito nada.")
            print(json.dumps(plan.changes, sort_keys=True, indent=2, default=str))
            print(f"Token: {plan.token}")
            print(f"Para ejecutarla: {_command_line(args.command, action_args, plan.token)}")
        return OK
    code, outcome, summary = actions.confirmed(system, args.command, action_args, args.confirm, args.origin)
    if args.json:
        print(json.dumps({"outcome": outcome, "result": summary}, sort_keys=True, default=str))
    if code == REFUSED:
        print(f"No se puede: {summary['reason']}", file=sys.stderr)
    elif code == FAILED:
        print(f"Falló: {summary['error']}", file=sys.stderr)
    elif not args.json:
        print(f"{args.command}: hecho.")
        print(json.dumps(summary, sort_keys=True, indent=2, default=str))
    return code


def main(argv: list[str] | None = None) -> None:
    from eda_retail.config import load_settings
    from eda_retail.infra.logs import setup_logging
    from eda_retail.support.system import LiveSystem

    settings = load_settings()
    setup_logging("support")
    sys.exit(run(argv, lambda: LiveSystem(settings)))
