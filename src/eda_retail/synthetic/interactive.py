"""Modo interactivo de `eda-notice`: envía avisos y sigue cómo avanza cada incidencia.

Solo envía comandos por REST (`RegisterCarrierNotice` a `intake`, `SeedSampleData` a `inventory`)
y solo lee la vista `incident_status`, como cualquier cliente del lado de consulta (spec cqrs-views).
"""

from __future__ import annotations

import time
from collections.abc import Callable

import psycopg

from eda_retail.infra.command_client import CommandUnavailable
from eda_retail.queries.incident_status import IncidentStatus, get_incident_status
from eda_retail.synthetic.client import NoticeClient, SeedRejected
from eda_retail.synthetic.sample import EXAMPLES

FINAL = ("resolved", "escalated")
_EXAMPLES_MENU = ["\nAvisos de ejemplo:"] + [
    f"  {i}) {name}: {text}" for i, (name, text) in enumerate(EXAMPLES.items(), 1)
]
MENU = [*_EXAMPLES_MENU, "  t) texto libre    s) cargar datos de ejemplo    q) salir"]
# Sin texto libre (`--no-free-text`, el front con WEB_FREE_TEXT apagado): solo los ejemplos.
MENU_NO_FREE_TEXT = [*_EXAMPLES_MENU, "  s) cargar datos de ejemplo    q) salir"]

Out = Callable[[str], None]


def render(status: IncidentStatus | None) -> str:
    """Una línea con el estado de la incidencia y de cada paso del plan."""
    if status is None:
        return "esperando a que el intérprete abra la incidencia…"
    head = f"{status.status}"
    if status.escalation_reason:
        head += f" ({status.escalation_reason})"
    if status.plan_version:
        head += f" · plan v{status.plan_version}"
    steps = "  ".join(f"{s['step_id']} {s['action']}={s['status']}" for s in status.steps)
    return f"{head}  {steps}".rstrip()


def watch(
    conn: psycopg.Connection,
    incident_id: str,
    out: Out,
    *,
    timeout: float = 180.0,
    interval: float = 0.5,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> IncidentStatus | None:
    """Muestra cada cambio de la vista hasta que la incidencia se cierra o vence `timeout`."""
    deadline = clock() + timeout
    last = None
    status = None
    while True:
        status = get_incident_status(conn, incident_id)
        line = render(status)
        if line != last:
            out(f"  {line}")
            last = line
        if status is not None and status.status in FINAL:
            return status
        if clock() >= deadline:
            out(f"  sin cerrar tras {timeout:.0f} s: mira logs/*.log (los procesos siguen trabajando)")
            return status
        sleep(interval)


def run(
    conn: psycopg.Connection,
    ask: Callable[[str], str],
    out: Out,
    client: NoticeClient,
    *,
    timeout: float = 180.0,
    free_text: bool = True,
    **kw,
) -> None:
    """Bucle del menú. `ask` lee una respuesta (input) y `out` escribe (print).

    Con `free_text=False` el menú no ofrece `t) texto libre` y `t` es una opción no válida.
    """
    names = list(EXAMPLES)
    menu = MENU if free_text else MENU_NO_FREE_TEXT
    while True:
        out("\n".join(menu))
        choice = ask("> ").strip().lower()
        if choice in ("q", "quit", "salir", ""):
            return
        if choice == "s":
            try:
                orders, stock = client.seed()
            except (CommandUnavailable, SeedRejected) as error:
                out(f"error: {error}")
                continue
            out(f"datos de ejemplo: {orders} pedidos y {stock} filas de stock nuevos")
            continue
        if choice == "t" and free_text:
            text = ask("texto del aviso: ").strip()
        elif choice.isdigit() and 1 <= int(choice) <= len(names):
            text = EXAMPLES[names[int(choice) - 1]]
        else:
            out(f"opción no válida: {choice}")
            continue
        if not text:
            out("aviso vacío: no se envía")
            continue
        try:
            sent = client.register(text)
        except CommandUnavailable as error:
            out(f"error: {error}")
            continue
        if not sent.accepted:
            out(f"aviso rechazado: {sent.reason}")
            continue
        out(f"aviso {sent.carrier_notice_id} registrado; incidencia {sent.incident_id}")
        try:
            watch(conn, sent.incident_id, out, timeout=timeout, **kw)
        except KeyboardInterrupt:
            out("\n  dejo de seguirla (la incidencia sigue su curso)")
