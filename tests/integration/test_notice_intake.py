"""eda-notice: datos de ejemplo, entrada de avisos y logs de los comandos."""

from __future__ import annotations

import pytest

from eda_retail.synthetic.cli import parse_args


def test_cli_arguments():
    """send usa el ejemplo de retraso por defecto y no admite --example y --text a la vez"""
    assert parse_args(["send"]).example == "delay"
    assert parse_args(["send", "--text", "hola"]).text == "hola"
    with pytest.raises(SystemExit):
        parse_args(["send", "--example", "loss", "--text", "x"])


def _clock(step: float = 1.0):
    now = [0.0]

    def tick() -> float:
        now[0] += step
        return now[0]

    return tick


def _invoke_via(apis: dict):
    """`invoke` del cliente REST que llama en proceso a la API de cada sistema (sin red)."""
    from eda_retail.infra.command_client import CommandReply

    def invoke(url, command_type, *, command_id, correlation_id, causation_id, payload, timeout):
        response = apis[url].post(f"/commands/{command_type}", json={
            "command_id": str(command_id), "correlation_id": correlation_id,
            "causation_id": str(causation_id) if causation_id else None, "payload": payload,
        })
        return CommandReply(response.status_code, response.json(),
                            response.headers.get("Idempotent-Replayed") == "true")

    return invoke


def test_interactive_sends_example_and_follows_incident(conn, base_settings, database_url):
    """El modo interactivo carga datos en inventory, envía el ejemplo a intake y sigue la incidencia"""
    from eda_retail.intake.handler import specs as intake_specs
    from eda_retail.inventory.handler import specs as inventory_specs
    from eda_retail.synthetic.client import NoticeClient
    from eda_retail.synthetic.interactive import run

    from .component_helpers import client, outbox

    settings = base_settings
    with client(database_url, "intake", intake_specs()) as intake, \
            client(database_url, "inventory", inventory_specs(settings)) as inventory:
        notices = NoticeClient(settings, invoke=_invoke_via({settings.intake_url: intake,
                                                             settings.inventory_url: inventory}))
        answers = iter(["s", "1", "9", "q"])
        lines: list[str] = []
        run(conn, lambda _prompt: next(answers), lines.append, notices, timeout=2, sleep=lambda _s: None,
            clock=_clock())
    text = "\n".join(lines)
    assert "datos de ejemplo:" in text
    [sent] = [line for line in lines if line.startswith("aviso ")]
    incident = sent.rsplit(" ", 1)[1]
    assert [e["event_type"] for e in outbox(conn, "intake", incident)] == ["CarrierNoticeReceived"]
    assert "esperando a que el intérprete abra la incidencia" in text  # sin intérprete en marcha
    assert "sin cerrar tras 2 s" in text
    assert "opción no válida: 9" in text




def test_default_command_is_interactive():
    """eda-notice sin subcomando abre el modo interactivo"""
    assert parse_args([]).command is None
    assert parse_args(["interactive", "--timeout", "30"]).timeout == 30
