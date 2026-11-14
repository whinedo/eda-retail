"""eda-notice: datos de ejemplo, entrada de avisos y logs de los comandos."""

from __future__ import annotations

import logging

import pytest

from eda_retail.synthetic.cli import parse_args
from eda_retail.synthetic.sample import EXAMPLES, ORDERS, seed, send_notice

from .command_helpers import events_of


def test_seed_is_idempotent(conn):
    """Los datos de ejemplo se cargan una vez; repetir seed no duplica pedidos ni stock"""
    seed(conn)
    assert seed(conn) == (0, 0)
    found = conn.execute(
        "SELECT count(*) AS n FROM orders WHERE order_id = ANY(%s)", ([o[0] for o in ORDERS],)
    ).fetchone()["n"]
    conn.commit()
    assert found == len(ORDERS)


def test_every_example_mentions_a_sample_order():
    """Cada aviso de ejemplo cita un pedido de los datos de ejemplo"""
    ids = {o[0] for o in ORDERS}
    assert all(any(i in text for i in ids) for text in EXAMPLES.values())


def test_send_registers_the_notice(conn, caplog):
    """Un aviso enviado entra como CarrierNoticeReceived de su incidencia y deja una línea de log"""
    caplog.set_level(logging.INFO, logger="eda_retail.command")
    sent = send_notice(conn, EXAMPLES["delay"])
    assert sent.result.accepted
    [ev] = events_of(conn, sent.incident_id)
    assert (ev["event_type"], ev["payload"]["raw_text"]) == ("CarrierNoticeReceived", EXAMPLES["delay"])
    assert f"RegisterCarrierNotice {sent.carrier_notice_id} aceptado -> CarrierNoticeReceived" in caplog.text
    again = send_notice(conn, "otro texto", carrier_notice_id=sent.carrier_notice_id)
    assert (again.result.accepted, again.result.reason) == (False, "duplicate_notice")
    assert "rechazado: duplicate_notice" in caplog.text


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


def test_interactive_sends_example_and_follows_incident(conn):
    """El modo interactivo carga datos, envía el ejemplo elegido y sigue la incidencia hasta el límite"""
    from eda_retail.synthetic.interactive import run

    answers = iter(["s", "1", "9", "q"])
    lines: list[str] = []
    run(conn, lambda _prompt: next(answers), lines.append, timeout=2, sleep=lambda _s: None, clock=_clock())
    text = "\n".join(lines)
    assert "datos de ejemplo:" in text
    [sent] = [line for line in lines if line.startswith("aviso ")]
    incident = sent.rsplit(" ", 1)[1]
    assert [e["event_type"] for e in events_of(conn, incident)] == ["CarrierNoticeReceived"]
    assert "esperando a que el intérprete abra la incidencia" in text  # sin agentes en marcha
    assert "sin cerrar tras 2 s" in text
    assert "opción no válida: 9" in text


def test_watch_stops_when_incident_is_closed(conn, settings):
    """El seguimiento termina en cuanto la vista muestra la incidencia cerrada"""
    import uuid

    from eda_retail.command.models import EscalateIncident
    from eda_retail.synthetic.interactive import watch

    from .command_helpers import handler, open_incident, version_of
    from .projection_helpers import project

    incident, _ = open_incident(conn)
    handler().handle(conn, EscalateIncident(command_id=uuid.uuid4(), expected_version=version_of(conn, incident),
                                            producer="test", incident_id=incident, reason="budget_exhausted"))
    conn.commit()
    project(settings, conn, "incident_status", incident)
    lines: list[str] = []
    status = watch(conn, incident, lines.append, timeout=60, sleep=lambda _s: None, clock=_clock())
    assert status is not None and status.status == "escalated"
    assert lines == ["  escalated (budget_exhausted)"]


def test_default_command_is_interactive():
    """eda-notice sin subcomando abre el modo interactivo"""
    assert parse_args([]).command is None
    assert parse_args(["interactive", "--timeout", "30"]).timeout == 30
