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
