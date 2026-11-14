"""`eda-notice` invoca a `intake` y a `inventory` por REST (T5, design.md §3 y §4.4)."""

from __future__ import annotations

from dataclasses import replace

from eda_retail.config import Settings
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.infra.command_client import CommandReply, CommandUnavailable
from eda_retail.synthetic import cli
from eda_retail.synthetic.client import NoticeClient
from eda_retail.synthetic.sample import CARRIER, EXAMPLES, ORDERS, STOCK

SETTINGS = replace(Settings(), intake_url="http://intake:1", inventory_url="http://inventory:2",
                   command_http_timeout_s=3.0)


class _Invoke:
    def __init__(self, reply: CommandReply | Exception) -> None:
        self.reply = reply
        self.calls: list[dict] = []

    def __call__(self, url, command_type, **kw):
        self.calls.append({"url": url, "command_type": command_type, **kw})
        if isinstance(self.reply, Exception):
            raise self.reply
        return self.reply


def test_register_sends_register_carrier_notice_to_intake():
    invoke = _Invoke(CommandReply(202, {"status": "accepted"}))
    registered = NoticeClient(SETTINGS, invoke=invoke).register("Retraso en P-1001", carrier_notice_id="N-1")

    assert (registered.accepted, registered.carrier_notice_id) == (True, "N-1")
    assert registered.incident_id == incident_id_for_notice("N-1")
    [call] = invoke.calls
    assert (call["url"], call["command_type"], call["timeout"]) == ("http://intake:1", "RegisterCarrierNotice", 3.0)
    assert call["correlation_id"] == incident_id_for_notice("N-1")
    assert call["causation_id"] is None
    assert call["payload"] == {"carrier_notice_id": "N-1", "carrier": CARRIER, "raw_text": "Retraso en P-1001"}


def test_rejected_notice_carries_its_reason():
    invoke = _Invoke(CommandReply(422, {"status": "rejected", "reason": "duplicate_notice"}))
    registered = NoticeClient(SETTINGS, invoke=invoke).register("x", carrier_notice_id="N-1")
    assert (registered.accepted, registered.reason) == (False, "duplicate_notice")


def test_seed_sends_the_sample_orders_and_stock_to_inventory():
    invoke = _Invoke(CommandReply(202, {"status": "accepted", "orders": 5, "stock": 4}))

    assert NoticeClient(SETTINGS, invoke=invoke).seed() == (5, 4)

    [call] = invoke.calls
    assert (call["url"], call["command_type"]) == ("http://inventory:2", "SeedSampleData")
    payload = call["payload"]
    assert [o["order_id"] for o in payload["orders"]] == [o[0] for o in ORDERS]
    assert payload["orders"][0] == {"order_id": "P-1001", "customer_ref": "CUST-001", "sku": "SKU-LAMP",
                                    "quantity": 1, "amount": "39.90", "carrier": CARRIER}
    assert payload["stock"] == [{"warehouse": w, "sku": s, "available": a} for w, s, a in STOCK]


def test_cli_send_reports_an_unavailable_intake(monkeypatch, capsys):
    invoke = _Invoke(CommandUnavailable("RegisterCarrierNotice: http://intake:1 no responde"))
    monkeypatch.setattr(cli, "_client", lambda: NoticeClient(SETTINGS, invoke=invoke))

    assert cli.main(["send"]) == 1

    assert "no responde" in capsys.readouterr().err
    assert invoke.calls[0]["payload"]["raw_text"] == EXAMPLES["delay"]


def test_cli_send_and_seed_print_the_result(monkeypatch, capsys):
    invoke = _Invoke(CommandReply(202, {"status": "accepted", "orders": 5, "stock": 4}))
    monkeypatch.setattr(cli, "_client", lambda: NoticeClient(SETTINGS, invoke=invoke))

    assert cli.main(["seed"]) == 0
    assert cli.main(["send", "--example", "loss"]) == 0

    out = capsys.readouterr().out
    assert "datos de ejemplo: 5 pedidos y 4 filas de stock nuevos" in out
    assert "registrado; incidencia I-" in out
