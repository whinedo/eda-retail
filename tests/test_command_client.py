"""Cliente REST de comandos (design.md §4.4): `POST <url>/commands/<Comando>` con urllib."""

from __future__ import annotations

import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import ClassVar

import pytest

from eda_retail.infra.command_client import CommandUnavailable, invoke


class _Handler(BaseHTTPRequestHandler):
    replies: ClassVar[dict[str, tuple[int, dict, dict]]] = {}
    received: ClassVar[list[tuple[str, dict]]] = []

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        type(self).received.append((self.path, body))
        status, reply, headers = type(self).replies[self.path]
        data = json.dumps(reply).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def server():
    _Handler.replies = {}
    _Handler.received = []
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}", _Handler
    finally:
        httpd.shutdown()


def test_accepted_command_is_posted_with_its_envelope(server):
    url, handler = server
    handler.replies["/commands/RegisterCarrierNotice"] = (202, {"status": "accepted", "incident_id": "I-1"}, {})
    command_id = uuid.uuid4()

    reply = invoke(url, "RegisterCarrierNotice", command_id=command_id, correlation_id="I-1", causation_id=None,
                   payload={"carrier_notice_id": "N-1"}, timeout=2)

    assert (reply.status_code, reply.accepted, reply.replayed) == (202, True, False)
    assert reply.body["incident_id"] == "I-1"
    [(path, body)] = handler.received
    assert path == "/commands/RegisterCarrierNotice"
    assert body == {"command_id": str(command_id), "correlation_id": "I-1", "causation_id": None,
                    "payload": {"carrier_notice_id": "N-1"}}


def test_rejected_and_replayed_responses_are_returned_not_raised(server):
    url, handler = server
    handler.replies["/commands/SeedSampleData"] = (422, {"status": "rejected", "reason": "invalid_payload"}, {})
    handler.replies["/commands/ExecuteStep"] = (202, {"status": "accepted"}, {"Idempotent-Replayed": "true"})

    rejected = invoke(url, "SeedSampleData", command_id=uuid.uuid4(), correlation_id="x", causation_id=None,
                      payload={}, timeout=2)
    replayed = invoke(url, "ExecuteStep", command_id=uuid.uuid4(), correlation_id="x", causation_id=uuid.uuid4(),
                      payload={}, timeout=2)

    assert (rejected.accepted, rejected.reason) == (False, "invalid_payload")
    assert (replayed.accepted, replayed.replayed) == (True, True)


def test_unreachable_system_raises_command_unavailable():
    with pytest.raises(CommandUnavailable) as err:
        invoke("http://127.0.0.1:1", "SeedSampleData", command_id=uuid.uuid4(), correlation_id="x",
               causation_id=None, payload={}, timeout=1)
    assert "127.0.0.1:1" in str(err.value)
