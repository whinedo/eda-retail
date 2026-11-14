"""Stream SSE de eventos del front (spec web-front, "Actualización en vivo")."""

from __future__ import annotations

import json

from eda_retail.web.stream import IDLE_SECONDS, event_stream, start_seq


def _frames(stream) -> list[dict]:
    out = []
    for frame in stream:
        lines = dict(line.split(": ", 1) for line in frame.strip().split("\n"))
        out.append({"id": int(lines["id"]), "event": lines["event"], "data": json.loads(lines["data"])})
    return out


def test_reconnection_resumes_after_last_event_id():
    """Reconexión: con Last-Event-ID 1290 el stream empieza en 1291"""
    assert start_seq(last_event_id="1290", after_seq=None, current_last=lambda: 2000) == 1290


def test_last_event_id_wins_over_after_seq():
    """Si llegan Last-Event-ID y after_seq, manda Last-Event-ID"""
    assert start_seq(last_event_id="1290", after_seq=1000, current_last=lambda: 2000) == 1290


def test_first_connection_uses_after_seq():
    """Primera conexión tras cargar el panel: after_seq marca la posición"""
    assert start_seq(last_event_id=None, after_seq=1290, current_last=lambda: 2000) == 1290


def test_connection_without_position_starts_after_the_last_seq():
    """Conexión sin posición: empieza tras el último seq del log"""
    assert start_seq(last_event_id=None, after_seq=None, current_last=lambda: 1290) == 1290


def test_malformed_last_event_id_is_ignored():
    """Un Last-Event-ID que no es un seq se ignora y se usa la siguiente posición"""
    assert start_seq(last_event_id="abc", after_seq=7, current_last=lambda: 9) == 7
    assert start_seq(last_event_id="", after_seq=None, current_last=lambda: 9) == 9


def test_stream_sends_each_event_once_in_seq_order():
    """Sin perder ni duplicar filas: cada lectura sigue tras el último seq enviado"""
    log = {seq: {"seq": seq, "event_type": "ShipmentDelayed"} for seq in range(1291, 1296)}
    asked: list[int] = []

    def fetch_after(after: int) -> list[dict]:
        asked.append(after)
        return [log[s] for s in sorted(log) if s > after][:2]

    frames = _frames(event_stream(fetch_after, 1290, sleep=lambda s: None, max_polls=4))

    assert [f["id"] for f in frames] == [1291, 1292, 1293, 1294, 1295]
    assert all(f["event"] == "event" and f["data"]["seq"] == f["id"] for f in frames)
    assert asked == [1290, 1292, 1294, 1295]


def test_idle_stream_polls_at_least_every_half_second():
    """Sin eventos nuevos, el stream espera como mucho 0,5 s entre lecturas"""
    sleeps: list[float] = []

    frames = list(event_stream(lambda after: [], 10, sleep=sleeps.append, max_polls=3))

    assert frames == []
    assert len(sleeps) == 3
    assert all(0 < s <= 0.5 for s in sleeps)
    assert IDLE_SECONDS <= 0.5
