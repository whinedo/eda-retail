"""Acceso a las trazas en Jaeger (spec web-front)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from eda_retail.web.app import create_app
from eda_retail.web.traces import DEFAULT_JAEGER_UI_URL, jaeger_ui_url, trace_id_from_context

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def test_trace_id_from_traceparent():
    """Ver la traza de un evento: el trace_id sale del traceparent de outbox.trace_context"""
    assert trace_id_from_context({"traceparent": TRACEPARENT}) == "4bf92f3577b34da6a3ce929d0e0e4736"


@pytest.mark.parametrize(
    "context",
    [
        None,
        {},
        {"traceparent": "basura"},
        {"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01"},  # 31 hex
        {"traceparent": "00-00000000000000000000000000000000-00f067aa0ba902b7-01"},  # id nulo
        {"traceparent": "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"},  # mayúsculas
        {"traceparent": 42},
    ],
)
def test_event_without_trace(context):
    """Evento sin traza: sin trace_context o con un traceparent mal formado no hay trace_id"""
    assert trace_id_from_context(context) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, DEFAULT_JAEGER_UI_URL),
        ("", DEFAULT_JAEGER_UI_URL),
        ("http://jaeger.local:16686/", "http://jaeger.local:16686"),
        ("https://traces.example/ui", "https://traces.example/ui"),
        ("javascript:alert(1)", DEFAULT_JAEGER_UI_URL),
        ("ftp://jaeger", DEFAULT_JAEGER_UI_URL),
        ("http://", DEFAULT_JAEGER_UI_URL),
    ],
)
def test_jaeger_ui_url_from_env(monkeypatch: pytest.MonkeyPatch, value, expected):
    """La URL base sale de JAEGER_UI_URL; un valor que no es http(s) se ignora"""
    if value is None:
        monkeypatch.delenv("JAEGER_UI_URL", raising=False)
    else:
        monkeypatch.setenv("JAEGER_UI_URL", value)
    assert jaeger_ui_url() == expected


def test_invalid_jaeger_url_in_config(monkeypatch: pytest.MonkeyPatch):
    """URL de Jaeger no válida: /api/config devuelve la de por defecto"""
    monkeypatch.setenv("JAEGER_UI_URL", "javascript:alert(1)")
    client = TestClient(create_app(database_url="postgresql://x:x@127.0.0.1:1/none"), base_url="http://localhost")

    assert client.get("/api/config").json() == {"jaeger_ui_url": "http://localhost:16686"}
