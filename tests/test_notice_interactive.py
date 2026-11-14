"""Menú interactivo de `eda-notice` sin texto libre (spec web-front, "Texto libre deshabilitado").

Sin base de datos: con el texto libre deshabilitado, `t` es una opción no válida y no se registra nada.
"""

from __future__ import annotations

from eda_retail.synthetic.cli import parse_args
from eda_retail.synthetic.interactive import run


def _run(answers: list[str], **kwargs) -> list[str]:
    replies = iter(answers)
    out: list[str] = []
    run(None, lambda _prompt: next(replies), out.append, **kwargs)  # type: ignore[arg-type]
    return out


def test_menu_without_free_text_rejects_t():
    """--no-free-text: el menú no ofrece texto libre y `t` no es una opción"""
    out = _run(["t", "q"], free_text=False)

    assert "texto libre" not in out[0]
    assert "s) cargar datos de ejemplo" in out[0]
    assert "opción no válida: t" in out


def test_menu_offers_free_text_by_default():
    """Sin la opción, el menú sigue ofreciendo `t) texto libre`"""
    out = _run(["q"])

    assert "t) texto libre" in out[0]


def test_cli_no_free_text_flag():
    assert parse_args(["interactive", "--no-free-text"]).no_free_text is True
    assert parse_args(["interactive"]).no_free_text is False
