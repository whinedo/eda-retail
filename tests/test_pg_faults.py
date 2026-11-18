"""Caída de la base de datos (spec synthetic-data): corte del contenedor de PostgreSQL con un ejecutor de Docker falso.

Sin Docker ni PostgreSQL. El corte de verdad está en tests/integration/test_e10_postgres_down.py.
"""

from __future__ import annotations

import pytest

from eda_retail.synthetic import pg_faults
from eda_retail.synthetic.pg_faults import PostgresOutage


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class FakeTimer:
    def __init__(self, seconds, fn) -> None:
        self.seconds, self.fn, self.started, self.cancelled, self.daemon = seconds, fn, False, False, False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True


def _outage(mode: str, ready_after: int = 2, **kwargs):
    calls: list[list[str]] = []
    answers = iter([False] * ready_after + [True] * 100)
    clock = Clock()
    outage = PostgresOutage(mode, ready=lambda: next(answers), container="pg-x", run=calls.append, clock=clock,
                            sleep=clock.sleep, **kwargs)
    return outage, calls, clock


@pytest.mark.parametrize("mode, cut, restore", [("stopped", "stop", "start"), ("paused", "pause", "unpause")])
def test_database_cut(mode, cut, restore):
    """Corte de la base de datos"""
    outage, calls, clock = _outage(mode)
    with outage:
        assert calls == [[cut, "pg-x"]]
        clock.now += 30
    assert calls == [[cut, "pg-x"], [restore, "pg-x"]]
    assert outage.report() == {"mode": mode, "cut_seconds": 30.0, "ready_seconds": 2.0}  # dos sondeos sin respuesta


def test_database_cut_ends_on_its_timer():
    """Corte de la base de datos: el temporizador lo termina a su hora y al salir no se restaura dos veces"""
    timers: list[FakeTimer] = []
    outage, calls, clock = _outage("paused", ready_after=0, duration_s=30,
                                   timer=lambda s, fn: timers.append(FakeTimer(s, fn)) or timers[-1])
    with outage:
        [timer] = timers
        assert (timer.seconds, timer.started, timer.daemon, outage.restored) == (30, True, True, False)
        clock.now += 30
        timer.fn()
        assert outage.restored and calls[-1] == ["unpause", "pg-x"]
    assert calls == [["pause", "pg-x"], ["unpause", "pg-x"]] and timer.cancelled
    assert outage.report()["cut_seconds"] == 30.0


def test_failure_during_the_database_cut():
    """Fallo durante el corte de la base de datos"""
    outage, calls, _ = _outage("stopped", ready_after=0)
    with pytest.raises(RuntimeError, match="boom"), outage:
        raise RuntimeError("boom")
    assert calls == [["stop", "pg-x"], ["start", "pg-x"]]


def test_failure_during_the_database_cut_keeps_the_original_error_if_it_never_answers():
    """Fallo durante el corte de la base de datos: la excepción del experimento no se sustituye por el plazo"""
    outage, _, _ = _outage("paused", ready_after=10_000, ready_timeout_s=5)
    with pytest.raises(RuntimeError, match="boom"), outage:
        raise RuntimeError("boom")


def test_database_that_never_answers_after_the_cut():
    outage, calls, _ = _outage("stopped", ready_after=10_000, ready_timeout_s=5)
    with pytest.raises(TimeoutError, match="la base de datos no responde"), outage:
        pass
    assert calls[-1] == ["start", "pg-x"]


def test_a_failed_database_cut_tries_to_restore_it():
    """Fallo durante el corte de la base de datos: si el propio corte falla, se intenta devolverla antes del error"""
    calls: list[list[str]] = []

    def run(args):
        calls.append(list(args))
        if args[0] == "stop":
            raise TimeoutError("docker no responde")

    with pytest.raises(TimeoutError), PostgresOutage("stopped", ready=lambda: True, container="pg", run=run):
        pass
    assert calls == [["stop", "pg"], ["start", "pg"]]


def test_unknown_mode_and_default_container(monkeypatch):
    with pytest.raises(ValueError):
        PostgresOutage("killed", ready=lambda: True)
    monkeypatch.delenv("POSTGRES_CONTAINER", raising=False)
    assert PostgresOutage("stopped", ready=lambda: True).container == "eda-postgres"
    monkeypatch.setenv("POSTGRES_CONTAINER", "otra")
    assert PostgresOutage("paused", ready=lambda: True).container == "otra"
    assert pg_faults.postgres_container() == "otra"


def test_postgres_ready_is_false_without_a_database():
    assert pg_faults.postgres_ready("postgresql://eda:eda@127.0.0.1:1/eda")() is False
