"""E9 Caída de Kafka (spec experiments) y Caída del broker (spec synthetic-data): corte del broker, turno del harness
con errores de entrega del relay, fallos de commit de offset, métricas y H9.

Sin Kafka ni PostgreSQL. La ejecución de punta a punta está en tests/integration/test_e9_kafka_down.py.
"""

from __future__ import annotations

import threading
from collections import Counter
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from eda_retail.experiments import e9_kafka_down as e9
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.infra.outbox import RelayDeliveryError
from eda_retail.synthetic import broker_faults
from eda_retail.synthetic.broker_faults import BrokerOutage


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _outage(mode: str, ready_after: int = 2, **kwargs):
    calls: list[list[str]] = []
    answers = iter([False] * ready_after + [True] * 100)
    clock = Clock()
    outage = BrokerOutage(mode, ready=lambda: next(answers), container="kafka-x", run=calls.append, clock=clock,
                          sleep=clock.sleep, **kwargs)
    return outage, calls, clock


@pytest.mark.parametrize("mode, cut, restore", [("stopped", "stop", "start"), ("paused", "pause", "unpause")])
def test_broker_cut(mode, cut, restore):
    """Corte del broker"""
    outage, calls, clock = _outage(mode)
    with outage:
        assert calls == [[cut, "kafka-x"]]
        clock.now += 30
    assert calls == [[cut, "kafka-x"], [restore, "kafka-x"]]
    assert outage.report() == {"mode": mode, "cut_seconds": 30.0, "ready_seconds": 2.0}  # dos sondeos sin respuesta


def test_failure_during_the_cut():
    """Fallo durante el corte"""
    outage, calls, _ = _outage("paused", ready_after=0)
    with pytest.raises(RuntimeError, match="boom"), outage:
        raise RuntimeError("boom")
    assert calls == [["pause", "kafka-x"], ["unpause", "kafka-x"]]


def test_failure_during_the_cut_with_a_broker_that_never_answers_keeps_the_original_error():
    """Fallo durante el corte: la excepción del experimento no se sustituye por el plazo de espera"""
    outage, _, _ = _outage("stopped", ready_after=10_000, ready_timeout_s=5)
    with pytest.raises(RuntimeError, match="boom"), outage:
        raise RuntimeError("boom")


def test_broker_that_never_answers_after_the_cut():
    outage, calls, _ = _outage("stopped", ready_after=10_000, ready_timeout_s=5)
    with pytest.raises(TimeoutError), outage:
        pass
    assert calls[-1] == ["start", "kafka-x"]


class FakeTimer:
    def __init__(self, seconds, fn) -> None:
        self.seconds, self.fn, self.started, self.cancelled, self.daemon = seconds, fn, False, False, False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True


def test_broker_cut_ends_on_its_timer_even_if_the_caller_is_blocked():
    """Corte del broker: el temporizador lo termina a su hora, y al salir no se restaura dos veces"""
    timers: list[FakeTimer] = []
    outage, calls, clock = _outage("stopped", ready_after=0, duration_s=30,
                                   timer=lambda s, fn: timers.append(FakeTimer(s, fn)) or timers[-1])
    with outage:
        [timer] = timers
        assert (timer.seconds, timer.started, timer.daemon, outage.restored) == (30, True, True, False)
        assert not outage.restoring
        clock.now += 30
        timer.fn()  # dispara, como si el hilo que usa el broker siguiera bloqueado
        assert outage.restored and calls[-1] == ["start", "kafka-x"]
    assert calls == [["stop", "kafka-x"], ["start", "kafka-x"]] and timer.cancelled
    assert outage.report()["cut_seconds"] == 30.0


def test_a_failed_cut_tries_to_restore_the_broker():
    """Fallo durante el corte: si el propio corte falla, se intenta devolver el broker antes de propagar el error"""
    calls: list[list[str]] = []

    def run(args):
        calls.append(list(args))
        if args[0] == "pause":
            raise TimeoutError("docker no responde")

    with pytest.raises(TimeoutError), BrokerOutage("paused", ready=lambda: True, container="k", run=run):
        pass
    assert calls == [["pause", "k"], ["unpause", "k"]]


def test_unknown_mode_and_default_container(monkeypatch):
    with pytest.raises(ValueError):
        BrokerOutage("killed", ready=lambda: True)
    monkeypatch.delenv("KAFKA_CONTAINER", raising=False)
    assert BrokerOutage("stopped", ready=lambda: True).container == "eda-kafka"
    monkeypatch.setenv("KAFKA_CONTAINER", "otro")
    assert broker_faults.kafka_container() == "otro"


def test_broker_ready_is_false_without_a_broker():
    assert broker_faults.broker_ready("127.0.0.1:1", "eda.events")() is False


# --- turno del harness con errores del relay ----------------------------------------------------------------


class FailingRelay:
    _schema = "planner"

    def __init__(self, exc: Exception | None = None) -> None:
        self._exc = exc
        self.timeouts: list[float] = []

    def run_once(self, flush_timeout: float = 30.0) -> int:
        self.timeouts.append(flush_timeout)
        if self._exc is not None:
            raise self._exc
        return 2

    def publish_dead_letters(self, flush_timeout: float = 30.0) -> int:
        self.timeouts.append(flush_timeout)
        return 1


def _turn(**config) -> SimpleNamespace:
    return SimpleNamespace(config=RunConfig(seed=1, incidents=1, **config), relay_errors=Counter(),
                           relay_max_seconds=0.0)


@pytest.mark.parametrize("exc", [RelayDeliveryError("sin confirmar"), KafkaException(KafkaError(KafkaError._TRANSPORT)),
                                 BufferError("cola llena")])
def test_turn_tolerates_relay_delivery_errors_when_asked(exc):
    """Broker parado: el turno cuenta el lote sin confirmar y sigue, como `ComponentRelay.run_until`"""
    h = _turn(tolerate_relay_errors=True, relay_flush_timeout_seconds=1.0)
    relay = FailingRelay(exc)
    assert Harness._publish(h, relay) == 0
    assert h.relay_errors == {"planner": 1} and relay.timeouts == [1.0]


def test_turn_without_tolerance_keeps_raising_with_the_default_deadline():
    """Sin `tolerate_relay_errors` el turno se comporta como antes: el error del relay sube"""
    h = _turn()
    with pytest.raises(RelayDeliveryError):
        Harness._publish(h, FailingRelay(RelayDeliveryError("x")))
    relay = FailingRelay()
    assert Harness._publish(h, relay) == 3 and relay.timeouts == [30.0, 30.0]


# --- fallos de commit de offset -----------------------------------------------------------------------------


class Msg:
    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 7


class Consumer:
    name = "orchestrator"

    def __init__(self) -> None:
        self.committed: list[Msg] = []
        self.closed = False

    def commit(self, msg) -> None:
        self.committed.append(msg)

    def close(self) -> None:
        self.closed = True


def _crashing_harness() -> SimpleNamespace:
    return SimpleNamespace(_crash_lock=threading.Lock(), consumer_crashes=0, poison_messages=0, commit_failures=0,
                           _failures=Counter())


def _crash(h, exc, times: int = 3):
    delivery = SimpleNamespace(consumer=Consumer(), current=Msg())
    first = delivery.consumer
    for _ in range(times):
        Harness._on_crash(h, 0, delivery, Consumer, exc)
    return first, delivery


def test_commit_failure_restarts_the_consumer_without_counting_as_poison():
    """Broker parado: un commit de offset fallido reinicia el consumidor y no salta el mensaje como veneno"""
    h = _crashing_harness()
    first, delivery = _crash(h, KafkaException(KafkaError(KafkaError._TRANSPORT)))
    assert (h.consumer_crashes, h.commit_failures, h.poison_messages) == (3, 3, 0)
    assert first.closed and first.committed == [] and delivery.consumer is not first


def test_processing_failures_are_still_poison_after_three_crashes():
    """Sin cambios para los demás experimentos: tres caídas con el mismo mensaje lo saltan como veneno"""
    h = _crashing_harness()
    delivery = SimpleNamespace(consumer=Consumer(), current=Msg())
    consumers = []
    for _ in range(3):
        consumers.append(delivery.consumer)
        Harness._on_crash(h, 0, delivery, Consumer, ValueError("roto"))
    assert (h.poison_messages, h.commit_failures) == (1, 0)
    assert consumers[-1].committed  # el tercero confirma el offset para saltar el mensaje


# --- métricas y H9 ------------------------------------------------------------------------------------------


def _metrics(**over) -> dict:
    base = {"open": 0, "pending_at_end": {"planner": 0}, "missing_in_log": 0, "accepted_during_outage": 20,
            "published_during_outage": 0,
            "extra_notices": 20, "relay_errors": {"planner": 2}, "max_pending_during_outage": {"planner": 5},
            "invariants": {f"I{i}": {"violations": 0} for i in range(1, 6)}}
    return {**base, **over}


def test_provoked_per_variant():
    assert e9.provoked("baseline", _metrics(relay_errors={}, max_pending_during_outage={}))
    for variant in ("stopped", "paused"):
        assert e9.provoked(variant, _metrics())
        assert not e9.provoked(variant, _metrics(relay_errors={}))
        assert not e9.provoked(variant, _metrics(max_pending_during_outage={"planner": 0}))


@pytest.mark.parametrize("over", [{"open": 1}, {"pending_at_end": {"planner": 1}}, {"missing_in_log": 1},
                                  {"published_during_outage": 1},
                                  {"accepted_during_outage": 19}, {"relay_errors": {}},
                                  {"invariants": {"I2": {"violations": 1}}}])
def test_h9_fails_on_each_broken_condition(over):
    assert e9.h9_variant("stopped", _metrics())
    assert not e9.h9_variant("stopped", _metrics(**over))


def test_pending_sampler_tracks_the_maximum_and_the_drain_time(monkeypatch):
    samples = iter([{"planner": 3, "intake": 1}, {"planner": 5, "intake": 0}, {"planner": 0, "intake": 0}])
    monkeypatch.setattr(e9, "pending_outbox", lambda h: next(samples))
    clock = Clock()
    sampler = e9.PendingSampler(object(), clock=clock)
    sampler()
    clock.now += 0.1
    sampler()  # antes de SAMPLE_EVERY_S: no muestrea
    clock.now += 1
    sampler()
    sampler.since = clock.now
    clock.now += 2
    sampler()
    assert sampler.max_pending == {"planner": 5, "intake": 1}
    assert sampler.drain_seconds() == 2.0


def test_p95_ratio_against_baseline():
    assert e9.p95_ratio({"latency_ms": {"p95": 300.0}}, {"latency_ms": {"p95": 100.0}}) == 3.0
    assert e9.p95_ratio({"latency_ms": {"p95": 300.0}}, {"latency_ms": {"p95": None}}) is None
