"""E9 Caída de Kafka (spec experiments) y Caída del broker (spec synthetic-data): corte del broker, turno del harness
con errores de entrega del relay, fallos de commit de offset, métricas y H9.

Sin Kafka ni PostgreSQL. E9 está retirado del catálogo (spec experiments, «E9 Caída de Kafka: retirado»): ya no tiene
ejecución de punta a punta, pero E10 reutiliza este módulo y la inyección de la caída del broker sigue en uso.
"""

from __future__ import annotations

import threading
from collections import Counter
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from eda_retail.experiments import e9_kafka_down as e9
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.infra.consumer import EventConsumer
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


# --- diagnóstico de consumidores --------------------------------------------------------------------------------
class FakeKafka:
    def __init__(self, positions: dict[int, int]) -> None:
        self._positions = positions

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition("t", p) for p in self._positions]

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        return [TopicPartition(tp.topic, tp.partition, self._positions[tp.partition]) for tp in partitions]


def test_consumer_positions_by_assigned_partition():
    """Diagnóstico: cada partición asignada con el siguiente offset que leerá el consumidor"""
    consumer = SimpleNamespace(_kafka=FakeKafka({0: 7, 2: -1001}))
    assert EventConsumer.positions(consumer) == {0: 7, 2: -1001}


class BrokenConsumer:
    name = "orchestrator"

    def positions(self) -> dict[int, int]:
        raise KafkaException(KafkaError(KafkaError._TRANSPORT))


def _never_called() -> dict[int, int]:
    raise AssertionError("no se consulta la asignación de un consumidor con hilo propio")


def test_consumer_state_reports_positions_thread_and_errors():
    """Diagnóstico: posiciones de los consumidores de los turnos, si el hilo de los demás sigue vivo (sin consultar su
    asignación, que se bloquea mientras su hilo está en `poll`) y, si Kafka falla, el error sin interrumpir"""
    interpreter = SimpleNamespace(name="interpreter", positions=_never_called)
    planner = SimpleNamespace(name="planner", positions=lambda: {1: 3})
    thread = threading.Thread(target=lambda: None, name="harness-interpreter")
    thread.start()
    thread.join()
    h = SimpleNamespace(_threaded={"interpreter"}, _stuck=set(), _threads=[thread], _deliveries=[(SimpleNamespace(consumer=interpreter), None),
                                                        (SimpleNamespace(consumer=planner), None),
                                                        (SimpleNamespace(consumer=BrokenConsumer()), None)])
    state = Harness.consumer_state(h)
    assert state["interpreter"] == {"thread_alive": False}
    assert state["planner"] == {"positions": {1: 3}}
    assert set(state["orchestrator"]) == {"error"} and "_TRANSPORT" in state["orchestrator"]["error"]


def test_diagnostics_keeps_the_lag_error_and_the_consumer_state():
    """Diagnóstico de E9: un fallo al leer el lag queda en el informe y no tumba la variante"""
    def lags() -> dict[str, int]:
        raise KafkaException(KafkaError(KafkaError._TRANSPORT))

    h = SimpleNamespace(lags=lags, consumer_state=lambda: {"interpreter": {"positions": {}}})
    d = e9.diagnostics(h)
    assert set(d["group_lag"]) == {"error"} and d["consumers"] == {"interpreter": {"positions": {}}}


def test_consumer_state_never_queries_a_threaded_consumer_after_its_thread_stopped():
    """Diagnóstico: tras parar los hilos (`_threads` vacío), un consumidor con hilo propio sigue sin consultarse"""
    h = SimpleNamespace(_threaded={"interpreter"}, _stuck=set(), _threads=[], _deliveries=[
        (SimpleNamespace(consumer=SimpleNamespace(name="interpreter", positions=_never_called)), None)])
    assert Harness.consumer_state(h) == {"interpreter": {"thread_alive": False}}


def test_in_process_consumers_get_the_session_and_max_poll_interval(monkeypatch):
    """E9: el intérprete y el orquestador en proceso reciben el `session.timeout.ms` y el `max.poll.interval.ms`"""
    from eda_retail.experiments import harness as harness_module

    calls = {}
    monkeypatch.setattr(harness_module, "make_consumer", lambda servers, group, **kw: calls.setdefault(group, kw))
    monkeypatch.setattr(harness_module.interpreter, "build_consumer", lambda *a, **kw: None)
    monkeypatch.setattr(harness_module.orchestrator, "build_consumer", lambda *a, **kw: None)
    h = SimpleNamespace(
        settings=SimpleNamespace(kafka_bootstrap_servers="k:9092"), _inbox_off=set(), llm=None, rules=None,
        _connect=lambda: None,
        config=RunConfig(seed=1, incidents=1, consumer_session_timeout_ms=6000, consumer_max_poll_interval_ms=15000))
    for factory in Harness._factories(h).values():
        factory()
    expected = {"session_timeout_ms": 6000, "max_poll_interval_ms": 15000}
    assert calls == {"interpreter": expected, "orchestrator": expected}


def test_with_kafka_debug_each_in_process_consumer_gets_its_own_log_ring(monkeypatch):
    """`consumer_kafka_debug`: cada consumidor en proceso recibe `debug` y un anillo propio, que se conserva al
    recrearlo"""
    from eda_retail.experiments import harness as harness_module

    calls: dict[str, list] = {}
    monkeypatch.setattr(harness_module, "make_consumer",
                        lambda servers, group, **kw: calls.setdefault(group, []).append(kw))
    monkeypatch.setattr(harness_module.interpreter, "build_consumer", lambda *a, **kw: None)
    monkeypatch.setattr(harness_module.orchestrator, "build_consumer", lambda *a, **kw: None)
    h = SimpleNamespace(
        settings=SimpleNamespace(kafka_bootstrap_servers="k:9092"), _inbox_off=set(), llm=None, rules=None,
        _connect=lambda: None, _kafka_logs={},
        config=RunConfig(seed=1, incidents=1, consumer_kafka_debug="cgrp"))
    factories = Harness._factories(h)
    factories["interpreter"]()
    factories["interpreter"]()
    factories["orchestrator"]()
    first, again = calls["interpreter"]
    assert first["debug"] == "cgrp" and first["log"] is again["log"] is h._kafka_logs["interpreter"]
    assert calls["orchestrator"][0]["log"] is h._kafka_logs["orchestrator"] is not h._kafka_logs["interpreter"]


def test_e9_bounds_the_rejoin_after_the_outage(monkeypatch):
    """Broker congelado: un consumidor que pierde sus particiones vuelve al grupo antes de que E9 se dé por quieto"""
    from eda_retail.infra.consumer import COMMIT_TIMEOUT_S

    seen = {}

    class Stop(Exception):
        pass

    def fake_harness(base, config, **kw):
        seen["config"] = config
        raise Stop

    monkeypatch.setattr(e9, "Harness", fake_harness)
    with pytest.raises(Stop):
        e9.run_variant(None, "paused", seed=1, incidents=2, extra=0)
    config = seen["config"]
    assert config.consumer_max_poll_interval_ms == e9.MAX_POLL_INTERVAL_MS
    assert config.consumer_session_timeout_ms <= e9.MAX_POLL_INTERVAL_MS < e9.IDLE_SECONDS * 1000
    assert COMMIT_TIMEOUT_S * 1000 < e9.MAX_POLL_INTERVAL_MS
    assert config.consumer_kafka_debug == e9.KAFKA_DEBUG == "cgrp"


def test_e9_is_retired_from_the_catalogs():
    """E9 fuera del catálogo"""
    from eda_retail.experiments.cli import DEFAULT_INCIDENTS, EXPERIMENTS, parse_args
    from eda_retail.expexecutor.catalog import CATALOG

    with pytest.raises(SystemExit):
        parse_args(["e9"])
    assert "e9" not in EXPERIMENTS and "e9" not in DEFAULT_INCIDENTS and "e9" not in CATALOG
    assert parse_args(["e10"]).experiment == "e10"  # el resto del catálogo sigue igual
    assert "e10" in CATALOG
