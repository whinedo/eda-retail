"""Estado que anota un consumidor de Kafka desde su propio hilo (E9): asignación, rebalanceos, errores de `poll` y
tiempo desde el último mensaje. Leerlo desde otro hilo no llama a Kafka. Sin Kafka: se sustituye el consumidor."""

from __future__ import annotations

from types import SimpleNamespace

from confluent_kafka import KafkaError, TopicPartition

from eda_retail.infra.consumer import EventConsumer


class _Kafka:
    def __init__(self, polled: list) -> None:
        self.polled = polled
        self.calls: list[str] = []

    def subscribe(self, _topics, **callbacks) -> None:
        self.callbacks = callbacks

    def poll(self, _timeout):
        return self.polled.pop(0) if self.polled else None

    def __getattr__(self, name):  # cualquier otra llamada a Kafka queda anotada: `health` no debe hacer ninguna
        self.calls.append(name)
        raise AssertionError(f"health llamó a Kafka: {name}")


def _error(code: int) -> SimpleNamespace:
    return SimpleNamespace(error=lambda: KafkaError(code))


def _message() -> SimpleNamespace:
    return SimpleNamespace(error=lambda: None, partition=lambda: 1, offset=lambda: 5)


def _consumer(kafka: _Kafka) -> EventConsumer:
    return EventConsumer("interpreter", object(), kafka, "t", {}, schema="interpreter")


def test_health_tracks_the_assignment_and_each_rebalance():
    """El consumidor anota sus particiones asignadas y cada rebalanceo, en el orden en que llegan"""
    kafka = _Kafka([])
    consumer = _consumer(kafka)
    tps = [TopicPartition("t", p) for p in (0, 1, 2)]
    kafka.callbacks["on_assign"](None, tps)
    kafka.callbacks["on_revoke"](None, tps[:1])
    kafka.callbacks["on_lost"](None, tps[1:])
    kafka.callbacks["on_assign"](None, tps[2:])
    health = consumer.health()
    assert health["assigned"] == [2]
    assert [(r["kind"], r["partitions"]) for r in health["rebalances"]] == [
        ("assigned", [0, 1, 2]), ("revoked", [0]), ("lost", [1, 2]), ("assigned", [2])]
    assert kafka.calls == []


def test_health_counts_the_poll_errors_it_used_to_drop():
    """Los mensajes de error de `poll` siguen sin entregarse, pero quedan contados por código"""
    kafka = _Kafka([_error(KafkaError._TRANSPORT), _error(KafkaError._TRANSPORT), _error(KafkaError.UNKNOWN_MEMBER_ID),
                    _message()])
    consumer = _consumer(kafka)
    assert [consumer.poll(0.1) for _ in range(3)] == [None, None, None]
    assert consumer.poll(0.1) is not None
    health = consumer.health()
    assert health["poll_errors"] == {"_TRANSPORT": 2, "UNKNOWN_MEMBER_ID": 1}
    assert health["polls"] == 4 and health["messages"] == 1
    assert health["seconds_since_message"] is not None and health["seconds_since_message"] < 5
    assert kafka.calls == []


def test_health_before_any_message():
    """Sin mensajes todavía, no hay tiempo desde el último"""
    consumer = _consumer(_Kafka([None]))
    consumer.poll(0.1)
    assert consumer.health()["seconds_since_message"] is None and consumer.health()["messages"] == 0
