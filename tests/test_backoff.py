"""Espera exponencial con jitter."""

from __future__ import annotations

from eda_retail.executors.retry import Backoff


def test_backoff_is_exponential_capped_and_jittered():
    """La espera crece exponencialmente hasta el máximo, con jitter dentro del tope"""
    b = Backoff(base_seconds=0.5, max_seconds=3, seed=7)
    assert [b.cap(n) for n in (1, 2, 3, 4)] == [0.5, 1.0, 2.0, 3]
    delays = [b.delay(n) for n in (1, 2, 3, 4)]
    assert all(0 <= d <= b.cap(n) for n, d in zip((1, 2, 3, 4), delays, strict=True))
    again = Backoff(base_seconds=0.5, max_seconds=3, seed=7)
    assert [again.delay(n) for n in (1, 2, 3, 4)] == delays  # repetible con semilla
