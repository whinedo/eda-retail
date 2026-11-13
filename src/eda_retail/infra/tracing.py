"""Trazas distribuidas con OpenTelemetry (spec observability-audit, Tracing distribuido 6.10).

- Asíncrono: el contexto W3C (`traceparent`) se guarda en `outbox.trace_context` al
  escribir el evento, el relay lo copia a las cabeceras de Kafka y el consumidor lo
  extrae. Así escritura → publicación → consumo → eventos derivados forman una traza.
- Síncrono: cada sentencia SQL (instrumentación de psycopg) y las funciones marcadas
  con `@traced` generan spans dentro de la operación en curso.

Las sentencias SQL sin operación en curso (p. ej. el polling del relay) no se trazan:
el sampler descarta los spans CLIENT que serían raíz.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Mapping
from typing import Any, ParamSpec, TypeVar

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, Decision, ParentBased, Sampler, SamplingResult
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from eda_retail.config import Settings
from eda_retail.events.envelope import EventEnvelope

TRACER_NAME = "eda_retail"
_propagator = TraceContextTextMapPropagator()
_provider: TracerProvider | None = None

P = ParamSpec("P")
R = TypeVar("R")


class DropOrphanClientSpans(Sampler):
    """Descarta los spans CLIENT sin padre (SQL fuera de una operación); el resto, ParentBased."""

    def __init__(self) -> None:
        self._delegate = ParentBased(ALWAYS_ON)

    def should_sample(self, parent_context, trace_id, name, kind=None, attributes=None, links=None, trace_state=None):
        parent = trace.get_current_span(parent_context).get_span_context()
        if kind is SpanKind.CLIENT and not parent.is_valid:
            return SamplingResult(Decision.DROP)
        return self._delegate.should_sample(parent_context, trace_id, name, kind, attributes, links, trace_state)

    def get_description(self) -> str:
        return "DropOrphanClientSpans"


def build_provider(service_name: str, processor: SpanProcessor) -> TracerProvider:
    provider = TracerProvider(
        resource=Resource.create({"service.name": service_name}), sampler=DropOrphanClientSpans()
    )
    provider.add_span_processor(processor)
    return provider


def install(provider: TracerProvider | None) -> None:
    """Fija el proveedor que usa este paquete (None = trazas desactivadas)."""
    global _provider
    _provider = provider


def provider() -> TracerProvider | None:
    return _provider


def setup_tracing(service_name: str, settings: Settings) -> TracerProvider | None:
    """Configura la exportación OTLP/HTTP hacia Jaeger para un proceso. None si está desactivado."""
    if not settings.tracing_enabled:
        install(None)
        return None
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    exporter = OTLPSpanExporter(endpoint=f"{settings.otlp_endpoint.rstrip('/')}/v1/traces")
    p = build_provider(service_name, BatchSpanProcessor(exporter))
    install(p)
    trace.set_tracer_provider(p)
    return p


def tracer() -> trace.Tracer:
    if _provider is None:
        return trace.NoOpTracer()
    return _provider.get_tracer(TRACER_NAME)


def inject_current() -> dict[str, str] | None:
    """Contexto W3C del span actual, o None si no hay traza activa."""
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return carrier or None


def extract(carrier: Mapping[str, str] | None) -> otel_context.Context:
    return _propagator.extract(dict(carrier or {}))


def event_attributes(event: EventEnvelope) -> dict[str, Any]:
    attrs: dict[str, Any] = {
        "eda.event_id": str(event.event_id),
        "eda.event_type": event.event_type,
        "eda.event_version": event.event_version,
        "eda.incident_id": event.correlation_id,
        "eda.producer": event.producer,
    }
    if event.causation_id is not None:
        attrs["eda.causation_id"] = str(event.causation_id)
    return attrs


def traced(name: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorador para trazar una operación síncrona en proceso."""

    def decorate(fn: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with tracer().start_as_current_span(name):
                return fn(*args, **kwargs)

        return wrapper

    return decorate
