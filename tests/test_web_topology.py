"""Mapa de sistemas, salud y detalle de nodo (spec web-front, "Mapa de sistemas" y "Detalle de un nodo").

Las comprobaciones de salud se sustituyen por funciones falsas o por servidores locales en puertos
libres: los tests no tocan PostgreSQL, Kafka ni Ollama reales.
"""

from __future__ import annotations

import http.server
import socket
import threading
import time

import pytest
from fastapi.testclient import TestClient

from eda_retail.events.catalog import CATALOG, Action
from eda_retail.web import topology
from eda_retail.web.app import create_app
from eda_retail.web.health import HealthChecker, check_kafka, check_ollama, check_postgres
from eda_retail.web.routing import receivers
from eda_retail.web.topology import EDGES, KINDS, MONITORED, NODES, node

SPEC_NODES = {
    "intake", "postgres", "relay", "kafka", "dlq", "event_log", "projectors", "interpreter", "planner", "replanner",
    "orchestrator", "ollama", "mail", "claims", "carrier", "warehouse", "payments", "jaeger",
    *(f"executor-{a.value}" for a in Action),
}


def _checker(up: set[str] = frozenset(), infra: dict | None = None) -> HealthChecker:
    checks = infra or {"postgres": lambda: True, "kafka": lambda: True, "ollama": lambda: True}
    return HealthChecker(checks, pgrep=lambda pattern: any(pattern == node(n).process for n in up))


def _client(checker: HealthChecker | None = None) -> TestClient:
    return TestClient(
        create_app(database_url="postgresql://x:x@127.0.0.1:1/none", health=checker or _checker()),
        base_url="http://localhost",
    )


def test_map_has_the_spec_nodes():
    """Mapa de sistemas: los nodos de la spec, cada uno con un tipo conocido"""
    assert {n.id for n in NODES} == SPEC_NODES
    assert all(n.kind in KINDS for n in NODES)
    assert len({n.id for n in NODES}) == len(NODES)


def test_thirteen_processes_are_monitored():
    """Estado de un nodo: se vigilan los procesos de scripts/run-agents.sh (13 nodos)"""
    assert len(MONITORED) == 13
    assert node("relay").process is None  # cada componente publica su outbox con su propio relay
    assert node("intake").process == "eda-intake"
    assert node("event_log").process == "eda-event-log"
    assert node("interpreter").process == "eda-interpreter"
    assert node("planner").process == "eda-planner"
    assert node("replanner").process == "eda-replanner"
    assert node("executor-issue_refund").process == "eda-step-executor issue_refund"
    assert node("executor-notify_customer").process == "eda-step-executor notify_customer"
    assert node("executor-reserve_stock").process == "eda-inventory"
    assert node("executor-release_stock").process == "eda-inventory"


def test_edges_join_existing_nodes():
    """Las aristas unen nodos del mapa"""
    ids = {n.id for n in NODES}
    assert EDGES
    assert all(source in ids and target in ids for source, target in EDGES)


def test_every_event_receiver_is_a_node():
    """El destino de cada evento es un nodo del mapa, para poder resaltar su arista"""
    ids = {n.id for n in NODES}
    for event_type in CATALOG:
        for action in Action:
            assert set(receivers(event_type, {"action": action.value})) <= ids


def test_topology_endpoint():
    """GET /api/topology devuelve nodos y aristas"""
    body = _client().get("/api/topology").json()

    assert {n["id"] for n in body["nodes"]} == SPEC_NODES
    planner = next(n for n in body["nodes"] if n["id"] == "planner")
    assert planner["kind"] == "llm" and planner["monitored"] is True and planner["label"]
    assert {"source": "planner", "target": "ollama"} in body["edges"]


def test_node_state_and_process_count():
    """Estado de un nodo: activo si el proceso existe, caído si no, y el recuento de procesos"""
    body = _client(_checker(up={"intake", "planner"})).get("/api/health").json()

    assert body["processes"]["intake"] is True
    assert body["processes"]["planner"] is True
    assert body["processes"]["orchestrator"] is False
    assert body["processes_up"] == 2 and body["processes_total"] == 13


def test_infrastructure_health():
    """Salud de la infraestructura: healthy o down por servicio"""
    infra = {"postgres": lambda: True, "kafka": lambda: False, "ollama": lambda: True}

    body = _client(_checker(infra=infra)).get("/api/health").json()

    assert body["infra"] == {"postgres": "healthy", "kafka": "down", "ollama": "healthy"}


def test_service_down_does_not_block_the_rest():
    """Servicio caído: una comprobación que falla o tarda más de 2 s cuenta como down"""

    def hangs() -> bool:
        time.sleep(3)
        return True

    def explodes() -> bool:
        raise ConnectionError("boom")

    checker = HealthChecker(
        {"postgres": lambda: True, "kafka": explodes, "ollama": hangs}, pgrep=lambda p: False, timeout=0.5
    )
    started = time.monotonic()

    report = checker.report()

    assert time.monotonic() - started < 2
    assert report["infra"] == {"postgres": "healthy", "kafka": "down", "ollama": "down"}


def test_unknown_node_is_404():
    """Un id de nodo desconocido responde 404 sin consultar la base de datos"""
    assert _client().get("/api/nodes/does-not-exist").status_code == 404


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_ollama_check_uses_api_tags():
    """Ollama está healthy si GET /api/tags responde 200"""
    paths: list[str] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            paths.append(self.path)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"models":[]}')

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        assert check_ollama(f"http://127.0.0.1:{server.server_port}") is True
        assert paths == ["/api/tags"]
    finally:
        server.shutdown()


@pytest.mark.parametrize(
    "check",
    [
        lambda port: check_ollama(f"http://127.0.0.1:{port}"),
        lambda port: check_postgres(f"postgresql://x:x@127.0.0.1:{port}/none"),
        lambda port: check_kafka(f"127.0.0.1:{port}"),
    ],
)
def test_checks_report_down_when_nothing_listens(check):
    """Sin servicio escuchando, cada comprobación devuelve False en vez de lanzar"""
    assert check(_free_port()) is False


def test_version_1_map_has_no_carrier_agent():
    """POC_VERSION=1: el mapa y los procesos vigilados son los de la primera versión"""
    assert topology.nodes(1) == NODES and topology.edges(1) == EDGES and topology.monitored(1) == MONITORED
    assert "carrier_agent" not in {n.id for n in NODES}


def test_version_2_map_shows_the_carrier_agent():
    """POC_VERSION=2: el agente del transportista (A2A) aparece como nodo vigilado, consultado por el planificador"""
    ids = {n.id for n in topology.nodes(2)}
    assert ids == SPEC_NODES | {"carrier_agent"}
    agent = next(n for n in topology.nodes(2) if n.id == "carrier_agent")
    assert (agent.kind, agent.process) == ("simulated", "eda-carrier-agent")
    assert ("planner", "carrier_agent") in topology.edges(2)
    assert len(topology.monitored(2)) == 14


def test_version_2_health_counts_the_carrier_agent():
    checker = HealthChecker({}, pgrep=lambda pattern: pattern == "eda-carrier-agent",
                            monitored=topology.monitored(2))
    report = checker.report()
    assert report["processes"]["carrier_agent"] is True
    assert (report["processes_up"], report["processes_total"]) == (1, 14)
