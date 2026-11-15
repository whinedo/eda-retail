"""Topología estática del mapa de sistemas (spec web-front, "Mapa de sistemas", design.md §3).

Los ids coinciden con el emisor y el destino de los eventos (`web.routing`), para que el front
pueda resaltar la arista de cada evento. `process` es el patrón de `pgrep -f` de
`scripts/run-agents.sh`; los nodos sin proceso propio no se vigilan.

`NODES`, `EDGES` y `MONITORED` son los de la versión 1. Con `POC_VERSION=2`, `nodes(2)`, `edges(2)` y
`monitored(2)` añaden el agente del transportista (A2A, spec a2a-carrier), un proceso aparte
(`eda-carrier-agent`) que consulta el planificador.
"""

from __future__ import annotations

from dataclasses import dataclass

from eda_retail.events.catalog import Action

KINDS = ("llm", "code", "data", "external", "simulated")
WRITER = "writer"  # redactor con LLM, invocado desde notify_customer y file_claim


@dataclass(frozen=True)
class Node:
    id: str
    label: str
    kind: str
    description: str
    process: str | None = None
    llm_agent: str | None = None

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "label": self.label,
            "kind": self.kind,
            "description": self.description,
            "monitored": self.process is not None,
        }


_EXECUTOR_SERVICE = {
    Action.NOTIFY_CUSTOMER: "mail",
    Action.FILE_CLAIM: "claims",
    Action.RESERVE_STOCK: "warehouse",
    Action.RELEASE_STOCK: "warehouse",
    Action.REISSUE_SHIPMENT: "carrier",
    Action.ISSUE_REFUND: "payments",
}
_WRITER_ACTIONS = {Action.NOTIFY_CUSTOMER, Action.FILE_CLAIM}
_AGENTS = {
    "interpreter": ("Intérprete", "Convierte el aviso del transportista en ShipmentDelayed (o NoticeParked si su pedido aún no existe). Sin herramientas."),
    "planner": ("Planificador", "Propone el plan de pasos; el command handler lo valida."),
    "replanner": ("Replanificador", "Revisa el plan cuando falla un paso."),
}

# Cada agente es un componente con su propio proceso (T5, T6 y T8).
_PROCESSES = {"interpreter": "eda-interpreter", "planner": "eda-planner", "replanner": "eda-replanner"}
# reserve_stock y release_stock los ejecuta inventory (T4); el resto, `eda-step-executor <acción>` (T9).
_EXECUTOR_PROCESSES = {Action.RESERVE_STOCK: "eda-inventory", Action.RELEASE_STOCK: "eda-inventory"}

NODES: tuple[Node, ...] = (
    Node("intake", "Recepción (intake)", "code",
         "Registra los avisos que envía eda-notice (RegisterCarrierNotice) y publica CarrierNoticeReceived.", "eda-intake"),
    Node("postgres", "PostgreSQL", "data", "Un esquema por componente: estado, outbox, inbox, el log event_log.events y las vistas views.*."),
    Node("relay", "Relay", "code",
         "Cada componente publica en Kafka su outbox con su propio relay (un hilo de su proceso)."),
    Node("kafka", "Kafka (eda.events)", "data", "Topic único con todos los eventos."),
    Node("dlq", "eda.dlq", "data", "Mensajes que un consumidor no pudo procesar (dead letters)."),
    Node("event_log", "Log de eventos", "code",
         "Guarda cada evento consumido de Kafka en event_log.events, ordenado; alimenta el panel de eventos.",
         "eda-event-log"),
    Node("projectors", "Proyector", "code", "Mantiene las vistas views.view_* consumiendo Kafka, con su offset por partición.", "eda-projector"),
    *(Node(agent, label, "llm", desc, _PROCESSES[agent], agent)
      for agent, (label, desc) in _AGENTS.items()),
    Node("orchestrator", "Orquestador", "code", "Avanza el plan, pide pasos y deriva compensaciones.",
         "eda-orchestrator"),
    *(
        Node(
            f"executor-{a.value}", a.value, "code", f"Ejecutor de {a.value}: aplica el efecto de forma idempotente.",
            _EXECUTOR_PROCESSES.get(a, f"eda-step-executor {a.value}"), WRITER if a in _WRITER_ACTIONS else None,
        )
        for a in Action
    ),
    Node("ollama", "Ollama", "external", "Modelo local que usan los agentes y el redactor."),
    Node("mail", "Correo", "simulated", "Servicio de correo simulado (notify_customer)."),
    Node("claims", "Aseguradora", "simulated", "Reclamaciones simuladas (file_claim)."),
    Node("carrier", "Transportista", "simulated", "Reenvíos simulados (reissue_shipment)."),
    Node("warehouse", "Almacén", "simulated", "Tablas de stock y reservas (reserve_stock, release_stock)."),
    Node("payments", "Pagos", "simulated", "Reembolsos simulados (issue_refund)."),
    Node("jaeger", "Jaeger", "external", "Trazas de OpenTelemetry de todos los procesos."),
)

_BY_ID = {n.id: n for n in NODES}
MONITORED: tuple[Node, ...] = tuple(n for n in NODES if n.process)

_CONSUMERS = ["interpreter", "planner", "replanner", "orchestrator", *(f"executor-{a.value}" for a in Action)]
EDGES: tuple[tuple[str, str], ...] = (
    ("intake", "postgres"),
    ("postgres", "relay"),
    ("relay", "kafka"),
    ("relay", "dlq"),
    ("kafka", "projectors"),
    ("kafka", "event_log"),
    ("event_log", "postgres"),
    ("projectors", "postgres"),
    *(("kafka", c) for c in _CONSUMERS),
    *((c, "postgres") for c in _CONSUMERS),  # escriben a través del command handler
    *((agent, "ollama") for agent in _AGENTS),
    *((f"executor-{a.value}", "ollama") for a in Action if a in _WRITER_ACTIONS),
    *((f"executor-{a.value}", service) for a, service in _EXECUTOR_SERVICE.items()),
)


def node(node_id: str, version: int = 1) -> Node:
    """Nodo por id en el mapa de la versión `version`; KeyError si no existe (la 1 no conoce `carrier_agent`)."""
    if version == 2 and node_id == CARRIER_AGENT.id:
        return CARRIER_AGENT
    return _BY_ID[node_id]


# Segunda versión (design.md §15): el agente del transportista, opaco y simulado, que consulta el planificador.
CARRIER_AGENT = Node(
    "carrier_agent", "Agente del transportista (A2A)", "simulated",
    "Agente externo y opaco del transportista: el planificador le pide opciones de entrega (delivery_options) "
    "por A2A antes del LLM. Reglas deterministas con semilla.",
    "eda-carrier-agent",
)
_V2_NODES = (*NODES, CARRIER_AGENT)
_V2_EDGES = (*EDGES, ("planner", CARRIER_AGENT.id))


def nodes(version: int) -> tuple[Node, ...]:
    """Los nodos del mapa de la versión `version` de la PoC (1 o 2)."""
    return _V2_NODES if version == 2 else NODES


def edges(version: int) -> tuple[tuple[str, str], ...]:
    return _V2_EDGES if version == 2 else EDGES


def monitored(version: int) -> tuple[Node, ...]:
    """Los nodos con proceso propio que vigila la salud, según la versión."""
    return tuple(n for n in nodes(version) if n.process)
