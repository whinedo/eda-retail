"""Escenarios de las specs cqrs-views y event-backbone que se prueban sin PostgreSQL ni Kafka.

- «El orquestador no recibe comandos»: el catálogo de comandos de docs/README.md y el enrutado de la saga.
- «Lectura del panel»: las lecturas del panel por HTTP, con una conexión falsa que registra cada SQL.
- «Dato de otro componente»: el proyector `orders` y el `read_context` del planificador sobre una base en
  memoria que registra cada SQL.
"""

from __future__ import annotations

import contextlib
import re
import uuid
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from eda_retail.config import Settings
from eda_retail.events.catalog import Action, build_event
from eda_retail.infra.positions import Position
from eda_retail.orchestrator import saga
from eda_retail.planner import handler as planner
from eda_retail.projectors.registry import PROJECTORS
from eda_retail.rules import BusinessRules
from eda_retail.web import app as web_app

ROOT = Path(__file__).resolve().parents[1]
RULES = BusinessRules(max_refund_amount=Decimal("500.00"))
_TABLE = re.compile(r"\b(?:FROM|JOIN)\s+([a-z_][\w.]*)", re.IGNORECASE)
_WRITE = re.compile(r"\b(?:INSERT|UPDATE|DELETE|TRUNCATE)\b", re.IGNORECASE)


# --- cqrs-views: «Catálogo de comandos» -----------------------------------------------------------


def _command_catalog() -> dict[str, str]:
    """Comando -> columna «Lo atiende» de la tabla «Catálogo de comandos» de docs/README.md."""
    text = (ROOT / "docs" / "README.md").read_text()
    section = text.split("## Catálogo de comandos", 1)[1].split("\n## ", 1)[0]
    catalog = {}
    for line in section.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3 or not cells[0].startswith("`"):
            continue
        catalog[cells[0].split("`")[1]] = cells[2]
    return catalog


def test_the_orchestrator_receives_no_commands():
    """El orquestador no recibe comandos"""
    catalog = _command_catalog()
    assert {"RegisterCarrierNotice", "SeedSampleData", "OpenIncident", "CreatePlan", "RevisePlan",
            "ExecuteStep"} <= set(catalog)
    receivers = {command: receiver.lower() for command, receiver in catalog.items()}
    assert all("orchestrator" not in r and "orquestador" not in r for r in receivers.values()), receivers

    # Los destinatarios de los comandos que invoca la saga (su `command_outbox`): ninguno es el orquestador.
    source = (ROOT / "src" / "eda_retail" / "orchestrator" / "saga.py").read_text()
    literal_targets = set(re.findall(r'\.enqueue\(\s*"\w+",\s*"(\w+)"', source))
    assert literal_targets == {"planner", "replanner"}
    step_targets = {saga.step_target(action.value) for action in Action}
    assert "orchestrator" not in literal_targets | step_targets
    # Sin URL de comandos del orquestador, `target_url` no puede enviarle ninguno.
    assert not hasattr(Settings(), "orchestrator_url")
    # Y el orquestador no expone API de comandos.
    package = ROOT / "src" / "eda_retail" / "orchestrator"
    sources = "\n".join(p.read_text() for p in package.glob("*.py"))
    assert "CommandSpec" not in sources and "build_app" not in sources


# --- cqrs-views: «Separación entre el modelo de escritura y el de lectura» ------------------------


class _RecordingConn:
    """Conexión falsa: registra cada SQL y responde sin filas."""

    def __init__(self) -> None:
        self.sql: list[str] = []

    @contextlib.contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=None):
        self.sql.append(" ".join(sql.split()))
        return self

    def fetchone(self):
        return None

    def fetchall(self):
        return []


def test_panel_reads_only_views():
    """Lectura del panel"""
    conn = _RecordingConn()
    app = web_app.create_app("postgresql://u:p@127.0.0.1:1/x")
    app.dependency_overrides[web_app._read_only_conn] = lambda: conn
    api = TestClient(app, base_url="http://localhost")

    replies = [api.get("/api/analytics/orders"), api.get("/api/analytics/orders", params={"incident_id": "I-1"}),
               api.get("/api/parked-notices")]

    assert [r.status_code for r in replies] == [200, 200, 200]
    tables = {t for sql in conn.sql for t in _TABLE.findall(sql)}
    assert {"views.view_order_status", "views.view_parked_notices"} <= tables
    # Los datos salen de `views.view_*`; `views.offsets` es la posición aplicada que cada respuesta debe
    # declarar (requirement «Consistencia eventual declarada»), no estado de escritura de ningún componente.
    data_tables = tables - {"views.offsets"}
    assert all(t.startswith("views.view_") for t in data_tables), tables
    assert not [sql for sql in conn.sql if _WRITE.search(sql)]


# --- event-backbone: «Propiedad de los datos» -----------------------------------------------------


class _MemoryDb:
    """`views.view_orders` en memoria; el resto de tablas, vacías. Registra cada SQL que se ejecuta."""

    def __init__(self) -> None:
        self.view_orders: dict[str, dict] = {}
        self.sql: list[str] = []

    def connect(self, _url: str | None = None) -> _MemoryConn:
        return _MemoryConn(self)


class _MemoryConn:
    def __init__(self, db: _MemoryDb) -> None:
        self._db = db
        self._rows: list[dict] = []
        self.rowcount = 0

    @contextlib.contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=()):
        sql = " ".join(sql.split())
        self._db.sql.append(sql)
        self._rows, self.rowcount = [], 1
        if sql.startswith("INSERT INTO views.view_orders"):
            order_id, customer_ref, amount = params
            self._db.view_orders.setdefault(order_id, {"order_id": order_id, "customer_ref": customer_ref,
                                                        "amount": Decimal(str(amount))})
        elif "FROM views.view_orders" in sql:
            self._rows = [self._db.view_orders[o] for o in params[0] if o in self._db.view_orders]
        return self

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self) -> None:
        pass


def _refund(amount: str) -> list[dict]:
    return [{"step_id": "S1", "action": "issue_refund", "params": {"order_id": "P-1002", "amount": amount},
             "depends_on": []}]


def test_planner_reads_the_order_amount_from_view_orders(monkeypatch):
    """Dato de otro componente"""
    db = _MemoryDb()
    registered = build_event("OrderRegistered", {"order_id": "P-1002", "customer_ref": "CUST-1002",
                                                 "amount": "40.00"},
                             correlation_id="sample-data", causation_id=uuid.uuid4(), producer="inventory")
    projector = PROJECTORS["orders"].build()
    assert projector.apply(db.connect(), Position("eda.events", 0, 0), registered)  # alimenta view_orders
    assert db.view_orders["P-1002"]["amount"] == Decimal("40.00")
    monkeypatch.setattr(planner, "connect", db.connect)
    db.sql.clear()

    context = planner.read_context(Settings(), RULES)(f"I-{uuid.uuid4().hex[:8]}", ["P-1002"])

    assert context.amounts == {"P-1002": Decimal("40.00")}
    rejected = planner.check_plan(_refund("45.00"), RULES, context.amounts)
    assert isinstance(rejected, str) and rejected.startswith("refund_limit")  # por encima del importe
    [step] = planner.check_plan(_refund("40.00"), RULES, context.amounts)
    assert step.action == "issue_refund"
    assert any("FROM views.view_orders" in sql for sql in db.sql)
    assert not [sql for sql in db.sql if "inventory." in sql]
