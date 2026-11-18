"""CLI de soporte técnico `eda-support` (spec support). Sin PostgreSQL ni Kafka: se sustituye el sistema por uno
falso que guarda lo que vería y lo que escribiría la herramienta."""

from __future__ import annotations

import json
from typing import Any

import pytest

from eda_retail.support import cli
from eda_retail.support.actions import FAILED, OK, PUBLISHERS, REFUSED


class FakeSystem:
    """Lo que vería `eda-support` de un sistema parado, con el topic intacto; `writes` guarda cada efecto."""

    def __init__(self) -> None:
        self.alive: dict[str, bool] = {"orchestrator": False, "projectors": False, "interpreter": False}
        self.members = {"interpreter": 0, "orchestrator": 0}
        self.marks: dict[int, tuple[int, int]] = {0: (0, 10), 1: (0, 5)}
        self.kafka_down = False
        self.outbox_rows = {schema: {"rows": 2, "pending": 0, "max_id": 2} for schema in PUBLISHERS}
        self.positions: dict[int, tuple[int, int]] = {0: (0, 9), 1: (0, 4)}
        self.dead = {"planner": {"rows": 0, "unpublished": 0}}
        self.failed = {"planner": 0}
        self.parked = 0
        self.read_rows: list[dict[str, Any]] = []
        self.outbox_ids = {"planner": [1, 2, 3, 4]}
        self.views = {"orders": {"views.view_orders": 7}}
        self.runs: list[dict[str, Any]] = []
        self.writes: list[tuple] = []

    # -- lectura
    def processes(self) -> dict[str, bool]:
        return dict(self.alive)

    def _kafka(self) -> None:
        if self.kafka_down:
            raise RuntimeError("Kafka no responde")

    def watermarks(self) -> dict[int, tuple[int, int]]:
        self._kafka()
        return dict(self.marks)

    def group_members(self) -> dict[str, int]:
        self._kafka()
        return dict(self.members)

    def group_lags(self, highs: dict[int, int]) -> dict[str, int]:
        self._kafka()
        return dict.fromkeys(self.members, 0)

    def outbox(self) -> dict[str, dict[str, Any]]:
        return {s: dict(v) for s, v in self.outbox_rows.items()}

    def outbox_positions(self) -> dict[int, tuple[int, int]]:
        return dict(self.positions)

    def dead_letters(self) -> dict[str, dict[str, int]]:
        return dict(self.dead)

    def failed_jobs(self) -> dict[str, int]:
        return dict(self.failed)

    def parked_notices(self) -> int:
        return self.parked

    def read_side_positions(self) -> list[dict[str, Any]]:
        return list(self.read_rows)

    def outbox_range(self, schema: str, from_id: int) -> dict[str, Any]:
        ids = [i for i in self.outbox_ids.get(schema, []) if i >= from_id]
        exists = from_id in self.outbox_ids.get(schema, [])
        return {"exists": exists, "rows": len(ids), "first_id": ids[0] if ids else None,
                "last_id": ids[-1] if ids else None}

    def view_rows(self, view: str) -> dict[str, int]:
        return dict(self.views.get(view, {}))

    # -- efectos
    def reset_for_recreated(self, partitions: list[int]) -> dict[str, Any]:
        self.writes.append(("reset", tuple(partitions)))
        return {"groups": ["interpreter", "orchestrator"], "positions": 3}

    def republish(self) -> dict[str, int]:
        self.writes.append(("republish",))
        return {s: v["rows"] for s, v in self.outbox_rows.items()}

    def replay_from(self, schema: str, outbox_id: int) -> int:
        self.writes.append(("replay_from", schema, outbox_id))
        return self.outbox_range(schema, outbox_id)["rows"]

    def rebuild(self, view: str) -> dict[str, Any]:
        self.writes.append(("rebuild", view))
        return {"view": view, "fingerprint_before": "a", "fingerprint_after": "a", "identical": True,
                "events_applied": 7}

    def start_run(self, action: str, args: dict, token: str, origin: str) -> int:
        self.runs.append({"action": action, "args": args, "token": token, "origin": origin, "outcome": None})
        return len(self.runs)

    def finish_run(self, run_id: int, outcome: str, summary: dict) -> None:
        self.runs[run_id - 1] |= {"outcome": outcome, "summary": summary}

    def close(self) -> None:
        pass


def _run(system: FakeSystem, *argv: str) -> int:
    return cli.run(list(argv), lambda: system)


def _json(capsys) -> dict:
    return json.loads(capsys.readouterr().out)


def _token(system: FakeSystem, capsys, *argv: str) -> str:
    assert _run(system, "--json", *argv) == OK
    return _json(capsys)["plan"]["token"]


def _lost(system: FakeSystem, mode: str = "truncated") -> FakeSystem:
    """Topic perdido: vaciado con `delete_records` (offsets siguen) o recreado (offsets en 0)."""
    system.marks = {0: (10, 10), 1: (5, 5)} if mode == "truncated" else {0: (0, 0), 1: (0, 0)}
    return system


# -- Diagnóstico ----------------------------------------------------------------------------------------------------
def test_status_of_a_healthy_system(capsys):
    """Diagnóstico de un sistema sano"""
    system = FakeSystem()
    assert _run(system, "--json", "status") == OK
    report = _json(capsys)
    assert {"processes", "topic", "groups", "read_side", "outbox", "dead_letters", "failed_jobs",
            "parked_notices", "alerts"} <= set(report)
    assert "topic_lost" not in report["alerts"]
    assert system.writes == [] and system.runs == []


def test_status_detects_a_lost_topic(capsys):
    """Topic perdido"""
    for mode in ("truncated", "recreated"):
        assert _run(_lost(FakeSystem(), mode), "--json", "status") == OK
        assert "topic_lost" in _json(capsys)["alerts"]


def test_status_when_kafka_does_not_answer(capsys):
    """Kafka no responde"""
    system = FakeSystem()
    system.kafka_down = True
    system.failed = {"planner": 2}
    assert _run(system, "--json", "status") == OK
    report = _json(capsys)
    assert "kafka_down" in report["alerts"] and "jobs_failed" in report["alerts"]
    assert report["outbox"] and report["failed_jobs"] == {"planner": 2} and report["parked_notices"] == 0
    assert report["topic"] is None


def test_status_lists_the_other_alerts(capsys):
    """Diagnóstico: cuentas que no son 0 dan su alerta"""
    system = FakeSystem()
    system.outbox_rows["planner"]["pending"] = 1
    system.dead = {"views": {"rows": 1, "unpublished": 0}}
    system.parked = 3
    assert _run(system, "--json", "status") == OK
    assert {"outbox_pending", "dead_letters", "parked_notices"} <= set(_json(capsys)["alerts"])


def test_status_as_text_names_each_alert(capsys):
    """Diagnóstico en texto: dice cada alerta"""
    assert _run(_lost(FakeSystem()), "status") == OK
    assert "topic_lost" in capsys.readouterr().out


# -- Simulación y confirmación --------------------------------------------------------------------------------------
def test_a_dry_run_writes_nothing(capsys):
    """Simulación sin efectos"""
    system = _lost(FakeSystem())
    assert _run(system, "--json", "republish", "--mode", "truncated") == OK
    plan = _json(capsys)["plan"]
    assert plan["token"] and plan["refusal"] is None
    assert plan["changes"]["total"] == 2 * len(PUBLISHERS)
    assert system.writes == [] and system.runs == []


def test_a_dry_run_as_text_gives_the_command_to_confirm(capsys):
    """Simulación en texto: da el token y el comando para ejecutarla"""
    system = _lost(FakeSystem())
    assert _run(system, "replay-from", "planner", "2") == OK
    out = capsys.readouterr().out
    token = _token(system, capsys, "replay-from", "planner", "2")
    assert f"eda-support replay-from planner 2 --confirm {token}" in out


def test_confirm_with_the_token_of_the_dry_run(capsys):
    """Confirmación con el token de la simulación"""
    system = _lost(FakeSystem())
    token = _token(system, capsys, "republish", "--mode", "truncated")
    assert _run(system, "--json", "republish", "--mode", "truncated", "--confirm", token) == OK
    assert system.writes == [("republish",)]
    assert _json(capsys)["outcome"] == "succeeded"


def test_a_token_is_stale_once_the_state_changes(capsys):
    """Token caducado"""
    system = _lost(FakeSystem())
    token = _token(system, capsys, "republish", "--mode", "truncated")
    system.outbox_rows["planner"] = {"rows": 3, "pending": 1, "max_id": 3}  # una fila nueva en el outbox
    assert _run(system, "republish", "--mode", "truncated", "--confirm", token) == REFUSED
    assert "el estado cambió desde la simulación" in capsys.readouterr().err
    assert system.writes == []
    assert system.runs[-1]["outcome"] == "refused"


def test_an_unmet_precondition_refuses_the_dry_run_and_the_execution(capsys):
    """Condición previa sin cumplir"""
    system = FakeSystem()  # topic con mensajes
    assert _run(system, "republish", "--mode", "truncated") == REFUSED
    assert "el topic tiene mensajes" in capsys.readouterr().err
    assert _run(system, "republish", "--mode", "truncated", "--confirm", "x") == REFUSED
    assert system.writes == []


def test_a_failed_execution_exits_with_1(capsys):
    """Una ejecución que falla termina con 1 y queda como `failed`"""
    system = _lost(FakeSystem())
    token = _token(system, capsys, "republish", "--mode", "truncated")

    def broken() -> dict:
        raise RuntimeError("republicación no confirmada")

    system.republish = broken
    assert _run(system, "republish", "--mode", "truncated", "--confirm", token) == FAILED
    assert system.runs[-1]["outcome"] == "failed"
    assert "republicación no confirmada" in system.runs[-1]["summary"]["error"]


# -- Sistema parado -------------------------------------------------------------------------------------------------
def test_a_live_process_means_the_system_is_not_stopped(capsys):
    """Un proceso sigue vivo"""
    system = _lost(FakeSystem())
    system.alive["orchestrator"] = True
    assert _run(system, "republish", "--mode", "truncated") == REFUSED
    assert "orchestrator" in capsys.readouterr().err


def test_a_group_with_members_means_the_system_is_not_stopped(capsys):
    """Un grupo de Kafka tiene miembros"""
    system = _lost(FakeSystem())
    system.members["interpreter"] = 1
    assert _run(system, "republish", "--mode", "truncated") == REFUSED
    assert "interpreter" in capsys.readouterr().err


# -- Republicación tras perder el topic -----------------------------------------------------------------------------
def test_republish_a_truncated_topic(capsys):
    """Republicación de un topic vaciado"""
    system = _lost(FakeSystem(), "truncated")
    token = _token(system, capsys, "republish", "--mode", "truncated")
    assert _run(system, "--json", "republish", "--mode", "truncated", "--confirm", token) == OK
    result = _json(capsys)["result"]
    assert result["republished"] == {s: 2 for s in PUBLISHERS} and result["reset"] is None
    assert system.writes == [("republish",)]


def test_republish_a_recreated_topic_resets_groups_and_positions_first(capsys):
    """Republicación de un topic recreado"""
    system = _lost(FakeSystem(), "recreated")
    token = _token(system, capsys, "republish", "--mode", "recreated")
    assert _run(system, "--json", "republish", "--mode", "recreated", "--confirm", token) == OK
    assert system.writes == [("reset", (0, 1)), ("republish",)]
    assert _json(capsys)["result"]["reset"]["groups"] == ["interpreter", "orchestrator"]


def test_republish_refuses_a_topic_with_messages(capsys):
    """Topic que no está vacío"""
    for mode in ("truncated", "recreated"):
        assert _run(FakeSystem(), "republish", "--mode", mode) == REFUSED
        assert "duplicaría" in capsys.readouterr().err


def test_republish_refuses_a_mode_that_does_not_match_the_topic(capsys):
    """Modo que no casa con el topic"""
    assert _run(_lost(FakeSystem(), "truncated"), "republish", "--mode", "recreated") == REFUSED
    assert "parece vaciado" in capsys.readouterr().err
    assert _run(_lost(FakeSystem(), "recreated"), "republish", "--mode", "truncated") == REFUSED
    assert "parece recreado" in capsys.readouterr().err


def test_republish_refuses_a_topic_that_does_not_exist(capsys):
    """Republicación sin topic: se niega en vez de dejar que Kafka lo cree"""
    system = FakeSystem()
    system.marks = {}
    assert _run(system, "republish", "--mode", "recreated") == REFUSED
    assert "no existe" in capsys.readouterr().err


def test_republish_refuses_when_kafka_does_not_answer(capsys):
    """Sin Kafka no se puede comprobar que el sistema esté parado"""
    system = _lost(FakeSystem())
    system.kafka_down = True
    assert _run(system, "republish", "--mode", "truncated") == REFUSED
    assert "Kafka no responde" in capsys.readouterr().err


# -- Republicación desde una fila -----------------------------------------------------------------------------------
def test_replay_from_marks_the_rows_from_outbox_id(capsys):
    """Filas marcadas para publicarse otra vez"""
    system = FakeSystem()
    assert _run(system, "--json", "replay-from", "planner", "2") == OK
    plan = _json(capsys)["plan"]
    assert plan["changes"] == {"rows": 3, "first_id": 2, "last_id": 4}
    assert _run(system, "--json", "replay-from", "planner", "2", "--confirm", plan["token"]) == OK
    assert system.writes == [("replay_from", "planner", 2)] and _json(capsys)["result"] == {"rows": 3}


def test_replay_from_refuses_an_unknown_component_or_row(capsys):
    """Componente o fila desconocidos"""
    assert _run(FakeSystem(), "replay-from", "views", "1") == REFUSED
    assert "no tiene outbox" in capsys.readouterr().err
    assert _run(FakeSystem(), "replay-from", "planner", "99") == REFUSED
    assert "99" in capsys.readouterr().err


# -- Reconstrucción de una vista ------------------------------------------------------------------------------------
def test_rebuild_with_the_projectors_stopped(capsys):
    """Reconstrucción con los proyectores parados"""
    system = FakeSystem()
    token = _token(system, capsys, "rebuild", "orders")
    assert _run(system, "--json", "rebuild", "orders", "--confirm", token) == OK
    result = _json(capsys)["result"]
    assert result["fingerprint_before"] == result["fingerprint_after"] and result["identical"] is True
    assert system.writes == [("rebuild", "orders")]


def test_rebuild_refuses_while_the_projectors_run(capsys):
    """Proyectores en marcha"""
    system = FakeSystem()
    system.alive["projectors"] = True
    assert _run(system, "rebuild", "orders") == REFUSED
    assert "eda-projector" in capsys.readouterr().err


def test_rebuild_takes_a_known_view():
    """eda-support rebuild solo acepta una de las vistas"""
    with pytest.raises(SystemExit):
        cli.parse_args(["rebuild", "view_orders"])


# -- Registro de las ejecuciones ------------------------------------------------------------------------------------
def test_an_execution_is_recorded(capsys):
    """Ejecución registrada"""
    system = FakeSystem()
    token = _token(system, capsys, "replay-from", "planner", "3")
    assert _run(system, "replay-from", "planner", "3", "--confirm", token, "--origin", "web") == OK
    [run] = system.runs
    assert run == {"action": "replay-from", "args": {"component": "planner", "outbox_id": 3}, "token": token,
                   "origin": "web", "outcome": "succeeded", "summary": {"rows": 2}}


def test_a_dry_run_is_not_recorded(capsys):
    """Simulación sin registro"""
    system = FakeSystem()
    for argv in (("replay-from", "planner", "1"), ("rebuild", "orders"), ("status",)):
        assert _run(system, *argv) == OK
    assert system.runs == []


def test_a_database_that_does_not_answer_stops_the_action(capsys):
    """Si PostgreSQL no responde, la acción no se ejecuta"""
    system = FakeSystem()

    def down(*_a, **_k):
        raise OSError("connection refused")

    system.outbox_range = down
    system.start_run = down
    assert _run(system, "replay-from", "planner", "1", "--confirm", "x") == FAILED
    assert "PostgreSQL" in capsys.readouterr().err and system.writes == []


def test_status_of_a_deleted_topic_is_a_lost_topic_not_kafka_down(capsys):
    """Topic perdido: borrado y aún sin crear, sin pedir el lag de los grupos sobre ninguna partición"""
    system = FakeSystem()
    system.marks = {}

    def no_partitions(highs: dict[int, int]) -> dict[str, int]:
        assert highs, "list_consumer_group_offsets sin particiones"
        return {}

    system.group_lags = no_partitions
    assert _run(system, "--json", "status") == OK
    alerts = _json(capsys)["alerts"]
    assert "topic_lost" in alerts and "kafka_down" not in alerts
