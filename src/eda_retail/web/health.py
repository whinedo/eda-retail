"""Salud de la infraestructura y de los procesos (spec web-front, "Mapa de sistemas").

Cada comprobación devuelve True o False y tiene un tiempo máximo: un servicio caído o lento cuenta
como down sin retrasar al resto. Los procesos se buscan con `pgrep -f`, como scripts/notice.sh.
"""

from __future__ import annotations

import subprocess
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout

import psycopg
from confluent_kafka.admin import AdminClient

from eda_retail.web.topology import MONITORED, Node

CHECK_TIMEOUT = 2.0


def check_postgres(database_url: str, timeout: float = CHECK_TIMEOUT) -> bool:
    try:
        with psycopg.connect(
            database_url, connect_timeout=max(1, int(timeout)), options="-c default_transaction_read_only=on"
        ) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:  # noqa: BLE001 - cualquier fallo es "down"
        return False


def check_kafka(bootstrap_servers: str, timeout: float = CHECK_TIMEOUT) -> bool:
    try:
        AdminClient({"bootstrap.servers": bootstrap_servers, "socket.timeout.ms": int(timeout * 1000)}).list_topics(
            timeout=timeout
        )
        return True
    except Exception:  # noqa: BLE001
        return False


def check_ollama(host: str, timeout: float = CHECK_TIMEOUT) -> bool:
    try:
        with urllib.request.urlopen(f"{host.rstrip('/')}/api/tags", timeout=timeout) as response:
            return response.status == 200
    except Exception:  # noqa: BLE001
        return False


def pgrep(pattern: str) -> bool:
    try:
        result = subprocess.run(["pgrep", "-f", pattern], check=False, capture_output=True, timeout=CHECK_TIMEOUT)
        return result.returncode == 0
    except Exception:  # noqa: BLE001
        return False


class HealthChecker:
    def __init__(
        self,
        checks: dict[str, Callable[[], bool]],
        *,
        pgrep: Callable[[str], bool] = pgrep,
        timeout: float = CHECK_TIMEOUT,
        monitored: tuple[Node, ...] = MONITORED,
    ) -> None:
        """`monitored`: los procesos que se vigilan; con POC_VERSION=2, `topology.monitored(2)`."""
        self._checks = checks
        self._monitored = monitored
        self._pgrep = pgrep
        self._timeout = timeout
        self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="health")

    def process_up(self, node: Node) -> bool | None:
        return self._safe(lambda: self._pgrep(node.process)) if node.process else None

    def report(self) -> dict:
        jobs = {f"infra:{name}": check for name, check in self._checks.items()}
        jobs |= {f"proc:{n.id}": (lambda n=n: self._pgrep(n.process)) for n in self._monitored}
        futures = {key: self._pool.submit(job) for key, job in jobs.items()}
        results = {key: self._result(future) for key, future in futures.items()}
        processes = {n.id: results[f"proc:{n.id}"] for n in self._monitored}
        return {
            "infra": {name: "healthy" if results[f"infra:{name}"] else "down" for name in self._checks},
            "processes": processes,
            "processes_up": sum(processes.values()),
            "processes_total": len(self._monitored),
        }

    def _safe(self, job: Callable[[], bool]) -> bool:
        return self._result(self._pool.submit(job))

    def _result(self, future) -> bool:
        try:
            return bool(future.result(timeout=self._timeout))
        except (FutureTimeout, Exception):  # noqa: BLE001 - lento o roto cuenta como down
            return False
