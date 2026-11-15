"""Ejecución de los comandos de `scripts/notice.sh` desde el front (spec web-front, "Menú de comandos").

- Lista blanca: `CommandRequest` solo admite `seed`, `send` (ejemplo de `EXAMPLES` o texto libre) e
  `interactive --timeout N`; cualquier otra cosa es 422 y no lanza ningún proceso.
- Sin shell: el proceso recibe una lista de argumentos; el texto libre va en un único `--text=...`.
- Una ejecución a la vez, con tiempo máximo: al cumplirse se mata el grupo de procesos entero.
- `interactive` corre en una pseudoterminal para poder contestar a su menú; el resto, con tuberías.
"""

from __future__ import annotations

import codecs
import json
import logging
import os
import pty
import select
import signal
import subprocess
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from eda_retail.synthetic.sample_data import EXAMPLES

NOTICE_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "notice.sh"
SCRIPT_LABEL = "scripts/notice.sh"
RUN_MAX_SECONDS = 15 * 60
DEFAULT_INTERACTIVE_TIMEOUT = 180
MAX_TEXT = 2000
MAX_LINE = 200
_KEEPALIVE_SECONDS = 15
_POLL_SECONDS = 1.0
_KILL_ATTEMPTS = 20
_KILL_RETRY_SECONDS = 0.05
# Alias propios para que los tests puedan sustituirlos sin tocar `os` en todo el proceso.
_read = os.read
_write = os.write
log = logging.getLogger("eda_retail.web.commands")


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: Literal["seed", "send", "interactive"]
    example: str | None = None
    text: str | None = Field(default=None, min_length=1, max_length=MAX_TEXT)
    timeout: int | None = Field(default=None, ge=1, le=600)

    @model_validator(mode="after")
    def _whitelist(self) -> CommandRequest:
        if self.command != "send" and (self.example is not None or self.text is not None):
            raise ValueError("example y text solo valen para send")
        if self.command != "interactive" and self.timeout is not None:
            raise ValueError("timeout solo vale para interactive")
        if self.example is not None and self.text is not None:
            raise ValueError("send admite example o text, no los dos")
        if self.example is not None and self.example not in EXAMPLES:
            raise ValueError(f"ejemplo no previsto; los admitidos son {sorted(EXAMPLES)}")
        if self.text is not None and "\x00" in self.text:
            raise ValueError("el texto no puede contener el carácter nulo")
        return self

    def args(self, free_text: bool = True) -> list[str]:
        """Argumentos de scripts/notice.sh. Sin texto libre, `interactive` lleva `--no-free-text`."""
        if self.command == "seed":
            return ["seed"]
        if self.command == "interactive":
            args = ["interactive", "--timeout", str(self.timeout or DEFAULT_INTERACTIVE_TIMEOUT)]
            return args if free_text else [*args, "--no-free-text"]
        if self.example is not None:
            return ["send", "--example", self.example]
        if self.text is not None:
            return ["send", f"--text={self.text}"]  # un único argumento, aunque empiece por guion
        return ["send"]


class InputLine(BaseModel):
    model_config = ConfigDict(extra="forbid")

    line: str = Field(max_length=MAX_LINE)

    @field_validator("line")
    @classmethod
    def _single_line(cls, value: str) -> str:
        if any(c in value for c in "\r\n\x00"):
            raise ValueError("una sola línea, sin saltos de línea")
        return value


class RunnerBusy(Exception):
    """Ya hay un comando en ejecución."""


class RunNotFound(Exception):
    """No existe una ejecución con ese `run_id`."""


class RunNotAcceptingInput(Exception):
    """La ejecución no es interactiva o ya terminó."""


class RunInputFull(Exception):
    """La entrada del proceso está llena: no la está leyendo."""


class RunLaunchError(Exception):
    """No se pudo lanzar scripts/notice.sh."""


@dataclass
class Run:
    run_id: str
    interactive: bool
    process: subprocess.Popen
    out_fd: int
    chunks: list[str] = field(default_factory=list)
    exit_code: int | None = None
    cond: threading.Condition = field(default_factory=threading.Condition)
    # Protege la escritura en la pseudoterminal y su cierre; es distinto de `cond` para que una
    # escritura nunca frene la lectura de la salida.
    input_lock: threading.Lock = field(default_factory=threading.Lock)
    fd_closed: bool = False


class CommandRunner:
    def __init__(self, script: Path = NOTICE_SCRIPT, max_seconds: float = RUN_MAX_SECONDS) -> None:
        self._script = script
        self._max_seconds = max_seconds
        self._lock = threading.Lock()
        self._runs: dict[str, Run] = {}  # solo la última ejecución
        self._current: Run | None = None

    def start(self, request: CommandRequest, free_text: bool = True) -> Run:
        with self._lock:
            if self._current is not None and self._current.exit_code is None:
                raise RunnerBusy
            argv = [str(self._script), *request.args(free_text)]
            interactive = request.command == "interactive"
            if interactive:
                master, slave = pty.openpty()
                try:
                    process = subprocess.Popen(
                        argv, stdin=slave, stdout=slave, stderr=slave, start_new_session=True, close_fds=True
                    )
                except OSError as error:
                    os.close(master)
                    raise RunLaunchError(error.strerror or str(error)) from error
                finally:
                    os.close(slave)
                os.set_blocking(master, False)  # escribir nunca bloquea; la lectura usa select
                out_fd = master
            else:
                try:
                    process = subprocess.Popen(
                        argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        start_new_session=True, close_fds=True,
                    )
                except OSError as error:
                    raise RunLaunchError(error.strerror or str(error)) from error
                out_fd = process.stdout.fileno()  # type: ignore[union-attr]
            run = Run(run_id=uuid.uuid4().hex, interactive=interactive, process=process, out_fd=out_fd)
            self._runs = {run.run_id: run}
            self._current = run
        timer = threading.Timer(self._max_seconds, _kill_group, args=(process,))
        timer.daemon = True
        timer.start()
        threading.Thread(target=self._pump, args=(run, timer), daemon=True).start()
        return run

    def get(self, run_id: str) -> Run:
        try:
            return self._runs[run_id]
        except KeyError:
            raise RunNotFound(run_id) from None

    def send_line(self, run_id: str, line: str) -> None:
        run = self.get(run_id)
        if not run.interactive:
            raise RunNotAcceptingInput(run_id)
        data = (line + "\n").encode()
        with run.input_lock:  # no `run.cond`: la salida sigue fluyendo mientras se escribe
            if run.fd_closed or run.exit_code is not None:
                raise RunNotAcceptingInput(run_id)
            try:
                written = _write(run.out_fd, data)
            except BlockingIOError:
                raise RunInputFull(run_id) from None
            except OSError:
                raise RunNotAcceptingInput(run_id) from None
        if written < len(data):
            raise RunInputFull(run_id)

    def shutdown(self) -> None:
        """Termina el comando en curso y su grupo (parada del backend)."""
        run = self._current
        if run is not None and run.exit_code is None:
            _kill_group(run.process)

    def _pump(self, run: Run, timer: threading.Timer) -> None:
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        code: int | None = None
        try:
            while True:
                ready, _, _ = select.select([run.out_fd], [], [], _POLL_SECONDS)
                if not ready:
                    continue
                try:
                    data = _read(run.out_fd, 4096)
                except BlockingIOError:
                    continue
                except OSError:  # la pseudoterminal devuelve EIO cuando el proceso cierra su lado
                    data = b""
                text = decoder.decode(data, final=not data)
                if text:
                    with run.cond:
                        run.chunks.append(text)
                        run.cond.notify_all()
                if not data:
                    break
            code = run.process.wait()
        except Exception:
            # Sin seguimiento de la salida no hay forma de cerrar la ejecución: se termina el grupo.
            log.exception("fallo siguiendo la salida de %s", run.run_id)
            _kill_group(run.process)
            code = run.process.wait()
        finally:
            timer.cancel()
            with run.input_lock:
                try:
                    if run.interactive:
                        os.close(run.out_fd)
                    else:
                        run.process.stdout.close()  # type: ignore[union-attr]
                except OSError:
                    pass
                run.fd_closed = True
            with run.cond:
                run.exit_code = code if code is not None else -1
                run.cond.notify_all()


def _kill_group(process: subprocess.Popen) -> None:
    """Mata el grupo del proceso. Justo tras lanzarlo, el hijo puede no haber creado aún su sesión
    (el grupo no existe: ESRCH, o macOS responde EPERM): se reintenta un momento y, si el proceso
    sigue vivo, se mata al menos a él."""
    for _ in range(_KILL_ATTEMPTS):
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGKILL)
            return
        except (ProcessLookupError, PermissionError):
            time.sleep(_KILL_RETRY_SECONDS)
    try:
        process.kill()
    except ProcessLookupError:
        pass


def output_stream(run: Run, last_event_id: str | None = None) -> Iterator[str]:
    """Tramas SSE con la salida (`id` = índice del trozo) y, al final, el código de salida."""
    index = int(last_event_id) + 1 if last_event_id and last_event_id.isascii() and last_event_id.isdigit() else 0
    while True:
        with run.cond:
            while index >= len(run.chunks) and run.exit_code is None:
                if not run.cond.wait(timeout=_KEEPALIVE_SECONDS):
                    break
            new = run.chunks[index:]
            code = run.exit_code
        if not new and code is None:
            yield ": keep-alive\n\n"
            continue
        for text in new:
            yield f"id: {index}\nevent: output\ndata: {json.dumps({'text': text})}\n\n"
            index += 1
        if code is not None:
            yield f"id: {index}\nevent: exit\ndata: {json.dumps({'code': code})}\n\n"
            return
