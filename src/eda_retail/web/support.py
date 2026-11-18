"""Soporte técnico desde el front (spec web-front, «Soporte técnico»): lista blanca de `eda-support` (spec support).

`SupportRequest` solo admite las cuatro acciones con sus argumentos; cualquier otra cosa es 422 y no lanza ningún
proceso. El backend las ejecuta con `scripts/support.sh`, siempre con `--json` y, al confirmar, con `--origin web`, a
través del mismo `CommandRunner` que el menú de comandos: una sola ejecución a la vez y el mismo tiempo máximo.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from eda_retail.projectors.registry import PROJECTORS
from eda_retail.support.actions import MODES, PUBLISHERS

SUPPORT_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "support.sh"
SUPPORT_LABEL = "scripts/support.sh"
TOKEN_PATTERN = r"^[0-9a-f]{16}$"

# Argumentos de cada acción: los demás campos deben venir vacíos.
_FIELDS = {"status": set(), "republish": {"mode"}, "replay-from": {"component", "outbox_id"}, "rebuild": {"view"}}


class SupportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["status", "republish", "replay-from", "rebuild"]
    mode: Literal[MODES] | None = None  # type: ignore[valid-type]
    component: str | None = None
    outbox_id: int | None = Field(default=None, ge=1)
    view: str | None = None
    confirm: str | None = Field(default=None, pattern=TOKEN_PATTERN)

    @model_validator(mode="after")
    def _whitelist(self) -> SupportRequest:
        given = {name for name in ("mode", "component", "outbox_id", "view") if getattr(self, name) is not None}
        wanted = _FIELDS[self.action]
        if given != wanted:
            raise ValueError(f"{self.action} lleva exactamente {sorted(wanted) or 'ningún argumento'}")
        if self.action == "status" and self.confirm is not None:
            raise ValueError("status no escribe: no lleva confirm")
        if self.component is not None and self.component not in PUBLISHERS:
            raise ValueError(f"componente sin outbox; los admitidos son {list(PUBLISHERS)}")
        if self.view is not None and self.view not in PROJECTORS:
            raise ValueError(f"vista desconocida; las admitidas son {sorted(PROJECTORS)}")
        return self

    def args(self) -> list[str]:
        """Argumentos de scripts/support.sh."""
        args = ["--json", self.action]
        if self.action == "republish":
            args += ["--mode", str(self.mode)]
        elif self.action == "replay-from":
            args += [str(self.component), str(self.outbox_id)]
        elif self.action == "rebuild":
            args += [str(self.view)]
        if self.confirm is not None:
            args += ["--confirm", self.confirm, "--origin", "web"]
        return args
