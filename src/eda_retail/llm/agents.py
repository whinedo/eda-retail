"""Registro de los `AgentSpec` de los agentes con LLM (spec agent-harness, «Contrato declarado por agente»).

`FINGERPRINTS` guarda, por agente, la `prompt_version` y la huella del prompt con la que se fijó. Si cambia el
texto del prompt de sistema o de una plantilla, cambia la huella: hay que subir la `prompt_version` del agente y
fijar aquí la nueva pareja (`stale_fingerprints` lo comprueba).
"""

from __future__ import annotations

from typing import Any

from eda_retail.llm.harness import AgentSpec
from eda_retail.llm.prompts import interpreter, planner, replanner, writer

AGENTS: dict[str, AgentSpec[Any]] = {s.agent: s for s in (interpreter.SPEC, planner.SPEC, replanner.SPEC, writer.SPEC)}

FINGERPRINTS: dict[str, tuple[str, str]] = {
    "interpreter": ("1", "27e99b3b1b09e36a2bae43208fd500910189436ac0302de9d94922b974f3175a"),
    "planner": ("2", "1e880ec52e9dd2fb7e83f8abea535bb68d6346852e3c89f4d2876363e32f8b3f"),
    "replanner": ("1", "617976d1a7e934da445f78b86978bfc8077aa6ddb00cc52783df9a999f109bfb"),
    "writer": ("1", "a2e8c43839c9c0776d453e1153e048167030c4912489505eab479fcfb15d5981"),
}


def stale_fingerprints(agents: dict[str, AgentSpec[Any]] = AGENTS,
                       pinned: dict[str, tuple[str, str]] = FINGERPRINTS) -> list[str]:
    """Un mensaje por agente cuya pareja (versión, huella) no coincide con la fijada."""
    problems = []
    for name, spec in agents.items():
        expected = pinned.get(name)
        if expected is None:
            problems.append(f"{name}: sin huella fijada (versión {spec.prompt_version}, huella {spec.prompt_hash})")
        elif expected[0] == spec.prompt_version and expected[1] != spec.prompt_hash:
            problems.append(f"{name}: el prompt cambió y la prompt_version sigue en {spec.prompt_version}; "
                            "sube la versión y fija la huella nueva")
        elif expected != (spec.prompt_version, spec.prompt_hash):
            problems.append(f"{name}: fija la huella nueva (versión {spec.prompt_version}, huella {spec.prompt_hash})")
    return problems
