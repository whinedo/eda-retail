"""Parámetros de ejecución de los experimentos para `eda-expexecutor run` (spec expexecutor).

Una entrada por experimento con los parámetros de `eda-experiment`: `seed`, `incidents`, `llm`,
`repetitions`, `threshold` y `temperature`. Un parámetro de la línea de comandos gana al de este
fichero, y el de este fichero, al valor por defecto de `eda-experiment`. Un experimento sin entrada
se ejecuta con los valores por defecto. `llm`, `repetitions` y `temperature` solo se usan en E5 y E6;
`threshold`, solo en E5. Otro fichero: `eda-expexecutor run <exp> --config <fichero>`.
"""

EXPERIMENTS = {
    # E5 Calidad del plan: LLM real, tres repeticiones como mínimo (spec) y umbral inicial de H5.
    "e5": {"llm": "ollama", "repetitions": 3, "threshold": 0.8, "temperature": 0.7},
    # E6 Eventos frente a síncrono: LLM real y tres repeticiones como mínimo (spec).
    "e6": {"llm": "ollama", "repetitions": 3, "temperature": 0.7},
}
