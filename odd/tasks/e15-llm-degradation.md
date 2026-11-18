# E15 Degradación del LLM

Objetivo: codificar E15 (H15). Rama `feat/e15-llm-degradation`, apilada sobre `feat/e14-burst-backpressure` (comparten la CLI, el catálogo, el CI y las specs). Entrega: un PR. Tests: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; la integración corre en CI (shard slow-a).

## Specs

- S1: specs/experiments/spec.md, «E15 Degradación del LLM (H15)» (escenarios: Salidas inválidas del LLM, LLM lento y plazo del plan, Errores del LLM y trabajos reintentados, Cumplimiento de H15).
- S2: specs/synthetic-data/spec.md, «Degradación del LLM» (escenarios: Elección reproducible, Agente no degradado).
- S3: guía de experimentos con la sección de E15 y `eda-expexecutor run e15` con su catálogo.
- S4: el test slow de E15 en slow-a.

## Tasks

- T1 (S1–S4), inline: `synthetic/llm_faults.DegradedLlm`, `experiments/e15_llm_degradation.py`, CLI, catálogo, guía, tests unitarios e integración, CI.

## Log

- L1 (2026-11-18): «Sigue con E14, E15, E19 y E20 (tabla en docs/guia-experimentos-eda-retail.md).» E15 no tenía spec; el usuario aprobó el diseño propuesto («Sí, adelante»): variantes `baseline`, `invalid`, `slow` y `error`; H15 = ninguna salida inválida se convierte en plan, toda incidencia se cierra, ningún paso de un plan escalado o tardío, I1 a I5.
- L2: solo se degradan `planner` y `replanner`: el intérprete y el redactor de avisos quedan fuera para aislar el efecto en los planes. Un 30 % de fallos en `reissue_shipment` hace trabajar al replanner. `invalid` sube al 60 % para que haya escaladas por `llm_output_invalid` (con el 30 % y 3 generaciones, apenas un 2,7 % de las incidencias).
- L3: el test de integración usa la semilla 15, 15 incidencias y un plazo de plan de 1,5 s. Calculado sin base de datos con `generate` y `should_fail`: 13 llaman al planner, 4 agotan sus generaciones en `invalid` y una falla en su primera llamada en `error`.
- L4: el verificador independiente encontró un blocker: con la semilla 42, el replanner de I-876a9512 falla 6 veces seguidas en `error`; el trabajo agota `JOB_MAX_ATTEMPTS` sin eventos y, con el plazo de plan por defecto (300 s), la incidencia quedaba abierta al parar el harness (15 s sin avance). Corrección: `error` usa también el plazo de 4 s; la spec y la guía lo dicen.
