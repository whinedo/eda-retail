# E14 Ráfaga y contrapresión

Objetivo: codificar E14 (H14). Rama `feat/e14-burst-backpressure` desde `main`. Entrega: un PR. Tests: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; la integración corre en CI (shard slow-a).

## Specs

- S1: specs/experiments/spec.md, «E14 Ráfaga y contrapresión (H14)» (escenarios: Ejecutor saturado, Las colas se vacían, Sin comandos duplicados, Cumplimiento de H14).
- S2: specs/synthetic-data/spec.md, «Capacidad limitada de un ejecutor y trabajo lento» (escenarios: Ejecutor lleno, Reentrega con el ejecutor lleno).
- S3: guía de experimentos con la sección de E14 y `eda-expexecutor run e14` con su catálogo.
- S4: el test slow de E14 en slow-a; `test_e17_parked.py` pasa de slow-b a slow-a para no superar el timeout de 20 min.

## Tasks

- T1 (S1–S4), inline: spec, `FaultPlan.executor_capacity` y `prepare_delay_seconds`, `InProcessCommands` (503) y `slowed` en el harness, `e14_burst_backpressure.py`, CLI, catálogo, guía, tests unitarios e integración, CI.

## Log

- L1 (2026-11-18): «Sigue con E14, E15, E19 y E20 (tabla en docs/guia-experimentos-eda-retail.md).» E14 no tenía spec; el usuario aprobó el diseño propuesto («Sí, adelante»): 200 avisos, 503 por capacidad de los ejecutores, `prepare` lento del planner, H14 = concurrencia ≤ límite, colas a 0, sin comandos perdidos ni duplicados, I1 a I5.
- L2: la capacidad se cuenta en trabajos abiertos del `command_jobs`: en proceso el dispatcher entrega un comando cada vez y un `ExecuteStep` solo se acepta. El `prepare` de los ejecutores pasa de 0,02 s a 0,1 s para que la cola de cada ejecutor crezca de verdad.
- L3: duraciones de CI previas: slow-a ~6 min, slow-b ~16 min (E17 154 s en el setup). Se mueven E17 y E14 a slow-a.
