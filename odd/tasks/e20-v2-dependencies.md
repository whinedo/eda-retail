# E20 Dependencias de V2

Objetivo: codificar E20 (H20): servidor MCP de correo y agente A2A del transportista inestables. Rama `feat/e20-v2-dependencies`, apilada sobre `feat/e19-topic-loss` (comparten la CLI, el catálogo, el CI y las specs). Entrega: un PR. Tests: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"`; la integración corre en CI (shard slow).

## Specs

- S1: specs/experiments/spec.md, «E20 Dependencias de V2 (H20)»: en proceso, `POC_VERSION=2`, 50 incidencias, semilla fija, un fallo por variante: `baseline`, `mail_down`, `mail_invalid`, `carrier_slow`, `carrier_401`, `carrier_bad_card`. Escenarios: Servidor de correo caído, Salidas inválidas del servidor de correo, Agente del transportista inestable, Cumplimiento de H20.
- S2: specs/synthetic-data/spec.md, «Fallos de las dependencias de V2» (escenarios: Elección reproducible, Servidor de correo inestable, Agente del transportista inestable).
- S3: métricas por variante: `CarrierConsulted` por `outcome`, latencia de la consulta, tareas que recibe el agente, `StepFailed` por `error`, intentos de `send_email` y correos por `idempotency_key`, incidencias abiertas, I1 a I5.
- S4: H20: en todas las variantes, 0 incidencias abiertas, I1 a I5 sin violaciones, toda consulta fallida termina en el mismo plan que `baseline`, el `outcome` coincide con el fallo inyectado en el 100 % de las consultas, `carrier_bad_card` (y `carrier_401`) sin tareas recibidas, ninguna `idempotency_key` con más de un correo, ninguna dirección de correo en el log; cada variante provoca su efecto.
- S5: el 401 se mide como `outcome` `failed`: la spec `a2a-carrier` no cambia (decisión del usuario al aprobar el diseño).
- S6: guía de experimentos con la sección de E20, `eda-expexecutor run e20` con su catálogo y el test slow repartido entre slow-a y slow-b según la duración de los shards de #90.

## Tasks

- T1 (S2), inline: `synthetic/v2_faults.py` (servidor MCP y agente A2A con fallos) y el parámetro del harness para el servidor de correo, con tests unitarios.
- T2 (S1, S3–S6), inline: `experiments/e20_v2_dependencies.py`, CLI, catálogo, guía, specs, tests unitarios e integración, CI.

## Log

- L1 (2026-11-18): «E20 (Dependencias de V2: MCP de correo y agente A2A inestables; tabla en docs/guia-experimentos-eda-retail.md). No tiene spec: antes de programar, propónme un diseño (escenario, variantes, métricas, criterio H20) y espera mi OK.» El usuario aprobó el diseño propuesto: «Sí, adelante».
- L2: el servidor MCP no tiene autenticación: el 401 solo existe en A2A (bearer). El cliente A2A lo clasifica como `failed`; no se añade un `outcome` propio (S5).
- L3: el agente del transportista se monta en cada consulta como app ASGI nueva sobre `httpx.ASGITransport`: cada consulta corre en su propio bucle (`asyncio.run`) y una app compartida ataría su almacén de tareas a otro bucle.
- L4: T1 y T2 escritas inline (sin motivo para delegar). ruff y suite unitaria en verde (840). La integración (semilla 20, 15 incidencias) corre en CI: calculado sin base de datos, 14 consultas (6 elegidas en `carrier_slow`, 7 en `carrier_401` y `carrier_bad_card`), 4 salidas inválidas en `mail_invalid` y 4 primeras llamadas caídas en `mail_down` (2 agotan los 3 intentos). `mail_down` provoca su efecto con un reintento de `send_email` (llamadas > claves), no con un `StepFailed`, porque con un 50 % por llamada solo una de cada ocho claves agota los intentos. Risk: item 3 (`mail_server_for`, helper compartido del harness; por defecto no cambia): verificador independiente lanzado.
- L5: CI de #90 en verde (64a3f9f): fast 6 min 15 s, slow-a 16 min 36 s, slow-b 14 min 17 s. El test de E20 va a slow-b sin tocar `ci.yml` (slow-b ejecuta todo slow fuera de `SLOW_A`): slow-a ya está a 3 min 24 s del timeout de 20 min.
- L6: verificador independiente: sin blockers (S1–S5 cumplidos; S6 sin cambio en `ci.yml`, ver L5). Corregidos dos avisos: el primer plan se compara por incidencia con sus pasos (no solo si hubo `PlanCreated`) y `step_failed_errors` cuenta solo los `StepFailed` de `executor-notify_customer`. Suite unitaria 841 en verde.
