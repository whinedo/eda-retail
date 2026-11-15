# Second version: MCP mail server and A2A carrier agent

Objective: specify (first) and later build the two communication modes the PoC lacks: MCP (mail server used by the notify_customer executor) and A2A (carrier agent consulted by the planner), both with guardrails.
Branch: `feat/v2-specs` (from `feat/eda-components`). Delivery: specs PR first, code later. Test runner: pytest (unit locally with closed ports; integration only in CI).

## Specs

- S1 (L1, L2) MCP mail server and its client in the notify_customer executor: `specs/mcp-mail/spec.md`.
- S2 (L1, L3) A2A carrier agent consulted by the planner: `specs/a2a-carrier/spec.md`.
- S3 (L4) Guardrails for MCP and A2A, inside S1 and S2 («Guardrails del cliente MCP», «Guardrails de A2A»).
- S4 design.md §15 and docs/README.md «Segunda versión» point to the specs.
- Assumptions (A1 confirmed; A2–A5 to confirm with the user):
  - A1 (confirmed, L5): the carrier agent is simulated with deterministic seeded rules, not a second LLM, so experiments stay repeatable.
  - A2: A2A uses JSON-RPC over HTTP with a bearer token (`A2A_CARRIER_TOKEN`).
  - A3: the MCP server has its own schema `mail_server` with a synthetic customer directory and the simulated mailbox, and dedups by `idempotency_key`.
  - A4: `tracking_ref` (asked in `input-required`) is a new synthetic reference per order.
  - A5: the event `CarrierConsulted` is added to the catalog. No new experiment is defined yet.

## Tasks

| ID | Specs | Task | Route | Status | Commit |
| --- | --- | --- | --- | --- | --- |
| T1 | S1–S4 | Specs and design links for review | inline | written | |
| T2 | S1, S3 | MCP mail server and MCP client in the notify_customer executor | pending review of T1 | pending | |
| T3 | S2, S3 | Carrier agent and A2A client in the planner, `CarrierConsulted` | pending review of T1 | pending | |

## Log

- L1 (2026-11-15) User: "añade que, para una segunda versión de la PoC (no la vamos a desarrollar por el momento) se añadirá un servidor de correo simulado para enviar las notificaciones a cliente que conecta mediante MCP y un agente que se conecte con alguno de la PoC mediante A2A. Qué agente nuevo sugieres para A2A y con qué agente existente? quiero cubrir todos los modos de comunicación en un sistema distribuido con agentes (EDA (rest y eventos), A2A y MCP) de manera que se muestre cuándo usar cada uno"
- L2 (2026-11-15) User: "me parece bien el del transportista"
- L3 (2026-11-15) User: "para A2A y MCP indica que se pondrán guardrails"
- L4 (2026-11-15) User: "sí, empieza por las specs de la v2". The specs start from design.md §15 (PR #56). The planner's A2A call moves into the `CreatePlan` job worker (PR #58).
- L5 (2026-11-15) User, on A1 (deterministic rules or its own LLM): "Reglas deterministas".
