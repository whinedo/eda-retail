# Operations profit and refund-over-reissue rule

Objective: add shipping costs (a percentage of the order amount, doubled for a reissued order), a profit query and a new screen with a waterfall chart reachable from the operations view; then a planning rule that picks a refund instead of a reissue when the reissue costs more than 120 % of the refund. Branch `feat/operations-profit` from main 4aafaa4; delivery: two PRs (T1–T3 profit, T4 rule).
Test runners: `uv run pytest -q --ignore=tests/integration -m "not integration and not slow"` (unit); `cd web && npm test && npm run typecheck` (front); integration only in CI.

## Specs

- S1 "Añade los costes de reembolso (coste del pedido)." Refunds are already a cost: `view_cost_per_incident.refunds_amount` sums `RefundIssued.amount` (at most the order amount, `refund_limit`); the profit keeps it as its own line.
- S2 "Además, en caso de un reenvío, hay que duplicar los costes del transporte. Añade costes del transporte como un porcentaje (10%) del precio del pedido." Shipping = `SHIPPING_COST_PCT` (default 10) % of each registered order amount; each `ShipmentReissued` adds the same percentage of its order amount.
- S3 "Desde la vista de operaciones quiero que se muestre una gráfica (no en la misma vista, sino en una pantalla nueva) donde se vea el beneficio total (ganancia menos los gastos)". User choice, verbatim: "Cascada total (Recommended)": revenue of every registered order, minus shipping, reissue shipping, reissue fixed cost, refunds, claims and LLM, down to the total profit.
- S4 "si el coste de reentrega es mayor que el reembolso en un 10%, elegir reembolso directamente". User choice, verbatim: "Producto + transporte": reissue cost = order amount + shipping (`SHIPPING_COST_PCT`) + `EFFECT_COST_REISSUE`; refund = order amount; pick the refund when the reissue cost exceeds 120 % of the refund (corrected in L4; was 110 %). Code rule over the LLM plan (project rule 1), in planner and replanner.

Design (assumptions):

- The query layer cannot read the log, and no view records which order was reissued: the `operations` projector gets a new fact table `view_reissued_shipments` (`event_id`, `incident_id`, `order_id`, position) from `ShipmentReissued` (migration 0027), included in the rebuild fingerprint like its other tables.
- The profit does not count the product given away in a reissue (S2 only asks for the doubled shipping); the rule in S4 does. Flagged to the user.
- No chart library: the waterfall is plain SVG in React.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S2 | inline | done | 7b80a78 |
| T2 | S1, S2, S3 | inline | done | 7b80a78 |
| T3 | S3 | inline | done | a7bb447 |
| T4 | S4 | inline | done | branch feat/refund-over-reissue |

T1: `view_reissued_shipments` (migration, projector, fingerprint). T2: `SHIPPING_COST_PCT`, `get_profit` and `GET /api/operations/profit`. T3: profit screen `#/analytics/operations/profit` with the waterfall and a link from the operations view. T4: rule in plan validation for planner and replanner.

## Log

- L1 (2026-11-18) User request, verbatim: "Sí, merge de #85 cuando pase el CI. Añade los costes de reembolso (coste del pedido). Además, en caso de un reenvío, hay que duplicar los costes del transporte. Añade costes del transporte como un porcentaje (10%) del precio del pedido. Desde la vista de operaciones quiero que se muestre una gráfica (no en la misma vista, sino en una pantalla nueva) donde se vea el beneficio total (ganancia menos los gastos)". Mid-turn, verbatim: "si el coste de reentrega es mayor que el reembolso en un 10%, elegir reembolso directamente".
- L2 (2026-11-18) Answers: chart "Cascada total (Recommended)"; reissue cost "Producto + transporte".
- L3 (2026-11-18) Map: costs in `queries/operations.py` `_money` with `Prices` (`EFFECT_COST_*`, default 0); `view_orders` has `order_id`, `customer_ref`, `amount` and no date; planner and replanner validate with `plan_validation.validate_plan(order_amounts=...)` from `view_orders`.
- L4 (2026-11-18) User correction, verbatim: "indiquemos mejor que reembolso se hará automáticamente cuando el coste total sea mayor al 120% porque sino, el coste del transporte (10%) haría que cualquier intento de reentrega se haga un reembolso". S4 threshold now 120 %.
- L5 (2026-11-18) T1–T3 on `feat/operations-profit`: 7b80a78 (view, query, endpoint) and a7bb447 (profit screen). RED observed (unit: missing `ProfitTotals`; front: 3 failing tests), then GREEN: unit 771, front 148, ruff, typecheck and hygiene ok. Screen checked in the browser with a stubbed `/api/operations/profit` (the backend was stopped). Risk: item 1 (migration 0027 and a new projected table): independent verifier launched. `.env.example` not updated: reading it is blocked here; `SHIPPING_COST_PCT` and `REFUND_OVER_REISSUE_PCT` are documented in docs/README.md.
- L6 (2026-11-18) T4 on `feat/refund-over-reissue` (stacked on `feat/operations-profit`): requirement «Reembolso en lugar de un reenvío caro» in specs/incident-planning; `BusinessRules` gets `shipping_cost_pct`, `effect_cost_reissue` and `refund_over_reissue_pct` (env, defaults 10, 0, 120); `plan_validation.refund_over_reissue` applied after validation in `planner.check_plan` and `replanner.check_revision`, then validated again. The refund gets a new numbered `step_id` and no reserve dependency, so a completed reserve is compensated by the saga (tested with `orchestrator.rules.revise`). RED (collection error) then GREEN: 11 tests. Not applied to `baseline_sync` (E6); with the defaults the rule never fires, so E1–E23 are unchanged.
- L7 (2026-11-18) Verifier on T1–T3 (a7bb447): S1, S2, S3 PASS; unit 771, front 148, ruff, typecheck ok; differential probe of `_money`, `Prices.from_settings` and `get_operations_summary` identical to the baseline. BLOCKER (item 1): on an existing volume the `operations` projector is already past old `ShipmentReissued`, so `view_reissued_shipments` starts empty and the reissue shipping is too low. Fix: migration 0027 empties the `operations` views, its `views.offsets` and `views.applied_events`, so it re-projects from the start (as `rebuild` does); new integration test `test_reissued_shipments_migration_reprojects_operations`. Advisories kept: `_reissued` touches `last_event_*` like the other fact handlers; `applied` reports only the `operations` position, as in the summary; `SHIPPING_COST_PCT` is not validated, like `EFFECT_COST_*`.
