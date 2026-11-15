# Parked notices warning in the front

Objective: show in the front the notices the interpreter parked while waiting for their orders. Branch `feat/eda-parked-notices`, based on `feat/eda-async-commands` (21fc1ad); delivery `single-pr`.
Test runners: `uv run pytest -q tests --ignore=tests/integration` (unit; integration only in CI), `cd web && npx vitest run && npx tsc --noEmit`.

## Specs

- S1 New event `NoticeParked` (producer `interpreter`, `event_version` 1) with payload `incident_id`, `order_ids` (the order references it waits for), `deadline_at` (ISO timestamp). The interpreter publishes it through its outbox in the same transaction where it inserts the row in `interpreter.pending_notices`. No personal data. Add it to the catalog, docs/README.md event catalog, design.md and the incident-interpretation / cqrs-views specs.
- S2 New projection `parked_notices` → table `views.view_parked_notices` (migration 0020, down_revision 0019; never edit `ddl()` or older migrations): `incident_id` PK, `order_ids` TEXT[], `deadline_at`, `parked_at` (event occurred_at), `last_event_id`, `kafka_partition`, `kafka_offset`. `NoticeParked` inserts (ON CONFLICT DO NOTHING); `ShipmentDelayed` (resumed by OrderRegistered or by expiry with known orders) and `IncidentEscalated` of that incident delete the row. Idempotent like the other projectors (views.applied_events / offsets), rebuildable (rebuild fingerprint spec in projectors/rebuild.py). Register it in the projector registry. Update every hardcoded view/projector list in tests.
- S3 Query + endpoint: `GET /api/parked-notices` returns the parked notices ordered by `deadline_at` with `applied` positions like the other queries.
- S4 Front: a warning banner ("aviso") visible in the main view AND in the analytics view while there is at least one parked notice, polling like the other views (≤ 2 s). Text (Spanish), e.g.: "⚠ 2 avisos aparcados esperan pedidos que aún no existen: P-1001 (vence 20:13:36), P-1004 (vence 20:14:02). Si no has sembrado los pedidos, usa «seed»." List each notice's incident (short), order_ids and deadline in local time; hidden when there are none. Container-presentational pattern (a hook + a presentational component) and styles.css conventions.
- S5 Specs: add scenarios (cqrs-views: «Aviso aparcado visible», «Aviso aparcado reanudado», «Aviso aparcado vencido»; web-front: «Aviso de avisos aparcados»). Every scenario needs a test whose docstring / test name is the scenario name. Update docs/README.md (endpoint, view) and design.md.

## Tasks

| ID | Specs | Route | Status | Commit |
| --- | --- | --- | --- | --- |
| T1 | S1 | inline (one delegated writer for the whole feature) | done | aa39e57 |
| T2 | S2, S3, S5 | inline | done | 0fca349 |
| T3 | S4, S5 | inline | done | this commit (feat(web-front): parked notices warning) |

## Log

- L1 (2026-11-15) "añade el aviso de avisos aparcados en el front". Context: a `delay` notice sent from the front while `inventory.orders` was empty was parked in `interpreter.pending_notices` and, after `PENDING_NOTICE_TIMEOUT_S`, escalated with `no_valid_orders`; nothing in the front showed it.
- L2 (2026-11-15) T1 RED: `test_catalog_has_every_event_of_readme`, `test_notice_parked_carries_order_references_and_its_deadline` and `test_parked_notice_publishes_notice_parked_with_its_orders_and_deadline` failed (3 failed). GREEN: catalog `NoticeParked` v1; the handler inserts `pending_notices ... RETURNING deadline_at` and appends `NoticeParked` to the same `Accepted` events (same transaction/outbox). Integration interpreter tests updated: parking/resume/expiry outboxes now include `NoticeParked` (not run locally).
- L3 (2026-11-15) T2 RED: `test_rebuild_fingerprint` KeyError on `parked_notices`, `test_projector_cli` list, `tests/test_parked_notices.py` import error. GREEN: migration 0020, projector, registry, `get_parked_notices`, `GET /api/parked-notices`. Integration: `tests/integration/test_parked_notices_view.py` (scenarios «Aviso aparcado visible/reanudado/vencido», duplicate, endpoint order) and `test_parked_notices_view_migration_reverts` (CI only).
- L4 (2026-11-15) T3 RED: vitest 3 files failed (client `getParkedNotices`, `ParkedNoticesBanner` missing, App scenario). GREEN: `useParkedNotices` (1 s polling, both views) + `ParkedNoticesBanner`, grid row `notice` under the header. Unit 433 passed, vitest 99 passed, tsc ok, ruff ok, pre-commit (SKIP=pytest) ok.
