# Pending Transfer Stock — Reconcile-by-Lot · Developer Handoff

**Date:** 2026-05-30 · **State:** code complete, **uncommitted**, not deployed · **Author:** pairing session with ai.1@candorfoods.in

> ## ⚑ FINAL DESIGN (supersedes the stock-pull approach described in §3 below)
> Ops decision: **clean accounting = the parked rows are physical truth; never move stock to match the order.**
> - **Reconcile is now FLAG-ONLY.** `reconcile_transfer_to_order()` compares total ordered (lines) vs total shipped (parked, keyed by `lot_no` to survive item-case mismatch and substitute lots) and writes the net gap to `interunit_transfers_header.unallocated_boxes`. It does **NOT** pull/deduct `cold_stocks` (that would mark physically-present boxes as shipped → corrupts the available count). The old tier-2 stock-pull was removed; `_park_cold_row`/`_find_available_cold_by_lot` are now unused.
> - **Hard-block over-orders at dispatch:** in `create_transfer`/`update_transfer`, a box-tracked line whose `qty` exceeds the boxes actually scanned for its lot raises **HTTP 400** — so `ordered == shipped` by construction going forward (prevents new gaps). (Warehouse auto-derive already rejects qty>available; line-only transfers park one row per unit.)
> - **Substitution is real and expected:** an order line of "600 of lot A" can legitimately ship as "407 of A + 193 of substitute lot B." Reconcile compares **transfer totals**, not per-lot, so it never over-parks a fully-shipped transfer.
> - Verified on the live prod RDS via a local uvicorn running this code: dry-run `boxes_topped_up_by_lot=0` (nothing moved); gaps flagged only. **No prod stock/order writes were made.** Gap flags populate automatically when the Pending modal auto-syncs after deploy.
> - 11 dependency-free tests pass: `python test_reconcile_transfer_to_order.py`.

Companion docs:
- Design/spec: `docs/superpowers/specs/2026-05-30-pending-stock-reconcile-by-lot-design.md`
- Implementation plan: `docs/superpowers/plans/2026-05-30-pending-stock-reconcile-by-lot.md`

---

## 1. The problem (what users saw)

The same dispatch showed **different numbers in different places**:
- A challan ordered **600 boxes** of a lot, but the Pending Transfers modal row, the challan hover header, and the directtransferform "+in transit" badge showed **407**.
- The cold-storage dashboard showed lots (e.g. 122909 → 18, 125320 → 193) **still "available"** even though they'd been ordered out — boxes that should have been deducted.
- "Cartons" always equalled "Boxes" in the pending modal.

## 2. Root cause

There are **two independently-maintained representations** of one dispatch that were never reconciled:

| Source of truth | Tables | Drives |
|---|---|---|
| **The order** (what was typed) | `interunit_transfers_lines` (+ `interunit_transfer_boxes`) | hover **item list**, transfer detail |
| **The in-transit ledger** (what got parked) | `pending_transfer_stock` | modal row, hover header badges, directtransferform "+in transit", dashboard (indirectly) |

`pending_transfer_stock` is populated per physical box, and parking/backfill only parked boxes whose `box_id + transaction_no` **strictly matched** `cfpl/cdpl_cold_stocks`. Boxes that didn't match (re-inwarded, box-id drift, etc.) were **silently dropped** — so pending under-counted (407) vs the order (600). The un-matched boxes also **stayed in `cold_stocks`**, which is why the dashboard still showed them as "available" (18 / 193). And `backfill` **skipped any transfer that already had pending rows**, so a partially-parked transfer was never corrected.

## 3. The fix — reconcile to the order, matching shortfall BY LOT

New core routine **`reconcile_transfer_to_order(transfer_out_id, db, dry_run)`** in `services/ims_service/pending_stock_tools.py`:
- Per `(lot_no, item_description)`: `ordered = SUM(lines.qty)`, `parked = COUNT(pending In Transit)`.
- For the shortfall, **FIFO-allocate matching `cold_stocks` rows by lot number** (tries both companies' sheets), parking them and **deducting `cold_stocks`** (real inventory movement + disposition audit entry, mirrors `park_in_pending`).
- **Receiving-aware:** if any box of the transfer has already been received (GRN started), it **skips** the top-up (otherwise it would re-deduct already-received stock — double count).
- Anything it genuinely can't find in the sheets is recorded as `interunit_transfers_header.unallocated_boxes` (a new column) and **flagged in the UI** — never faked.
- **Idempotent** (re-run safe).

Once reconcile runs, `pending == ordered`, the shortfall boxes are deducted from `cold_stocks`, and **every screen agrees** (they all read these same tables).

## 4. Where it's wired (full lifecycle sync)

| Operation | Path | File |
|---|---|---|
| **Create** | `park_in_pending` → **`reconcile_transfer_to_order`** | `interunit_tools.py` `create_transfer` |
| **Update / edit** | `restore_to_source` → re-park → **`reconcile`** | `interunit_tools.py` `update_transfer` |
| **Delete / cancel** | `unpick_to_pending` → `restore_to_source` (restores all pending incl. reconcile-added rows) | `interunit_tools.py` `delete_transfer` |
| **Receive (GRN)** | `pick_from_pending` (reconcile auto-skips once receiving started) | `create_transfer_in` / `finalize_transfer_in` |
| **Delete GRN** | `unpick_to_pending` | `delete_transfer_in` |
| **Sync existing** | now **reconciles every** in-transit transfer (no longer skips already-parked); `dry_run` aware | `backfill_pending_from_existing_transfers` |

`restore_to_source` reconstructs the source row from the pending row's own columns + `cold_storage_data` JSONB, so **reconcile-added rows restore correctly on cancel/edit** (verified) — no inventory-loss risk.

## 5. Files changed (all uncommitted)

### Backend — `ims-app-backend/`
- **`services/ims_service/pending_stock_tools.py`**
  - `_find_available_cold_by_lot()` — FIFO by-lot stock lookup
  - `_guess_company_from_site()`, `_park_cold_row()` — park a by-lot match (INSERT pending + DELETE source + `_write_disposition`)
  - `reconcile_transfer_to_order()` — core reconciler (receiving-aware, both-company by-lot fill, unallocated flagging)
  - `backfill_pending_from_existing_transfers(db, dry_run=False)` — reconciles every in-transit transfer; read-only in dry-run (write-router `_w`)
  - `in_transit_by_lot(db, company)` — batched `lot → {cartons, kg, box_count}` map for the dashboard overlay
  - `list_pending_transfers()` — now returns `unallocated_boxes`
  - schema: `unallocated_boxes` column added in `_ensure_reconciliation_schema`
- **`services/ims_service/interunit_tools.py`** — `create_transfer` & `update_transfer` call reconcile; cold **over-order guard** (currently **warns**, doesn't block)
- **`services/ims_service/interunit_server.py`** — `dry_run` param on `POST /interunit/pending-stock/backfill`; new `GET /interunit/pending-stock/in-transit-by-lot`
- **`reconcile_transfers_dryrun.py`** — defaults to **dry-run** (`RECONCILE_APPLY=true` to apply); creds via env (`WMS_DB_*`)
- **`_forensic_readonly.py`** — strictly read-only forensic (session `readonly=True`), creds via env. **Throwaway — delete after use.**
- **`test_reconcile_transfer_to_order.py`** — 10 dependency-free unit tests (run: `python test_reconcile_transfer_to_order.py`)

### Frontend — `frontend-/`
- **`components/transfer/PendingTransfersModal.tsx`** — `unallocated_boxes` on the row type + a rose **`⚠ N short`** badge when a transfer has boxes that couldn't be matched in the sheets
- **`app/[company]/cold-storage/dashboard/page.tsx`** — `InTransitContext`, one batched fetch of `in-transit-by-lot` per company, and a **`+N in transit`** badge on each lot row

## 6. Data-model notes (important for whoever extends this)

- **In-transit boxes are deleted from `cold_stocks` at dispatch.** So `cold_stocks` (and the dashboard) already exclude properly-dispatched in-transit boxes. **Do NOT subtract pending from displayed stock** — that double-counts (see the existing `directtransferform` note re: lot 125860: net 9, pending 26 → 0). The dashboard badge is **display context only**.
- "Cartons" in pending = `SUM(no_of_cartons)`; per row it's the source `cold_stocks.no_of_cartons` for cold, hardcoded `1` for warehouse/line-level. It is a property of the parked rows, not the order.

## 7. Verification done in-session

- **Backend:** 10/10 unit tests pass; all touched files `py_compile` clean; `pending_stock_tools` imports cleanly.
- **Frontend:** `tsc --noEmit` ran — **0 errors in the 3 changed files** (160 pre-existing errors live elsewhere, e.g. `developer/page.tsx`, `outward/page.tsx`).
- **Not** runtime/DB-integration tested (no app/DB access in-session). Unit tests use a mock DB.

## 8. Rollout — gated, in order

1. **Resolve the in-progress git merge** (see blockers) so the app boots.
2. Run **read-only forensic** (`python _forensic_readonly.py` with `WMS_DB_*` env set) → confirm per-lot rescuable counts.
3. `POST /interunit/pending-stock/backfill?dry_run=true` → review the reconcile report (`boxes_topped_up_by_lot`, `boxes_unallocatable`, per-transfer `reconciled`).
4. Approve → `dry_run=false` → apply → verify all surfaces agree.

## 9. Known blockers

- **Pre-existing merge conflict:** `services/ims_service/inward_models.py` (and `inward_tools.py`, `rtv_server.py`) still contain git conflict markers (`<<<<<<< Updated upstream`). The backend **won't import/boot** until this merge is resolved. None of these are files this work touched.
- Nothing is committed (developer pushes manually).

## 9a. "Edited / last-updated" indicator (DONE)

The header had only `created_ts` — no way to tell an initial entry from a later edit. Added:
- **Backend:** `updated_ts TIMESTAMP` column on `interunit_transfers_header` (via `_ensure_reconciliation_schema`); `update_transfer` stamps it on every edit; surfaced in `list_pending_transfers` (modal) and `pending_by_lot` (transfer-out form hover).
- **Frontend:** a violet **`Edited <date>`** badge on the Pending modal row and an **`Edited`** chip in the directtransferform "Pending Transfers" hover, shown whenever `updated_ts` is set.
- The cold dashboard is per-*lot*, not per-transfer, so it carries no "edited" badge — its **numbers** already update via reconcile on edit.

## 10. Open follow-ups (not done)

- Surface `unallocated_boxes` on the **directtransferform** "Pending Transfers" hover too (consistency with the modal).
- **Over-order guard** currently warns; could hard-reject dispatches that exceed available sheet stock (one-line change).
- **Over-order guard** currently warns; could hard-reject dispatches that exceed available sheet stock (one-line change).
- By-lot company scoping uses a site-name heuristic with a both-companies fallback; confirm against real data in the dry-run.
