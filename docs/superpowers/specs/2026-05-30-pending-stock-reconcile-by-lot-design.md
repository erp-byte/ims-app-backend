# Pending Transfer Stock — Reconcile-by-Lot Design

- **Date:** 2026-05-30
- **Status:** Approved (design) — pending spec review
- **Author:** investigation + design with ai.1@candorfoods.in
- **Scope:** `ims-app-backend` (backend logic + reconcile tooling). Frontend surfaces are read-only consumers and need no per-screen changes.

## 1. Problem

The same dispatch shows different totals in different places:

- **Order detail / hover item list** (`GET /interunit/transfers/{id}` → `interunit_transfers_lines/boxes`) shows the **ordered** quantity, e.g. `600 boxes / 3000 kg`.
- **Pending modal columns, hover header badges, directtransferform "+in transit", cold-storage dashboard** all read `pending_transfer_stock` and show the **parked** quantity, e.g. `407 / 2035`.

These are two independently-maintained representations that are never reconciled, so they diverge.

### Root cause (confirmed at code level)

1. `interunit_transfers_lines.qty` stores the **ordered** quantity (600). `interunit_transfer_boxes` only ever holds the boxes actually scanned/derived. `pending_transfer_stock` holds one row per box that **parking could resolve against the main stock sheet**.
2. Parking / backfill match each box to `cfpl/cdpl_cold_stocks` **strictly by `box_id + transaction_no`** (`park_in_pending` lines 1077-1081; `reconcile_transfers_dryrun.py` `find_cold_strict`). Boxes that fail that strict match are **silently dropped** (the `ns` "not settled" bucket) — they never reach pending. That is the 600 → 407 gap (the missing 193).
3. `no_of_cartons` is per-row source-dependent (cold: from `cold_stocks`; warehouse/line: hardcoded 1), so `SUM(no_of_cartons)` is a property of the parked rows, not the order.
4. **"Sync existing" never corrects it**: `backfill_pending_from_existing_transfers` skips any transfer that already has ≥1 pending row (lines 2079-2081). So a transfer parked partially stays partial forever.

### Data-level "why" (to confirm via read-only forensic)

Hypothesis: the 193 missing boxes have `box_id/transaction_no` values that do not match any `cold_stocks` row (re-inwarded under a new transaction, box-id formatting drift, or already FIFO-consumed), **but the same lot still has allocatable stock in the sheet**. Confirm with `_forensic_readonly.py` (read-only) per lot 122909 / 125320 / 123296.

## 2. Goal

The **ordered quantity is the contract.** `pending_transfer_stock` must contain one properly-allocated row per ordered unit, each **deducted from the main stock sheet** (real inventory allocation, not display-only tracking rows). When pending == order, every screen agrees automatically because they all read this one table.

## 3. Design — reconcile-by-lot allocation

### 3.1 New core routine: `reconcile_transfer_to_order(transfer_out_id, db, dry_run)`

For one in-transit transfer:

1. Compute, per `(lot_no, item_description)`, `ordered = SUM(line.qty)` and `parked = COUNT(pending rows In Transit)`.
2. `shortfall = ordered − parked`. If `shortfall <= 0`: no-op (idempotent).
3. For the shortfall, allocate from the main stock sheet using a **two-tier match**:
   - **Tier 1 (strict):** unparked `interunit_transfer_boxes` rows whose `box_id + transaction_no` still match `cold_stocks` → park those (existing behaviour).
   - **Tier 2 (by lot — the new fix):** for any remaining shortfall, pick available `cold_stocks` rows **by `lot_no` + `item_description`, FIFO (`inward_dt`, `id` ASC)**, scoped to the transfer's `from_company`/site, and park them — deducting `cold_stocks` exactly like a normal dispatch (DELETE source row, write disposition ledger entry).
4. If `shortfall` cannot be fully covered (sheet genuinely lacks stock for that lot): **do not fabricate rows.** Record the residual on the header as `unallocated_boxes = N` (new nullable column) and surface it as a "stock shortfall" flag in the module. (Realises "should not show missing boxes".)

`no_of_cartons` and `weight_kg` for Tier-2 rows come from the matched `cold_stocks` row, so cartons/kg are real, not synthesised.

### 3.2 "Sync existing" becomes "reconcile"

`backfill_pending_from_existing_transfers` stops skipping already-parked transfers. Instead it calls `reconcile_transfer_to_order` for **every** in-transit transfer (top-up only; never double-parks because Tier-1/Tier-2 both check existing pending and use `ON CONFLICT (box_id, transaction_no) DO NOTHING`). The `/interunit/pending-stock/backfill` endpoint gains a `dry_run` query param.

### 3.3 Create / edit always allocate the full order

- `create_transfer`: after parking scanned boxes, run `reconcile_transfer_to_order` so parked == ordered (Tier-2 fills any gap). Validate `ordered_qty <= available stock for the lot` and reject/flag over-orders so the mismatch cannot be created going forward.
- `update_transfer`: already does `restore_to_source` → re-park (lines 1353-1355); add the same `reconcile_transfer_to_order` call at the end so edits converge too.

### 3.4 Auto-refresh & propagation

- **On edit:** handled by `update_transfer` (above).
- **On open:** `PendingTransfersModal` already auto-syncs on open; it now hits the reconcile endpoint. directtransferform and the cold-storage dashboard read the same `pending_transfer_stock`, so they reflect corrected totals with **no per-screen code changes**.

### 3.5 Legacy history (approved: option A)

For already-shipped transfers whose missing boxes' source stock is **no longer in the sheet**, leave them at their parked count and flag `unallocated_boxes` for manual review — never top-up with rows that can't deduct real stock.

## 4. Safety

1. **Dry-run first.** Reconcile runs in `dry_run` mode producing a per-transfer / per-lot report (ordered vs parked vs tier-1 rescuable vs tier-2 by-lot rescuable vs unallocatable). Reviewed before any apply. Extends the existing `reconcile_transfers_dryrun.py` intent but **safely** (see hazards).
2. **Per-transfer transaction** + disposition ledger entries so `restore_to_source` can fully undo an apply.
3. **Read-only forensic** (`_forensic_readonly.py`) for investigation; `set_session(readonly=True)` guarantees no writes.

## 5. Hazards found (fix as part of this work)

- `reconcile_transfers_dryrun.py` has `DRY_RUN = False` committed — anyone running it **writes to prod**. Flip to `True` and/or gate behind an explicit env flag.
- Hardcoded prod RDS admin credentials live in `reconcile_transfers_dryrun.py` (and the temporary `_forensic_readonly.py`). Move to env/secret; `_forensic_readonly.py` is a throwaway and should be deleted after use.
- Tier-2 by-lot FIFO against **current** `cold_stocks` risks grabbing boxes re-inwarded later. Mitigation: scope strictly by `lot_no + item_description + company`, prefer rows whose `inward_dt <= transfer.created_ts`, and require dry-run sign-off before apply.

## 6. Testing

Extend `test_park_lines_in_pending.py`:
- parked < ordered, lot stock available → reconcile fills to ordered (Tier-2), cold_stocks deducted.
- parked < ordered, lot stock insufficient → flagged `unallocated_boxes`, no phantom rows.
- parked == ordered → no-op (idempotent; re-run safe).
- edit changes qty up/down → pending follows.
- cold vs warehouse source paths.

## 7. Rollout & gates

1. Land code + tests (no prod access needed).
2. **[gate: prod DB access]** Run read-only forensic to quantify per-lot mismatch.
3. **[gate: review]** Run reconcile `dry_run=true`; review report.
4. **[gate: approval]** Apply reconcile (writes); verify all surfaces agree.

## 8. Open items

- Confirm the `unallocated_boxes` surfacing UX (badge on the modal row / a "shortfall" chip).
- Confirm company scoping for Tier-2 (CFPL vs CDPL sheets) per site.
