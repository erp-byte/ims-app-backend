# Pending Transfer Stock — Reconcile & GRN Auto-Finalize: Change Log

**Status:** Code-complete, tested, **UNCOMMITTED** (push manually). Agent is blocked from prod
writes — apply scripts are confirm-gated and **run by the developer**.
**Date:** 2026-05-30 → 2026-06-01

---

## 1. Problem

The Pending Transfer modal, cold-storage dashboard, and transfer-out hover showed **mismatched
boxes / cartons / kg / qty** (e.g. ordered 600 vs parked 407; dashboard showing un-deducted stock;
transfers stuck as "Partial (GRN raised)").

**Root cause:** `pending_transfer_stock` is a *separate ledger* from the order
(`interunit_transfers_lines`), and the system held **three unreconciled views** of one dispatch.
Parking only kept boxes that strict-matched `cold_stocks` by box_id/txn, silently dropping the
rest — compounded by the fact that **cold→warehouse has no real scanning** (the frontend fabricates
"scanned" boxes; ordered qty = shipped qty).

### Three mismatch modes (all addressed)
1. **Wrong-lot parking** — pending held a different lot than ordered.
2. **False shortage** — warehouse PM lines are 1 box each with `qty` = pack-count, so `SUM(qty)`
   invented huge shortfalls.
3. **Acknowledged-but-never-finalized GRN** — boxes received but transfer never closed.

---

## 2. Design policy (locked)

- Parked rows = **physical truth**; never silently substitute/fabricate stock.
- **Order lot is truth** for cold→warehouse.
- Reconcile is **corrective for cold** (restore wrong-lot/excess to source, pull ordered lot FIFO,
  flag genuine shortage) and **flag-only for warehouse**.
- **Receiving-aware**: reconcile skips once a GRN has started.
- Warehouse "expected boxes" = `COUNT(interunit_transfer_boxes)`, **not** `SUM(qty)`.

---

## 3. Backend changes

### `services/ims_service/pending_stock_tools.py`
| Symbol | Change |
|---|---|
| `_find_available_cold_by_lot(db, company, lot_no, item_description, limit)` | **New.** FIFO by-lot stock lookup. |
| `_guess_company_from_site(site)` | **New.** Rishi/CDPL → `cdpl`, else `cfpl`. |
| `_park_cold_row(...)` | **New.** Parks a by-lot match (INSERT pending + DELETE source + disposition write). |
| `_restore_pending_row(db, p, dry_run)` | **New.** Restores one pending row back to source `cold_stocks`. |
| `reconcile_transfer_to_order(transfer_out_id, db, dry_run)` | **New/rewritten.** Corrective for cold, flag-only for warehouse; keyed by `lot_no`; receiving-aware. |
| `backfill_pending_from_existing_transfers(db, dry_run=False)` | **New.** Reconciles every in-transit transfer; dry-run-aware via write-router. |
| `in_transit_by_lot(db, company)` | **New.** Batched lot → {cartons, kg, box_count} map for dashboard overlay. |
| `list_pending_transfers` | Dedupe (GROUP BY `transfer_out_id`+challan; `'mixed'` when cross-company); returns `unallocated_boxes` + `updated_ts`. |
| `pending_by_lot` | Returns `updated_ts`. |
| `_ensure_reconciliation_schema` | Adds columns (see §5). |

### `services/ims_service/interunit_tools.py`
| Symbol | Change |
|---|---|
| `create_transfer` / `update_transfer` | Call `reconcile_transfer_to_order`; `update_transfer` stamps `updated_ts`; old over-order hard-block removed (cold→warehouse has no scanning). |
| `acknowledge_pending_box(header_id, data, db, autofinalize=True)` | New `autofinalize` param; auto-finalizes after a single-box ack when complete. |
| `acknowledge_pending_boxes_batch(...)` | Passes `autofinalize=False` per box, finalizes **once** after the loop; returns `auto_finalized`. |
| `finalize_transfer_in(...)` | **Now idempotent** — a 2nd call on a `Received` GRN returns `already_finalized=True` (no HTTP 400). |
| `_autofinalize_if_complete(db, header_id)` | **New.** Finalizes a Pending GRN once `acked ≥ in_transit > 0` (COUNT transfer_in_boxes vs COUNT pending 'In Transit'). **SAVEPOINT-isolated** (`db.begin_nested()`) so a finalize failure preserves the acknowledgement. |
| `finalize_complete_pending_grns(db, dry_run)` | **New.** Backlog sweep over stuck Pending GRNs; commits per GRN. |
| `delete_transfer(...)` | Cancel path: unpick transfer-ins → restore to source → delete boxes/lines/header. |

### `services/ims_service/interunit_server.py`
- `dry_run: bool = Query(False)` added to `POST /pending-stock/backfill`.
- New endpoint `GET /pending-stock/in-transit-by-lot`.
- Imports `in_transit_by_lot`.

---

## 4. Frontend changes
| File | Change |
|---|---|
| `frontend-/components/transfer/PendingTransfersModal.tsx` | `⚠ N short` badge; `Edited <date>` badge; `unallocated_boxes`/`updated_ts` types; status label `Partial → "Partial (GRN raised)"`. |
| `frontend-/app/[company]/cold-storage/dashboard/page.tsx` | `InTransitContext` + `+N in transit` overlay badge per lot. |
| `frontend-/app/[company]/transfer/directtransferform/page.tsx` | `Edited` chip in the pending hover card. |

---

## 5. Schema changes — `interunit_transfers_header`
Added in `_ensure_reconciliation_schema` (idempotent `ALTER ... ADD COLUMN IF NOT EXISTS`):
- `unallocated_boxes INTEGER DEFAULT 0` — net shortfall flag from reconcile.
- `updated_ts TIMESTAMP` — last-edited stamp (surfaced as the "Edited" badge).

---

## 6. Scripts (all confirm-gated / dry-run by default — developer runs)
| Script | Purpose |
|---|---|
| `apply_reconcile.py <tid> [--all] --confirm` | Apply corrective reconcile to a transfer. |
| `cancel_transfer.py <tid> --confirm` | Cancel/close a stale dispatch via `delete_transfer`. |
| `finalize_grns.py [--confirm]` | Backlog sweep: finalize acknowledged-but-not-finalized GRNs. |
| `_inspect_grn_targets.py` | **Read-only** inspector of the GRNs a sweep would finalize. |
| `_forensic_readonly.py`, `reconcile_transfers_dryrun.py` | Read-only forensics (creds moved to env vars, dry-run default). |
| `_build_report.py` | Generates the HTML/PDF root-cause + architecture report. |

---

## 7. Tests (dependency-free, `python <file>.py`)
- `test_reconcile_transfer_to_order.py` — **11 tests** (corrective behavior, box-count, warehouse
  flag-only, receiving-aware, restore-wrong-lot). All pass.
- `test_grn_autofinalize.py` — **10 tests** (auto-finalize decision logic, idempotent finalize,
  SAVEPOINT failure-isolation, backlog sweep dry-run/apply). All pass.

---

## 8. Production state & remaining developer actions

| Item | State |
|---|---|
| **tid 468** (wrong-lot corruption) | ✅ Applied — 50× lot 183033 → 50× ordered lot 124679. Re-run confirms zero wrong-lot corruption remains. |
| **GRN backlog** | Live dry-run: 5 Pending GRNs → **2 to finalize** (GRN 195/tid 419 acked 61≥44; GRN 248/tid 486 acked 259≥160); 3 genuinely-partial left alone (199/422, 261/494, 464/893). |
| **tid 403** (stale April dispatch) | ⏳ Cancel pending. |

### Commands the developer runs
```bat
cd d:\test\ims-app-backend

REM Clear the 2 stuck-but-fully-received transfers (deletes 204 in-transit ledger rows,
REM flips both to Received; NO box-count change; idempotent):
.venv\Scripts\python.exe finalize_grns.py --confirm

REM Cancel the stale April dispatch:
.venv\Scripts\python.exe cancel_transfer.py 403 --confirm
```

### Start the backend (port 8000) — use the venv python explicitly
```bat
cd d:\test\ims-app-backend
.venv\Scripts\python.exe main.py
```
> A prior background launch failed with exit 127 because it didn't use the venv path.

---

## 9. Notes / caveats
- **Observed (pre-existing, not caused by this work):** the 2 finalized GRNs have more
  acknowledged boxes than in-transit (61 vs 44; 259 vs 160) — a cold→warehouse no-scanning
  artifact. Finalize only clears the in-transit ledger and flips status; it does **not** change
  stored box counts, so any over-count remains a separate reconcile concern.
- **Going forward:** every new acknowledgement self-finalizes once it completes, so transfers
  should no longer get stuck as "Partial (GRN raised)".
- Existing handoff doc: `docs/superpowers/2026-05-30-pending-transfer-reconcile-HANDOFF.md`.
