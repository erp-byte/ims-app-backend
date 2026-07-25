# Cold-Transfer "Duplicate Box IDs From Source" — Incident, Fix & Deferred Migration Plan

**Date:** 2026-07-08 · **Reported:** Cold-transfer form blocked for *Fresho Kimia Dates 500 Gm*, lot 125859 (67 boxes available, error on Add).

## Symptom

Cold-transfer form aborts a pile-add with **"Duplicate Box IDs From Source — Cold storage
returned duplicate box_id values … Aborting."** Switching company (CFPL⇄CDPL) does **not**
help — the company only selects which `*_cold_stocks` table `pick-boxes` queries; the block is a
data problem in the rows.

## Root cause (verified against live DB + code, high confidence)

- The frontend fetches per-box IDs via `GET /cold-storage/stocks/pick-boxes`
  (`services/ims_service/cold_storage_server.py:505`), which filters a pile by
  `(item_description, CAST(lot_no AS TEXT), COALESCE(inward_no,''))` `ORDER BY id ASC`
  and **ignores `transaction_no`**.
- The form then aborts if any `box_id` repeats:
  `new Set(pickedBoxes.map(b=>b.box_id)).size !== pickedBoxes.length`
  (`legacy-frontend/.../coldtransferform/page.tsx:1840`).
- `cold_stocks` enforces only **`UNIQUE(transaction_no, box_id)`** — box_id is NOT unique on its
  own. A one-off **migration generator** (not committed) minted box_id bases as
  `str(int(time.time())*1000)[-8:]` = epoch **seconds × 1000** on an IST server (→ bases end in
  `000`), **one base per lot**, then emitted a separate `transaction_no` per weight/count group
  while numbering `{base}-{n}` **restarting at n=1** each group. So `{base}-1` (and overlapping
  suffixes) land in two transactions. Rows loaded verbatim by
  `scripts/bulk_load_cold_stocks_csv.py` / `scripts/replace_cold_stocks_from_excel.py`.
- Result: within one pile, `pick-boxes` returns two rows with the same box_id → guard trips.
- The guard is **correct to block**: the destination RECEIVE table
  `interunit_transfer_in_boxes` keys on `(header_id, box_id)` (unique index +
  `acknowledge` `ON CONFLICT (header_id, box_id)` at `interunit_tools.py:2294,2368`), so two
  same-box_id boxes would **collapse into one on receive → inventory loss** (the prior
  TRANS202605131331 incident). The cold-transfer **OUT** save is safe — it keys on composite
  `(box_id, transaction_no)`.

### Two failure modes (both trip the same guard)

- **Mode A — cross-transaction box_id collision** (non-null). Same base reused across txns.
- **Mode B — NULL/empty box_id rows.** From `#N/A` / empty Excel "Box ID Range" cells
  (`replace_cold_stocks_from_excel.py` `parse_box_range` → `[None]`); NULLs are exempt from
  dedupe and the partial unique index, so many coexist.

## What was fixed (2026-07-08)

Data repairs (all rows are `auto_created_from_inward=FALSE` / `inward_transaction_no=NULL`, so
`sync_cold_stocks_from_inward` (`inward_tools.py:2835`) never rebuilds/reverts them — **durable**):

| Pile | Table | Fix | Script |
|------|-------|-----|--------|
| Fresho Kimia 125859 | cdpl | box_id `90671000-1`→`90671000-67` on the 1×5kg row (id 782623, TR-…751) | `fix_lot125859_dupbox.py` |
| AL BARAKAH 93289 | cdpl | renumber TR-…513's 77 boxes `90512000-1..77`→`115..191` | `fix_lot93289_dupbox.py` |
| Wet Dates 127890 / King Solomon 17066 / Organic Khidri 13788 | cdpl/cfpl | assign fresh `TR-…` + `{base}-N` to NULL rows | `fix_null_boxid_piles.py` |

Verify: `diag_cold_pile_integrity.py` → **0 broken piles**.

Code hardening (safe, no behavior change):
- Both cold-transfer guards now name the exact colliding box_id / NULL count in the toast
  (`coldtransferform/page.tsx:1840`, `transfer/job-work/material-out/page.tsx:973`).
- `scripts/replace_cold_stocks_from_excel.py` `report_pile_box_collisions()` flags piles that
  will block cold-transfer at import time (closes the `dedupe_rows` gap).
- `diag_cold_pile_integrity.py` — reusable, exit-code-aware monitor for cron/CI.

## Deferred: true systemic fix (transaction-aware box identity) — NOT YET DONE

Decision 2026-07-08: **held** (data clean, source one-off, high blast radius). Schedule as a
reviewed, staging-tested change if cross-transaction box_ids must be allowed to flow end-to-end.

Change set (all must land together):
1. Frontend guard key `box_id` → `box_id|transaction_no` — `coldtransferform/page.tsx:1840`,
   `material-out/page.tsx:973`.
2. `interunit_transfer_in_boxes` unique index `(header_id, box_id)` →
   **`(header_id, box_id, COALESCE(transaction_no,''))`** — `interunit_tools.py:2294`.
   (MUST use `COALESCE` — 20,577 / 51,188 rows have NULL transaction_no; raw NULLs would be
   treated as distinct and re-open the collapse for 40% of rows. Verified 0 existing violations
   under the COALESCE key, so the migration is data-safe.)
3. `acknowledge` `ON CONFLICT` target → the new expression index — `interunit_tools.py:2368`.
4. `create_transfer_in` one-shot INSERT — add matching `ON CONFLICT` — `interunit_tools.py:2023`.
5. `unacknowledge_pending_box` — add `transaction_no` to the DELETE — `interunit_tools.py:2437`;
   thread it through `InterunitApiService.unacknowledgeBox`
   (`legacy-frontend/lib/interunitApiService.ts:656`) and its callers
   (`cold-transfer/coldtransfer-in/page.tsx`, `transfer/transferIn/page.tsx`).
6. Review STBR reconciliation (`pending_stock_tools.py` `reconcile_box_in_pending`) for any
   box_id-alone assumptions.

Caveat (verify agent): box_id is a **physical sticker label**. Making the digital layer tolerate
non-unique box_ids without reprinting stickers can desync the QR-scan receive path. Preferred
long-term invariant: guarantee box_id **global/pile uniqueness at mint** so downstream keys on
box_id stay valid.
