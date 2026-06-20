# Pending Stock Reconcile-by-Lot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make `pending_transfer_stock` always equal the ordered quantity by allocating any shortfall from the main stock sheet matched **by lot number** (not just strict box-id), so every screen (modal, hover, directtransferform, dashboards) shows consistent boxes/cartons/kg.

**Architecture:** A new idempotent `reconcile_transfer_to_order()` routine tops up each in-transit transfer to its ordered qty using a two-tier match (strict box-id, then by-lot FIFO from `cold_stocks`, deducting real stock). "Sync existing" and create/edit all call it. Un-allocatable units are flagged on the header, never faked.

**Tech Stack:** Python, SQLAlchemy `text()` raw SQL, Postgres. Tests are dependency-free (`FakeDB` mock, run with `python <file>.py`) following the existing `test_park_lines_in_pending.py` pattern.

**Spec:** [docs/superpowers/specs/2026-05-30-pending-stock-reconcile-by-lot-design.md](../specs/2026-05-30-pending-stock-reconcile-by-lot-design.md)

**Files map:**
- Modify `services/ims_service/pending_stock_tools.py` — new helpers + `reconcile_transfer_to_order`; rework `backfill_pending_from_existing_transfers`.
- Modify `services/ims_service/interunit_tools.py` — call reconcile in `create_transfer`/`update_transfer`; over-order guard.
- Modify `services/ims_service/interunit_server.py` — `dry_run` param on backfill; new reconcile report endpoint.
- Create `test_reconcile_transfer_to_order.py` — unit tests.
- Modify `reconcile_transfers_dryrun.py` — flip `DRY_RUN=True`, gate creds behind env.
- Delete `_forensic_readonly.py` after the live forensic is run (throwaway with hardcoded creds).

> **Branch first** (the backend repo is mid-merge with unresolved conflicts in `inward_*`/`rtv_*`). Resolve that merge or branch from a clean point before executing.

---

### Task 1: `_find_available_cold_by_lot` helper (by-lot FIFO lookup)

**Files:**
- Modify: `services/ims_service/pending_stock_tools.py` (add after `_find_in_cold_stocks`, ~line 75)
- Test: `test_reconcile_transfer_to_order.py`

- [ ] **Step 1: Write the failing test**

```python
# test_reconcile_transfer_to_order.py
import sys
from types import SimpleNamespace
from services.ims_service import pending_stock_tools as P

class Res:
    def __init__(self, rows=None, scalar=None):
        self._rows, self._scalar = rows or [], scalar
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar

def test_find_available_cold_by_lot_returns_fifo_rows():
    captured = {}
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql:
                return Res(scalar="public.cfpl_cold_stocks")
            if "FROM cfpl_cold_stocks" in sql and "lot_no" in sql:
                captured["params"] = params
                return Res(rows=[SimpleNamespace(id=1, lot_no="125320",
                            item_description="DATES", weight_kg=5.0, no_of_cartons=1)])
            return Res()
    rows = P._find_available_cold_by_lot(DB(), "cfpl", "125320", "DATES", 3)
    assert captured["params"]["lot"] == "125320"
    assert captured["params"]["item"] == "DATES"
    assert captured["params"]["n"] == 3
    assert rows[0][0] == "cfpl_cold_stocks"
    assert rows[0][1].id == 1
    print("PASS test_find_available_cold_by_lot_returns_fifo_rows")

if __name__ == "__main__":
    test_find_available_cold_by_lot_returns_fifo_rows()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd d:/test/ims-app-backend && python test_reconcile_transfer_to_order.py`
Expected: FAIL — `AttributeError: module ... has no attribute '_find_available_cold_by_lot'`

- [ ] **Step 3: Write minimal implementation**

```python
def _find_available_cold_by_lot(db: Session, company: str, lot_no: str,
                                item_description: Optional[str], limit: int):
    """FIFO-pick up to `limit` available cold_stocks rows for (company, lot_no,
    item_description). Used to rescue the shortfall between ordered qty and parked
    boxes by LOT NUMBER when strict box_id matching failed. Returns [(table, row), ...]."""
    if limit <= 0:
        return []
    table = f"{company}_cold_stocks"
    if not _table_exists(db, table):
        return []
    item_clause = "AND item_description = :item" if item_description else ""
    rows = db.execute(
        text(f"""
            SELECT * FROM {table}
            WHERE lot_no = :lot {item_clause}
            ORDER BY inward_dt ASC NULLS LAST, id ASC
            LIMIT :n
        """),
        {"lot": lot_no, "item": item_description, "n": limit},
    ).fetchall()
    return [(table, r) for r in rows]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd d:/test/ims-app-backend && python test_reconcile_transfer_to_order.py`
Expected: `PASS test_find_available_cold_by_lot_returns_fifo_rows`

- [ ] **Step 5: Commit**

```bash
git add services/ims_service/pending_stock_tools.py test_reconcile_transfer_to_order.py
git commit -m "feat(pending): add _find_available_cold_by_lot FIFO helper"
```

---

### Task 2: `reconcile_transfer_to_order` core routine

**Files:**
- Modify: `services/ims_service/pending_stock_tools.py` (add after `park_lines_in_pending`, ~line 1304)
- Test: `test_reconcile_transfer_to_order.py`

**Contract:** for one `transfer_out_id`, per `(lot_no, item_description)`:
`ordered = SUM(interunit_transfers_lines.qty)`, `parked = COUNT(pending In Transit)`.
For `shortfall = ordered - parked > 0`: (Tier 1) park unparked `interunit_transfer_boxes` that still match cold_stocks strictly; (Tier 2) for remaining shortfall, `_find_available_cold_by_lot`, park + DELETE source. Residual that can't be filled → `unallocated`. Returns a report dict; writes nothing when `dry_run=True`.

- [ ] **Step 1: Write the failing test** (idempotent no-op + shortfall fill)

```python
def test_reconcile_noop_when_parked_equals_ordered():
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="x")
            if "FROM interunit_transfers_lines" in sql:
                return Res(rows=[SimpleNamespace(lot_no="125320", item_description="DATES", ordered=600)])
            if "FROM pending_transfer_stock" in sql and "COUNT" in sql:
                return Res(rows=[SimpleNamespace(lot_no="125320", item_description="DATES", parked=600)])
            return Res()
    rep = P.reconcile_transfer_to_order(transfer_out_id=1, db=DB(), dry_run=True)
    assert rep["allocated"] == 0 and rep["unallocated"] == 0
    assert rep["groups"][0]["shortfall"] == 0
    print("PASS test_reconcile_noop_when_parked_equals_ordered")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd d:/test/ims-app-backend && python test_reconcile_transfer_to_order.py`
Expected: FAIL — `AttributeError: ... 'reconcile_transfer_to_order'`

- [ ] **Step 3: Write minimal implementation**

```python
def reconcile_transfer_to_order(transfer_out_id: int, db: Session,
                                dry_run: bool = False) -> dict:
    """Top up pending_transfer_stock for one transfer so parked == ordered, matching
    shortfall stock BY LOT from the main sheet. Idempotent. Returns a report dict.

    report = {transfer_out_id, allocated, unallocated, groups:[{lot, item, ordered,
              parked, shortfall, tier1, tier2, unallocated}]}"""
    report = {"transfer_out_id": transfer_out_id, "allocated": 0,
              "unallocated": 0, "groups": []}
    if not _table_exists(db, "pending_transfer_stock"):
        return report

    hdr = db.execute(
        text("""SELECT id, challan_no, from_site, to_site, created_by, created_ts
                FROM interunit_transfers_header WHERE id = :tid"""),
        {"tid": transfer_out_id},
    ).fetchone()
    if not hdr:
        return report

    from_storage_type = "cold" if _is_cold_site(hdr.from_site) else "warehouse"
    to_storage_type = "cold" if _is_cold_site(hdr.to_site) else "warehouse"

    ordered_rows = db.execute(
        text("""SELECT lot_number AS lot_no, item_desc_raw AS item_description,
                       COALESCE(SUM(qty),0) AS ordered
                FROM interunit_transfers_lines WHERE header_id = :tid
                GROUP BY lot_number, item_desc_raw"""),
        {"tid": transfer_out_id},
    ).fetchall()
    parked_rows = db.execute(
        text("""SELECT lot_no, item_description, COUNT(*) AS parked
                FROM pending_transfer_stock
                WHERE transfer_out_id = :tid AND status = 'In Transit'
                GROUP BY lot_no, item_description"""),
        {"tid": transfer_out_id},
    ).fetchall()
    parked_by_key = {(r.lot_no or "", (r.item_description or "")): int(r.parked or 0)
                     for r in parked_rows}

    now = datetime.now()
    for o in ordered_rows:
        lot = o.lot_no or ""
        item = o.item_description or ""
        ordered = int(o.ordered or 0)
        parked = parked_by_key.get((lot, item), 0)
        shortfall = ordered - parked
        g = {"lot": lot, "item": item, "ordered": ordered, "parked": parked,
             "shortfall": max(shortfall, 0), "tier1": 0, "tier2": 0, "unallocated": 0}
        if shortfall <= 0:
            report["groups"].append(g)
            continue

        company = "cdpl" if any(x in (hdr.from_site or "").lower()
                                for x in ("rishi", "cdpl")) else "cfpl"

        # Tier 2 — by-lot FIFO from the main sheet (the new fix). Tier 1 (strict
        # box-id rescue) is covered by the existing backfill loop; here we fill the
        # remaining gap by lot so re-inwarded / box-id-drifted stock is still found.
        remaining = shortfall
        if from_storage_type == "cold":
            candidates = _find_available_cold_by_lot(db, company, lot, item, remaining)
            for table, src in candidates:
                box_id = f"RC-{transfer_out_id}-{getattr(src, 'id')}"
                if not dry_run:
                    _park_cold_row(db, hdr, transfer_out_id, table, src, box_id,
                                   from_storage_type, to_storage_type, now)
                g["tier2"] += 1
                report["allocated"] += 1
                remaining -= 1
        g["unallocated"] = max(remaining, 0)
        report["unallocated"] += g["unallocated"]
        report["groups"].append(g)

    if not dry_run:
        db.execute(
            text("""UPDATE interunit_transfers_header
                    SET unallocated_boxes = :u WHERE id = :tid"""),
            {"u": report["unallocated"], "tid": transfer_out_id},
        )
    return report
```

Also add the `_park_cold_row` helper (mirrors `park_in_pending`'s INSERT + source DELETE + disposition):

```python
def _park_cold_row(db, hdr, transfer_out_id, source_table, src, box_id,
                   from_storage_type, to_storage_type, now):
    """Insert one pending row for a by-lot-matched cold_stocks row and deduct it."""
    cold_data = _cold_row_to_json(src)
    from_company = _company_from_table(source_table)
    to_company = from_company
    db.execute(
        text("""INSERT INTO pending_transfer_stock
            (transfer_type, transfer_out_id, transfer_out_challan_no, box_id, transaction_no,
             from_company, to_company, from_site, to_site, from_storage_type, to_storage_type,
             source_table, source_row_id, destination_table, item_description, lot_no,
             weight_kg, no_of_cartons, cold_storage_data, status, dispatched_at, dispatched_by)
            VALUES (:tt, :toid, :chal, :bid, :tno, :fc, :tc, :fs, :ts, :fst, :tst,
             :src, :srid, :dst, :item, :lot, :wt, :noc, CAST(:cd AS JSONB),
             'In Transit', :da, :db_)
            ON CONFLICT (box_id, transaction_no) DO NOTHING"""),
        {"tt": "INTERUNIT", "toid": transfer_out_id, "chal": hdr.challan_no,
         "bid": box_id, "tno": hdr.challan_no, "fc": from_company, "tc": to_company,
         "fs": hdr.from_site, "ts": hdr.to_site, "fst": from_storage_type,
         "tst": to_storage_type, "src": source_table, "srid": getattr(src, "id", None),
         "dst": _destination_table(to_storage_type, to_company),
         "item": getattr(src, "item_description", None) or "",
         "lot": getattr(src, "lot_no", None),
         "wt": float(getattr(src, "weight_kg", 0) or 0),
         "noc": int(getattr(src, "no_of_cartons", 1) or 1),
         "cd": json.dumps(cold_data) if cold_data else None,
         "da": now, "db_": getattr(hdr, "created_by", None) or "reconcile"},
    )
    db.execute(text(f"DELETE FROM {source_table} WHERE id = :rid"),
               {"rid": getattr(src, "id")})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd d:/test/ims-app-backend && python test_reconcile_transfer_to_order.py`
Expected: `PASS test_reconcile_noop_when_parked_equals_ordered`

- [ ] **Step 5: Add shortfall + unallocated tests, run, confirm PASS**

```python
def test_reconcile_fills_shortfall_by_lot_dry_run():
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="x")
            if "FROM interunit_transfers_header WHERE id" in sql:
                return Res(rows=[SimpleNamespace(id=1, challan_no="TRANS-X",
                    from_site="Cold Storage", to_site="W202",
                    created_by="u", created_ts=None)])
            if "FROM interunit_transfers_lines" in sql:
                return Res(rows=[SimpleNamespace(lot_no="125320", item_description="DATES", ordered=600)])
            if "FROM pending_transfer_stock" in sql and "COUNT" in sql:
                return Res(rows=[SimpleNamespace(lot_no="125320", item_description="DATES", parked=407)])
            if "FROM cfpl_cold_stocks" in sql and "lot_no" in sql:
                return Res(rows=[SimpleNamespace(id=i, lot_no="125320",
                    item_description="DATES", weight_kg=5.0, no_of_cartons=1)
                    for i in range(193)])
            return Res()
    rep = P.reconcile_transfer_to_order(1, DB(), dry_run=True)
    g = rep["groups"][0]
    assert g["shortfall"] == 193 and g["tier2"] == 193 and g["unallocated"] == 0
    assert rep["allocated"] == 193
    print("PASS test_reconcile_fills_shortfall_by_lot_dry_run")

def test_reconcile_flags_unallocatable_when_sheet_short():
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="x")
            if "FROM interunit_transfers_header WHERE id" in sql:
                return Res(rows=[SimpleNamespace(id=1, challan_no="T", from_site="Cold Storage",
                    to_site="W202", created_by="u", created_ts=None)])
            if "FROM interunit_transfers_lines" in sql:
                return Res(rows=[SimpleNamespace(lot_no="999", item_description="DATES", ordered=600)])
            if "FROM pending_transfer_stock" in sql and "COUNT" in sql:
                return Res(rows=[SimpleNamespace(lot_no="999", item_description="DATES", parked=407)])
            if "FROM cfpl_cold_stocks" in sql and "lot_no" in sql:
                return Res(rows=[])  # sheet has nothing for this lot
            return Res()
    rep = P.reconcile_transfer_to_order(1, DB(), dry_run=True)
    assert rep["unallocated"] == 193 and rep["groups"][0]["tier2"] == 0
    print("PASS test_reconcile_flags_unallocatable_when_sheet_short")
```

Run both; expected PASS.

- [ ] **Step 6: Commit**

```bash
git add services/ims_service/pending_stock_tools.py test_reconcile_transfer_to_order.py
git commit -m "feat(pending): reconcile_transfer_to_order tops up shortfall by lot"
```

---

### Task 3: `unallocated_boxes` schema column

**Files:**
- Modify: `services/ims_service/pending_stock_tools.py` — add column in `_ensure_reconciliation_schema` (the ALTER list near line 273).

- [ ] **Step 1:** Add to the existing `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` block:

```python
"ALTER TABLE interunit_transfers_header ADD COLUMN IF NOT EXISTS unallocated_boxes INTEGER DEFAULT 0",
```

- [ ] **Step 2:** Call `_ensure_reconciliation_schema(db)` once at the start of `reconcile_transfer_to_order` (before the UPDATE that sets `unallocated_boxes`).

- [ ] **Step 3: Commit**

```bash
git add services/ims_service/pending_stock_tools.py
git commit -m "feat(pending): track unallocated_boxes on transfer header"
```

---

### Task 4: "Sync existing" reconciles instead of skipping

**Files:**
- Modify: `services/ims_service/pending_stock_tools.py` — `backfill_pending_from_existing_transfers` (~line 2044).

- [ ] **Step 1:** Add a `dry_run: bool = False` parameter. After the existing per-box Tier-1 parking loop for a transfer, **remove the early `continue` that skips already-parked transfers** (lines 2079-2081) — instead always call:

```python
rec = reconcile_transfer_to_order(t.id, db, dry_run=dry_run)
summary.setdefault("reconciled", []).append(rec)
summary["boxes_topped_up_by_lot"] = summary.get("boxes_topped_up_by_lot", 0) + rec["allocated"]
summary["boxes_unallocatable"] = summary.get("boxes_unallocatable", 0) + rec["unallocated"]
```

- [ ] **Step 2:** Guard all existing INSERT/DELETE in the function with `if not dry_run:` so dry-run is read-only.

- [ ] **Step 3: Commit**

```bash
git commit -am "feat(pending): sync-existing now reconciles to ordered qty (dry_run aware)"
```

---

### Task 5: Endpoint `dry_run` + reconcile report

**Files:**
- Modify: `services/ims_service/interunit_server.py` — `/pending-stock/backfill` (line 126).

- [ ] **Step 1:** Add `dry_run: bool = Query(False)` to `backfill_pending_stock_endpoint` and pass it through:

```python
return backfill_pending_from_existing_transfers(db, dry_run=dry_run)
```

- [ ] **Step 2: Commit**

```bash
git commit -am "feat(api): dry_run param on pending-stock backfill"
```

---

### Task 6: Wire create/update to reconcile

**Files:**
- Modify: `services/ims_service/interunit_tools.py` — end of `create_transfer` (before re-fetch, ~line 1021) and end of `update_transfer` (~line 1606). Import `reconcile_transfer_to_order` from `pending_stock_tools`.

- [ ] **Step 1:** In both functions, after the park step and before the final header re-fetch, add:

```python
from services.ims_service.pending_stock_tools import reconcile_transfer_to_order
reconcile_transfer_to_order(header_id, db, dry_run=False)
```

- [ ] **Step 2: Commit**

```bash
git commit -am "feat(transfer): reconcile pending to ordered on create/edit"
```

---

### Task 7: Over-order guard at dispatch

**Files:**
- Modify: `services/ims_service/interunit_tools.py` — in `create_transfer`, after lines are inserted (~line 860), before parking, for cold sources.

- [ ] **Step 1:** For each line, compare ordered qty against available sheet stock for the lot; if insufficient, set a header flag (do not hard-fail unless desired):

```python
# Surface over-orders so dispatch can't silently create a pending/order mismatch.
for l in lines:
    if _is_cold_site(data.header.from_warehouse) and (l.lot_number or ""):
        company = "cdpl" if any(x in (data.header.from_warehouse or "").lower()
                                for x in ("rishi", "cdpl")) else "cfpl"
        avail = db.execute(
            text(f"SELECT COUNT(*) FROM {company}_cold_stocks "
                 "WHERE lot_no = :lot AND item_description = :item"),
            {"lot": l.lot_number, "item": l.item_desc_raw},
        ).scalar() or 0
        if int(l.qty) > int(avail):
            logger.warning("OVER_ORDER: transfer %s lot %s ordered=%s available=%s",
                           header_id, l.lot_number, l.qty, avail)
```

(Hard-reject is a one-line `raise HTTPException(400, ...)` swap if business wants it — left as a warning per "flag, don't block" default; confirm with user.)

- [ ] **Step 2: Commit**

```bash
git commit -am "feat(transfer): warn on cold over-order vs available sheet stock"
```

---

### Task 8: Hazard fixes (prod-write script + creds)

**Files:**
- Modify: `reconcile_transfers_dryrun.py`
- Delete: `_forensic_readonly.py` (after the live forensic has been run)

- [ ] **Step 1:** In `reconcile_transfers_dryrun.py` set `DRY_RUN = True` and read creds from env:

```python
import os
DRY_RUN = os.environ.get("RECONCILE_APPLY", "").lower() != "true"  # default dry-run
conn = psycopg2.connect(
    host=os.environ["WMS_DB_HOST"], port=int(os.environ.get("WMS_DB_PORT", 5432)),
    dbname=os.environ["WMS_DB_NAME"], user=os.environ["WMS_DB_USER"],
    password=os.environ["WMS_DB_PASSWORD"],
)
```

- [ ] **Step 2:** Delete `_forensic_readonly.py` once its read-only forensic output is captured.

- [ ] **Step 3: Commit**

```bash
git commit -am "chore(security): default reconcile script to dry-run, creds via env"
```

---

## Self-Review

- **Spec coverage:** §3.1 reconcile-by-lot → Tasks 1-2; §3.1 unallocated flag → Task 3; §3.2 sync-existing reconcile → Task 4; dry-run → Tasks 4-5; §3.3 create/edit → Task 6; over-order guard → Task 7; §5 hazards → Task 8; §6 tests → Tasks 1-2. Frontend propagation (§3.4) needs no code (read-only consumers). `unallocated_boxes` UX surfacing (spec §8 open item) is intentionally deferred — flagged for follow-up after data is confirmed.
- **Placeholder scan:** none — all steps carry real code/commands.
- **Type consistency:** `reconcile_transfer_to_order(transfer_out_id, db, dry_run)` and report keys (`allocated`/`unallocated`/`groups[].tier2`) used consistently across Tasks 2,4,5,6 and tests.

## Gates (require your action / prod access)
1. Run read-only forensic (`_forensic_readonly.py`) → confirm per-lot rescuable counts.
2. `POST /interunit/pending-stock/backfill?dry_run=true` → review report.
3. Approve → `dry_run=false` apply → verify all surfaces agree.
