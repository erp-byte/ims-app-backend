"""Dependency-free e2e/logic tests for the Steps 1-4 transfer changes.

Drives the REAL service functions with a mock DB that routes SQL substrings to canned
rows (same style as test_reconcile_transfer_to_order.py / test_grn_autofinalize.py).
No database required:  python test_transfer_steps_e2e.py

Covers:
  Step 1  pending_stock_tools._find_in_cold_stocks / _find_in_bulk_entry
          (box_id+txn+lot matching; collision disambiguation; no-guess)
  Step 2  pending_stock_tools.pick_from_pending (scoped by acknowledged_keys)
          + count_remaining_in_transit (the bridge completion gate)
  Step 3  interunit_tools._validate_cold_boxes_in_stock (cold guard)
  Step 4  interunit_tools.close_transfer_in_with_shortage (write-off + gates)
"""
from contextlib import contextmanager
from types import SimpleNamespace

from services.ims_service import pending_stock_tools as P
from services.ims_service import interunit_tools as I

try:
    from fastapi import HTTPException
except Exception:  # pragma: no cover
    HTTPException = Exception


class Res:
    def __init__(self, rows=None, scalar=None):
        self._rows, self._scalar = rows or [], scalar
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar


def _ok(): pass


# ════════════════════════════════════════════════════════════════════
# Step 1 — _find_in_cold_stocks
# ════════════════════════════════════════════════════════════════════
def _cold_db(cfpl_rows, cdpl_rows):
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql:
                return Res(scalar="x")
            if "FROM cfpl_cold_stocks" in sql and "box_id" in sql:
                return Res(rows=cfpl_rows)
            if "FROM cdpl_cold_stocks" in sql and "box_id" in sql:
                return Res(rows=cdpl_rows)
            return Res()
    return DB()


def test_cold_single_match_returns_that_row():
    row = SimpleNamespace(lot_no="125320", item_description="Wet Date Sayer")
    tbl, r = P._find_in_cold_stocks(_cold_db([row], []), "90644000-2", "TR-724")
    assert tbl == "cfpl_cold_stocks" and r is row, (tbl, r)
    print("PASS test_cold_single_match_returns_that_row")


def test_cold_collision_disambiguates_by_lot():
    # Same (box_id, txn) in BOTH companies with different lots -> pick the lot match.
    cf = SimpleNamespace(lot_no="125320", item_description="Wet Date Sayer")
    cd = SimpleNamespace(lot_no="183027", item_description="Deri Dates")
    tbl, r = P._find_in_cold_stocks(_cold_db([cf], [cd]), "90644000-2", "TR-724", lot_no="183027")
    assert tbl == "cdpl_cold_stocks" and r is cd, (tbl, getattr(r, "lot_no", None))
    print("PASS test_cold_collision_disambiguates_by_lot")


def test_cold_collision_without_lot_returns_none():
    # Two candidates, no lot given -> never guess.
    cf = SimpleNamespace(lot_no="125320")
    cd = SimpleNamespace(lot_no="183027")
    tbl, r = P._find_in_cold_stocks(_cold_db([cf], [cd]), "90644000-2", "TR-724")
    assert (tbl, r) == (None, None), (tbl, r)
    print("PASS test_cold_collision_without_lot_returns_none")


def test_cold_collision_lot_mismatch_returns_none():
    cf = SimpleNamespace(lot_no="125320")
    cd = SimpleNamespace(lot_no="183027")
    tbl, r = P._find_in_cold_stocks(_cold_db([cf], [cd]), "90644000-2", "TR-724", lot_no="999999")
    assert (tbl, r) == (None, None), (tbl, r)
    print("PASS test_cold_collision_lot_mismatch_returns_none")


def test_cold_no_candidates_returns_none():
    tbl, r = P._find_in_cold_stocks(_cold_db([], []), "nope", "TR-x")
    assert (tbl, r) == (None, None), (tbl, r)
    print("PASS test_cold_no_candidates_returns_none")


# ════════════════════════════════════════════════════════════════════
# Step 1 — _find_in_bulk_entry (warehouse: preserves first match when lot can't resolve)
# ════════════════════════════════════════════════════════════════════
def _bulk_db(by_table):
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql:
                return Res(scalar="x")
            for tbl, rows in by_table.items():
                if f"FROM {tbl}" in sql and "box_id" in sql:
                    return Res(rows=rows)
            return Res()
    return DB()


def test_bulk_single_match():
    r0 = SimpleNamespace(lot_number="L1")
    tbl, r = P._find_in_bulk_entry(_bulk_db({"cfpl_boxes_v2": [r0]}), "B1", "T1")
    assert tbl == "cfpl_boxes_v2" and r is r0
    print("PASS test_bulk_single_match")


def test_bulk_collision_disambiguates_by_lot():
    v2 = SimpleNamespace(lot_number="L1")
    legacy = SimpleNamespace(lot_number="L2")
    tbl, r = P._find_in_bulk_entry(
        _bulk_db({"cfpl_boxes_v2": [v2], "cfpl_bulk_entry_boxes": [legacy]}), "B1", "T1", lot_no="L2")
    assert tbl == "cfpl_bulk_entry_boxes" and r is legacy
    print("PASS test_bulk_collision_disambiguates_by_lot")


def test_bulk_collision_no_lot_keeps_first_v2():
    v2 = SimpleNamespace(lot_number="L1")
    legacy = SimpleNamespace(lot_number="L2")
    tbl, r = P._find_in_bulk_entry(
        _bulk_db({"cfpl_boxes_v2": [v2], "cfpl_bulk_entry_boxes": [legacy]}), "B1", "T1")
    assert tbl == "cfpl_boxes_v2" and r is v2, (tbl,)
    print("PASS test_bulk_collision_no_lot_keeps_first_v2")


# ════════════════════════════════════════════════════════════════════
# Step 2 — count_remaining_in_transit (completion gate)
# ════════════════════════════════════════════════════════════════════
def test_count_remaining_returns_scalar():
    class DB:
        def execute(self, stmt, params=None): return Res(scalar=3)
    assert P.count_remaining_in_transit(42, DB()) == 3
    print("PASS test_count_remaining_returns_scalar")


def test_count_remaining_none_is_zero():
    class DB:
        def execute(self, stmt, params=None): return Res(scalar=None)
    assert P.count_remaining_in_transit(42, DB()) == 0
    print("PASS test_count_remaining_none_is_zero")


# ════════════════════════════════════════════════════════════════════
# Step 2 — pick_from_pending scoped by acknowledged_keys
# ════════════════════════════════════════════════════════════════════
def _prow(pid, box_id, txn, dest="cfpl_boxes_v2"):
    return SimpleNamespace(id=pid, box_id=box_id, transaction_no=txn, destination_table=dest,
                           cold_storage_data=None, item_description="X", no_of_cartons=1,
                           weight_kg=5.0, lot_no="L", to_site="W202", transfer_out_challan_no="TR-X")


def _pick_db(pending_rows, deleted):
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql:
                return Res(scalar="x")
            if "SELECT * FROM pending_transfer_stock" in sql and "In Transit" in sql:
                return Res(rows=pending_rows)
            if sql.strip().upper().startswith("DELETE FROM PENDING_TRANSFER_STOCK"):
                deleted.append(params["id"])
                return Res()
            return Res()
    return DB()


def test_pick_scoped_only_acked_plus_line():
    rows = [_prow(1, "A", "T"), _prow(2, "B", "T"), _prow(3, "LINE-1", "T")]
    deleted = []
    n = P.pick_from_pending(42, _pick_db(rows, deleted), acknowledged_keys={("A", "T")})
    assert n == 2, n                       # A (acked) + LINE-1 (always); B skipped
    assert sorted(deleted) == [1, 3], deleted
    print("PASS test_pick_scoped_only_acked_plus_line")


def test_pick_none_acked_is_full_legacy_pick():
    rows = [_prow(1, "A", "T"), _prow(2, "B", "T"), _prow(3, "LINE-1", "T")]
    deleted = []
    n = P.pick_from_pending(42, _pick_db(rows, deleted), acknowledged_keys=None)
    assert n == 3 and sorted(deleted) == [1, 2, 3], (n, deleted)
    print("PASS test_pick_none_acked_is_full_legacy_pick")


def test_pick_empty_ack_picks_only_line():
    rows = [_prow(1, "A", "T"), _prow(2, "B", "T"), _prow(3, "LINE-1", "T")]
    deleted = []
    n = P.pick_from_pending(42, _pick_db(rows, deleted), acknowledged_keys=set())
    assert n == 1 and deleted == [3], (n, deleted)   # only LINE-% picked
    print("PASS test_pick_empty_ack_picks_only_line")


def test_pick_cold_dest_inserts_then_deletes():
    rows = [_prow(1, "A", "T", dest="cdpl_cold_stocks")]
    deleted, inserts = [], []
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt); up = sql.upper()
            if "to_regclass" in sql: return Res(scalar="x")
            if "SELECT * FROM pending_transfer_stock" in sql and "In Transit" in sql:
                return Res(rows=rows)
            if "INSERT INTO CDPL_COLD_STOCKS" in up:
                inserts.append(params["box_id"]); return Res()
            if "DELETE FROM PENDING_TRANSFER_STOCK" in up:
                deleted.append(params["id"]); return Res()
            return Res()
    n = P.pick_from_pending(42, DB(), acknowledged_keys={("A", "T")})
    assert n == 1 and inserts == ["A"] and deleted == [1], (n, inserts, deleted)
    print("PASS test_pick_cold_dest_inserts_then_deletes")


# ════════════════════════════════════════════════════════════════════
# Step 3 — _validate_cold_boxes_in_stock (cold guard)
# ════════════════════════════════════════════════════════════════════
def _box(bid, txn): return SimpleNamespace(box_id=bid, transaction_no=txn)


def _validate_db(in_cold=False, in_transit=False):
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="x")
            if "_cold_stocks WHERE box_id" in sql:
                return Res(rows=[SimpleNamespace(x=1)] if in_cold else [])
            if "FROM pending_transfer_stock WHERE box_id" in sql and "In Transit" in sql:
                return Res(rows=[SimpleNamespace(x=1)] if in_transit else [])
            return Res()
    return DB()


def test_validate_noop_for_warehouse_source():
    # not a cold site -> returns immediately, never raises (even with junk box).
    I._validate_cold_boxes_in_stock(_validate_db(), "W202", [_box("junk", "T")])
    print("PASS test_validate_noop_for_warehouse_source")


def test_validate_accepts_box_in_cold():
    I._validate_cold_boxes_in_stock(_validate_db(in_cold=True), "Cold Storage", [_box("A", "T")])
    print("PASS test_validate_accepts_box_in_cold")


def test_validate_accepts_box_in_transit():
    # not in cold, but already parked In Transit (safe for edits) -> no raise.
    I._validate_cold_boxes_in_stock(_validate_db(in_cold=False, in_transit=True),
                                    "Cold Storage", [_box("A", "T")])
    print("PASS test_validate_accepts_box_in_transit")


def test_validate_rejects_unknown_box():
    try:
        I._validate_cold_boxes_in_stock(_validate_db(in_cold=False, in_transit=False),
                                        "Cold Storage", [_box("GHOST", "T")])
    except HTTPException as e:
        assert getattr(e, "status_code", None) == 400, e
        print("PASS test_validate_rejects_unknown_box"); return
    raise AssertionError("expected HTTPException(400) for unknown box")


def test_validate_skips_direct_and_blank():
    # DIRECT / blank txn boxes are skipped, so no raise even though they're not in cold.
    I._validate_cold_boxes_in_stock(_validate_db(in_cold=False, in_transit=False),
                                    "Cold Storage", [_box("A", "DIRECT"), _box("", ""), _box("B", None)])
    print("PASS test_validate_skips_direct_and_blank")


# ════════════════════════════════════════════════════════════════════
# Step 4 — close_transfer_in_with_shortage
# ════════════════════════════════════════════════════════════════════
@contextmanager
def _patch(obj, name, val):
    orig = getattr(obj, name)
    setattr(obj, name, val)
    try:
        yield
    finally:
        setattr(obj, name, orig)


def _close_db(status, shortage, deleted_ids, out_updates):
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt); up = sql.upper()
            if "to_regclass" in sql: return Res(scalar="x")
            if "SELECT id, status, transfer_out_id, transfer_out_no FROM interunit_transfer_in_header" in sql:
                return Res(rows=[SimpleNamespace(id=5, status=status, transfer_out_id=42, transfer_out_no="TR-OUT")])
            if "SELECT box_id, transaction_no FROM interunit_transfer_in_boxes" in sql:
                return Res(rows=[SimpleNamespace(box_id="A", transaction_no="T")])
            if "SELECT * FROM pending_transfer_stock" in sql and "In Transit" in sql:
                return Res(rows=[])                              # pick: nothing left to move
            if "COUNT(*) FROM pending_transfer_stock" in sql:
                return Res(scalar=shortage)                      # count_remaining
            if "DELETE FROM PENDING_TRANSFER_STOCK" in up:
                return Res(rows=[SimpleNamespace(id=i) for i in deleted_ids])
            if "SELECT condition_remarks FROM interunit_transfer_in_header" in sql:
                return Res(scalar=None)
            if "UPDATE INTERUNIT_TRANSFER_IN_HEADER" in up:
                return Res(rows=[SimpleNamespace(id=5, transfer_out_id=42, status="Received")])
            if "UPDATE INTERUNIT_TRANSFERS_HEADER SET STATUS = 'RECEIVED'" in up:
                out_updates.append(params["toid"]); return Res()
            return Res()
    return DB()


def test_close_404_when_header_missing():
    class DB:
        def execute(self, stmt, params=None):
            if "to_regclass" in str(stmt): return Res(scalar="x")
            return Res()
    try:
        I.close_transfer_in_with_shortage(5, "reason", "u", DB())
    except HTTPException as e:
        assert e.status_code == 404, e
        print("PASS test_close_404_when_header_missing"); return
    raise AssertionError("expected 404")


def test_close_400_when_already_received():
    try:
        I.close_transfer_in_with_shortage(5, "r", "u", _close_db("Received", 0, [], []))
    except HTTPException as e:
        assert e.status_code == 400, e
        print("PASS test_close_400_when_already_received"); return
    raise AssertionError("expected 400")


def test_close_400_when_not_pending():
    try:
        I.close_transfer_in_with_shortage(5, "r", "u", _close_db("Dispatch", 0, [], []))
    except HTTPException as e:
        assert e.status_code == 400, e
        print("PASS test_close_400_when_not_pending"); return
    raise AssertionError("expected 400")


def test_close_writes_off_and_marks_both_received():
    out_updates = []
    db = _close_db("Pending", shortage=2, deleted_ids=[11, 12], out_updates=out_updates)
    with _patch(I, "_map_transfer_in_header", lambda r: {"id": r.id, "status": r.status, "transfer_out_id": r.transfer_out_id}), \
         _patch(I, "_fetch_transfer_in_boxes", lambda db, hid: []):
        res = I.close_transfer_in_with_shortage(5, "missing in transit", "alice", db)
    assert res["shortage"] == 2, res
    assert res["written_off"] == 2, res
    assert res["status"] == "Received", res
    assert out_updates == [42], f"transfer-OUT header must be set Received: {out_updates}"
    print("PASS test_close_writes_off_and_marks_both_received")


ALL = [
    test_cold_single_match_returns_that_row,
    test_cold_collision_disambiguates_by_lot,
    test_cold_collision_without_lot_returns_none,
    test_cold_collision_lot_mismatch_returns_none,
    test_cold_no_candidates_returns_none,
    test_bulk_single_match,
    test_bulk_collision_disambiguates_by_lot,
    test_bulk_collision_no_lot_keeps_first_v2,
    test_count_remaining_returns_scalar,
    test_count_remaining_none_is_zero,
    test_pick_scoped_only_acked_plus_line,
    test_pick_none_acked_is_full_legacy_pick,
    test_pick_empty_ack_picks_only_line,
    test_pick_cold_dest_inserts_then_deletes,
    test_validate_noop_for_warehouse_source,
    test_validate_accepts_box_in_cold,
    test_validate_accepts_box_in_transit,
    test_validate_rejects_unknown_box,
    test_validate_skips_direct_and_blank,
    test_close_404_when_header_missing,
    test_close_400_when_already_received,
    test_close_400_when_not_pending,
    test_close_writes_off_and_marks_both_received,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
