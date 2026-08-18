"""Dependency-free unit tests for the CORRECTIVE reconcile (clean-accounting model).

Model (per ops): cold->warehouse has no real scanning, so ordered qty == shipped qty and
the ORDER LOT is the truth. reconcile_transfer_to_order corrects pending to the order:
restores wrong-lot / excess parked rows to source, tops up the ordered lot FIFO from
cold_stocks, and flags any genuine shortage. Warehouse sources are flag-only.

Mocks route SQL substrings to canned results (same style as test_park_lines_in_pending.py).
No database required:  python test_reconcile_transfer_to_order.py
"""
from types import SimpleNamespace

from services.ims_service import pending_stock_tools as P


class Res:
    def __init__(self, rows=None, scalar=None):
        self._rows, self._scalar = rows or [], scalar
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar


def _hdr_row(from_site="Cold Storage"):
    return SimpleNamespace(id=1, challan_no="TRANS-X", from_site=from_site,
                           to_site="W202", created_by="u", created_ts=None)


def _prow(lot, i=1):
    """A full pending_transfer_stock row (what 'SELECT *' returns)."""
    return SimpleNamespace(lot_no=lot, id=i, box_id=f"B{i}", transaction_no="T",
                           source_table="cdpl_cold_stocks", cold_storage_data=None,
                           item_description="DATES", no_of_cartons=1, weight_kg=5.0,
                           from_site="Cold Storage")


def _coldrow(lot, i):
    return SimpleNamespace(id=i, lot_no=lot, item_description="DATES",
                           weight_kg=5.0, no_of_cartons=1)


# ---------------------------------------------------------------- helper tests
def test_find_available_cold_by_lot_returns_fifo_rows():
    captured = {}
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="public.cfpl_cold_stocks")
            if "FROM cfpl_cold_stocks" in sql and "lot_no" in sql:
                captured["params"] = params
                return Res(rows=[_coldrow("125320", 1)])
            return Res()
    rows = P._find_available_cold_by_lot(DB(), "cfpl", "125320", "DATES", 3)
    assert captured["params"]["lot"] == "125320" and captured["params"]["n"] == 3
    assert rows[0][0] == "cfpl_cold_stocks" and rows[0][1].id == 1
    print("PASS test_find_available_cold_by_lot_returns_fifo_rows")


def test_find_available_cold_by_lot_zero_limit_is_noop():
    class DB:
        def execute(self, stmt, params=None):
            raise AssertionError("should not query when limit<=0")
    assert P._find_available_cold_by_lot(DB(), "cfpl", "125320", "DATES", 0) == []
    print("PASS test_find_available_cold_by_lot_zero_limit_is_noop")


# ---------------------------------------------------------------- corrective reconcile
def _db(ordered_rows, parked_rows, cold_by_table=None, received=0):
    cold_by_table = cold_by_table or {}
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="x")
            if "FROM interunit_transfers_header WHERE id" in sql:
                return Res(rows=[_hdr_row()])
            if "interunit_transfer_in_boxes" in sql:
                return Res(scalar=received)
            if "FROM interunit_transfers_lines" in sql:
                return Res(rows=ordered_rows)
            if "SELECT * FROM pending_transfer_stock" in sql:
                return Res(rows=parked_rows)
            for tbl, rows in cold_by_table.items():
                if f"FROM {tbl}" in sql and "lot_no" in sql:
                    return Res(rows=rows)
            return Res()
    return DB()


def test_reconcile_noop_when_parked_matches_order():
    db = _db([SimpleNamespace(lot_no="125320", item_description="DATES", ordered=600)],
             [_prow("125320", i) for i in range(600)])
    rep = P.reconcile_transfer_to_order(1, db, dry_run=True)
    assert rep["total_ordered"] == 600 and rep["total_parked"] == 600, rep
    assert rep["pulled_ordered"] == 0 and rep["restored_wrong_lot"] == 0, rep
    assert rep["unallocated"] == 0, rep
    print("PASS test_reconcile_noop_when_parked_matches_order")


def test_reconcile_corrects_under_park_from_ordered_lot():
    db = _db([SimpleNamespace(lot_no="125320", item_description="DATES", ordered=600)],
             [_prow("125320", i) for i in range(407)],
             cold_by_table={"cdpl_cold_stocks": [_coldrow("125320", i) for i in range(193)]})
    rep = P.reconcile_transfer_to_order(1, db, dry_run=True)
    assert rep["pulled_ordered"] == 193 and rep["unallocated"] == 0, rep
    assert rep["restored_wrong_lot"] == 0, rep
    print("PASS test_reconcile_corrects_under_park_from_ordered_lot")


def test_reconcile_flags_shortage_when_ordered_lot_absent():
    db = _db([SimpleNamespace(lot_no="125320", item_description="DATES", ordered=600)],
             [_prow("125320", i) for i in range(407)],
             cold_by_table={})  # ordered lot not in any sheet
    rep = P.reconcile_transfer_to_order(1, db, dry_run=True)
    assert rep["pulled_ordered"] == 0 and rep["unallocated"] == 193, rep
    print("PASS test_reconcile_flags_shortage_when_ordered_lot_absent")


def test_reconcile_restores_wrong_lot_and_pulls_ordered():
    # order = 50 of lot 124679; parked = 50 of WRONG lot 183033; sheet has 124679.
    db = _db([SimpleNamespace(lot_no="124679", item_description="DATES", ordered=50)],
             [_prow("183033", i) for i in range(50)],
             cold_by_table={"cdpl_cold_stocks": [_coldrow("124679", i) for i in range(50)]})
    rep = P.reconcile_transfer_to_order(1, db, dry_run=True)
    assert rep["restored_wrong_lot"] == 50, rep   # wrong-lot boxes restored to source
    assert rep["pulled_ordered"] == 50, rep        # ordered lot pulled in
    assert rep["unallocated"] == 0, rep
    print("PASS test_reconcile_restores_wrong_lot_and_pulls_ordered")


def test_reconcile_returns_empty_when_header_missing():
    class DB:
        def execute(self, stmt, params=None):
            if "to_regclass" in str(stmt): return Res(scalar="x")
            return Res()  # header lookup empty
    rep = P.reconcile_transfer_to_order(999, DB(), dry_run=True)
    assert rep["allocated"] == 0 and rep["groups"] == []
    print("PASS test_reconcile_returns_empty_when_header_missing")


def test_reconcile_skips_when_receiving_started():
    db = _db([SimpleNamespace(lot_no="125320", item_description="DATES", ordered=600)],
             [_prow("125320", i) for i in range(407)], received=50)
    rep = P.reconcile_transfer_to_order(1, db, dry_run=True)
    assert rep.get("skipped_receiving_in_progress") is True, rep
    assert rep["allocated"] == 0 and rep["received"] == 50, rep
    print("PASS test_reconcile_skips_when_receiving_started")


def test_reconcile_warehouse_uses_box_count_not_qty_sum():
    """Warehouse: each line = 1 box, qty = PACK count. Expected boxes = COUNT(transfer_boxes),
    NOT SUM(qty). 75 boxes parked but SUM(qty)=1963 → must be 0 short (was a false 1888)."""
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="x")
            if "FROM interunit_transfers_header WHERE id" in sql:
                return Res(rows=[_hdr_row(from_site="A68")])  # warehouse site
            if "interunit_transfer_in_boxes" in sql: return Res(scalar=0)
            if "FROM interunit_transfers_lines" in sql:
                return Res(rows=[SimpleNamespace(lot_no="", item_description="PM", ordered=1963)])
            if "SELECT * FROM pending_transfer_stock" in sql:
                return Res(rows=[_prow("", i) for i in range(75)])
            if "COUNT(*) FROM interunit_transfer_boxes WHERE header_id" in sql:
                return Res(scalar=75)
            if "cold_stocks" in sql and "lot_no" in sql:
                raise AssertionError("warehouse reconcile must not pull cold stock")
            return Res()
    rep = P.reconcile_transfer_to_order(1, DB(), dry_run=True)
    assert rep["total_ordered"] == 75, rep        # box count, not 1963
    assert rep["pulled_ordered"] == 0 and rep["restored_wrong_lot"] == 0, rep
    assert rep["unallocated"] == 0, rep            # the false 1888 shortage is gone
    print("PASS test_reconcile_warehouse_uses_box_count_not_qty_sum")


# ---------------------------------------------------------------- in-transit overlay
def test_in_transit_by_lot_groups_and_filters():
    captured = {}
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="x")
            if "GROUP BY lot_no" in sql:
                captured["params"] = params
                return Res(rows=[
                    SimpleNamespace(lot_no="122909", cartons=18.0, kg=90.0, box_count=18),
                    SimpleNamespace(lot_no="125320", cartons=424.0, kg=2120.0, box_count=424),
                ])
            return Res()
    out = P.in_transit_by_lot(DB(), company="CFPL")
    assert out["122909"]["cartons"] == 18.0 and out["125320"]["kg"] == 2120.0
    assert captured["params"]["co"] == "cfpl"
    print("PASS test_in_transit_by_lot_groups_and_filters")


# ---------------------------------------------------------------- backfill (dry-run)
def test_backfill_dry_run_is_readonly_and_corrects():
    writes = []
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt); up = sql.upper()
            if any(k in up for k in ("INSERT ", "DELETE ", "UPDATE ", "ALTER ")):
                writes.append(up.split()[0]); return Res()
            if "to_regclass" in sql: return Res(scalar="x")
            if "FROM interunit_transfers_header h" in sql:  # candidates
                return Res(rows=[SimpleNamespace(id=1, challan_no="TRANS-X",
                    from_site="Cold Storage", to_site="W202", status="Dispatch",
                    created_by="u", created_ts=None, stock_trf_date=None)])
            if "COUNT(*) FROM pending_transfer_stock WHERE transfer_out_id" in sql:
                return Res(scalar=407)
            if "FROM interunit_transfer_boxes WHERE header_id" in sql:
                return Res(rows=[])
            if "interunit_transfer_in_boxes" in sql: return Res(scalar=0)
            if "FROM interunit_transfers_header WHERE id" in sql:
                return Res(rows=[_hdr_row()])
            if "FROM interunit_transfers_lines" in sql:
                return Res(rows=[SimpleNamespace(lot_no="125320", item_description="DATES", ordered=600)])
            if "SELECT * FROM pending_transfer_stock" in sql:
                return Res(rows=[_prow("125320", i) for i in range(407)])
            if "FROM cdpl_cold_stocks" in sql and "lot_no" in sql:
                return Res(rows=[_coldrow("125320", i) for i in range(193)])
            return Res()
        def commit(self): writes.append("COMMIT")
    summary = P.backfill_pending_from_existing_transfers(DB(), dry_run=True)
    assert writes == [], f"dry-run must not write, but did: {writes}"
    assert summary["boxes_topped_up_by_lot"] == 193, summary
    assert summary["boxes_unallocatable"] == 0, summary
    print("PASS test_backfill_dry_run_is_readonly_and_corrects")


def _db_cold(ordered_rows, parked_rows, cold_by_table=None,
             interunit_received=0, cold_received=0, box_rows=None):
    """Like _db, but can answer the COLD receipt ledger and the dispatch's box rows.

    The cold receive path writes cold_transfer_in_headers/cold_transfer_inboxes and
    then DELETEs the interunit staging rows, so `interunit_received=0` alongside
    `cold_received>0` is the real post-cold-receive shape.
    """
    cold_by_table = cold_by_table or {}
    pulls = []
    class DB:
        pulls_log = pulls
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="x")
            if "FROM interunit_transfers_header WHERE id" in sql:
                return Res(rows=[_hdr_row()])
            if "cold_transfer_inboxes" in sql:
                return Res(scalar=cold_received)
            if "interunit_transfer_in_boxes" in sql:
                return Res(scalar=interunit_received)
            if "FROM interunit_transfer_boxes" in sql:          # dispatch box rows
                return Res(rows=box_rows or [])
            if "FROM interunit_transfers_lines" in sql:
                return Res(rows=ordered_rows)
            if "SELECT * FROM pending_transfer_stock" in sql:
                return Res(rows=parked_rows)
            for tbl, rows in cold_by_table.items():
                if f"FROM {tbl}" in sql and "lot_no" in sql:
                    pulls.append(tbl)
                    return Res(rows=rows)
            return Res()
    return DB()


def test_reconcile_skips_when_only_the_COLD_ledger_knows_about_the_receipt():
    """The cold receive purges the interunit staging rows this guard used to read.

    Seeing received=0, reconcile fell into the cold branch and pulled real
    cold_stocks rows to close a shortfall the receipt itself had created."""
    db = _db_cold(
        [SimpleNamespace(lot_no="125320", item_description="DATES", ordered=100)],
        [_prow("125320", i) for i in range(40)],          # 60 received, 40 still parked
        cold_by_table={"cdpl_cold_stocks": [_coldrow("125320", i) for i in range(60)]},
        interunit_received=0, cold_received=60,
    )
    rep = P.reconcile_transfer_to_order(1, db, dry_run=True)
    assert rep.get("skipped_receiving_in_progress") is True, rep
    assert rep["received"] == 60, rep
    assert rep["allocated"] == 0, rep
    assert db.pulls_log == [], f"must not touch cold_stocks, but read: {db.pulls_log}"
    print("PASS test_reconcile_skips_when_only_the_COLD_ledger_knows_about_the_receipt")


def test_reconcile_cold_uses_dispatch_box_rows_not_inflated_qty_sum():
    """The 198-for-100-boxes shape: SUM(qty)=198 but 100 real boxes shipped.

    Reading 198 as 'ordered' made reconcile pull 98 unrelated in-store rows out of
    cold_stocks to close a shortfall that never existed."""
    db = _db_cold(
        [SimpleNamespace(lot_no="L1", item_description="DATES", ordered=120),
         SimpleNamespace(lot_no="L2", item_description="DATES", ordered=78)],   # 198
        [_prow("L1", i) for i in range(60)] + [_prow("L2", 100 + i) for i in range(40)],
        cold_by_table={"cdpl_cold_stocks": [_coldrow("L1", i) for i in range(98)]},
        box_rows=[SimpleNamespace(lot_no="L1", n=60), SimpleNamespace(lot_no="L2", n=40)],
    )
    rep = P.reconcile_transfer_to_order(1, db, dry_run=True)
    assert rep["total_ordered"] == 100, f"box rows must win over SUM(qty): {rep}"
    assert rep["pulled_ordered"] == 0, f"nothing to top up: {rep}"
    assert rep["unallocated"] == 0, rep
    assert db.pulls_log == [], f"must not touch cold_stocks, but read: {db.pulls_log}"
    print("PASS test_reconcile_cold_uses_dispatch_box_rows_not_inflated_qty_sum")


def test_reconcile_cold_keeps_typed_qty_for_a_lot_with_no_boxes():
    """A weight-only / manually typed cold row has no box rows — its qty is all
    there is, so it must NOT be zeroed out by the box-count override."""
    db = _db_cold(
        [SimpleNamespace(lot_no="L1", item_description="DATES", ordered=12)],
        [],
        cold_by_table={"cdpl_cold_stocks": [_coldrow("L1", i) for i in range(12)]},
        box_rows=[],                                     # no scanned boxes at all
    )
    rep = P.reconcile_transfer_to_order(1, db, dry_run=True)
    assert rep["total_ordered"] == 12, f"typed qty must stand: {rep}"
    print("PASS test_reconcile_cold_keeps_typed_qty_for_a_lot_with_no_boxes")


def test_received_box_count_sums_both_ledgers():
    db = _db_cold([], [], interunit_received=3, cold_received=7)
    assert P._received_box_count(db, 1) == 10


def test_received_box_count_fails_closed_on_error():
    """Guessing 0 would let the caller pull and DELETE cold stock."""
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="x")
            raise RuntimeError("connection reset")
    assert P._received_box_count(DB(), 1) >= 1, "must report receiving-in-progress"
    print("PASS test_received_box_count_fails_closed_on_error")


ALL = [
    test_find_available_cold_by_lot_returns_fifo_rows,
    test_find_available_cold_by_lot_zero_limit_is_noop,
    test_reconcile_noop_when_parked_matches_order,
    test_reconcile_corrects_under_park_from_ordered_lot,
    test_reconcile_flags_shortage_when_ordered_lot_absent,
    test_reconcile_restores_wrong_lot_and_pulls_ordered,
    test_reconcile_returns_empty_when_header_missing,
    test_reconcile_skips_when_receiving_started,
    test_reconcile_warehouse_uses_box_count_not_qty_sum,
    test_in_transit_by_lot_groups_and_filters,
    test_backfill_dry_run_is_readonly_and_corrects,
    test_reconcile_skips_when_only_the_COLD_ledger_knows_about_the_receipt,
    test_reconcile_cold_uses_dispatch_box_rows_not_inflated_qty_sum,
    test_reconcile_cold_keeps_typed_qty_for_a_lot_with_no_boxes,
    test_received_box_count_sums_both_ledgers,
    test_received_box_count_fails_closed_on_error,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
