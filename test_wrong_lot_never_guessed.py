"""A dispatch must never book stock against a lot nobody asked for.  (C3 → C1)

THE INCIDENT
    A July dispatch of 16 kg was booked against March lot CF100326 and recorded
    at 10 kg. Neither the lot nor the weight came from the transfer line — both
    came from a source row the lookup picked on its own.

THE MECHANISM
    _find_in_bulk_entry matches a warehouse box on (box_id, transaction_no).
    `lot_no` is only a tie-break. When several rows carried the same
    (box_id, transaction_no) and NONE of them matched the lot on the line, the
    function fell off the end of the lot loop and returned candidates[0] — a row
    from a different lot — with no exception and no log line.

    park_in_pending then read the weight from that row:
        weight_kg = float(source_row.net_weight or box.net_weight or 0)
    source_row first, the scanned box only as a fallback. So the wrong row's
    10 kg overwrote the line's 16 kg. One silent fallback, both symptoms.

    The guard comment read "Warehouse box_ids rarely collide within a txn" —
    the assumption C2 disproves: 1,511 real box_ids span multiple GRNs.

THE RULE (already true for cold, see test_cold_collision_lot_mismatch_returns_none)
    A lot on the line is a CONSTRAINT, not a hint. If it cannot be satisfied,
    return no match. Never substitute a different lot.

    Returning (None, None) is the same signal the cold path already gives, so
    park_in_pending handles it on a path that exists. Making that skip visible
    to the operator is C4, tracked separately — this test only settles that the
    wrong row is no longer chosen.

Dependency-free:  python test_wrong_lot_never_guessed.py
"""
from types import SimpleNamespace

from services.ims_service import pending_stock_tools as P


class Res:
    def __init__(self, rows=None, scalar=None):
        self._rows, self._scalar = rows or [], scalar
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar


def _bulk_db(by_table):
    """Route `SELECT ... FROM <tbl> WHERE box_id ...` to canned rows."""
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


def test_lot_mismatch_is_never_guessed():
    """THE BUG. Two candidates, the line's lot matches neither -> no match.

    Before the fix this returned ("cfpl_boxes_v2", march) — the March lot the
    operator never asked for, which is exactly how CF100326 got booked.
    """
    july = SimpleNamespace(lot_number="CF150726", net_weight=16)
    march = SimpleNamespace(lot_number="CF100326", net_weight=10)
    tbl, r = P._find_in_bulk_entry(
        _bulk_db({"cfpl_boxes_v2": [march], "cfpl_bulk_entry_boxes": [july]}),
        "B1", "T1", lot_no="CF999999",
    )
    assert (tbl, r) == (None, None), (
        f"guessed lot {getattr(r, 'lot_number', None)!r} from {tbl} "
        f"when the line asked for CF999999"
    )


def test_warehouse_now_matches_cold_on_lot_mismatch():
    """Parity: the cold path has always refused to guess. Warehouse must too."""
    cold_db_rows = [SimpleNamespace(lot_no="125320"), SimpleNamespace(lot_no="183027")]

    class ColdDB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql:
                return Res(scalar="x")
            if "FROM cfpl_cold_stocks" in sql and "box_id" in sql:
                return Res(rows=cold_db_rows)
            return Res()

    cold = P._find_in_cold_stocks(ColdDB(), "B1", "T1", lot_no="999999")
    warehouse = P._find_in_bulk_entry(
        _bulk_db({"cfpl_boxes_v2": [SimpleNamespace(lot_number="125320"),
                                    SimpleNamespace(lot_number="183027")]}),
        "B1", "T1", lot_no="999999",
    )
    assert cold == warehouse == (None, None), (cold, warehouse)


# ── these already pass; they must keep passing ──────────────────────────

def test_single_match_is_still_returned():
    """One candidate is unambiguous — the lot is not consulted at all."""
    only = SimpleNamespace(lot_number="L1")
    tbl, r = P._find_in_bulk_entry(_bulk_db({"cfpl_boxes_v2": [only]}), "B1", "T1")
    assert tbl == "cfpl_boxes_v2" and r is only


def test_lot_still_disambiguates_a_collision():
    """The tie-break itself is correct and must survive the fix."""
    v2 = SimpleNamespace(lot_number="L1")
    legacy = SimpleNamespace(lot_number="L2")
    tbl, r = P._find_in_bulk_entry(
        _bulk_db({"cfpl_boxes_v2": [v2], "cfpl_bulk_entry_boxes": [legacy]}),
        "B1", "T1", lot_no="L2",
    )
    assert tbl == "cfpl_bulk_entry_boxes" and r is legacy


def test_no_candidates_still_returns_none():
    tbl, r = P._find_in_bulk_entry(_bulk_db({}), "nope", "T1", lot_no="L1")
    assert (tbl, r) == (None, None)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
    print("\nA lot on the line is a constraint, not a hint.")
