"""Unit tests for pick_from_pending's article-count reconciliation backstop (Phase 3).

Bug (TRANS202606111352, A185->W202): OUT lines had blank lot, so the receive screen's
box->line map (keyed on article|lot) attached nothing, the receiver hit "Generate QR"
which minted fresh box_ids + a NULL lot, and finalize's pick_from_pending then matched
neither by (box_id, txn) [Phase 1] nor by lot [Phase 2] -> 0 picked -> every box stuck
'In Transit' -> transfer frozen at Dispatch/Pending even though all boxes were acknowledged.

Fix: Phase 3 article-count backstop — acknowledged boxes that matched nothing else are
reconciled against THIS transfer's remaining pending rows of the SAME article, bounded by
the acknowledged count per article (so a partial receipt still can't leak unacked boxes).

Dependency-free (mock DB routes SQL substrings).  Run:  python test_pick_from_pending_article_backstop.py
"""
from types import SimpleNamespace
from services.ims_service import pending_stock_tools as P


class Res:
    def __init__(self, rows=None):
        self._rows = rows or []
    def fetchall(self):
        return self._rows


class DB:
    """Returns canned 'In Transit' pending rows; records DELETEd ids as 'picked'."""
    def __init__(self, pending):
        self._pending = pending
        self.deleted = []
    def execute(self, stmt, params=None):
        sql = str(stmt)
        if "FROM pending_transfer_stock" in sql and "In Transit" in sql:
            return Res(rows=list(self._pending))
        if sql.strip().upper().startswith("DELETE FROM PENDING_TRANSFER_STOCK"):
            self.deleted.append((params or {}).get("id"))
            return Res()
        return Res()


def row(rid, box_id, txn, lot, article):
    return SimpleNamespace(id=rid, box_id=box_id, transaction_no=txn, lot_no=lot,
                           article=article, destination_table="cfpl_boxes_v2")


def pending(n, base, txn, lot, article, start_id=1000):
    return [row(start_id + i, f"{base}-{i+1}", txn, lot, article) for i in range(n)]


# ── The bug scenario: must now pick all 14 via article backstop ──
def test_bug_scenario_blank_lot_fresh_ids_picks_all():
    pend = pending(14, "47645835", "TR-20260316132349", "CF160326",
                   "Afghan Black Raisins Seedless 1*2")
    ack = [{"box_id": f"30644074-{i}", "transaction_no": "TR-20260613113404",
            "lot_number": None, "article": "AFGHAN BLACK RAISINS SEEDLESS 1*2"}
           for i in range(1, 15)]
    db = DB(pend)
    picked = P.pick_from_pending(1017, db, acknowledged_boxes=ack)
    assert picked == 14, f"expected 14 picked, got {picked}"
    assert len(db.deleted) == 14, db.deleted
    print("PASS test_bug_scenario_blank_lot_fresh_ids_picks_all")


# ── Partial receipt stays bounded: only acked count is picked ──
def test_partial_ack_bounded_by_count():
    pend = pending(14, "B", "T", "CF160326", "RAISINS")
    ack = [{"box_id": f"X-{i}", "transaction_no": "T2", "lot_number": None, "article": "RAISINS"}
           for i in range(1, 6)]  # only 5 acknowledged
    db = DB(pend)
    picked = P.pick_from_pending(1, db, acknowledged_boxes=ack)
    assert picked == 5, f"partial receipt must pick exactly 5, got {picked}"
    print("PASS test_partial_ack_bounded_by_count")


# ── Article backstop must NOT pick across a different article ──
def test_does_not_overpick_other_article():
    pend = pending(5, "AAA", "T", "L1", "ARTX") + \
           [row(2000 + i, f"BBB-{i+1}", "T", "L2", "ARTY") for i in range(5)]
    ack = [{"box_id": f"Z-{i}", "transaction_no": "T9", "lot_number": None, "article": "ARTX"}
           for i in range(1, 6)]  # 5 of ARTX only
    db = DB(pend)
    picked = P.pick_from_pending(1, db, acknowledged_boxes=ack)
    assert picked == 5, f"only ARTX should be picked, got {picked}"
    assert all(d < 2000 for d in db.deleted), f"ARTY rows leaked: {db.deleted}"
    print("PASS test_does_not_overpick_other_article")


# ── Regression: exact (box_id, txn) match still works (Phase 1) ──
def test_exact_match_regression():
    pend = pending(3, "B", "T1", "L1", "ART")  # box_ids B-1,B-2,B-3
    ack = [{"box_id": f"B-{i}", "transaction_no": "T1", "lot_number": "L1", "article": "ART"}
           for i in range(1, 4)]
    db = DB(pend)
    picked = P.pick_from_pending(1, db, acknowledged_boxes=ack)
    assert picked == 3, picked
    print("PASS test_exact_match_regression")


# ── Regression: same-lot fungible fallback still works (Phase 2) ──
def test_lot_fallback_regression():
    pend = pending(3, "B", "T1", "L1", "ART")
    ack = [{"box_id": f"X-{i}", "transaction_no": "T9", "lot_number": "L1", "article": "ART"}
           for i in range(1, 4)]  # different ids, same lot
    db = DB(pend)
    picked = P.pick_from_pending(1, db, acknowledged_boxes=ack)
    assert picked == 3, picked
    print("PASS test_lot_fallback_regression")


# ── Regression: LINE- tracking sentinels are always picked ──
def test_line_sentinels_always_picked():
    pend = [row(1, "LINE-5-1", None, "", "ART"), row(2, "LINE-5-2", None, "", "ART")]
    db = DB(pend)
    picked = P.pick_from_pending(1, db, acknowledged_boxes=[])
    assert picked == 2, picked
    print("PASS test_line_sentinels_always_picked")


ALL = [
    test_bug_scenario_blank_lot_fresh_ids_picks_all,
    test_partial_ack_bounded_by_count,
    test_does_not_overpick_other_article,
    test_exact_match_regression,
    test_lot_fallback_regression,
    test_line_sentinels_always_picked,
]

if __name__ == "__main__":
    failed = 0
    for t in ALL:
        try:
            t()
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
    print(f"\n{len(ALL) - failed}/{len(ALL)} passed, {failed} failed")
