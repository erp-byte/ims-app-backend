"""Dependency-free unit tests for Item 1A: recalc_article_aggregates.

Bug: article-level aggregates (quantity_units / net_weight / total_weight) are written once
at creation from the payload and never recomputed when boxes are appended, so an article that
starts at 1000 boxes still reads 1000 after boxes are bulk-added up to 1720.

Fix (services/ims_service/inward_tools.py):
  - recalc_article_aggregates(db, tables, transaction_no, article_description): the box rows are
    the source of truth. quantity_units = COUNT(boxes), net_weight = SUM(box.net_weight),
    total_weight = SUM(box.gross_weight) for that (transaction_no, article). Called after every
    box mutation (upsert_box, update_inward box section) so the article can never drift again.

Note the column split: the BOX table keys on `article_description`; the ARTICLE table keys on
`item_description`. Both filter by the same string value.

Mocks route SQL substrings to canned results (same style as test_grn_autofinalize.py).
No database required:  python test_recalc_article_aggregates.py
"""
from types import SimpleNamespace

from services.ims_service.inward_tools import recalc_article_aggregates


class Res:
    def __init__(self, rows=None):
        self._rows = rows or []
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows


class RecalcDB:
    """Routes the aggregate SELECT to a canned (cnt, net, gross) row; records UPDATEs."""
    def __init__(self, cnt, net, gross):
        self._agg = SimpleNamespace(cnt=cnt, net=net, gross=gross)
        self.selects = []
        self.updates = []
    def execute(self, stmt, params=None):
        sql = str(stmt)
        up = sql.upper()
        if up.lstrip().startswith("UPDATE"):
            self.updates.append({"sql": sql, "params": params})
            return Res()
        if "COUNT(*)" in up and "FROM" in up:        # the box aggregate SELECT
            self.selects.append({"sql": sql, "params": params})
            return Res(rows=[self._agg])
        return Res()


CFPL = {"art": "cfpl_articles_v2", "box": "cfpl_boxes_v2"}
BULK = {"art": "cfpl_bulk_entry_articles", "box": "cfpl_bulk_entry_boxes"}


def test_recalc_sets_aggregates_from_box_sums():
    # 3 boxes: net 10+20+30=60, gross 12+22+32=66
    db = RecalcDB(cnt=3, net=60, gross=66)
    result = recalc_article_aggregates(db, CFPL, "TX1", "ART-A")
    assert result == {"quantity_units": 3, "net_weight": 60, "total_weight": 66}, result
    assert len(db.updates) == 1, db.updates
    u = db.updates[0]
    assert "cfpl_articles_v2" in u["sql"], u["sql"]
    assert u["params"]["quantity_units"] == 3
    assert u["params"]["net_weight"] == 60
    assert u["params"]["total_weight"] == 66
    print("PASS test_recalc_sets_aggregates_from_box_sums")


def test_recalc_scopes_to_transaction_and_article():
    db = RecalcDB(cnt=1720, net=11340, gross=11900)
    recalc_article_aggregates(db, CFPL, "INW-1000", "Onion 50kg")
    # the box SELECT is scoped to the (txn, article) pair
    s = db.selects[0]
    assert s["params"]["txno"] == "INW-1000"
    assert s["params"]["art_desc"] == "Onion 50kg"
    # the article UPDATE is scoped to the same pair, keyed on item_description
    u = db.updates[0]
    assert "item_description" in u["sql"], "article table keys on item_description"
    assert u["params"]["txno"] == "INW-1000"
    assert u["params"]["art_desc"] == "Onion 50kg"
    print("PASS test_recalc_scopes_to_transaction_and_article")


def test_recalc_zero_boxes_zeroes_aggregates():
    # the locked decision: quantity_units is strictly the box count, so 0 boxes -> 0/0/0
    db = RecalcDB(cnt=0, net=0, gross=0)
    result = recalc_article_aggregates(db, CFPL, "TX1", "ART-A")
    assert result == {"quantity_units": 0, "net_weight": 0, "total_weight": 0}, result
    print("PASS test_recalc_zero_boxes_zeroes_aggregates")


def test_recalc_handles_null_sums_as_zero():
    # COALESCE handles it in SQL, but a None slipping through must not crash / must store 0
    db = RecalcDB(cnt=0, net=None, gross=None)
    result = recalc_article_aggregates(db, CFPL, "TX1", "ART-A")
    assert result == {"quantity_units": 0, "net_weight": 0, "total_weight": 0}, result
    print("PASS test_recalc_handles_null_sums_as_zero")


def test_recalc_targets_resolved_table_set():
    # upsert_box can fall back to _bulk_entry_* tables; recalc must hit the SAME tables it is given
    db = RecalcDB(cnt=5, net=50, gross=55)
    recalc_article_aggregates(db, BULK, "BE-1", "ART-B")
    assert "cfpl_bulk_entry_boxes" in db.selects[0]["sql"], db.selects[0]["sql"]
    assert "cfpl_bulk_entry_articles" in db.updates[0]["sql"], db.updates[0]["sql"]
    print("PASS test_recalc_targets_resolved_table_set")


ALL = [
    test_recalc_sets_aggregates_from_box_sums,
    test_recalc_scopes_to_transaction_and_article,
    test_recalc_zero_boxes_zeroes_aggregates,
    test_recalc_handles_null_sums_as_zero,
    test_recalc_targets_resolved_table_set,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
