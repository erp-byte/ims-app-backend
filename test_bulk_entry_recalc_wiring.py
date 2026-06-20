"""Dependency-free wiring tests for Item 1A on the bulk-entry / cold-storage path.

The cold-storage edit screen adds boxes via bulkEntryApiService.upsertBox -> the bulk_entry_service
backend (NOT the inward upsert_box). That service has the SAME desync bug: appending boxes never
recomputed the parent article. These tests pin that bulk_entry_service.upsert_box and
update_bulk_entry now recompute the article aggregates (reusing the inward recalc helper, which is
generic over the resolved table set).

Mocks route SQL substrings to canned results (same style as test_grn_autofinalize.py).
No database required:  python test_bulk_entry_recalc_wiring.py
"""
from types import SimpleNamespace

from services.bulk_entry_service.tools import upsert_box, update_bulk_entry


class Row:
    """SQLAlchemy Row stand-in: known cols via kwargs, unknown cols read as None (sparse rows)."""
    def __init__(self, **kw):
        object.__setattr__(self, "_data", dict(kw))
    @property
    def _mapping(self):
        return object.__getattribute__(self, "_data")
    def __getattr__(self, k):
        return object.__getattribute__(self, "_data").get(k)


class Result:
    def __init__(self, rows=None):
        self._rows = rows or []
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return self._rows


def _route_recalc(db, up, params):
    if up.lstrip().startswith("UPDATE") and "QUANTITY_UNITS" in up:
        db.article_updates.append(params)
        return True, Result()
    if "COALESCE(SUM" in up or "AS CNT" in up:
        return True, Result(rows=[db.agg])
    return False, None


class BEUpsertDB:
    def __init__(self, agg):
        self.agg = agg
        self.inserts = []
        self.article_updates = []
        self.commits = 0
    def execute(self, stmt, params=None):
        up = str(stmt).upper()
        handled, res = _route_recalc(self, up, params)
        if handled:
            return res
        if "SELECT TRANSACTION_NO FROM" in up:
            return Result(rows=[Row(transaction_no="BE-1")])
        if "SELECT ID, BOX_ID FROM" in up:
            return Result(rows=[])              # new box -> insert path
        if up.lstrip().startswith("INSERT INTO"):
            self.inserts.append(up)
            return Result()
        return Result()
    def commit(self):
        self.commits += 1


def test_bulk_entry_upsert_box_recomputes_article():
    db = BEUpsertDB(agg=SimpleNamespace(cnt=300, net=3000, gross=3300))
    data = {"article_description": "Mango", "box_number": 300, "net_weight": 10.0, "gross_weight": 11.0}
    upsert_box("CFPL", "BE-1", data, db)
    assert db.article_updates, "bulk_entry upsert_box must recompute the parent article"
    p = db.article_updates[0]
    assert p["quantity_units"] == 300, p
    assert p["txno"] == "BE-1" and p["art_desc"] == "Mango", p
    assert db.commits == 1, f"exactly one commit, got {db.commits}"
    print("PASS test_bulk_entry_upsert_box_recomputes_article")


class BEUpdateDB:
    def __init__(self, agg):
        self.agg = agg
        self.inserts = []
        self.article_updates = []
        self.commits = 0
    def execute(self, stmt, params=None):
        up = str(stmt).upper()
        handled, res = _route_recalc(self, up, params)
        if handled:
            return res
        if "SELECT * FROM" in up and "WHERE TRANSACTION_NO" in up:
            if "_BULK_ENTRY_BOXES" in up:
                return Result(rows=[])                       # no existing boxes -> insert path
            if "_BULK_ENTRY_ARTICLES" in up:
                return Result(rows=[])
            return Result(rows=[Row(transaction_no="BE-1")])  # tx (initial fetch + re-fetch)
        if up.lstrip().startswith("INSERT INTO"):
            self.inserts.append(up)
            return Result()
        return Result()
    def commit(self):
        self.commits += 1


def _be_box(desc, n):
    return SimpleNamespace(model_dump=lambda **k: {
        "article_description": desc, "box_number": n,
        "net_weight": 10.0, "gross_weight": 11.0, "lot_number": "L", "status": "available",
    })


def test_update_bulk_entry_recomputes_each_distinct_article_once():
    db = BEUpdateDB(agg=SimpleNamespace(cnt=2, net=20, gross=22))
    payload = SimpleNamespace(
        transaction=None,
        articles=[],
        boxes=[_be_box("A", 1), _be_box("A", 2), _be_box("B", 1)],
    )
    update_bulk_entry("CFPL", "BE-1", payload, db)
    descs = sorted({p["art_desc"] for p in db.article_updates})
    assert descs == ["A", "B"], descs
    assert len(db.article_updates) == 2, db.article_updates  # A appears twice but recomputed once
    assert db.commits == 1, f"exactly one commit, got {db.commits}"
    print("PASS test_update_bulk_entry_recomputes_each_distinct_article_once")


ALL = [
    test_bulk_entry_upsert_box_recomputes_article,
    test_update_bulk_entry_recomputes_each_distinct_article_once,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
