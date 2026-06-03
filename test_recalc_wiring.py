"""Dependency-free wiring tests for Item 1A: the recalc must actually fire after box mutations.

The original bug was not that the sum was uncomputable, but that nothing recomputed the parent
article when boxes were appended. These tests pin the wiring: after a box is upserted (upsert_box)
or boxes are saved (update_inward), the parent article's aggregates are recomputed and persisted
in the SAME transaction (one commit).

Mocks route SQL substrings to canned results (same style as test_grn_autofinalize.py).
No database required:  python test_recalc_wiring.py
"""
from types import SimpleNamespace

from services.ims_service.inward_tools import upsert_box, update_inward


class Row:
    """A stand-in for a SQLAlchemy Row: attribute access + ._mapping dict."""
    def __init__(self, **kw):
        object.__setattr__(self, "_data", dict(kw))
    @property
    def _mapping(self):
        return object.__getattribute__(self, "_data")
    def __getattr__(self, k):
        try:
            return object.__getattribute__(self, "_data")[k]
        except KeyError:
            raise AttributeError(k)


class Result:
    def __init__(self, rows=None, scalar=None, rowcount=1):
        self._rows = rows or []
        self._scalar = scalar
        self.rowcount = rowcount
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return self._rows
    def scalar(self):
        return self._scalar


def _route_common(db, up, params):
    """Shared routing for the recalc statements; returns (handled, Result) or (False, None)."""
    if up.lstrip().startswith("UPDATE") and "QUANTITY_UNITS" in up:
        db.article_updates.append(params)
        return True, Result()
    if "COALESCE(SUM" in up or "AS CNT" in up:          # the recalc box-aggregate SELECT
        return True, Result(rows=[db.agg])
    return False, None


class UpsertDB:
    def __init__(self, agg):
        self.agg = agg
        self.inserts = []
        self.article_updates = []
        self.commits = 0
    def execute(self, stmt, params=None):
        up = str(stmt).upper()
        handled, res = _route_common(self, up, params)
        if handled:
            return res
        if "SELECT TRANSACTION_NO FROM" in up:           # tx-exists check
            return Result(rows=[Row(transaction_no="TX1")])
        if "SELECT ID, BOX_ID FROM" in up:               # existing-box check -> none (new box)
            return Result(rows=[])
        if up.lstrip().startswith("INSERT INTO"):
            self.inserts.append(up)
            return Result()
        return Result()
    def commit(self):
        self.commits += 1


def test_upsert_box_recomputes_article_aggregates():
    db = UpsertDB(agg=SimpleNamespace(cnt=1720, net=11340, gross=11900))
    payload = SimpleNamespace(
        article_description="Onion 50kg", box_number=1720,
        net_weight=6.6, gross_weight=6.93, lot_number="L1", count=1,
    )
    upsert_box("CFPL", "INW-1000", payload, db)
    assert db.article_updates, "upsert_box must recompute the parent article after a box mutation"
    p = db.article_updates[0]
    assert p["quantity_units"] == 1720, p
    assert p["txno"] == "INW-1000" and p["art_desc"] == "Onion 50kg", p
    assert db.commits == 1, f"exactly one commit, got {db.commits}"
    print("PASS test_upsert_box_recomputes_article_aggregates")


class UpdateDB:
    def __init__(self, agg):
        self.agg = agg
        self.inserts = []
        self.article_updates = []
        self.commits = 0
    def execute(self, stmt, params=None):
        up = str(stmt).upper()
        handled, res = _route_common(self, up, params)
        if handled:
            return res
        if "COUNT(*)" in up:                              # plain article/box COUNT branches
            return Result(scalar=0)
        if "SELECT * FROM" in up and "WHERE TRANSACTION_NO" in up:
            if "_BOXES_V2" in up:
                return Result(rows=[])                    # no existing boxes -> insert path
            return Result(rows=[Row(transaction_no="TX1")])  # existing tx
        if up.lstrip().startswith("INSERT INTO"):
            self.inserts.append(up)
            return Result()
        return Result()
    def commit(self):
        self.commits += 1


def _box(desc, n):
    return SimpleNamespace(model_dump=lambda **k: {
        "transaction_no": "TX1", "article_description": desc, "box_number": n,
        "net_weight": 6.6, "gross_weight": 6.93, "lot_number": "L1", "count": 1,
    })


def test_update_inward_recomputes_article_after_box_insert():
    db = UpdateDB(agg=SimpleNamespace(cnt=1720, net=11340, gross=11900))
    payload = SimpleNamespace(
        transaction=SimpleNamespace(transaction_no="TX1", model_dump=lambda **k: {"transaction_no": "TX1"}),
        articles=[],
        boxes=[_box("Onion 50kg", 1720)],
    )
    update_inward("CFPL", "TX1", payload, db)
    assert db.article_updates, "update_inward must recompute the article after saving boxes"
    descs = {p["art_desc"] for p in db.article_updates}
    assert "Onion 50kg" in descs, descs
    assert db.commits == 1, f"exactly one commit, got {db.commits}"
    print("PASS test_update_inward_recomputes_article_after_box_insert")


def test_update_inward_recomputes_each_distinct_article_once():
    db = UpdateDB(agg=SimpleNamespace(cnt=2, net=12, gross=14))
    payload = SimpleNamespace(
        transaction=SimpleNamespace(transaction_no="TX1", model_dump=lambda **k: {"transaction_no": "TX1"}),
        articles=[],
        boxes=[_box("ART-A", 1), _box("ART-A", 2), _box("ART-B", 1)],
    )
    update_inward("CFPL", "TX1", payload, db)
    descs = sorted({p["art_desc"] for p in db.article_updates})
    assert descs == ["ART-A", "ART-B"], descs
    # ART-A appears in two boxes but must be recomputed once, not twice
    assert len(db.article_updates) == 2, db.article_updates
    print("PASS test_update_inward_recomputes_each_distinct_article_once")


ALL = [
    test_upsert_box_recomputes_article_aggregates,
    test_update_inward_recomputes_article_after_box_insert,
    test_update_inward_recomputes_each_distinct_article_once,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
