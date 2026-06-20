"""Dependency-free unit tests for Item 1A: the one-time backfill.

The runtime recalc (recalc_article_aggregates wired into upsert_box / update_inward) stops NEW
drift, but rows that already desynced (e.g. the article stuck at 1000 while it has 1720 boxes)
need a one-shot repair. backfill_article_aggregates scans every article that has boxes for a
given table set and recomputes its aggregates. Safe-by-default: dry-run unless apply=True.

Mocks route SQL substrings to canned results (same style as test_grn_autofinalize.py).
No database required:  python test_backfill_article_aggregates.py
"""
from types import SimpleNamespace

from services.ims_service.inward_tools import backfill_article_aggregates


class Row:
    def __init__(self, **kw):
        object.__setattr__(self, "_data", dict(kw))
    def __getattr__(self, k):
        try:
            return object.__getattribute__(self, "_data")[k]
        except KeyError:
            raise AttributeError(k)


class Result:
    def __init__(self, rows=None):
        self._rows = rows or []
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return self._rows


class BackfillDB:
    def __init__(self, pairs, agg):
        self._pairs = [Row(transaction_no=t, article_description=a) for t, a in pairs]
        self._agg = agg
        self.article_updates = []
        self.commits = 0
    def execute(self, stmt, params=None):
        up = str(stmt).upper()
        if up.lstrip().startswith("UPDATE") and "QUANTITY_UNITS" in up:
            self.article_updates.append(params)
            return Result()
        if "COALESCE(SUM" in up or "AS CNT" in up:       # recalc box-aggregate SELECT
            return Result(rows=[self._agg])
        if "DISTINCT" in up and "ARTICLE_DESCRIPTION" in up:
            return Result(rows=self._pairs)
        return Result()
    def commit(self):
        self.commits += 1


CFPL = {"art": "cfpl_articles_v2", "box": "cfpl_boxes_v2"}


def test_backfill_dry_run_scans_but_writes_nothing():
    db = BackfillDB(
        pairs=[("INW-1000", "Onion 50kg"), ("INW-2", "Garlic")],
        agg=SimpleNamespace(cnt=1720, net=11340, gross=11900),
    )
    summary = backfill_article_aggregates(db, CFPL, apply=False)
    assert db.article_updates == [], "dry-run must not write"
    assert db.commits == 0, "dry-run must not commit"
    assert summary["articles_with_boxes"] == 2, summary
    assert summary["articles_recomputed"] == 0, summary
    assert summary["applied"] is False, summary
    print("PASS test_backfill_dry_run_scans_but_writes_nothing")


def test_backfill_apply_recomputes_every_article_with_boxes():
    db = BackfillDB(
        pairs=[("INW-1000", "Onion 50kg"), ("INW-2", "Garlic")],
        agg=SimpleNamespace(cnt=1720, net=11340, gross=11900),
    )
    summary = backfill_article_aggregates(db, CFPL, apply=True)
    assert len(db.article_updates) == 2, db.article_updates
    assert db.commits == 1, f"one commit for the whole backfill, got {db.commits}"
    assert summary["articles_recomputed"] == 2, summary
    assert summary["applied"] is True, summary
    # the stuck 1000/1720 entry is repaired to the box count + summed weights
    onion = [f for f in summary["fixed"] if f["transaction_no"] == "INW-1000"][0]
    assert onion["quantity_units"] == 1720 and onion["net_weight"] == 11340, onion
    print("PASS test_backfill_apply_recomputes_every_article_with_boxes")


def test_backfill_no_boxes_is_a_noop():
    db = BackfillDB(pairs=[], agg=SimpleNamespace(cnt=0, net=0, gross=0))
    summary = backfill_article_aggregates(db, CFPL, apply=True)
    assert summary["articles_with_boxes"] == 0 and summary["articles_recomputed"] == 0, summary
    assert db.article_updates == [], db.article_updates
    print("PASS test_backfill_no_boxes_is_a_noop")


ALL = [
    test_backfill_dry_run_scans_but_writes_nothing,
    test_backfill_apply_recomputes_every_article_with_boxes,
    test_backfill_no_boxes_is_a_noop,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
