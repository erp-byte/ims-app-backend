"""The repair tool must compute the TRUE consignment, not a plausible one.

This runs repair_transfer_data's analyze/repair against stubbed rows shaped like
the real damage, so the arithmetic is proven before it is pointed at production.

The case that matters is TRANS202608171318: 100 boxes across 2 piles, written as
100 line rows, SUM(qty) = 198 (2B - K). The tool must report TRUE=100, collapse to
2 lines, delete 98 orphans, and clear only the phantom sentinels — never the
legitimate ones belonging to box-less manually typed rows.

Dependency-free:  python test_repair_transfer_data.py
"""
import os

os.environ.setdefault("DATABASE_URL", "postgresql://u:p@localhost/stub")

from types import SimpleNamespace  # noqa: E402

import repair_transfer_data as R  # noqa: E402


class Res:
    def __init__(self, rows=None, scalar=None, rowcount=0):
        self._rows, self._scalar, self.rowcount = rows or [], scalar, rowcount
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar


class StubDB:
    """Serves one transfer's rows and records every mutation."""

    def __init__(self, boxes, lines, sentinels=()):
        self.boxes = boxes            # [(article, lot, net, gross)]
        self.lines = lines            # [(id, article, lot, qty)]
        self.sentinels = list(sentinels)   # [(id, article, lot)]
        self.updates, self.deleted_lines, self.deleted_pending = [], [], []
        self.repointed = 0
        self.status_set = None

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        p = params or {}

        if "SELECT article, lot_number FROM interunit_transfer_boxes" in sql:
            return Res([SimpleNamespace(article=a, lot_number=l)
                        for a, l, _, _ in self.boxes])
        if "SELECT id, article, lot_number, net_weight, gross_weight" in sql:
            return Res([SimpleNamespace(id=i, article=a, lot_number=l,
                                        net_weight=n, gross_weight=g)
                        for i, (a, l, n, g) in enumerate(self.boxes, start=1)])
        if "GROUP BY item_desc_raw, lot_number" in sql:
            agg = {}
            for _, a, l, q in self.lines:
                agg[(a, l)] = agg.get((a, l), 0) + q
            return Res([SimpleNamespace(item_desc_raw=a, lot_number=l, q=q)
                        for (a, l), q in agg.items()])
        if "SELECT id, item_desc_raw, lot_number, qty" in sql:
            return Res([SimpleNamespace(id=i, item_desc_raw=a, lot_number=l, qty=q)
                        for i, a, l, q in self.lines])
        if "SELECT COUNT(*) FROM pending_transfer_stock" in sql:
            return Res(scalar=len(self.sentinels))
        # analyze() selects (article, lot_no); repair() selects (id, article, lot_no).
        if "FROM pending_transfer_stock" in sql and "article" in sql and "SELECT" in sql:
            return Res([SimpleNamespace(id=i, article=a, lot_no=l)
                        for i, a, l in self.sentinels])
        if "UPDATE interunit_transfers_lines" in sql:
            self.updates.append((p["id"], p["q"], p["n"])); return Res()
        if "UPDATE interunit_transfer_boxes SET transfer_line_id" in sql:
            self.repointed += 1; return Res(rowcount=1)
        if "DELETE FROM interunit_transfers_lines" in sql:
            self.deleted_lines = list(p["ids"]); return Res(rowcount=len(p["ids"]))
        if "DELETE FROM pending_transfer_stock" in sql:
            ids = list(p["ids"]); self.deleted_pending = ids
            self.sentinels = [s for s in self.sentinels if s[0] not in ids]
            return Res(rowcount=len(ids))
        if "UPDATE interunit_transfers_header SET status" in sql:
            self.status_set = p["s"]; return Res()
        if "SELECT COALESCE(SUM(qty),0)" in sql:
            corrected = {i: q for i, q, _ in self.updates}
            total = sum(corrected.get(i, q) for i, _, _, q in self.lines
                        if i not in self.deleted_lines)
            return Res(scalar=total)
        return Res()


A, B = "BLUEBERRY POUCH 100 GM", "NUTS AND SEED MIX 100 GM"


def _t(**kw):
    base = dict(id=1, challan_no="TRANS202608171318", from_site="Cold Storage",
                to_site="A185", status="Dispatch", line_rows=100, real_lines=2,
                sum_qty=198, boxes=100, sentinels=0, real_pending=100)
    base.update(kw)
    return SimpleNamespace(**base)


def _the_1318_shape():
    boxes = ([(A, "L1", 1.5, 1.5)] * 60) + ([(B, "L2", 2.0, 2.0)] * 40)
    lines = ([(i, A, "L1", 1) for i in range(1, 61)]
             + [(i, B, "L2", 1) for i in range(61, 101)])
    # _apply_box_totals corrected the last line of each pile; the rest kept qty 1.
    lines[59] = (60, A, "L1", 60)
    lines[99] = (100, B, "L2", 40)
    return boxes, lines


def test_reports_the_true_consignment_not_the_displayed_qty():
    boxes, lines = _the_1318_shape()
    db = StubDB(boxes, lines)
    assert sum(l[3] for l in lines) == 198, "fixture must reproduce 2B-K"
    a = R.analyze(db, _t(), received=0)
    assert a["ordered"] == 100, f"TRUE must be the box count, got {a['ordered']}"
    assert "QTY_INFLATED" in a["flags"], a["flags"]
    assert "PER_BOX_LINES" in a["flags"], a["flags"]
    assert a["expected"] == 100, a


def test_repair_collapses_to_one_line_per_pile():
    boxes, lines = _the_1318_shape()
    db = StubDB(boxes, lines)
    r = R.repair(db, _t(), received=0, ordered=100)
    assert r["lines_deleted"] == 98, r
    assert r["new_qty"] == 100, r
    assert sorted(q for _, q, _ in db.updates) == [40, 60], db.updates
    assert db.repointed == 100, "every box must point at its surviving line"


def test_weights_are_rebuilt_from_the_boxes():
    boxes, lines = _the_1318_shape()
    db = StubDB(boxes, lines)
    R.repair(db, _t(), received=0, ordered=100)
    nets = sorted(round(n, 3) for _, _, n in db.updates)
    assert nets == [80.0, 90.0], f"60x1.5 and 40x2.0 expected, got {nets}"


def test_a_legitimate_sentinel_is_never_deleted():
    """A box-less manually typed row's LINE- rows are real in-transit units."""
    boxes = [(A, "L1", 1.5, 1.5)] * 10
    lines = [(1, A, "L1", 10), (2, "TYPED ITEM", "L9", 5)]
    sentinels = [(500 + i, "TYPED ITEM", "L9") for i in range(5)]
    db = StubDB(boxes, lines, sentinels)
    a = R.analyze(db, _t(line_rows=2, real_lines=2, sum_qty=15, boxes=10,
                         sentinels=5, real_pending=10), received=0)
    assert a["phantom"] == 0, f"typed-row sentinels are legitimate: {a}"
    assert a["ordered"] == 15, f"10 boxes + 5 typed units: {a}"
    r = R.repair(db, _t(), received=0, ordered=15)
    assert r["phantom_deleted"] == 0, "must not delete a real in-transit unit"
    assert len(db.sentinels) == 5


def test_phantom_sentinels_on_a_box_backed_pile_are_deleted():
    boxes = [(A, "L1", 1.5, 1.5)] * 10
    lines = [(1, A, "L1", 10)]
    sentinels = [(600 + i, A, "L1") for i in range(8)]   # phantom: pile shipped boxes
    db = StubDB(boxes, lines, sentinels)
    a = R.analyze(db, _t(line_rows=1, real_lines=1, sum_qty=10, boxes=10,
                         sentinels=8, real_pending=10), received=0)
    assert a["phantom"] == 8, a
    r = R.repair(db, _t(), received=0, ordered=10)
    assert r["phantom_deleted"] == 8, r
    assert db.sentinels == []


def test_a_ghost_received_transfer_is_reopened():
    """The sentinel-sweep bug stamped 'Received' after one box. Must go back."""
    boxes, lines = _the_1318_shape()
    db = StubDB(boxes, lines)
    t = _t(status="Received")
    a = R.analyze(db, t, received=1)
    assert "GHOST_RECEIVED" in a["flags"], a["flags"]
    r = R.repair(db, t, received=1, ordered=100)
    assert db.status_set == "Partial", db.status_set
    assert r["status"] == "Received -> Partial", r


def test_a_genuinely_complete_transfer_stays_received():
    boxes, lines = _the_1318_shape()
    db = StubDB(boxes, lines)
    R.repair(db, _t(status="Received"), received=100, ordered=100)
    assert db.status_set is None, "no status change needed"


def test_an_untouched_transfer_is_not_flagged():
    boxes = [(A, "L1", 1.5, 1.5)] * 16
    lines = [(1, A, "L1", 16)]
    db = StubDB(boxes, lines)
    a = R.analyze(db, _t(line_rows=1, real_lines=1, sum_qty=16, boxes=16,
                         sentinels=0, real_pending=16), received=0)
    assert a["flags"] == [], f"a clean transfer must not be touched: {a['flags']}"


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"  ok  {name}")
        except AssertionError as exc:
            failures += 1
            print(f"  FAIL {name}: {exc}")
    print()
    if failures:
        raise SystemExit(f"{failures} repair-tool test(s) failed.")
    print("All repair-tool tests passed.")
