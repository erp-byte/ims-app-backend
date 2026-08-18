"""A cold receive discharges only the boxes it actually received.

THE BUGS THIS PINS

1. UNBOUNDED SENTINEL SWEEP (the dangerous one)
    `park_lines_in_pending` writes one `LINE-<line_id>-<n>` row per unit for
    box-less lines. The cold receive swept EVERY sentinel for the transfer as
    soon as `inserted > 0`.

    For a warehouse->cold transfer that is total loss: warehouse sources have no
    per-box cold stock, so park_lines_in_pending is the ONLY parker and every
    pending row is a sentinel. Receiving 1 box of 100 deleted all 100 rows,
    `_reconcile_statuses` counted 0 remaining and stamped BOTH headers
    'Received', and 99 units left the in-transit ledger with no shortage record
    and no route back — the transfer vanishes from the Pending Transfers modal.

    The interunit side already holds the invariant: `_claimed_pending_box_ids`
    hands out "at most as many sentinels as that article has unclaimed boxes",
    and `count_remaining_in_transit` keeps LINE- rows out of the completion gate.

    The bound is the number of boxes that consumed NO real pending row. A box
    that already deleted its own pending row must not also retire a sentinel, or
    one physical carton discharges two units of the consignment.

2. NON-IDEMPOTENT FINALIZE
    The loop did bare INSERTs into cold_transfer_inboxes and <company>_cold_stocks.
    finalize is documented "Idempotent on resume" and the hdr-exists branch
    UPDATEs the header then falls through to this loop without clearing prior
    boxes, so a second submit duplicated every box and every cold_stocks row.
    The cold_stocks UNIQUE (transaction_no, box_id) is no backstop: the TX-In page
    regenerates fresh {epoch}-{n} ids before submit, so a replay slips past it.

Dependency-free:  python test_cold_receive_sentinel_bound.py
"""
from types import SimpleNamespace

from services.ims_service.cold_transfer_in_tools import _process_box_loop


def _box(box_id, article="DATES", txn="TR-1", lot="L1"):
    return SimpleNamespace(
        box_id=box_id, transaction_no=txn, lot_no=lot, item_description=article,
        weight_kg=5.0, no_of_cartons=1.0, unit=None, cold_storage_data=None,
    )


class Res:
    def __init__(self, rows=None, rowcount=0):
        self._rows, self.rowcount = rows or [], rowcount
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows
    def scalar(self): return self._rows[0] if self._rows else None


class FakeDB:
    """Tracks sentinel deletes and box inserts.

    `real_pending` = box_ids that still have a genuine pending_transfer_stock row.
    `sentinels` = list of (id, article) LINE- rows currently In Transit.
    `existing` = (box_id, txn) pairs already recorded on this header.
    """

    def __init__(self, real_pending=(), sentinels=(), existing=()):
        self.real_pending = set(real_pending)
        self.sentinels = list(sentinels)
        self.existing = set(existing)
        self.inbox_inserts = []
        self.stock_inserts = []
        self.sentinels_deleted = []
        self.real_deleted = []

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        p = params or {}

        if "SELECT 1 FROM cold_transfer_inboxes" in sql:
            return Res([(1,)] if (p.get("bid"), p.get("txn")) in self.existing else [])

        if "FROM pending_transfer_stock" in sql and "SELECT id, cold_storage_data" in sql:
            if p.get("box_id") in self.real_pending:
                return Res([SimpleNamespace(_mapping={
                    "id": 900, "cold_storage_data": None, "weight_kg": 5.0,
                    "item_description": "DATES", "no_of_cartons": 1.0})])
            return Res([])

        if "INSERT INTO cold_transfer_inboxes" in sql:
            self.inbox_inserts.append(p.get("box_id")); return Res()
        if "INSERT INTO" in sql and "cold_stocks" in sql:
            self.stock_inserts.append(p.get("box_id")); return Res()

        if "DELETE FROM pending_transfer_stock WHERE id = :pid" in sql:
            self.real_deleted.append(p.get("pid")); return Res()

        if "DELETE FROM pending_transfer_stock" in sql and "LIKE 'LINE-%'" in sql:
            n = int(p.get("n") or 0)
            art = p.get("article")
            pool = [s for s in self.sentinels
                    if art is None or (s[1] or "").strip().upper() == art]
            taken = pool[:n]
            for t in taken:
                self.sentinels.remove(t)
                self.sentinels_deleted.append(t[0])
            return Res(rowcount=len(taken))

        return Res()


def _sentinels(n, article="DATES", start=1):
    return [(start + i, article) for i in range(n)]


def test_one_box_of_a_hundred_retires_one_sentinel():
    """The reported catastrophe: 100 LINE- units, 1 box received."""
    db = FakeDB(sentinels=_sentinels(100))
    n = _process_box_loop(db, 5, [_box("1700000-1")], "cfpl_cold_stocks", transfer_out_id=42)
    assert n == 1, n
    assert len(db.sentinels_deleted) == 1, (
        f"exactly one sentinel may be retired, {len(db.sentinels_deleted)} were")
    assert len(db.sentinels) == 99, (
        f"99 units must stay in transit, {len(db.sentinels)} remain")


def test_a_full_receipt_clears_every_sentinel():
    """The bound must not block a genuine complete receive."""
    db = FakeDB(sentinels=_sentinels(10))
    boxes = [_box(f"1700000-{i}") for i in range(10)]
    _process_box_loop(db, 5, boxes, "cfpl_cold_stocks", transfer_out_id=42)
    assert db.sentinels == [], f"a complete receipt must clear the bridge: {db.sentinels}"


def test_a_box_with_a_real_pending_row_does_not_also_eat_a_sentinel():
    """Otherwise one physical carton discharges two units of the consignment."""
    db = FakeDB(real_pending={"1700000-1"}, sentinels=_sentinels(5))
    _process_box_loop(db, 5, [_box("1700000-1")], "cfpl_cold_stocks", transfer_out_id=42)
    assert db.real_deleted == [900], db.real_deleted
    assert db.sentinels_deleted == [], (
        "the box consumed its own pending row; it must not retire a sentinel too")
    assert len(db.sentinels) == 5


def test_sentinels_are_matched_by_article():
    """A mixed consignment discharges the line actually received against."""
    db = FakeDB(sentinels=_sentinels(3, "DATES", 1) + _sentinels(3, "RAISINS", 10))
    _process_box_loop(db, 5, [_box("b1", article="RAISINS")],
                      "cfpl_cold_stocks", transfer_out_id=42)
    assert db.sentinels_deleted == [10], (
        f"should retire a RAISINS sentinel, retired {db.sentinels_deleted}")


def test_an_unmatched_article_still_discharges_one_unit():
    """Blank/renamed article on the line must not strand the receipt."""
    db = FakeDB(sentinels=_sentinels(4, "DATES"))
    _process_box_loop(db, 5, [_box("b1", article="SOMETHING ELSE")],
                      "cfpl_cold_stocks", transfer_out_id=42)
    assert len(db.sentinels_deleted) == 1, db.sentinels_deleted
    assert len(db.sentinels) == 3


def test_no_sentinels_swept_when_nothing_was_received():
    db = FakeDB(sentinels=_sentinels(10))
    n = _process_box_loop(db, 5, [], "cfpl_cold_stocks", transfer_out_id=42)
    assert n == 0 and db.sentinels_deleted == [] and len(db.sentinels) == 10


def test_resubmitting_the_same_payload_inserts_nothing():
    """Idempotency: a replayed finalize must not double cold_stocks."""
    db = FakeDB(sentinels=_sentinels(5),
                existing={("1700000-1", "TR-1"), ("1700000-2", "TR-1")})
    n = _process_box_loop(db, 5, [_box("1700000-1"), _box("1700000-2")],
                          "cfpl_cold_stocks", transfer_out_id=42)
    assert n == 0, f"already-recorded boxes must not re-insert, got {n}"
    assert db.inbox_inserts == [], db.inbox_inserts
    assert db.stock_inserts == [], db.stock_inserts
    assert db.sentinels_deleted == [], "a no-op replay must not retire sentinels"


def test_a_partial_replay_inserts_only_the_new_box():
    db = FakeDB(sentinels=_sentinels(5), existing={("1700000-1", "TR-1")})
    n = _process_box_loop(db, 5, [_box("1700000-1"), _box("1700000-2")],
                          "cfpl_cold_stocks", transfer_out_id=42)
    assert n == 1, n
    assert db.stock_inserts == ["1700000-2"], db.stock_inserts
    assert len(db.sentinels_deleted) == 1


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
        raise SystemExit(f"{failures} cold-receive test(s) failed.")
    print("All cold-receive sentinel-bound tests passed.")
