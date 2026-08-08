"""The backfill must never re-park a box that has already been received.

THE BUG THIS PINS
    `backfill_pending_from_existing_transfers` walks dispatched transfers and
    parks a pending row per box. It read interunit_transfer_boxes — the DISPATCH
    record, which is permanent — and never consulted interunit_transfer_in_boxes,
    the receipt. So for a transfer completed in April it saw the dispatch rows,
    found no pending row (the receipt had correctly deleted them), and parked
    them again in June.

    1,960 rows / 18,108 kg across 30 challans came back that way, 1,757 of them
    in one run on 2026-06-05. Every re-parked row was created AFTER its own GRN.
    Nothing re-runs the pick afterwards, so they sat In Transit indefinitely and
    were counted both in transit and as received.

    The transfer-level filter did not catch it: it excludes transfers whose
    transfer-in header status is 'received', and all 30 of these sat at
    'Pending' — the GRN was created and the boxes scanned, but never finalised.
    Receipt is recorded per BOX, so the guard has to be per box too.

Dependency-free:  python test_backfill_skips_received.py
"""
from services.ims_service.pending_stock_tools import _already_received


class FakeDB:
    """Answers the receipt-lookup query from a set of (box_id, transfer_out_id)."""

    def __init__(self, received=()):
        self.received = set(received)
        self.queries = 0

    def execute(self, stmt, params=None):
        self.queries += 1
        params = params or {}
        key = (params.get("bid"), params.get("toid"))
        hit = key in self.received
        return _Result([(1,)] if hit else [])


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


def test_a_received_box_is_skipped():
    """The exact shape of the bug: box was received, backfill must not re-park."""
    db = FakeDB(received={("90584000-2", 41)})
    assert _already_received(db, "90584000-2", 41) is True


def test_an_unreceived_box_is_parked():
    db = FakeDB(received={("90584000-2", 41)})
    assert _already_received(db, "90584000-9", 41) is False


def test_receipt_is_scoped_to_the_SAME_transfer():
    """Box ids repeat across GRNs (1,511 real ids span multiple GRNs), so a
    receipt on a different transfer must not block this one."""
    db = FakeDB(received={("90584000-2", 41)})
    assert _already_received(db, "90584000-2", 99) is False


def test_missing_inputs_do_not_claim_receipt():
    """Fail open here: with no box id we cannot prove receipt, and wrongly
    claiming it would silently drop stock from the backfill."""
    db = FakeDB()
    assert _already_received(db, "", 41) is False
    assert _already_received(db, None, 41) is False
    assert _already_received(db, "X-1", None) is False
    assert db.queries == 0, "no id -> should not even hit the database"


def test_a_db_error_does_not_claim_receipt():
    class Boom:
        def execute(self, *a, **k):
            raise RuntimeError("connection lost")
    assert _already_received(Boom(), "X-1", 41) is False


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
    print("\nA received box is never re-parked.")
