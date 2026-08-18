"""The backfill must treat a cold receive as a receive.

THE BUG THIS PINS
    `_already_received` decides whether the backfill re-parks a dispatched box.
    It queried ONE receipt ledger — interunit_transfer_in_boxes/_header — but
    there are two, and the cold path is the one that erases the other:

      * a cold receive writes cold_transfer_in_headers + cold_transfer_inboxes;
      * on completion it DELETEs the interunit staging header + boxes
        (cold_transfer_in_tools.py:348-358), making the cold tables the system
        of record by design;
      * the out-header only flips to 'Received' when pending_remaining hits 0,
        so a PARTIALLY cold-received transfer stays 'Dispatch' and remains a
        backfill candidate.

    So every cold-received box read as never-received. And because the receipt
    re-inserts the box under the SAME (box_id, transaction_no) at the
    destination (cold_transfer_in_tools.py:685-702), `_find_in_cold_stocks`
    resolved to that DESTINATION row and the backfill ran
    `DELETE FROM <company>_cold_stocks WHERE id = :rid` on it — physically
    present stock vanishing from the cold sheet and reappearing as 'In Transit'.

    That is the 1,960-row incident `_already_received`'s own docstring
    describes, reproduced through the path its fix never covered. It is reachable
    from the UI: PendingTransfersModal fires the backfill on every modal open and
    `dry_run` defaults to False.

    `_already_received` fails OPEN (parks when it cannot prove receipt), so a
    blind UNION over a missing cold table would raise and land on the destructive
    branch. The gate is therefore built from the ledgers that actually exist.

Dependency-free:  python test_backfill_cold_receipt_gate.py
"""
from services.ims_service.pending_stock_tools import _already_received

INTERUNIT = ("interunit_transfer_in_boxes", "interunit_transfer_in_header")
COLD = ("cold_transfer_inboxes", "cold_transfer_in_headers")


class FakeResult:
    def __init__(self, value):
        self._v = value

    def scalar(self):
        return self._v

    def fetchone(self):
        return (1,) if self._v else None


class FakeDB:
    """Answers to_regclass from `tables`, and the receipt probe from `receipts`.

    `receipts` maps a box-ledger table name -> set of (box_id, transfer_out_id).
    The probe SQL names every ledger it covers, so the stub reports a hit when
    any named ledger holds the pair — which is exactly the UNION's semantics.
    """

    def __init__(self, tables, receipts=None, raise_on_probe=False):
        self.tables = set(tables)
        self.receipts = receipts or {}
        self.raise_on_probe = raise_on_probe
        self.probes = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}
        if "to_regclass" in sql:
            return FakeResult(params["t"].replace("public.", "") in self.tables)
        if self.raise_on_probe:
            raise RuntimeError("connection reset")
        self.probes.append(sql)
        key = (params.get("bid"), params.get("toid"))
        hit = any(key in self.receipts.get(tbl, set())
                  for tbl in self.receipts if tbl in sql)
        return FakeResult(hit)


ALL = [*INTERUNIT, *COLD]


def test_a_cold_receive_counts_as_received():
    """The regression: only the cold ledger knows, because the cold path purged
    the interunit rows this gate used to be the only one reading."""
    db = FakeDB(ALL, {"cold_transfer_inboxes": {("BOX-1", 77)}})
    assert _already_received(db, "BOX-1", 77) is True, (
        "a cold-received box must not be re-parked — that DELETEs it from cold_stocks"
    )


def test_an_interunit_receive_still_counts():
    db = FakeDB(ALL, {"interunit_transfer_in_boxes": {("BOX-1", 77)}})
    assert _already_received(db, "BOX-1", 77) is True


def test_an_unreceived_box_is_still_parked():
    db = FakeDB(ALL, {})
    assert _already_received(db, "BOX-1", 77) is False, (
        "genuinely unreceived stock must still be parked"
    )


def test_receipt_on_another_transfer_does_not_suppress_this_one():
    """Box ids repeat across GRNs — the gate is scoped to ONE transfer."""
    db = FakeDB(ALL, {"cold_transfer_inboxes": {("BOX-1", 999)}})
    assert _already_received(db, "BOX-1", 77) is False


def test_both_ledgers_are_queried():
    db = FakeDB(ALL, {})
    _already_received(db, "BOX-1", 77)
    probe = " ".join(db.probes)
    for tbl in ALL:
        assert tbl in probe, f"{tbl} was never consulted"


def test_a_missing_cold_table_does_not_break_the_gate():
    """Older DBs have no cold ledger; the gate must degrade, not raise."""
    db = FakeDB(INTERUNIT, {"interunit_transfer_in_boxes": {("BOX-1", 77)}})
    assert _already_received(db, "BOX-1", 77) is True
    probe = " ".join(db.probes)
    assert "cold_transfer_inboxes" not in probe, (
        "a missing table must be left out of the SQL, not UNIONed blind — the "
        "resulting error would fail OPEN and re-park received stock"
    )


def test_no_ledger_at_all_means_not_proven_received():
    db = FakeDB([])
    assert _already_received(db, "BOX-1", 77) is False
    assert db.probes == [], "nothing to probe when neither ledger exists"


def test_a_probe_failure_fails_open():
    """Documented policy: unprovable receipt parks the box for a human to see."""
    db = FakeDB(ALL, raise_on_probe=True)
    assert _already_received(db, "BOX-1", 77) is False


def test_blank_inputs_are_not_treated_as_received():
    db = FakeDB(ALL, {})
    assert _already_received(db, None, 77) is False
    assert _already_received(db, "", 77) is False
    assert _already_received(db, "BOX-1", None) is False


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
        raise SystemExit(f"{failures} backfill-receipt-gate test(s) failed.")
    print("All backfill cold-receipt-gate tests passed.")
