"""Dependency-free unit tests for the GRN auto-finalize fix.

Bug: transfer-INs got their boxes acknowledged but `finalize` was never called, so the GRN
header stayed 'Pending', the transfer-OUT never became 'Received', pending_transfer_stock was
never picked, and the dispatch lingered in the Pending modal as "Partial (GRN raised)".

Fix (in services/ims_service/interunit_tools.py):
  - _autofinalize_if_complete(db, header_id): finalizes once acked boxes cover the in-transit
    set (acked >= in_transit > 0), SAVEPOINT-isolated so a finalize failure never corrupts the
    acknowledgement. acknowledge_pending_box / ..._batch call it.
  - finalize_transfer_in: idempotent — a second call on a 'Received' GRN is a no-op, not a 400.
  - finalize_complete_pending_grns(db, dry_run): backlog sweep over the stuck GRNs.

Mocks route SQL substrings to canned results (same style as test_reconcile_transfer_to_order.py).
No database required:  python test_grn_autofinalize.py
"""
from contextlib import contextmanager
from types import SimpleNamespace

from services.ims_service import interunit_tools as I


class Res:
    def __init__(self, rows=None, scalar=None):
        self._rows, self._scalar = rows or [], scalar
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar


@contextmanager
def _patch_finalize(stub):
    """Swap interunit_tools.finalize_transfer_in for a recording stub, then restore."""
    orig = I.finalize_transfer_in
    I.finalize_transfer_in = stub
    try:
        yield
    finally:
        I.finalize_transfer_in = orig


class _NestedTxn:
    """Minimal db.begin_nested() context manager for the SAVEPOINT in _autofinalize."""
    def __enter__(self): return self
    def __exit__(self, *a): return False


# ---------------------------------------------------------------- _autofinalize_if_complete
def _autofinalize_db(status, acked, in_transit, on_finalize_raise=False):
    calls = []
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "FROM interunit_transfer_in_header WHERE id" in sql:
                return Res(rows=[SimpleNamespace(id=5, status=status, transfer_out_id=42)])
            if "COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id" in sql:
                return Res(scalar=acked)
            if "FROM pending_transfer_stock" in sql and "In Transit" in sql:
                return Res(scalar=in_transit)
            return Res()
        def begin_nested(self): return _NestedTxn()
    def stub(header_id, data, db):
        calls.append(header_id)
        if on_finalize_raise:
            raise RuntimeError("simulated pick_from_pending failure")
        return {"id": header_id, "status": "Received"}
    return DB(), stub, calls


def test_autofinalize_finalizes_when_acked_covers_in_transit():
    db, stub, calls = _autofinalize_db("Pending", acked=176, in_transit=176)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is True
    assert calls == [5], calls
    print("PASS test_autofinalize_finalizes_when_acked_covers_in_transit")


def test_autofinalize_finalizes_when_acked_exceeds_in_transit():
    # Over-acknowledged (extra/replacement boxes) still counts as complete.
    db, stub, calls = _autofinalize_db("Pending", acked=61, in_transit=44)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is True
    assert calls == [5], calls
    print("PASS test_autofinalize_finalizes_when_acked_exceeds_in_transit")


def test_autofinalize_skips_when_incomplete():
    db, stub, calls = _autofinalize_db("Pending", acked=100, in_transit=176)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is False
    assert calls == [], "must not finalize a partial receipt"
    print("PASS test_autofinalize_skips_when_incomplete")


def test_autofinalize_skips_when_already_received():
    db, stub, calls = _autofinalize_db("Received", acked=176, in_transit=176)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is False
    assert calls == [], "idempotent: already-Received GRN must not re-finalize"
    print("PASS test_autofinalize_skips_when_already_received")


def test_autofinalize_skips_when_nothing_in_transit():
    # Nothing in transit (already picked / legacy) — must not finalize on a stray ack.
    db, stub, calls = _autofinalize_db("Pending", acked=3, in_transit=0)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is False
    assert calls == [], calls
    print("PASS test_autofinalize_skips_when_nothing_in_transit")


def test_autofinalize_swallows_finalize_failure():
    # Finalize blows up inside the SAVEPOINT -> return False, acknowledgement preserved.
    db, stub, calls = _autofinalize_db("Pending", acked=176, in_transit=176, on_finalize_raise=True)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is False
    assert calls == [5], "finalize was attempted"
    print("PASS test_autofinalize_swallows_finalize_failure")


# ---------------------------------------------------------------- idempotent finalize
def test_finalize_transfer_in_idempotent_when_received():
    writes = []
    full = SimpleNamespace(
        id=5, transfer_out_id=42, transfer_out_no="TRANS261427", grn_number="GRN-1",
        grn_date=None, receiving_warehouse="W202", received_by="u", received_at=None,
        box_condition="Good", condition_remarks=None, status="Received",
        created_at=None, updated_at=None,
    )
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt); up = sql.upper()
            if any(k in up for k in ("INSERT ", "DELETE ", "UPDATE ")):
                writes.append(up.split()[0]); return Res()
            if "receiving_warehouse" in sql and "FROM interunit_transfer_in_header WHERE id" in sql:
                return Res(rows=[full])                       # the full re-fetch
            if "FROM interunit_transfer_in_header WHERE id" in sql:
                return Res(rows=[SimpleNamespace(id=5, status="Received",
                                                 transfer_out_id=42, transfer_out_no="TRANS261427")])
            if "FROM interunit_transfer_in_boxes" in sql:     # _fetch_transfer_in_boxes
                return Res(rows=[])
            return Res()
    res = I.finalize_transfer_in(5, I.FinalizeTransferIn(), DB())
    assert res.get("already_finalized") is True, res
    assert res["status"] == "Received" and res["boxes"] == [], res
    assert writes == [], f"idempotent finalize must not write, but did: {writes}"
    print("PASS test_finalize_transfer_in_idempotent_when_received")


# ---------------------------------------------------------------- backlog sweep
def _sweep_db(rows):
    calls, commits = [], []
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "FROM interunit_transfer_in_header tih" in sql and "tih.status = 'Pending'" in sql:
                return Res(rows=rows)
            return Res()
        def commit(self): commits.append(1)
    def stub(header_id, data, db):
        calls.append(header_id)
        return {"id": header_id, "status": "Received"}
    return DB(), stub, calls, commits


def _grn(grn_id, toid, acked, in_transit):
    return SimpleNamespace(grn_id=grn_id, transfer_out_id=toid,
                           grn_number=f"GRN-{grn_id}", acked=acked, in_transit=in_transit)


def test_sweep_dry_run_writes_nothing_and_classifies():
    # GRN 2 is over-acknowledged (acked 61 >= in_transit 44) -> still complete.
    rows = [_grn(1, 101, 176, 176), _grn(2, 102, 61, 44), _grn(3, 103, 10, 50)]
    db, stub, calls, commits = _sweep_db(rows)
    with _patch_finalize(stub):
        summary = I.finalize_complete_pending_grns(db, dry_run=True)
    assert calls == [] and commits == [], "dry-run must not finalize or commit"
    assert summary["pending_grns_scanned"] == 3
    assert [r["grn_id"] for r in summary["finalized"]] == [1, 2], summary
    assert [r["grn_id"] for r in summary["skipped"]] == [3], summary
    print("PASS test_sweep_dry_run_writes_nothing_and_classifies")


def test_sweep_apply_finalizes_complete_grns_only():
    # GRN 2 is over-acknowledged (acked 61 >= in_transit 44) -> still complete.
    rows = [_grn(1, 101, 176, 176), _grn(2, 102, 61, 44), _grn(3, 103, 10, 50)]
    db, stub, calls, commits = _sweep_db(rows)
    with _patch_finalize(stub):
        summary = I.finalize_complete_pending_grns(db, dry_run=False)
    assert calls == [1, 2], f"only complete GRNs finalized: {calls}"
    assert len(commits) == 2, f"one commit per finalized GRN: {commits}"
    assert [r["grn_id"] for r in summary["skipped"]] == [3], summary
    print("PASS test_sweep_apply_finalizes_complete_grns_only")


def test_sweep_skips_grn_with_zero_in_transit():
    # acked>0 but nothing in transit -> not complete (don't finalize an empty dispatch).
    rows = [_grn(1, 101, 5, 0)]
    db, stub, calls, commits = _sweep_db(rows)
    with _patch_finalize(stub):
        summary = I.finalize_complete_pending_grns(db, dry_run=False)
    assert calls == [] and commits == [], calls
    assert [r["grn_id"] for r in summary["skipped"]] == [1], summary
    print("PASS test_sweep_skips_grn_with_zero_in_transit")


ALL = [
    test_autofinalize_finalizes_when_acked_covers_in_transit,
    test_autofinalize_finalizes_when_acked_exceeds_in_transit,
    test_autofinalize_skips_when_incomplete,
    test_autofinalize_skips_when_already_received,
    test_autofinalize_skips_when_nothing_in_transit,
    test_autofinalize_swallows_finalize_failure,
    test_finalize_transfer_in_idempotent_when_received,
    test_sweep_dry_run_writes_nothing_and_classifies,
    test_sweep_apply_finalizes_complete_grns_only,
    test_sweep_skips_grn_with_zero_in_transit,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
