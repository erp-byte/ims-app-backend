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


class Row(SimpleNamespace):
    """A SQLAlchemy Row stand-in.

    Production code reads some columns by attribute and others through
    `row._mapping[...]` (the cold-destination guard in finalize_transfer_in does
    the latter). A plain SimpleNamespace only supports the first, so a mock built
    from one fails on any code path that uses _mapping — which looks like a
    production bug but is only the mock being narrower than a real Row.
    """
    @property
    def _mapping(self):
        return self.__dict__


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


def test_autofinalize_refuses_on_count_parity_without_box_evidence():
    """176 acknowledged against 176 in transit used to be treated as complete.

    It is not. The counts can match while every outstanding row names a box the
    GRN never recorded, which is exactly how a short receipt looks once its
    acknowledged boxes have already been picked. Completeness has to name boxes.
    """
    db, stub, calls = _autofinalize_db("Pending", acked=176, in_transit=176)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is False
    assert calls == [], "count parity alone is not evidence of receipt"
    print("PASS test_autofinalize_refuses_on_count_parity_without_box_evidence")


def test_autofinalize_refuses_when_over_acknowledged_without_box_evidence():
    """Over-acknowledging (61 boxes against 44 outstanding) was read as complete.

    On the live data that shape belongs to receipts whose counts do not reconcile
    against the dispatch at all — more boxes recorded than the challan declared.
    Those need a human, not an automatic close.
    """
    db, stub, calls = _autofinalize_db("Pending", acked=61, in_transit=44)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is False
    assert calls == [], "an unreconciled over-receipt must not auto-close"
    print("PASS test_autofinalize_refuses_when_over_acknowledged_without_box_evidence")


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
    # Needs genuinely claimable boxes, otherwise completeness refuses before finalize runs.
    db, stub, calls = _claim_db(
        "Pending", [("B1", "Deri Dates", None)], [("B1", "Deri Dates")], on_finalize_raise=True)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is False
    assert calls == [5], "finalize was attempted"
    print("PASS test_autofinalize_swallows_finalize_failure")


# ------------------------------------------------- per-box claim matching (short receipts)
def _claim_db(status, grn_boxes, pending, dispatch_boxes=(), on_finalize_raise=False):
    """DB mock serving BOX-LEVEL data, not just counts.

    grn_boxes      : (box_id, article, transfer_out_box_id) acknowledged on the GRN
    pending        : (box_id, article) rows still 'In Transit'
    dispatch_boxes : (id, box_id) rows on the transfer-out, for transfer_out_box_id lookups
    """
    calls = []
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "FROM interunit_transfer_in_header WHERE id" in sql:
                return Res(rows=[Row(id=5, status=status, transfer_out_id=42)])
            if "COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id" in sql:
                return Res(scalar=len(grn_boxes))
            # Order matters: the box-level reads are matched before the COUNT fallbacks.
            if "box_id, article, transfer_out_box_id" in sql:
                return Res(rows=[Row(box_id=b, article=a, transfer_out_box_id=t)
                                 for b, a, t in grn_boxes])
            if "FROM pending_transfer_stock" in sql and "box_id" in sql and "COUNT" not in sql:
                return Res(rows=[Row(box_id=b, article=a, item_description=a)
                                 for b, a in pending])
            if "FROM interunit_transfer_boxes" in sql and "COUNT" not in sql:
                return Res(rows=[Row(id=i, box_id=b) for i, b in dispatch_boxes])
            if "FROM pending_transfer_stock" in sql and "In Transit" in sql:
                return Res(scalar=len(pending))
            return Res()
        def begin_nested(self): return _NestedTxn()
    def stub(header_id, data, db):
        calls.append(header_id)
        if on_finalize_raise:
            raise RuntimeError("simulated pick_from_pending failure")
        return {"id": header_id, "status": "Received"}
    return DB(), stub, calls


def test_autofinalize_refuses_when_in_transit_boxes_were_never_scanned():
    """GRN-20260411173112: 29 dispatched, 27 acknowledged, 2 never scanned.

    `acked >= in_transit` reads 27 >= 2 and calls that complete, so finalizing
    would post two cartons nobody received as live destination stock. The two
    outstanding box ids appear nowhere on the GRN — nothing claims them.
    """
    grn = [(f"B{i}", "Deri Dates", None) for i in range(1, 28)]
    pending = [("X1", "Deri Dates"), ("X2", "Pecan Nuts")]
    db, stub, calls = _claim_db("Pending", grn, pending)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is False
    assert calls == [], "must not finalize: 2 boxes on the bridge were never scanned"
    print("PASS test_autofinalize_refuses_when_in_transit_boxes_were_never_scanned")


def test_autofinalize_finalizes_when_every_in_transit_box_is_on_the_grn():
    """The honest complete case: every outstanding box id was acknowledged."""
    grn = [("B1", "Deri Dates", None), ("B2", "Deri Dates", None)]
    pending = [("B1", "Deri Dates"), ("B2", "Deri Dates")]
    db, stub, calls = _claim_db("Pending", grn, pending)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is True
    assert calls == [5], calls
    print("PASS test_autofinalize_finalizes_when_every_in_transit_box_is_on_the_grn")


def test_autofinalize_claims_a_box_through_transfer_out_box_id():
    """The received sticker differs from the dispatched one, but the FK still links
    them. The link is stronger evidence than the id, so the box is claimed."""
    grn = [("SCANNED-1", "Deri Dates", 900)]
    pending = [("DISPATCHED-1", "Deri Dates")]
    db, stub, calls = _claim_db("Pending", grn, pending,
                                dispatch_boxes=[(900, "DISPATCHED-1")])
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is True
    assert calls == [5], calls
    print("PASS test_autofinalize_claims_a_box_through_transfer_out_box_id")


def test_autofinalize_claims_line_sentinels_by_article():
    """Quantity-only dispatch: park_lines invents LINE-<line>-<n> and the receive
    screen mints an unrelated id, so the article is the only link that exists."""
    grn = [("MINTED-1", "Deri Dates", None), ("MINTED-2", "Deri Dates", None)]
    pending = [("LINE-77-1", "Deri Dates"), ("LINE-77-2", "Deri Dates")]
    db, stub, calls = _claim_db("Pending", grn, pending)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is True
    assert calls == [5], calls
    print("PASS test_autofinalize_claims_line_sentinels_by_article")


def test_autofinalize_refuses_when_only_some_real_boxes_are_claimed():
    """A partial receipt stays Pending so the shortfall remains visible."""
    grn = [("B1", "Deri Dates", None)]
    pending = [("B1", "Deri Dates"), ("B2", "Deri Dates")]
    db, stub, calls = _claim_db("Pending", grn, pending)
    with _patch_finalize(stub):
        assert I._autofinalize_if_complete(db, 5) is False
    assert calls == [], "one of two boxes received is not a complete receipt"
    print("PASS test_autofinalize_refuses_when_only_some_real_boxes_are_claimed")


# ---------------------------------------------------------------- idempotent finalize
def test_finalize_transfer_in_idempotent_when_received():
    writes = []
    full = Row(
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
                return Res(rows=[Row(id=5, status="Received",
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
def _sweep_db(rows, boxes_by_grn=None, pending_by_toid=None, raise_on=()):
    """Sweep mock.

    boxes_by_grn    : {grn_id: [(box_id, article, transfer_out_box_id), ...]}
    pending_by_toid : {transfer_out_id: [(box_id, article), ...]}
    raise_on        : grn_ids whose finalize blows up

    Defaults make every GRN's acknowledged boxes claim its in-transit rows, so
    tests that care only about classification need not spell out box data.
    """
    calls, commits, rollbacks = [], [], []
    boxes_by_grn = boxes_by_grn or {}
    pending_by_toid = pending_by_toid or {}

    def _default_pair(row):
        n = int(row.in_transit or 0)
        ids = [(f"D{row.grn_id}-{i}", "Article") for i in range(n)]
        return [(b, a, None) for b, a in ids], ids

    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt); p = params or {}
            if "FROM interunit_transfer_in_header tih" in sql and "tih.status = 'Pending'" in sql:
                return Res(rows=rows)
            if "box_id, article, transfer_out_box_id" in sql:
                gid = p.get("hid")
                row = next((r for r in rows if r.grn_id == gid), None)
                if gid in boxes_by_grn:
                    grn = boxes_by_grn[gid]
                elif row is not None:
                    grn, _ = _default_pair(row)
                else:
                    grn = []
                return Res(rows=[Row(box_id=b, article=a, transfer_out_box_id=t)
                                 for b, a, t in grn])
            if "FROM pending_transfer_stock" in sql and "box_id" in sql and "COUNT" not in sql:
                tid = p.get("tid")
                row = next((r for r in rows if r.transfer_out_id == tid), None)
                if tid in pending_by_toid:
                    pend = pending_by_toid[tid]
                elif row is not None:
                    _, pend = _default_pair(row)
                else:
                    pend = []
                return Res(rows=[Row(box_id=b, article=a, item_description=a)
                                 for b, a in pend])
            if "FROM interunit_transfer_boxes" in sql and "COUNT" not in sql:
                return Res(rows=[])
            return Res()
        def commit(self): commits.append(1)
        def rollback(self): rollbacks.append(1)
    def stub(header_id, data, db):
        calls.append(header_id)
        if header_id in raise_on:
            raise RuntimeError("simulated finalize failure")
        return {"id": header_id, "status": "Received"}
    return DB(), stub, calls, commits, rollbacks


def _grn(grn_id, toid, acked, in_transit):
    return SimpleNamespace(grn_id=grn_id, transfer_out_id=toid,
                           grn_number=f"GRN-{grn_id}", acked=acked, in_transit=in_transit)


def test_sweep_dry_run_writes_nothing_and_classifies():
    rows = [_grn(1, 101, 176, 176), _grn(2, 102, 44, 44), _grn(3, 103, 10, 50)]
    db, stub, calls, commits, _ = _sweep_db(
        rows, boxes_by_grn={3: [("Z", "Article", None)]},
        pending_by_toid={103: [(f"P{i}", "Article") for i in range(50)]})
    with _patch_finalize(stub):
        summary = I.finalize_complete_pending_grns(db, dry_run=True)
    assert calls == [] and commits == [], "dry-run must not finalize or commit"
    assert summary["pending_grns_scanned"] == 3
    assert [r["grn_id"] for r in summary["finalized"]] == [1, 2], summary
    assert [r["grn_id"] for r in summary["skipped"]] == [3], summary
    print("PASS test_sweep_dry_run_writes_nothing_and_classifies")


def test_sweep_apply_finalizes_complete_grns_only():
    rows = [_grn(1, 101, 176, 176), _grn(2, 102, 44, 44), _grn(3, 103, 10, 50)]
    db, stub, calls, commits, _ = _sweep_db(
        rows, boxes_by_grn={3: [("Z", "Article", None)]},
        pending_by_toid={103: [(f"P{i}", "Article") for i in range(50)]})
    with _patch_finalize(stub):
        summary = I.finalize_complete_pending_grns(db, dry_run=False)
    assert calls == [1, 2], f"only complete GRNs finalized: {calls}"
    assert len(commits) == 2, f"one commit per finalized GRN: {commits}"
    assert [r["grn_id"] for r in summary["skipped"]] == [3], summary
    print("PASS test_sweep_apply_finalizes_complete_grns_only")


def test_sweep_skips_grn_with_zero_in_transit():
    # acked>0 but nothing in transit -> not complete (don't finalize an empty dispatch).
    rows = [_grn(1, 101, 5, 0)]
    db, stub, calls, commits, _ = _sweep_db(rows)
    with _patch_finalize(stub):
        summary = I.finalize_complete_pending_grns(db, dry_run=False)
    assert calls == [] and commits == [], calls
    assert [r["grn_id"] for r in summary["skipped"]] == [1], summary
    print("PASS test_sweep_skips_grn_with_zero_in_transit")


def test_sweep_refuses_grn_whose_in_transit_boxes_were_never_scanned():
    """The live backlog shape: 27 boxes received of 29, the other 2 never scanned.

    `acked >= in_transit` reads 27 >= 2 and would post both missing cartons.
    """
    rows = [_grn(1, 101, 27, 2)]
    db, stub, calls, commits, _ = _sweep_db(
        rows,
        boxes_by_grn={1: [(f"B{i}", "Deri Dates", None) for i in range(27)]},
        pending_by_toid={101: [("X1", "Deri Dates"), ("X2", "Pecan Nuts")]})
    with _patch_finalize(stub):
        summary = I.finalize_complete_pending_grns(db, dry_run=False)
    assert calls == [], "must not finalize a short receipt"
    assert [r["grn_id"] for r in summary["skipped"]] == [1], summary
    print("PASS test_sweep_refuses_grn_whose_in_transit_boxes_were_never_scanned")


def test_sweep_records_failure_instead_of_claiming_success():
    """A finalize that raises must not be reported as finalized, and must not abort
    the sweep — the summary is what an operator trusts about what happened."""
    rows = [_grn(1, 101, 5, 5), _grn(2, 102, 7, 7)]
    db, stub, calls, commits, rollbacks = _sweep_db(rows, raise_on=(1,))
    with _patch_finalize(stub):
        summary = I.finalize_complete_pending_grns(db, dry_run=False)
    assert calls == [1, 2], f"the sweep must continue past a failure: {calls}"
    assert [r["grn_id"] for r in summary["finalized"]] == [2], summary
    assert [r["grn_id"] for r in summary.get("failed", [])] == [1], summary
    assert len(commits) == 1, f"only the successful GRN commits: {commits}"
    assert rollbacks, "a failed finalize must roll back its partial writes"
    print("PASS test_sweep_records_failure_instead_of_claiming_success")


ALL = [
    test_autofinalize_refuses_on_count_parity_without_box_evidence,
    test_autofinalize_refuses_when_over_acknowledged_without_box_evidence,
    test_autofinalize_skips_when_incomplete,
    test_autofinalize_skips_when_already_received,
    test_autofinalize_skips_when_nothing_in_transit,
    test_autofinalize_swallows_finalize_failure,
    test_autofinalize_refuses_when_in_transit_boxes_were_never_scanned,
    test_autofinalize_finalizes_when_every_in_transit_box_is_on_the_grn,
    test_autofinalize_claims_a_box_through_transfer_out_box_id,
    test_autofinalize_claims_line_sentinels_by_article,
    test_autofinalize_refuses_when_only_some_real_boxes_are_claimed,
    test_finalize_transfer_in_idempotent_when_received,
    test_sweep_dry_run_writes_nothing_and_classifies,
    test_sweep_apply_finalizes_complete_grns_only,
    test_sweep_skips_grn_with_zero_in_transit,
    test_sweep_refuses_grn_whose_in_transit_boxes_were_never_scanned,
    test_sweep_records_failure_instead_of_claiming_success,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
