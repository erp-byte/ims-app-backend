"""Dependency-free unit tests for the cold-finalize staging-purge fix.

Bug: a transfer-IN to a COLD destination is scanned/acknowledged through an
interunit_transfer_in_header used as a staging area, then finalized via
finalize_cold_transfer_in which creates the real cold_transfer_in_headers row
REUSING the staging id. finalize never purged the interunit staging header, so
every completed cold receipt left an orphan in interunit_transfer_in_header /
interunit_transfer_in_boxes (14 found in prod, ~1683 orphan box rows).

Fix (services/ims_service/cold_transfer_in_tools.py, finalize_cold_transfer_in):
  After _reconcile_statuses, when the receipt is fully 'Received', purge the
  interunit staging header + its acknowledged boxes (guard on transfer_out_id).
  When still 'Pending' (multi-session partial receive) the staging is KEPT so the
  next session resumes on the same id instead of spawning a duplicate cold header.

Mocks route SQL substrings to canned results. No database required:
    python test_cold_finalize_purges_staging.py
"""
from services.ims_service import cold_transfer_in_tools as C


class Row:
    """Row supporting both attribute (.x) and ._mapping["x"] access."""
    def __init__(self, **kw):
        self._mapping = kw
        for k, v in kw.items():
            setattr(self, k, v)


class Res:
    def __init__(self, rows=None, scalar=None):
        self._rows, self._scalar = rows or [], scalar
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar


HEADER_ID = 605
OUT_ID = 1073


def _make_db(in_transit_remaining):
    """Mock DB for finalize_cold_transfer_in else-branch (no existing cold header).
    in_transit_remaining controls _reconcile_statuses: 0 -> Received, >0 -> Pending.
    Returns (db, executed) where executed is the list of SQL strings run.
    """
    executed = []

    class DB:
        def execute(self, stmt, params=None):
            sql = " ".join(str(stmt).split())  # normalize whitespace
            executed.append(sql)

            # finalize: existing cold header lookup -> None (forces else branch)
            if "SELECT id, transfer_out_id, to_site FROM cold_transfer_in_headers WHERE id" in sql:
                return Res(rows=[])
            # finalize else: read interunit staging header to build cold header
            if "FROM interunit_transfer_in_header WHERE id" in sql and "receiving_warehouse" in sql:
                return Res(rows=[Row(
                    transfer_out_id=OUT_ID, transfer_out_no="TRANS202606161854",
                    grn_number="GRN-X", grn_date=None, receiving_warehouse="Savla D-39",
                    received_by="u", box_condition="Good", condition_remarks=None,
                    inward_transaction_no=None,
                )])
            # finalize else: read transfer-out for from_site/to_site
            if "from_site, to_site FROM interunit_transfers_header" in sql:
                return Res(rows=[Row(from_site="W202", to_site="Savla D-39")])
            # _process_box_loop: pending lookup per box -> none
            if "FROM pending_transfer_stock WHERE box_id" in sql:
                return Res(rows=[])
            # _reconcile_statuses: count still-in-transit
            if "COUNT(*) FROM pending_transfer_stock" in sql and "In Transit" in sql:
                return Res(scalar=in_transit_remaining)
            # the purge guard SELECT (only runs in Received branch)
            if "SELECT id FROM interunit_transfer_in_header WHERE id" in sql and "transfer_out_id" in sql:
                return Res(rows=[Row(id=HEADER_ID)])
            return Res()

        def commit(self): executed.append("COMMIT")

    return DB(), executed


def _finalize_payload():
    return C.ColdTransferInFinalize(
        to_company=None,
        item_description="ANJEER 1*2",
        lot_no="10557",
        boxes=[C.ColdTransferInBoxInput(
            box_id="62145674-1", transaction_no="TR-20260619151225",
            lot_no="10557", item_description="ANJEER 1*2",
            weight_kg=10.0, no_of_cartons=1,
        )],
    )


def _interunit_deletes(executed):
    return [s for s in executed if s.startswith("DELETE FROM interunit_transfer_in")]


def test_finalize_purges_staging_when_received():
    db, executed = _make_db(in_transit_remaining=0)   # all received
    res = C.finalize_cold_transfer_in(db, HEADER_ID, _finalize_payload())
    assert res.status == "Received", res
    dels = _interunit_deletes(executed)
    assert any("interunit_transfer_in_boxes" in s for s in dels), \
        f"must delete staging boxes, got: {dels}"
    assert any("interunit_transfer_in_header" in s for s in dels), \
        f"must delete staging header, got: {dels}"
    print("PASS test_finalize_purges_staging_when_received")


def test_finalize_keeps_staging_when_pending():
    db, executed = _make_db(in_transit_remaining=5)   # boxes still in transit
    res = C.finalize_cold_transfer_in(db, HEADER_ID, _finalize_payload())
    assert res.status == "Pending", res
    dels = _interunit_deletes(executed)
    assert dels == [], f"partial receive must NOT purge interunit staging, got: {dels}"
    print("PASS test_finalize_keeps_staging_when_pending")


ALL = [
    test_finalize_purges_staging_when_received,
    test_finalize_keeps_staging_when_pending,
]

if __name__ == "__main__":
    failures = 0
    for t in ALL:
        try:
            t()
        except AssertionError as e:
            failures += 1
            print(f"FAIL {t.__name__}: {e}")
    if failures:
        print(f"\n{failures}/{len(ALL)} TESTS FAILED")
        raise SystemExit(1)
    print(f"\nALL {len(ALL)} TESTS PASSED")
