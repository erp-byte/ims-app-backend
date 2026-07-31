"""
Dependency-free unit test for unique_challan_no.

Guards the 500 "duplicate key ... interunit_transfers_header_challan_no_key" that hit
an operator after scanning 20 boxes: the form mints TRANS<yyyymmddhhmm> in the browser,
so a draft restored the next day (or two submits in the same minute) re-sends a challan
that already exists. The server must re-mint instead of failing the save.
No database required:  python test_unique_challan_no.py
"""
from types import SimpleNamespace

from fastapi import HTTPException

from services.ims_service.interunit_tools import unique_challan_no


class FakeDB:
    def __init__(self, taken):
        self.taken = set(taken)
        self.checked = []

    def execute(self, stmt, params=None):
        c = (params or {})["c"]
        self.checked.append(c)
        return SimpleNamespace(fetchone=lambda: (1,) if c in self.taken else None)


def run():
    # 1. free number is kept verbatim — the operator's DC number doesn't move for nothing
    db = FakeDB(taken=[])
    assert unique_challan_no(db, "TRANS202607311105") == "TRANS202607311105"

    # 2. the reported case: stale draft re-sends yesterday's number (transfer 1615)
    db = FakeDB(taken=["TRANS202607301239"])
    got = unique_challan_no(db, "TRANS202607301239")
    assert got != "TRANS202607301239" and got.startswith("TRANS"), got
    assert len(got) == len("TRANS") + 14, f"expected seconds precision, got {got}"

    # 3. that fresh number taken too → suffixed, never an exception
    db = FakeDB(taken=[])
    base = unique_challan_no(db, None)          # what the server would mint now
    db2 = FakeDB(taken=["TRANS202607301239", base])
    assert unique_challan_no(db2, "TRANS202607301239") == f"{base}-2"

    # 4. no number sent at all (API clients) → server allocates
    assert unique_challan_no(FakeDB(taken=[]), "   ").startswith("TRANS")

    # 5. everything taken → 409, not a raw 500 IntegrityError
    db3 = FakeDB(taken=[base] + [f"{base}-{n}" for n in range(2, 12)] + ["X"])
    try:
        unique_challan_no(db3, "X")
    except HTTPException as e:
        assert e.status_code == 409, e.status_code
    else:
        raise AssertionError("expected HTTPException(409)")

    print("ALL ASSERTIONS PASSED — free numbers kept, taken ones re-minted, 409 not 500.")


if __name__ == "__main__":
    run()
