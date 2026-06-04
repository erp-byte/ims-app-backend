"""Dependency-free e2e/logic tests for Step 6 — ICT reversal.
Drives the REAL cold_storage_server.reverse_inner_transfer_line with a mock DB.
No database required:  python test_ict_reverse_e2e.py
"""
from contextlib import contextmanager
from types import SimpleNamespace

from services.ims_service import cold_storage_server as C


class Res:
    def __init__(self, rows=None, scalar=None):
        self._rows, self._scalar = rows or [], scalar
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar


@contextmanager
def _patch(name, val):
    orig = getattr(C, name)
    setattr(C, name, val)
    try:
        yield
    finally:
        setattr(C, name, orig)


def _audit(qty=3, old_loc="Rishi"):
    return SimpleNamespace(id=7, stock_record_id=100, new_lot_number="125320",
                           old_lot_number="183027", item_description="Deri Dates",
                           quantity=qty, old_storage_location=old_loc)


def _db(audit_row, available_ids, updates, deletes, commits):
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt); up = sql.upper()
            if "to_regclass" in sql or "TO_REGCLASS" in up:
                return Res(scalar="x")
            if "FROM inner_cold_transfer WHERE id" in sql and "SELECT *" in sql:
                return Res(rows=[audit_row] if audit_row else [])
            if "SELECT id FROM" in sql and "lot_no = :newlot" in sql:
                n = (params or {}).get("n", 0)
                return Res(rows=[SimpleNamespace(id=i) for i in available_ids[:n]])
            if up.startswith("UPDATE ") and "SET LOT_NO" in up:
                updates.append({"rid": params.get("rid"), "old": params.get("old"),
                                "loc": params.get("loc")})
                return Res()
            if "DELETE FROM inner_cold_transfer" in sql:
                deletes.append(params.get("id")); return Res()
            return Res()
        def commit(self): commits.append(1)
    return DB()


def test_ict_reverse_already_reversed_when_audit_missing():
    db = _db(None, [], [], [], [])
    out = C.reverse_inner_transfer_line("CH-1", 7, db)
    assert out == {"status": "already_reversed", "reversed_rows": 0}, out
    print("PASS test_ict_reverse_already_reversed_when_audit_missing")


def test_ict_reverse_flips_lot_and_location_up_to_qty():
    updates, deletes, commits = [], [], []
    # 5 rows available but qty=3 -> LIMIT must cap at 3
    db = _db(_audit(qty=3, old_loc="Rishi"), [11, 12, 13, 14, 15], updates, deletes, commits)
    with _patch("_ensure_inner_cold_transfer_table", lambda db: None), \
         _patch("_resolve_record_table", lambda sid, db: "cdpl_cold_stocks"):
        out = C.reverse_inner_transfer_line("CH-1", 7, db)
    assert out["status"] == "reversed" and out["reversed_rows"] == 3, out
    assert out["freed_lot"] == "183027" and out["freed_location"] == "Rishi", out
    assert [u["rid"] for u in updates] == [11, 12, 13], updates
    assert all(u["old"] == "183027" and u["loc"] == "Rishi" for u in updates), updates
    assert deletes == [7] and commits == [1], (deletes, commits)
    print("PASS test_ict_reverse_flips_lot_and_location_up_to_qty")


def test_ict_reverse_without_location_only_flips_lot():
    updates, deletes, commits = [], [], []
    # no reverse_location arg and audit.old_storage_location=None -> lot-only UPDATE branch
    db = _db(_audit(qty=2, old_loc=None), [21, 22, 23], updates, deletes, commits)
    with _patch("_ensure_inner_cold_transfer_table", lambda db: None), \
         _patch("_resolve_record_table", lambda sid, db: "cfpl_cold_stocks"):
        out = C.reverse_inner_transfer_line("CH-1", 7, db)
    assert out["reversed_rows"] == 2 and out["freed_location"] is None, out
    assert [u["rid"] for u in updates] == [21, 22], updates
    assert all(u["loc"] is None for u in updates), updates  # lot-only branch (no loc param)
    print("PASS test_ict_reverse_without_location_only_flips_lot")


ALL = [
    test_ict_reverse_already_reversed_when_audit_missing,
    test_ict_reverse_flips_lot_and_location_up_to_qty,
    test_ict_reverse_without_location_only_flips_lot,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
