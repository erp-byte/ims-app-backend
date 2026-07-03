"""Dependency-free tests for company-agnostic Cold Storage Direct Out.

Design: a single Direct Out may pick boxes from BOTH cfpl_cold_stocks and
cdpl_cold_stocks. On submit, create_direct_out groups lines by their source
company and writes ONE Direct-Out record per company (auto-split) — each
deletes only its own company's cold_stocks and inserts into that company's
{co}_cold_storage_direct_out. One DB transaction (single commit).

No database required:  python test_cold_directout_merge.py
"""
import re
import json
from services.cold_storage_service import tools
from services.cold_storage_service.models import DirectOutCreate, DirectOutLine

# The disposition ledger is a separate concern (and _ensure_reconciliation_schema
# commits internally). Stub both to no-ops so `commits` reflects ONLY the create
# flow's own commit — the single-transaction invariant we're asserting.
from services.ims_service import pending_stock_tools as _pst
_pst._ensure_reconciliation_schema = lambda db: None
_pst._write_disposition = lambda *a, **k: None


# ── Fake DB that simulates just the SQL create_direct_out issues ──

class PickRow:
    def __init__(self, row, id):
        self.row = row
        self.id = id


class InsertRow:
    """Exposes every DIRECT_OUT_COLS attribute _map_direct_out_row reads."""
    def __init__(self, params):
        self.id = 1
        self.transaction_no = params["transaction_no"]
        self.transaction_type = params["transaction_type"]
        self.company = params["company"]
        self.entry_date = params.get("entry_date")
        self.authority_person = params.get("authority_person")
        self.to_customer = params.get("to_customer")
        self.warehouse = params.get("warehouse")
        self.vehicle_no = params.get("vehicle_no")
        self.invoice_no = params.get("invoice_no")
        self.remarks = params.get("remarks")
        self.lines = params.get("lines")
        self.line_count = None
        self.total_issue_qty = params.get("total_issue_qty")
        self.status = "completed"
        self.created_by = params.get("created_by")
        self.created_at = None
        self.updated_at = None
        self.removed_stock_snapshot = params.get("removed_stock_snapshot")
        self.lot_no = params.get("lot_no")


class Result:
    def __init__(self, rows=None, scalar=0, rowcount=0):
        self._rows = rows or []
        self._scalar = scalar
        self.rowcount = rowcount
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return self._rows
    def scalar(self):
        return self._scalar


class FakeDB:
    def __init__(self, stocks):
        # stocks: {table_name: {id: rowdict}}
        self.stocks = stocks
        self.deleted = {}        # table -> [ids]
        self.inserted = []       # (table, params)
        self.commits = 0

    def execute(self, clause, params=None):
        sql = str(clause)
        params = params or {}

        # Pick the user-selected stock row: to_jsonb(t) AS row, t.id ... WHERE t.id = :sid
        m = re.search(r"FROM (\w+_cold_stocks) t", sql)
        if m and "t.id = :sid" in sql:
            tbl = m.group(1)
            sid = int(params["sid"])
            row = self.stocks.get(tbl, {}).get(sid)
            return Result(rows=[PickRow(row, sid)] if row else [])
        # FIFO extras query — none needed (tests use issue_qty=1)
        if m and "t.lot_no = :lot" in sql:
            return Result(rows=[])
        # DELETE FROM {stocks_table} WHERE id = ANY(:ids)
        md = re.search(r"DELETE FROM (\w+_cold_stocks)", sql)
        if md:
            self.deleted.setdefault(md.group(1), []).extend(params.get("ids", []))
            return Result(rowcount=len(params.get("ids", [])))
        # INSERT INTO {co}_cold_storage_direct_out ... RETURNING ...
        mi = re.search(r"INSERT INTO (\w+_cold_storage_direct_out)", sql)
        if mi:
            self.inserted.append((mi.group(1), params))
            return Result(rows=[InsertRow(params)])
        # Anything else (disposition schema/writes) — tolerate
        return Result()

    def commit(self):
        self.commits += 1


def _box(id, box_id, lot, txn="TR-X"):
    return {"id": id, "box_id": box_id, "transaction_no": txn, "lot_no": lot,
            "item_description": "medjoul", "unit": "D-39", "storage_location": "Savla D-39",
            "weight_kg": 10, "no_of_cartons": 1}


def _line(stock_id, company, lot, box_id):
    return DirectOutLine(stock_id=stock_id, company=company, lot_no=lot,
                         box_id=box_id, issue_qty=1, item_description="medjoul")


def _payload(lines, company="CFPL"):
    return DirectOutCreate(
        company=company, entry_date="2026-07-03", authority_person="MAHESH",
        to_customer="ACME", lines=lines, created_by="tester",
    )


# ── Test 1: line model carries a per-box company ──

def test_directoutline_has_company():
    l = _line(1, "CDPL", "L1", "B1")
    assert l.company == "CDPL", "DirectOutLine must carry a per-line source company"
    print("test_directoutline_has_company: PASS")


# ── Test 2: all-CFPL submit → one record, only cfpl tables touched ──

def test_single_company_one_record():
    db = FakeDB({"cfpl_cold_stocks": {1: _box(1, "B1", "L1"), 2: _box(2, "B2", "L1")}})
    res = tools.create_direct_out(_payload([_line(1, "CFPL", "L1", "B1"),
                                            _line(2, "CFPL", "L1", "B2")]), db)
    assert len(res["records"]) == 1, f"expected 1 record, got {len(res['records'])}"
    assert res["records"][0]["transaction_no"].startswith("DO-")
    assert not res["records"][0]["transaction_no"].endswith(("-CFPL", "-CDPL")), \
        "single-company txn should have no company suffix"
    assert set(db.deleted) == {"cfpl_cold_stocks"}, f"only cfpl deleted, got {db.deleted}"
    assert sorted(db.deleted["cfpl_cold_stocks"]) == [1, 2]
    assert [t for t, _ in db.inserted] == ["cfpl_cold_storage_direct_out"]
    assert db.commits == 1, "must commit exactly once"
    print("test_single_company_one_record: PASS")


# ── Test 3: mixed CFPL+CDPL → auto-split into two records, per-company routing ──

def test_mixed_company_splits_into_two():
    db = FakeDB({
        "cfpl_cold_stocks": {1: _box(1, "B1", "L1")},
        "cdpl_cold_stocks": {2: _box(2, "B2", "L2")},
    })
    res = tools.create_direct_out(_payload([_line(1, "CFPL", "L1", "B1"),
                                            _line(2, "CDPL", "L2", "B2")]), db)
    assert len(res["records"]) == 2, f"mixed submit must split into 2, got {len(res['records'])}"
    txns = {r["transaction_no"] for r in res["records"]}
    assert any(t.endswith("-CFPL") for t in txns), f"missing -CFPL txn: {txns}"
    assert any(t.endswith("-CDPL") for t in txns), f"missing -CDPL txn: {txns}"
    # Each box deleted ONLY from its own company's stock table
    assert db.deleted.get("cfpl_cold_stocks") == [1], f"cfpl delete wrong: {db.deleted}"
    assert db.deleted.get("cdpl_cold_stocks") == [2], f"cdpl delete wrong: {db.deleted}"
    # One header per company, in its own table
    tables = sorted(t for t, _ in db.inserted)
    assert tables == ["cdpl_cold_storage_direct_out", "cfpl_cold_storage_direct_out"], tables
    # Company stamped correctly on each header
    for tbl, params in db.inserted:
        want = "CFPL" if tbl.startswith("cfpl") else "CDPL"
        assert params["company"] == want, f"{tbl} header company={params['company']} != {want}"
    assert db.commits == 1, "auto-split must still be ONE transaction (single commit)"
    print("test_mixed_company_splits_into_two: PASS")


# ── Read-path fake: records executed SQL, returns configured results ──

class ReadDB:
    def __init__(self, regclass=lambda t: t, count=0, rows=None, exists_txn_in=None):
        self._regclass = regclass          # fn(table)->truthy/None
        self._count = count
        self._rows = rows or []
        self._exists_txn_in = exists_txn_in or set()  # tables that contain the txn
        self.sqls = []
    def execute(self, clause, params=None):
        sql = str(clause); params = params or {}
        self.sqls.append(sql)
        if "to_regclass" in sql:
            return Result(scalar=self._regclass(params.get("t", "")))
        if sql.strip().startswith("SELECT 1 FROM") and "transaction_no" in sql:
            m = re.search(r"FROM (\w+_cold_storage_direct_out)", sql)
            hit = m and m.group(1) in self._exists_txn_in
            return Result(rows=[InsertRow({"transaction_no": params.get("tn"),
                                           "transaction_type": "DIRECT_OUT",
                                           "company": "X"})] if hit else [])
        if "COUNT(*)" in sql:
            return Result(scalar=self._count)
        return Result(rows=self._rows)
    def commit(self):
        pass


def test_list_merges_both_companies():
    db = ReadDB(count=0, rows=[])
    tools.list_direct_out("CFPL", 1, 20, None, None, None, None, db)
    joined = " ".join(db.sqls)
    assert "cfpl_cold_storage_direct_out" in joined, "list must query cfpl table"
    assert "cdpl_cold_storage_direct_out" in joined, "list must ALSO query cdpl table (merged)"
    print("test_list_merges_both_companies: PASS")


def test_get_resolves_other_company_table():
    # Record lives ONLY in cdpl table, but caller passes company=CFPL (navbar).
    row = InsertRow({"transaction_no": "DO-1", "transaction_type": "DIRECT_OUT", "company": "CDPL"})
    db = ReadDB(rows=[row], exists_txn_in={"cdpl_cold_storage_direct_out"})
    rec = tools.get_direct_out("CFPL", "DO-1", db)
    assert rec["transaction_no"] == "DO-1", "get must resolve the txn in cdpl even when CFPL is passed"
    print("test_get_resolves_other_company_table: PASS")


if __name__ == "__main__":
    test_directoutline_has_company()
    test_single_company_one_record()
    test_mixed_company_splits_into_two()
    test_list_merges_both_companies()
    test_get_resolves_other_company_table()
    print("\nAll direct-out merge tests passed.")
