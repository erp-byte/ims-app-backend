"""Dependency-free tests for the cold_stocks re-sync on inward approve/edit.

Bug: a cold inward's boxes are mirrored into *_cold_stocks only once, at creation
(bulk_entry_service.create_bulk_entry). Warehouse / lot / item_mark / vakkal /
spl_remarks and box edits are finalised later on the approve and edit pages, so the
mirror went stale or missing. Fix: sync_cold_stocks_from_inward rebuilds the
inward's own (auto_created_from_inward) cold rows from the FINAL article+box rows,
gated to cold warehouses. Also: ArticleIn now declares the 3 cold fields so the edit
path stops dropping them.

No database required:  python test_inward_cold_sync.py
"""
from services.ims_service.inward_models import ArticleIn
from services.ims_service import inward_tools


# ---- Test 1: ArticleIn keeps the cold fields (edit-path regression) ----

def test_articlein_retains_cold_fields():
    a = ArticleIn(
        transaction_no="TR-1",
        item_description="indian green raisins",
        item_mark="MARK-1",
        spl_remarks="handle cold",
        vakkal="VK-99",
    )
    d = a.model_dump(exclude_none=True)
    for f in ("item_mark", "spl_remarks", "vakkal"):
        assert f in d, f"{f} dropped by ArticleIn — Pydantic would discard it on edit"
    assert d["item_mark"] == "MARK-1"
    assert d["spl_remarks"] == "handle cold"
    assert d["vakkal"] == "VK-99"
    print("test_articlein_retains_cold_fields: PASS")


# ---- Fake DB plumbing (routes SQL by substring; records every statement) ----

class Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)
    def __getitem__(self, i):  # information_schema rows are read as row[0]
        return list(self.__dict__.values())[i]


class Result:
    def __init__(self, rows, rowcount=0):
        self._rows = rows
        self.rowcount = rowcount
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return self._rows


class FakeDB:
    def __init__(self, warehouse, art_columns):
        self.warehouse = warehouse
        self.art_columns = art_columns
        self.log = []  # list of (sql, params)

    def execute(self, clause, params=None):
        sql = str(clause)
        params = params or {}
        self.log.append((sql, params))

        if "information_schema.columns" in sql:
            return Result([Row(column_name=c) for c in self.art_columns])
        if "SELECT warehouse" in sql:
            return Result([Row(warehouse=self.warehouse, entry_date="2026-06-16",
                               vendor_supplier_name="DEV INTERNATIONAL")])
        if sql.strip().startswith("DELETE"):
            return Result([], rowcount=3)
        if "INSERT INTO" in sql:
            return Result([], rowcount=5)
        return Result([])

    def commit(self):
        pass


_BULK_TABLES = {
    "tx": "cfpl_bulk_entry_transactions",
    "art": "cfpl_bulk_entry_articles",
    "box": "cfpl_bulk_entry_boxes",
}
_FULL_COLS = ["item_mark", "spl_remarks", "vakkal", "lot_number",
              "item_category", "sub_category", "item_description", "unit_rate"]


def _stmts(db):
    return [s for s, _ in db.log]


# ---- Test 2: cold warehouse → delete own auto rows + rebuild from boxes ----

def test_cold_warehouse_rebuilds():
    db = FakeDB("Savla D-39", _FULL_COLS)
    n = inward_tools.sync_cold_stocks_from_inward("CFPL", "TR-1", _BULK_TABLES, db)
    assert n == 5, f"expected 5 inserted, got {n}"

    deletes = [s for s in _stmts(db) if s.strip().startswith("DELETE")]
    assert any("cfpl_cold_stocks" in s and "auto_created_from_inward" in s
               and "inward_transaction_no" in s for s in deletes), \
        "must delete only the inward's own auto rows"

    inserts = [(s, p) for s, p in db.log if "INSERT INTO" in s]
    assert inserts, "must insert rebuilt cold rows"
    sql, params = inserts[0]
    assert "cfpl_cold_stocks" in sql
    assert params["unit"] == "D-39", "Savla D-39 must map unit → D-39"
    assert params["wh"] == "Savla D-39"
    assert "a.vakkal" in sql, "vakkal column present → should be selected"
    assert "b.box_id IS NOT NULL" in sql, "only boxes with a box_id are mirrored"
    print("test_cold_warehouse_rebuilds: PASS")


# ---- Test 3: dry warehouse → clear auto rows, insert nothing ----

def test_dry_warehouse_no_insert():
    db = FakeDB("W202", _FULL_COLS)
    n = inward_tools.sync_cold_stocks_from_inward("CFPL", "TR-1", _BULK_TABLES, db)
    assert n == 0, "dry warehouse must not mirror cold"
    assert any(s.strip().startswith("DELETE") for s in _stmts(db)), \
        "dry warehouse should still clear stale auto rows (cold→dry relabel)"
    assert not any("INSERT INTO" in s for s in _stmts(db)), \
        "dry warehouse must not INSERT cold rows"
    print("test_dry_warehouse_no_insert: PASS")


# ---- Test 4: v2 article table (no `vakkal` column) → NULL, no crash ----

def test_v2_missing_vakkal_uses_null():
    cols = [c for c in _FULL_COLS if c != "vakkal"]  # v2 has no vakkal
    db = FakeDB("Rishi", cols)
    n = inward_tools.sync_cold_stocks_from_inward("CFPL", "TR-1", _BULK_TABLES, db)
    sql = [s for s in _stmts(db) if "INSERT INTO" in s][0]
    assert "a.vakkal" not in sql, "vakkal column absent → must not reference a.vakkal"
    assert "a.item_mark" in sql, "item_mark present → should still be selected"
    print("test_v2_missing_vakkal_uses_null: PASS")


if __name__ == "__main__":
    test_articlein_retains_cold_fields()
    test_cold_warehouse_rebuilds()
    test_dry_warehouse_no_insert()
    test_v2_missing_vakkal_uses_null()
    print("\nAll cold-sync tests passed.")
