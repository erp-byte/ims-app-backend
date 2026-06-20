"""Dependency-free test: the approve flow must persist the cold per-article fields.

Bug: the approve page (inward/[id]/approve) shows Item Mark / Spl. Remarks / Vakkal
for cold warehouses, but they were never saved. Root cause: ApprovalArticleFields did
not declare item_mark/spl_remarks/vakkal, so FastAPI/Pydantic (default extra="ignore")
silently dropped them and approve_inward's generic UPDATE never wrote them — even though
the columns exist on the *_bulk_entry_articles tables.

Secondary: the v2 article tables (*_articles_v2) have item_mark/spl_remarks but NOT vakkal,
so approve_inward must filter each update to the columns the target table actually has, or
a cold v2 approval would 500 on "column vakkal does not exist".

No database required:  python test_approve_inward_cold_fields.py
"""
from services.ims_service.inward_models import ApprovalArticleFields, ApprovalRequest
from services.ims_service import inward_tools


# ---- Test 1: model keeps the cold fields (the direct root-cause regression) ----

def test_model_retains_cold_fields():
    art = ApprovalArticleFields(
        item_description="al barakah khalas dates",
        item_mark="MARK-1",
        spl_remarks="handle cold",
        vakkal="VK-99",
    )
    dumped = art.model_dump(exclude_none=True)
    for f in ("item_mark", "spl_remarks", "vakkal"):
        assert f in dumped, f"{f} dropped by ApprovalArticleFields — Pydantic would discard it"
    assert dumped["item_mark"] == "MARK-1"
    assert dumped["spl_remarks"] == "handle cold"
    assert dumped["vakkal"] == "VK-99"
    print("test_model_retains_cold_fields: PASS")


# ---- Mock DB plumbing (same style as test_dashboard_includes_bulk_entry.py) ----

class Result:
    def __init__(self, rows):
        self._rows = rows
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return self._rows


class FakeDB:
    """Routes SQL by substring; records every UPDATE for assertions.

    `tx_table_with_row` selects which transaction table 'contains' the txn, which in
    turn decides tables['art'] (v2 vs bulk_entry). `art_columns` is what
    information_schema returns for that article table.
    """
    def __init__(self, tx_table_with_row, art_columns):
        self.tx_table_with_row = tx_table_with_row
        self.art_columns = art_columns
        self.updates = []  # (sql, params)
        self.committed = False

    def execute(self, clause, params=None):
        sql = str(clause)
        params = params or {}

        if "information_schema.columns" in sql:
            return Result([(c,) for c in self.art_columns])

        # The two transaction lookups: "SELECT transaction_no, status FROM <tbl> ..."
        if "status FROM" in sql:
            present = self.tx_table_with_row in sql
            return Result([("TR-1", "pending")] if present else [])

        if sql.strip().startswith("UPDATE") or "UPDATE " in sql.split("\n")[0]:
            self.updates.append((sql, params))
            return Result([])

        # Box existence lookups, etc.
        return Result([])

    def commit(self):
        self.committed = True


def _run_approve(tx_table_with_row, art_columns):
    payload = ApprovalRequest(
        approved_by="tester",
        articles=[ApprovalArticleFields(
            item_description="al barakah khalas dates",
            item_mark="MARK-1",
            spl_remarks="handle cold",
            vakkal="VK-99",
            quality_grade="A",
        )],
        boxes=None,
    )
    db = FakeDB(tx_table_with_row, art_columns)
    inward_tools.approve_inward("CDPL", "TR-1", payload, db)
    art_updates = [u for u in db.updates if "_articles" in u[0]]
    assert art_updates, "expected an article UPDATE"
    assert db.committed
    return art_updates[0]  # (sql, params)


# ---- Test 2: bulk_entry path writes all three cold fields ----

def test_bulk_entry_writes_all_cold_fields():
    # First tx lookup (cdpl_transactions_v2) misses → falls back to bulk_entry table.
    sql, params = _run_approve(
        tx_table_with_row="cdpl_bulk_entry_transactions",
        art_columns=["item_description", "quality_grade", "item_mark", "spl_remarks", "vakkal"],
    )
    assert "cdpl_bulk_entry_articles" in sql
    for f in ("item_mark", "spl_remarks", "vakkal", "quality_grade"):
        assert f in params, f"{f} missing from bulk_entry article UPDATE params"
    print("test_bulk_entry_writes_all_cold_fields: PASS")


# ---- Test 3: v2 path (no `vakkal` column) drops vakkal, keeps the rest, no crash ----

def test_v2_path_filters_missing_vakkal():
    sql, params = _run_approve(
        tx_table_with_row="cdpl_transactions_v2",
        art_columns=["item_description", "quality_grade", "item_mark", "spl_remarks"],  # no vakkal
    )
    assert "cdpl_articles_v2" in sql
    assert "vakkal" not in params, "vakkal must be filtered out for v2 (column does not exist → would 500)"
    for f in ("item_mark", "spl_remarks", "quality_grade"):
        assert f in params, f"{f} should still be written on the v2 path"
    print("test_v2_path_filters_missing_vakkal: PASS")


if __name__ == "__main__":
    test_model_retains_cold_fields()
    test_bulk_entry_writes_all_cold_fields()
    test_v2_path_filters_missing_vakkal()
    print("\nAll tests passed.")
