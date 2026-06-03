"""Dependency-free test: the Inward Summary feed must include bulk-entry inwards.

Bug: /inward-dashboard/all-data (and /filter-options) only ever query the *_v2 tables. Every inward
created through the bulk-entry / cold-storage bulk path lives in *_bulk_entry_transactions/_articles
and was therefore invisible on the dashboard — pending or not. Fix: UNION the bulk-entry source into
the feed so those inwards appear and reconcile in the totals/chips.

Mocks route SQL substrings to canned rows (same style as test_grn_autofinalize.py).
No database required:  python test_dashboard_includes_bulk_entry.py
"""
import asyncio

from services.ims_service.inward_dashboard_server import get_all_data, get_filter_options


class Row:
    """SQLAlchemy Row stand-in: ._fields + attribute access + positional [i]."""
    def __init__(self, **data):
        object.__setattr__(self, "_data", data)
        object.__setattr__(self, "_fields", tuple(data.keys()))
    def __getattr__(self, k):
        return object.__getattribute__(self, "_data")[k]
    def __getitem__(self, i):
        return tuple(object.__getattribute__(self, "_data").values())[i]


class Result:
    def __init__(self, rows):
        self._rows = rows
    def fetchall(self):
        return self._rows


def _all_data_row(**over):
    base = dict(
        transaction_no="X", entry_date="2026-06-01", entry_month="2026-06",
        warehouse="W1", vendor="V1", customer="C1", status="approved",
        invoice_number="", po_number="", purchased_by="", grn_number="",
        item_description="Item", sku_id=1, item_category="Cat", sub_category="Sub",
        material_type="Mat", quality_grade="", uom="BOX", lot_number="L1",
        qty=10, net_weight=100.0, total_weight=110.0, unit_rate=5.0, total_amount=500.0,
    )
    base.update(over)
    return Row(**base)


class AllDataDB:
    """Returns a v2 row and a bulk-entry row depending on which tables the SQL touches."""
    def __init__(self):
        self.sqls = []
    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.sqls.append(sql)
        rows = []
        if "_transactions_v2" in sql:
            rows.append(_all_data_row(transaction_no="V2-1", status="approved"))
        if "_bulk_entry_transactions" in sql:
            rows.append(_all_data_row(transaction_no="BE-1", status="pending"))
        return Result(rows)


def test_all_data_includes_bulk_entry_transactions():
    db = AllDataDB()
    out = asyncio.run(get_all_data(company="CFPL", only_entries=False, db=db))
    txns = {r["transaction_no"] for r in out["records"]}
    assert "V2-1" in txns, "v2 inwards must still appear"
    assert "BE-1" in txns, f"bulk-entry inwards must appear on the dashboard feed; got {txns}"
    # the bulk-entry one is pending — exactly the kind that was going missing
    assert any(r["transaction_no"] == "BE-1" and r["status"] == "pending" for r in out["records"])
    print("PASS test_all_data_includes_bulk_entry_transactions")


class FilterOptsDB:
    """Routes each filter-options query; bulk-entry contributes a 'pending' status + its own warehouse."""
    def execute(self, stmt, params=None):
        sql = str(stmt).lower()
        has_v2 = "_transactions_v2" in sql or "_articles_v2" in sql
        has_be = "_bulk_entry" in sql
        if "as cnt" in sql and "warehouse" in sql:
            rows = []
            if has_v2: rows.append(Row(k="W1", cnt=3))
            if has_be: rows.append(Row(k="W2-bulk", cnt=2))
            return Result(rows)
        if "as cnt" in sql:  # vendors / customers
            return Result([Row(k="X", cnt=1)])
        if "t.status" in sql:
            rows = []
            if has_v2: rows.append(Row(k="approved"))
            if has_be: rows.append(Row(k="pending"))
            return Result(rows)
        # item_category / sub_category / material_type / purchased_by distinct lists
        return Result([Row(k="Cat")])


def test_filter_options_statuses_include_bulk_entry_pending():
    out = asyncio.run(get_filter_options(company="CFPL", only_entries=False, db=FilterOptsDB()))
    assert "pending" in out["statuses"], f"bulk-entry 'pending' must be a status chip; got {out['statuses']}"
    assert "approved" in out["statuses"]
    wh_names = {w["name"] for w in out["warehouses"]}
    assert "W2-bulk" in wh_names, f"bulk-entry-only warehouse must appear; got {wh_names}"
    print("PASS test_filter_options_statuses_include_bulk_entry_pending")


ALL = [
    test_all_data_includes_bulk_entry_transactions,
    test_filter_options_statuses_include_bulk_entry_pending,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
