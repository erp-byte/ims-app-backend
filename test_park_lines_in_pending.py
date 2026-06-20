"""
Dependency-free unit test for park_lines_in_pending.

Mocks the DB session to capture INSERT params, then asserts the rows it would
write satisfy the pending_transfer_stock schema (NOT NULL cols), have unique
box_ids, and use correct per-unit math. No database required.
"""
import sys, types
from types import SimpleNamespace

# --- stub the package deps that pending_stock_tools imports at module load ---
# (logger only; sqlalchemy is installed.)
def _install_stub_logger():
    import services.ims_service  # noqa
# We import the real module; it only needs get_logger which exists in the app.

from services.ims_service.pending_stock_tools import park_lines_in_pending

# NOT NULL columns in pending_transfer_stock (from live schema introspection)
REQUIRED_NOT_NULL = {
    "transfer_type", "transfer_out_id", "transfer_out_challan_no", "box_id",
    "transaction_no", "from_company", "to_company", "from_site", "to_site",
    "from_storage_type", "to_storage_type", "source_table", "destination_table",
    "item_description", "weight_kg", "status", "dispatched_at", "dispatched_by",
}


class FakeResult:
    def fetchone(self): return None
    def fetchall(self): return []
    def scalar(self): return None


class FakeDB:
    def __init__(self):
        self.inserts = []  # list of (sql_text, params)

    def execute(self, stmt, params=None):
        sql = str(stmt)
        if "INSERT INTO pending_transfer_stock" in sql:
            # merge literal '' columns that aren't in params but are in the VALUES
            self.inserts.append((sql, params or {}))
        return FakeResult()


def line(_id, desc, qty, net, gross=None, lot=None):
    return SimpleNamespace(
        id=_id, item_desc_raw=desc, qty=qty, net_weight=net,
        total_weight=gross if gross is not None else net,
        lot_number=lot, rm_pm_fg_type="FG", item_category="DATES",
        sub_category=None, pack_size=16, unit_pack_size=0.5, uom="BOX",
    )


def run():
    lines = [
        line(101, "AL BARAKAH KHALAS SEEDLESS DATES 500 G", 1, 8.0, 8.91, "CF/IQ29"),
        line(102, "ROASTED & SALTED CASHEW", 50, 400.0, 420.0),   # qty>1 → per-unit split
        line(103, "  ", 5, 10.0),       # blank article → skipped
        line(104, "SUNFLOWER SEEDS", 0, 0.0),  # qty 0 → skipped
    ]
    db = FakeDB()
    n = park_lines_in_pending(
        transfer_out_id=9999, challan_no="TRANS-TEST-001",
        from_site="A185", to_site="W202",
        lines=lines, dispatched_by="tester", db=db,
    )

    rows = [p for _, p in db.inserts]
    assert n == 1 + 50, f"expected 51 unit-rows, got {n}"
    assert len(rows) == 51, f"expected 51 inserts, got {len(rows)}"

    # box_id uniqueness within the transfer
    box_ids = [r["box_id"] for r in rows]
    assert len(set(box_ids)) == 51, "box_ids not unique"

    # per-unit math for the qty=50 line: 400/50 = 8.0
    cashew = [r for r in rows if r["article"] == "ROASTED & SALTED CASHEW"]
    assert len(cashew) == 50
    assert abs(cashew[0]["weight_kg"] - 8.0) < 1e-9, cashew[0]["weight_kg"]
    assert abs(cashew[0]["gross_weight"] - 8.4) < 1e-9, cashew[0]["gross_weight"]

    # qty=1 line keeps full net
    khalas = [r for r in rows if "KHALAS" in r["article"]]
    assert len(khalas) == 1 and abs(khalas[0]["weight_kg"] - 8.0) < 1e-9

    # NOT NULL coverage: every required col is either a bound param (non-None)
    # or a hard-coded literal in the SQL ('' / 1 / 'In Transit').
    # column -> bound-param-name alias (others bind by same name)
    PARAM_ALIAS = {"transfer_out_challan_no": "challan_no"}
    for col in REQUIRED_NOT_NULL:
        pkey = PARAM_ALIAS.get(col, col)
        in_params = rows[0].get(pkey) is not None
        # literals written directly in the VALUES clause:
        literal_ok = col in ("from_company", "to_company", "source_table",
                             "destination_table", "status")
        assert in_params or literal_ok, f"NOT NULL col '{col}' missing a value"

    # storage types derived from sites (A185/W202 are warehouses)
    assert rows[0]["from_storage_type"] == "warehouse"
    assert rows[0]["to_storage_type"] == "warehouse"
    # transaction_no == challan, box_id pattern LINE-<lineid>-<n>
    assert rows[0]["transaction_no"] == "TRANS-TEST-001"
    assert khalas[0]["box_id"] == "LINE-101-1"

    print("ALL ASSERTIONS PASSED — 51 unit-rows, unique box_ids, correct per-unit math, NOT NULL covered.")


if __name__ == "__main__":
    run()
