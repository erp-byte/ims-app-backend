"""Dependency-free: RTV cold boxes mirror into *_cold_stocks for cold warehouses.
Run: python test_rtv_cold_sync.py
"""
from services.ims_service import rtv_tools


class _Row:
    def __init__(self, **kw): self.__dict__.update(kw)
    def __getattr__(self, n): return None


class ColdSyncDB:
    def __init__(self, factory_unit):
        self.factory_unit = factory_unit
        self.calls = []
    def execute(self, clause, params=None):
        sql = str(clause); self.calls.append((sql, params or {}))
        class R:
            def __init__(self, row, rowcount=0): self._row = row; self.rowcount = rowcount
            def fetchone(self): return self._row
        if "_rtv_header" in sql and "factory_unit" in sql:
            return R(_Row(factory_unit=self.factory_unit, customer="ACME", rtv_id="RTV-1", rtv_date=None))
        if "INSERT INTO" in sql and "_cold_stocks" in sql:
            return R(None, rowcount=3)
        return R(None)
    def commit(self): pass


def _sqls(db, needle):
    return [s for s, _ in db.calls if needle in s]


def test_cold_warehouse_inserts_into_cold_stocks():
    db = ColdSyncDB("Savla D-39")
    n = rtv_tools.sync_cold_stocks_from_rtv("CFPL", 1, db)
    assert n == 3, n
    assert _sqls(db, "DELETE FROM cfpl_cold_stocks"), "should clear its own auto rows first"
    ins = [(s, p) for s, p in db.calls if "INSERT INTO cfpl_cold_stocks" in s]
    assert ins, "should insert cold rows"
    sql, params = ins[0]
    for col in ("lot_no", "item_mark", "vakkal", "spl_remarks", "box_id",
                "canonical_warehouse", "total_inventory_kgs", "transaction_no",
                "inward_transaction_no", "auto_created_from_inward"):
        assert col in sql, f"{col} missing from cold INSERT"
    assert params["unit"] == "D-39", params["unit"]
    assert params["wh"] == "Savla D-39", params["wh"]
    print("test_cold_warehouse_inserts_into_cold_stocks: PASS")


def test_dry_warehouse_clears_but_no_insert():
    db = ColdSyncDB("W202")
    n = rtv_tools.sync_cold_stocks_from_rtv("CFPL", 1, db)
    assert n == 0
    assert _sqls(db, "DELETE FROM cfpl_cold_stocks"), "should still clear stale auto rows"
    assert not [s for s, _ in db.calls if "INSERT INTO cfpl_cold_stocks" in s], "no insert for dry"
    print("test_dry_warehouse_clears_but_no_insert: PASS")


def test_legacy_alias_maps_to_unit():
    db = ColdSyncDB("new savla")  # alias -> Savla D-514 -> unit D-514
    rtv_tools.sync_cold_stocks_from_rtv("CFPL", 1, db)
    ins = [(s, p) for s, p in db.calls if "INSERT INTO cfpl_cold_stocks" in s]
    assert ins and ins[0][1]["unit"] == "D-514", ins
    print("test_legacy_alias_maps_to_unit: PASS")


def test_missing_header_is_noop():
    class EmptyDB:
        def __init__(self): self.calls = []
        def execute(self, clause, params=None):
            self.calls.append((str(clause), params or {}))
            class R:
                def fetchone(self): return None
                rowcount = 0
            return R()
        def commit(self): pass
    db = EmptyDB()
    assert rtv_tools.sync_cold_stocks_from_rtv("CFPL", 999, db) == 0
    assert not [s for s, _ in db.calls if "cold_stocks" in s], "no cold writes when header missing"
    print("test_missing_header_is_noop: PASS")


if __name__ == "__main__":
    test_cold_warehouse_inserts_into_cold_stocks()
    test_dry_warehouse_clears_but_no_insert()
    test_legacy_alias_maps_to_unit()
    test_missing_header_is_noop()
    print("ALL PASS")
