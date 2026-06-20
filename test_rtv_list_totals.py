"""Dependency-free: the RTV list query must NOT fan-out total_qty across boxes
(qty × box_count) and must return an actual net-weight total (kg).
Run: python test_rtv_list_totals.py
"""
from services.ims_service import rtv_tools


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)
    def __getattr__(self, n):
        return None


class ListDB:
    def __init__(self):
        self.calls = []

    def execute(self, clause, params=None):
        sql = str(clause)
        self.calls.append(sql)

        class R:
            def scalar(self):
                return 1
            def fetchall(self):
                return [_Row(
                    id=1, rtv_id="CR-1", rtv_date=None, factory_unit="W202", customer="c",
                    invoice_number=None, challan_no=None, dn_no=None, conversion=0,
                    sales_poc=None, business_head=None, remark=None, status="Pending",
                    created_by=None, created_ts=None, updated_at=None,
                    vehicle_number=None, transporter_name=None, driver_name=None, inward_manager=None,
                    items_count=1, boxes_count=100, total_qty=85, total_net_weight=850.0,
                )]
        return R()

    def commit(self):
        pass


def test_list_query_no_fanout_and_net_weight():
    db = ListDB()
    res = rtv_tools.list_rtvs("CFPL", 1, 10, None, None, None, None, None, "created_ts", "desc", db)
    sql = [s for s in db.calls if "items_count" in s][0]
    # The header SELECT must not LEFT JOIN lines/boxes (that fanned out SUM(l.qty)).
    assert "LEFT JOIN" not in sql, "list query still LEFT JOINs — fan-out risk persists"
    assert "total_net_weight" in sql, "net-weight total missing from query"
    rec = res["records"][0]
    assert rec["total_qty"] == 85, f"total_qty wrong: {rec['total_qty']}"
    assert rec["total_net_weight"] == 850.0, f"total_net_weight wrong: {rec['total_net_weight']}"
    print("test_list_query_no_fanout_and_net_weight: PASS")


if __name__ == "__main__":
    test_list_query_no_fanout_and_net_weight()
    print("ALL PASS")
