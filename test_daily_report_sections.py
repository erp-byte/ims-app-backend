"""Job work and CR roll-ups — the part of the daily report that is not just SQL.

Both sections arrive as line-level rows and are reported per challan / per CR, so
a bad roll-up shows up as plausible-looking totals rather than an error. Pure
functions, no database: run it with `python test_daily_report_sections.py`.
"""
from datetime import date

from services.ims_service.daily_report_ops import aggregate_cr, aggregate_jobwork

DAY = date(2026, 8, 3)


def _jw_out(hid, challan, kg, boxes):
    return {"id": hid, "challan_no": challan, "from_warehouse": "Cold storage",
            "to_party": "HAG CORPORATION", "status": "sent",
            "created_by": "mayuresh@candorfoods.in", "expected_return_date": None,
            "purpose_of_work": None, "item_description": "wet dates",
            "item_category": "dates", "quantity_kgs": kg, "quantity_boxes": boxes,
            "uom": "CARTON"}


def _jw_in(hid, ir, sent, fg, waste, rej):
    return {"id": hid, "ir_number": ir, "challan_no": "HAG/26-27/125",
            "receipt_type": "final", "inward_warehouse": "A-185",
            "created_by": "stores-a185@candorfoods.in", "to_party": "HAG CORPORATION",
            "from_warehouse": "W202", "item_description": "khajur",
            "sent_kgs": sent, "finished_goods_kgs": fg, "finished_goods_boxes": 2,
            "waste_kgs": waste, "rejection_kgs": rej, "process_type": "Vacuum Packaging"}


def test_jobwork():
    jw = aggregate_jobwork({
        # one challan keyed as three box-lines, plus a second challan
        "out": [_jw_out(1, "JB1", 10, 1), _jw_out(1, "JB1", 10, 1), _jw_out(1, "JB1", 10, 1),
                _jw_out(2, "JB2", 500, 50)],
        "in": [_jw_in(9, "IR-1", 100, 60, 30, 10), _jw_in(9, "IR-1", 50, 50, 0, 0)],
    })
    assert jw["out_challans"] == 2, jw["out_challans"]          # NOT 4 line rows
    assert jw["out_kg"] == 530 and jw["out_boxes"] == 53
    assert jw["in_receipts"] == 1
    assert (jw["in_fg_kg"], jw["in_waste_kg"], jw["in_rej_kg"]) == (110, 30, 10)
    assert jw["out_rows"][0]["challan_no"] == "JB2"             # heaviest first
    assert jw["in_rows"][0]["site"] == "A185"                   # 'A-185' canonicalised
    assert jw["in_rows"][0]["by"] == "Stores A185"              # email -> name
    assert jw["parties"] == ["HAG CORPORATION"]
    assert not jw["empty"]

    empty = aggregate_jobwork({"out": [], "in": []})
    assert empty["empty"] and empty["out_kg"] == 0 and empty["in_receipts"] == 0

    # a challan saved before any line was added still counts as a challan
    header_only = aggregate_jobwork({"out": [dict(_jw_out(3, "JB3", None, None),
                                                  item_description=None)], "in": []})
    assert header_only["out_challans"] == 1 and header_only["out_kg"] == 0


def _cr(hid, rtv, status, kg, qty, value, raised, approved, cust="Reliance"):
    return {"id": hid, "rtv_id": rtv, "factory_unit": "W-202", "customer": cust,
            "status": status, "created_by": "samal.kumar@candorfoods.in",
            "approved_by": "rakesh@candorfoods.in" if approved else None,
            "raised_on": raised, "approved_on": approved,
            "qty": qty, "value": value, "net_kg": kg, "company": "CFPL"}


def test_cr():
    cr = aggregate_cr({
        "headers": [_cr(1, "CR-1", "Approved", 324, 3240, 107568.90, DAY, DAY),
                    # raised earlier, approved today — belongs to today either way
                    _cr(2, "CR-2", "Approved", 20, 20, 5560, date(2026, 7, 29), DAY),
                    _cr(3, "CR-3", "Pending", 16.4, 136, 38346.40, DAY, None, "Align Retail")],
        "categories": [{"item_category": "PISTA", "sub_category": "pista - inshell",
                        "lines": 5, "qty": 70, "value": 92169, "net_kg": 70},
                       {"item_category": "pista", "sub_category": "Pista - Inshell",
                        "lines": 1, "qty": 10, "value": 1000, "net_kg": 10}],
    }, DAY)

    assert cr["total_crs"] == 3
    assert round(cr["total_kg"], 2) == 360.40
    assert round(cr["total_value"], 2) == 151475.30
    assert cr["approved"] == 2                       # approved TODAY, not status=Approved
    assert cr["by_status"] == {"Approved": 2, "Pending": 1}
    assert list(cr["by_site"]) == ["W202"]           # 'W-202' canonicalised
    assert cr["by_site"]["W202"]["crs"] == 3
    assert cr["rows"][0]["cr_id"] == "CR-1"          # heaviest first
    assert cr["rows"][0]["by"] == "Samal Kumar" and cr["rows"][0]["approver"] == "Rakesh"
    assert cr["rows"][2]["approver"] == "–"
    assert sorted(cr["customers"]) == ["Align Retail", "Reliance"]
    # case variants of the same category must not become two rows
    assert cr["by_category"] == {("Pista", "Pista - Inshell"):
                                 {"lines": 6, "kg": 80.0, "qty": 80.0, "value": 93169.0}}
    assert not cr["empty"]

    assert aggregate_cr({"headers": [], "categories": []}, DAY)["empty"]


if __name__ == "__main__":
    test_jobwork()
    test_cr()
    print("daily report job work + CR roll-ups: OK")
