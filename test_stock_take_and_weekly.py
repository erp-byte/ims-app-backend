"""Stock take aggregation and weekly windowing — the pure parts, no database.

The rules worth pinning down here are the ones whose failure looks like a
plausible number rather than an error: a test floor counted as stock, an
off-grade pile added to fresh, one person's counting credited to another, or a
weekly that quietly reports a part-week.

Run it with `python test_stock_take_and_weekly.py`.
"""
from datetime import date

from services.ims_service.stock_take import (
    _clean_name, aggregate, best_name, canon_floor, canon_group, canon_wh,
    fingerprint, in_scope, outstanding, roster_split, scope_warehouses, top_skus,
)
from services.ims_service.weekly_report import internal_only, week_bounds

DAY = date(2026, 8, 14)


def _row(item="BLACK CHIA SEEDS", cat="SEEDS", floor="TERRACE", wh="W202",
         qty=10, uom=1.0, kg=10.0, by="SUMITBAIKAR", authority="SUMIT BAIKAR",
         stock="Fresh Stock", status="submitted", verified=False, itype="RM",
         entry_id="26080001"):
    return {"id": 1, "entry_id": entry_id, "item_name": item, "item_type": itype,
            "item_category": cat, "item_subcategory": None, "floor_name": floor,
            "warehouse": wh, "total_quantity": qty, "unit_uom": uom,
            "total_weight": kg, "entered_by": by, "entered_by_email": None,
            "authority": authority, "stock_type": stock, "status": status,
            "is_checked": False, "verified": verified, "verified_by": None,
            "created_at": DAY}


def test_canonicalisation():
    # trailing space and case are the same floor, not two half-counted ones
    assert canon_floor("TERRACE ") == canon_floor("terrace") == "TERRACE"
    assert canon_floor("1 ST FLOOR") == canon_floor("FIRST FLOOR") == "1ST FLOOR"
    assert canon_floor("COLD2") == "COLD 2"
    assert canon_floor("") == "(No floor named)"
    assert canon_wh(" w202 ") == "W202"
    assert canon_group("SEEDS") == canon_group("seeds") == "Seeds"
    assert canon_group("MISCELLANEOUS - RM") == "Miscellaneous - RM"
    assert canon_group(None) == "(Uncategorised)"


def test_clean_name_rejects_the_literal_null():
    # nine roster rows carry the four-character string 'null'
    assert _clean_name("null") == "" and _clean_name("NULL") == ""
    assert _clean_name("-") == "" and _clean_name(None) == ""
    assert _clean_name("  Raju   Paikrao ") == "Raju Paikrao"
    # the directory turns a login into the person
    assert best_name({"RAJUPAIKRAO": "RAJU PAIKRAO"}, "null", "rajupaikrao") \
        == "Raju Paikrao"
    # ...and the alias table does it even with no directory entry
    assert best_name({}, "madhurishewale") == "Madhuri Shewale"


def test_test_floors_are_excluded_from_every_figure():
    agg = aggregate({"rows": [_row(kg=100), _row(floor="TEST FLOOR", kg=999),
                              _row(floor="TESTINGG", kg=999)]}, DAY)
    assert agg["test_rows"] == 2
    assert agg["head"]["n"] == 1 and agg["head"]["kg"] == 100
    assert len(agg["floor"]) == 1          # the test floors are not locations


def test_off_grade_is_kept_apart_from_fresh():
    agg = aggregate({"rows": [
        _row(kg=100, stock="Fresh Stock"),
        _row(kg=25, stock="Off Grade/Rejection"),
    ]}, DAY)
    h = agg["head"]
    assert h["kg"] == 125                  # both are stock...
    assert h["fresh_kg"] == 100 and h["off_kg"] == 25   # ...but never pooled
    assert set(agg["by_stock"]) == {"Fresh stock", "Off grade / rejection"}


def test_counting_is_credited_to_the_keyer_not_the_authority():
    # Sumit keys entries under Swapnil's authority; the count is Sumit's work
    agg = aggregate({"rows": [_row(by="SUMITBAIKAR", authority="SWAPNIL RAIKAR")]},
                    DAY)
    assert list(agg["user"]) == ["SUMITBAIKAR"]
    # the authority is not borrowed as Sumit's display name
    assert agg["user"]["SUMITBAIKAR"]["name"] != "Swapnil Raikar"
    # ...but it IS used when it names the person who keyed it
    agg2 = aggregate({"rows": [_row(by="SUMITBAIKAR", authority="SUMIT BAIKAR")]}, DAY)
    assert agg2["user"]["SUMITBAIKAR"]["name"] == "Sumit Baikar"


def test_roster_split_and_unrostered():
    agg = aggregate({"rows": [_row(by="SUMITBAIKAR"), _row(by="GHOSTLOGIN",
                                                          authority="")]}, DAY)
    roster = [
        {"id": 1, "username": "sumitbaikar", "name": None, "email": None,
         "role": "floorhead", "warehouse": "A185", "is_active": True},
        {"id": 2, "username": "shabanasayyed", "name": "Shabana", "email": None,
         "role": "FLOORHEAD", "warehouse": "W202", "is_active": True},
        {"id": 3, "username": "bhrithik", "name": "null", "email": None,
         "role": "manager", "warehouse": "W202", "is_active": True},
    ]
    # The directory is what production gets from stocktake_entries.authority.
    out = roster_split({"roster": roster}, agg,
                       {"SHABANASAYYED": "SHABANA SAYYED"})
    assert [u["name"] for u in out["entered"]] == ["Sumit Baikar"]
    # a counting role that did not count is chased by name, resolved in full
    assert [u["name"] for u in out["missing"]] == ["Shabana Sayyed"]
    # a manager can key entries but is never listed as having failed to count;
    # 'null' in the name column must not become a person called Null
    assert [u["name"] for u in out["other"]] == ["B Hrithik"]
    # a login that counted but is on no roster is reported, never dropped
    assert [u["name"] for u in out["unrostered"]] == ["Ghostlogin"]
    assert out["expected"] == 2


def test_top_skus_share_sums_to_the_day():
    agg = aggregate({"rows": [_row(item="A", kg=75), _row(item="B", kg=25)]}, DAY)
    tops = top_skus(agg)
    assert [t["item"] for t in tops] == ["A", "B"]        # heaviest first
    assert abs(sum(t["share"] for t in tops) - 100.0) < 1e-6


def test_outstanding_is_as_of_the_report_day():
    data = {"backlog": [
        {"warehouse": "F53", "total": 282, "drafts": 0, "unverified": 282,
         "unchecked": 272, "last_count": date(2026, 4, 1), "kg": 7620.55},
        {"warehouse": "W202", "total": 100, "drafts": 5, "unverified": 90,
         "unchecked": 90, "last_count": DAY, "kg": 10.0},
    ]}
    out = outstanding(data, DAY)
    assert out["drafts"] == 5 and out["unverified"] == 372
    assert out["rows"][0]["warehouse"] == "F53"          # stalest first
    assert out["rows"][0]["days_since"] == 135
    assert [r["warehouse"] for r in out["stale"]] == ["F53"]
    assert out["rows"][1]["days_since"] == 0             # never negative


def test_fingerprint_moves_with_the_figures():
    a = aggregate({"rows": [_row(kg=10)]}, DAY)
    b = aggregate({"rows": [_row(kg=11)]}, DAY)
    empty = {"entered": [], "missing": [], "other": [], "unrostered": [],
             "expected": 0}
    out = {"drafts": 0, "unverified": 0, "unchecked": 0, "rows": [], "total": 0,
           "stale": []}
    assert fingerprint(a, empty, out) != fingerprint(b, empty, out)
    assert fingerprint(a, empty, out) == fingerprint(a, empty, out)


def test_week_bounds_is_always_a_complete_week():
    # run on Monday 17 Aug -> the week that just ended (Mon 10 to Sun 16)
    a, b = week_bounds(date(2026, 8, 17))
    assert (a, b) == (date(2026, 8, 10), date(2026, 8, 16))
    assert a.weekday() == 0 and b.weekday() == 6 and (b - a).days == 6
    # a catch-up run mid-week still reports the last COMPLETE week, never a
    # part-week that the next send would restate
    for d in (date(2026, 8, 18), date(2026, 8, 20), date(2026, 8, 23)):
        assert week_bounds(d) == (date(2026, 8, 10), date(2026, 8, 16))
    # ...and the next Monday moves on by exactly one week
    assert week_bounds(date(2026, 8, 24)) == (date(2026, 8, 17), date(2026, 8, 23))


def test_internal_only_allowlists_external_and_dedupes():
    keep, dropped = internal_only([
        "yash@candorfoods.in", "YASH@candorfoods.in", " b.hrithik@candorfoods.in ",
        "dipesh.sharma@ofbusiness.in",          # cleared by name
        "someone.else@ofbusiness.in",           # same domain, NOT cleared
        "", None,
    ])
    assert keep == ["yash@candorfoods.in", "b.hrithik@candorfoods.in",
                    "dipesh.sharma@ofbusiness.in"]
    # the allowlist is per address, never per domain
    assert dropped == ["someone.else@ofbusiness.in"]


def test_warehouse_scope_keeps_the_two_primaries():
    # a day where only W202 moved: A185 still gets a row, F53 does not
    agg = aggregate({"rows": [_row(wh="W202", kg=10)]}, DAY)
    assert scope_warehouses(agg) == ["W202", "A185"]
    # ...and a warehouse that WAS counted earns its row
    agg2 = aggregate({"rows": [_row(wh="W202"), _row(wh="F53")]}, DAY)
    assert scope_warehouses(agg2) == ["W202", "A185", "F53"]
    # hyphenated spellings are the same place
    assert canon_wh("W-202") == "W202" and canon_wh("A-185") == "A185"
    # staff with no warehouse, or 'All', are never filtered out by the rule
    scope = ["W202", "A185"]
    assert in_scope("W-202", scope) and not in_scope("F53", scope)
    assert in_scope("All", scope) and in_scope(None, scope)


def test_out_of_scope_warehouses_leave_the_mail_entirely():
    agg = aggregate({"rows": [_row(wh="W202")]}, DAY)
    scope = scope_warehouses(agg)
    roster = [
        {"id": 1, "username": "swadhinjoshi", "name": "Swadhin Joshi", "email": None,
         "role": "floorhead", "warehouse": "F53", "is_active": True},
        {"id": 2, "username": "shabanaansari", "name": "Shabana Ansari", "email": None,
         "role": "floorhead", "warehouse": "W202", "is_active": True},
    ]
    out = roster_split({"roster": roster}, agg, {}, scope)
    # F53's floor head is not chased on a day F53 was not in scope
    assert [u["name"] for u in out["missing"]] == ["Shabana Ansari"]

    data = {"backlog": [
        {"warehouse": "F53", "total": 282, "drafts": 0, "unverified": 282,
         "unchecked": 272, "last_count": date(2026, 4, 1), "kg": 7620.55},
        {"warehouse": "W202", "total": 100, "drafts": 5, "unverified": 90,
         "unchecked": 90, "last_count": DAY, "kg": 10.0},
    ]}
    o = outstanding(data, DAY, scope)
    assert [r["warehouse"] for r in o["rows"]] == ["W202"]
    # the totals under the table are the totals OF the table
    assert o["unverified"] == 90 and o["total"] == 100
    assert o["stale"] == []          # F53 is gone, so its 135 days go with it


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
    print("stock take + weekly tests passed")
