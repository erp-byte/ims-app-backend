"""Weekly roll-ups — Monday 10:00 AM IST, one per established report mail.

WHAT THIS IS
    Every recurring mail this system already sends gets a weekly companion,
    addressed to that mail's OWN recipient list rather than to a new committee:
    the people who read the daily inward report get the weekly inward report,
    the customer-return audience gets the customer-return week, and so on. A
    weekly that goes to a different list is a new report, not a summary of an
    existing one.

        daily        Daily Inward & Transfer report  -> its TO + CC
        stock_take   Stock Take daily report         -> its TO
        cr           Customer return notifications   -> the standing RTV CC set
        job_work     Job work notifications          -> billing + its CC

    The job-work stream REPLACES the Monday 09:00 digest that used to run from
    `shared.scheduler.job_work_weekly_digest`. Two job-work summaries an hour
    apart is worse than either alone, and the older one reported a narrower
    slice to a narrower list.

THE WEEK
    Monday to Sunday, the week that ENDED yesterday. Running at 10:00 on Monday
    means the week just gone is closed and cannot still be moving, which is the
    whole reason the weekly is not sent on Sunday night.

BUILT FROM THE DAILY AGGREGATORS, NOT FROM NEW SQL
    Each stream re-runs the same functions the daily mails use, one day at a
    time, and merges. It costs about ninety queries once a week and it buys the
    one property that matters: the weekly cannot disagree with the seven dailies
    it summarises. A separate set of week-range queries would drift the first
    time a measure rule changed in one place and not the other — and the drift
    would show up as a number nobody could reconcile, six weeks later.

EXTERNAL RECIPIENTS
    Allowed, by name only. A weekly roll-up of every customer return is a wider
    disclosure than the single-return CC that earned someone their place on the
    RTV list, so an outside address has to be written into `EXTERNAL_ALLOWED`
    here before it can receive one. `dipesh.sharma@ofbusiness.in` is on that
    list at the business's explicit instruction.

    Anything outside candorfoods.in and not on that list is dropped and named in
    the log — so adding an external reader stays a decision someone makes on
    purpose rather than a default nobody noticed.
"""
from __future__ import annotations

import smtplib
from collections import defaultdict
from datetime import date, timedelta
from email.message import EmailMessage

from sqlalchemy.orm import Session

from shared.config_loader import settings
from shared.database import SessionLocal
from shared.logger import get_logger
from shared.timezone import now_ist
from shared.mail_identity import Module, SubjectPolicy, stamp
from services.ims_service.report_delivery import Ledger

logger = get_logger("weekly_report")

INTERNAL_DOMAIN = "@candorfoods.in"

# Outside addresses cleared to receive a weekly roll-up. Named individually
# rather than by domain: this is the list that decides who outside the company
# sees a whole week of returns at once, and it should be short enough to read.
EXTERNAL_ALLOWED = {
    "dipesh.sharma@ofbusiness.in",     # OfBusiness — on the standing RTV CC
}

WEEKLY_HOUR, WEEKLY_MIN = 10, 0        # Monday, IST


def week_bounds(today: date) -> tuple[date, date]:
    """The Monday-to-Sunday week that ended before `today`.

    Run on Monday, that is the seven days just gone. Run on any other day (a
    manual re-send, a catch-up after an outage) it is still the last COMPLETE
    week, never a part-week that would be restated by the next send.
    """
    monday_this_week = today - timedelta(days=today.weekday())
    start = monday_this_week - timedelta(days=7)
    return start, start + timedelta(days=6)


def internal_only(addrs: list[str]) -> tuple[list[str], list[str]]:
    """Split a recipient list into (send to, dropped), de-duplicated.

    Internal addresses always pass; external ones pass only when explicitly
    cleared in `EXTERNAL_ALLOWED`.
    """
    seen, keep, dropped = set(), [], []
    for a in addrs:
        a = (a or "").strip()
        low = a.lower()
        if not a or low in seen:
            continue
        seen.add(low)
        allowed = low.endswith(INTERNAL_DOMAIN) or low in EXTERNAL_ALLOWED
        (keep if allowed else dropped).append(a)
    return keep, dropped


def _f(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _n(v) -> str:
    try:
        i = int(v or 0)
    except (TypeError, ValueError):
        return "–"
    return f"{i:,}" if i else "–"


def _kg(v) -> str:
    f = _f(v)
    return f"{f:,.2f}" if f else "–"


def _inr(v) -> str:
    from services.ims_service.daily_report import inr
    return inr(v)


def _days(a: date, b: date) -> list[date]:
    return [a + timedelta(days=i) for i in range((b - a).days + 1)]


def _table(title, headers, rows, aligns=None, *, total=None, note=None,
           widths=None, keep=None, cap=None, empty=None) -> dict:
    """One table in the normalised structure the weekly renderer walks.

    A total row on an empty table would read as a real zero-value week rather
    than as "nothing happened", so it is dropped when there are no rows.
    """
    return {"title": title, "headers": headers, "rows": rows, "aligns": aligns,
            "total": total if rows else None, "note": note, "widths": widths,
            "keep": keep, "cap": cap, "empty": empty}


# ═════════════════════════════════════════════════════════════════════════
#  STREAM 1 — the daily inward & transfer report
# ═════════════════════════════════════════════════════════════════════════
def build_daily(db: Session, a: date, b: date) -> dict:
    from services.ims_service.daily_report import (
        Measure, aggregate, canon_group, fetch, user_sort_key,
    )
    from services.ims_service.daily_report_ops import fetch_and_aggregate as ops_data
    from services.ims_service.daily_report_users import idle_over

    day_rows = []
    wh = defaultdict(lambda: {"itx": 0, "im": Measure(), "ival": 0.0,
                              "och": 0, "om": Measure(), "igrn": 0, "inm": Measure()})
    usr = defaultdict(lambda: {"itx": 0, "och": 0, "igrn": 0, "m": Measure()})
    cat = defaultdict(lambda: {"m": Measure(), "val": 0.0})
    tot = {"itx": 0, "och": 0, "igrn": 0, "ival": 0.0, "crs": 0, "cr_kg": 0.0,
           "cr_val": 0.0, "jc": 0, "jc_kg": 0.0, "sm": 0, "jw_out": 0,
           "jw_out_kg": 0.0, "jw_in": 0, "jw_fg": 0.0, "lines": 0, "missing": 0}
    t_im, t_om, t_in = Measure(), Measure(), Measure()
    cr_seen: set = set()

    for d in _days(a, b):
        agg = aggregate(fetch(db, d))
        ops = ops_data(db, d)
        h, vg = agg["head"], agg["val_gap"]
        jc, sm, jw, cr = ops["jobcards"], ops["samples"], ops["jobwork"], ops["cr"]

        day_rows.append([
            f"{d:%a %d %b}", _n(h["inw_txns"]), _kg(h["inw_m"].kg),
            _inr(h["inw_val"]), _n(h["out_chl"]), _n(h["in_grn"]),
            _n(cr["total_crs"]), _n(jc["total_cards"]),
            _n(len(sm["requisitions"]) + len(sm["npd_jobcards"])),
        ])

        tot["itx"] += h["inw_txns"]; tot["och"] += h["out_chl"]
        tot["igrn"] += h["in_grn"]; tot["ival"] += h["inw_val"]
        tot["lines"] += vg["lines"]; tot["missing"] += vg["missing"]
        tot["jc"] += jc["total_cards"]; tot["jc_kg"] += jc["total_kg"]
        tot["sm"] += len(sm["requisitions"]) + len(sm["npd_jobcards"])
        tot["jw_out"] += jw["out_challans"]; tot["jw_out_kg"] += jw["out_kg"]
        tot["jw_in"] += jw["in_receipts"]; tot["jw_fg"] += jw["in_fg_kg"]
        t_im.merge(h["inw_m"]); t_om.merge(h["out_m"]); t_in.merge(h["in_m"])

        # A CR appears on the day it was raised AND the day it was approved, so
        # a week that spans both would count it twice without this.
        for r in cr["rows"]:
            if r["cr_id"] in cr_seen:
                continue
            cr_seen.add(r["cr_id"])
            tot["crs"] += 1
            tot["cr_kg"] += r["kg"]
            tot["cr_val"] += r["value"]

        for name, w in agg["wh"].items():
            v = wh[name]
            v["itx"] += len(w["itx"]); v["och"] += len(w["och"])
            v["igrn"] += len(w["igrn"]); v["ival"] += w["ival"]
            v["im"].merge(w["im"]); v["om"].merge(w["om"]); v["inm"].merge(w["inm"])
        for name, u in agg["usr"].items():
            v = usr[name]
            v["itx"] += len(u["itx"]); v["och"] += len(u["och"])
            v["igrn"] += len(u["igrn"])
            for m in (u["im"], u["om"], u["inm"]):
                v["m"].merge(m)
        for (_w, g), v in agg["inw_wg"].items():
            cat[g]["m"].merge(v["m"]); cat[g]["val"] += v["val"]

    idle = idle_over(db, a, b)

    tiles = [
        ("Inward", _n(tot["itx"]) + " txns", _kg(t_im.kg) + " kg · Rs. " + _inr(tot["ival"])),
        ("Transfers", f'{tot["och"]} out · {tot["igrn"]} in',
         f'{_kg(t_om.kg)} kg out · {_kg(t_in.kg)} kg in'),
        ("Customer returns", _n(tot["crs"]),
         f'{_kg(tot["cr_kg"])} kg · Rs. {_inr(tot["cr_val"])}'),
        ("Job cards", _n(tot["jc"]), _kg(tot["jc_kg"]) + " kg planned"),
    ]

    flags = []
    if tot["missing"]:
        flags.append((f'Value not entered on {tot["missing"]:,} of '
                      f'{tot["lines"]:,} inward lines this week — the inward '
                      f'value above is understated.', "warn"))

    tables = []
    wh_rows = []
    for name in sorted(wh, key=lambda k: -(wh[k]["im"].kg + wh[k]["om"].kg + wh[k]["inm"].kg)):
        v = wh[name]
        wh_rows.append([name, _n(v["itx"]), _kg(v["im"].kg), _inr(v["ival"]),
                        _n(v["och"]), _kg(v["om"].kg), _n(v["igrn"]), _kg(v["inm"].kg)])
    tables.append(_table(
        "Warehouse-wise (week)",
        ["Warehouse", "Inward txns", "Inward kg", "Inward value (Rs.)",
         "Out challans", "Out kg", "In GRNs", "In kg"],
        wh_rows, ["left"] + ["right"] * 7, keep=[0, 1, 2, 3],
        total=["TOTAL", _n(tot["itx"]), _kg(t_im.kg), _inr(tot["ival"]),
               _n(tot["och"]), _kg(t_om.kg), _n(tot["igrn"]), _kg(t_in.kg)]))

    usr_rows = []
    for name in sorted(usr, key=user_sort_key):
        v = usr[name]
        n = v["itx"] + v["och"] + v["igrn"]
        if n:
            usr_rows.append([name, _n(v["itx"]), _n(v["och"]), _n(v["igrn"]),
                             _n(n), _kg(v["m"].kg)])
    tables.append(_table(
        "Who keyed it (week)",
        ["User", "Inward", "Trf out", "Trf in", "Total txns", "Total kg"],
        usr_rows, ["left"] + ["right"] * 5, keep=[0, 4, 5],
        note="The IMS login that saved the record, not the approval authority."))

    cat_rows = [[g, _kg(v["m"].kg), _inr(v["val"])]
                for g, v in sorted(cat.items(), key=lambda kv: -kv[1]["m"].kg)]
    tables.append(_table("Inward by item group (week)",
                         ["Category / Group", "Net kg", "Value (Rs.)"], cat_rows,
                         ["left", "right", "right"], cap=15))

    if not idle["unavailable"]:
        # `last_active` is None for an account with no trace at all, and a date
        # format string on None raises rather than printing a blank.
        rows = [[r["name"], "+".join(r["logins"]),
                 f'{r["last_active"]:%d %b %Y}' if r["last_active"] else "never",
                 _n(r["days_since"]) if r["days_since"] else "–"]
                for r in idle["silent"]]
        tables.append(_table(
            f'No activity all week ({len(idle["silent"])} of {idle["total"]} accounts)',
            ["User", "Logins", "Last active", "Days idle"], rows,
            ["left", "left", "right", "right"], keep=[0, 2, 3],
            note="Nobody on this list keyed or approved anything in IMS or ERP on "
                 "any day of the week. The roster is every active user account on "
                 "IMS, ERP and Stock Take — not names typed as free text elsewhere. "
                 "'never' means the account has no recorded activity at all.",
            cap=None))

    fp = (f'{tot["itx"]}:{t_im.kg:.2f}:{tot["ival"]:.2f}:{tot["och"]}:{tot["igrn"]}'
          f':{tot["crs"]}:{tot["jc"]}:{tot["sm"]}:{len(idle["silent"])}')
    return {"tiles": tiles, "flags": flags,
            "days": {"headers": ["Day", "Inward txns", "Inward kg", "Value (Rs.)",
                                 "Trf out", "Trf in", "CRs", "Job cards", "Samples"],
                     "aligns": ["left"] + ["right"] * 8, "rows": day_rows,
                     "keep": [0, 1, 2, 4, 5]},
            "tables": tables, "fingerprint": fp}


# ═════════════════════════════════════════════════════════════════════════
#  STREAM 2 — stock take
# ═════════════════════════════════════════════════════════════════════════
def build_stock_take(db: Session, a: date, b: date) -> dict:
    from services.ims_service.stock_take import build_range

    rep = build_range(db, a, b)
    agg, roster, out = rep["agg"], rep["roster"], rep["outstanding"]
    h = agg["head"]

    counted_days = {r["d"] for r in rep["by_day"]}
    day_rows = []
    for d in _days(a, b):
        r = next((x for x in rep["by_day"] if x["d"] == d), None)
        day_rows.append([
            f"{d:%a %d %b}",
            _n(r["entries"]) if r else "–",
            _kg(r["kg"]) if r else "–",
            _n(r["qty"]) if r else "–",
            _n(r["counters"]) if r else "–",
            _n(r["floors"]) if r else "–",
        ])

    tiles = [
        ("Entries", _n(h["n"]), f'on {len(counted_days)} of 7 days'),
        ("Total weight", _kg(h["kg"]) + " kg", _n(h["qty"]) + " units"),
        ("Floors counted", _n(len(agg["floor"])), f'{len(agg["wh"])} warehouses'),
        ("Counted by", _n(len(roster["entered"])),
         f'{len(roster["missing"])} rostered did not'),
    ]

    flags = []
    if not counted_days:
        flags.append(("No stock take entry was keyed on any day this week.", "warn"))
    if out["stale"]:
        flags.append(("Not counted in over 30 days: " + ", ".join(
            f'{r["warehouse"]} ({r["days_since"]}d)' for r in out["stale"]), "warn"))

    # W-202 and A-185 always get a row so a blank week at either is visible;
    # anywhere else appears only if it was counted. Same rule as the daily mail.
    blank = {"n": 0, "kg": 0.0, "qty": 0.0, "off_kg": 0.0, "floors": (), "users": ()}
    scope = rep["scope"]
    tables = [
        _table("Warehouse-wise (week)",
               ["Warehouse", "Entries", "Floors", "Counters", "Net kg", "Units",
                "Off grade kg"],
               [[w, _n(v["n"]), _n(len(v["floors"])), _n(len(v["users"])),
                 _kg(v["kg"]), _n(v["qty"]), _kg(v["off_kg"])]
                for w, v in sorted(((w, agg["wh"].get(w, blank)) for w in scope),
                                   key=lambda kv: -kv[1]["kg"])],
               ["left"] + ["right"] * 6, keep=[0, 1, 2, 4],
               total=["TOTAL", _n(h["n"]), _n(len(agg["floor"])),
                      _n(len(agg["user"])), _kg(h["kg"]), _n(h["qty"]),
                      _kg(h["off_kg"])],
               note="W-202 and A-185 are always shown; any other warehouse "
                    "appears only when something was counted there."),
        _table("Floor-wise (week)",
               ["Warehouse", "Floor", "Entries", "Net kg", "Units"],
               [[w, f, _n(v["n"]), _kg(v["kg"]), _n(v["qty"])]
                for (w, f), v in sorted(agg["floor"].items(),
                                        key=lambda kv: -kv[1]["kg"])],
               ["left", "left", "right", "right", "right"], cap=20, keep=[0, 1, 3]),
        _table("Item group (week)",
               ["Item group", "Entries", "Net kg", "Units", "SKUs"],
               [[g, _n(v["n"]), _kg(v["kg"]), _n(v["qty"]), _n(len(v["skus"]))]
                for g, v in sorted(agg["cat"].items(), key=lambda kv: -kv[1]["kg"])],
               ["left"] + ["right"] * 4, cap=15, keep=[0, 1, 2]),
        _table("Top 10 SKUs (week)",
               ["#", "SKU", "Group", "Net kg", "Share"],
               [[str(i), s["item"], s["group"], _kg(s["kg"]), f'{s["share"]:.1f}%']
                for i, s in enumerate(rep["top_skus"], 1)],
               ["right", "left", "left", "right", "right"],
               widths=[6, 40, 18, 20, 16], keep=[0, 1, 3]),
        _table("Who counted (week)",
               ["User", "Warehouse", "Entries", "Net kg", "Floors"],
               [[u["name"], u["warehouse"], _n(u["n"]), _kg(u["kg"]), _n(u["floors"])]
                for u in roster["entered"]],
               ["left", "left", "right", "right", "right"], keep=[0, 1, 2, 3],
               empty="Nobody counted anything this week"),
        _table(f'Rostered and did not count all week ({len(roster["missing"])})',
               ["Warehouse", "Count", "Users"],
               [[w, _n(len(v)), ", ".join(sorted(v))]
                for w, v in sorted(_by_wh(roster["missing"]).items())],
               ["left", "right", "left"], widths=[16, 10, 74], keep=[0, 2],
               note="Floor heads and floor managers only."),
        _table("Still open",
               ["Warehouse", "Last counted", "Days", "Drafts", "Unverified",
                "Unchecked"],
               [[r["warehouse"],
                 f'{r["last_count"]:%d %b %Y}' if r["last_count"] else "never",
                 _n(r["days_since"]), _n(r["drafts"]), _n(r["unverified"]),
                 _n(r["unchecked"])] for r in out["rows"]],
               ["left", "left", "right", "right", "right", "right"],
               keep=[0, 1, 2, 4],
               total=["TOTAL", "", "", _n(out["drafts"]), _n(out["unverified"]),
                      _n(out["unchecked"])],
               note="Standing position across all counts to date, not just this week."),
    ]

    return {"tiles": tiles, "flags": flags,
            "days": {"headers": ["Day", "Entries", "Net kg", "Units", "Counters",
                                 "Floors"],
                     "aligns": ["left"] + ["right"] * 5, "rows": day_rows,
                     "keep": [0, 1, 2, 4]},
            "tables": tables, "fingerprint": rep["fingerprint"]}


def _by_wh(rows) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for r in rows:
        out.setdefault(r["warehouse"], []).append(r["name"])
    return out


# ═════════════════════════════════════════════════════════════════════════
#  STREAM 3 — customer returns
# ═════════════════════════════════════════════════════════════════════════
def build_cr(db: Session, a: date, b: date) -> dict:
    from services.ims_service.daily_report_ops import aggregate_cr, fetch_cr

    seen: set = set()
    rows: list[dict] = []
    by_site = defaultdict(lambda: {"crs": 0, "kg": 0.0, "qty": 0.0, "value": 0.0})
    by_status = defaultdict(int)
    by_cust = defaultdict(lambda: {"crs": 0, "kg": 0.0, "value": 0.0})
    by_cat = defaultdict(lambda: {"kg": 0.0, "qty": 0.0, "value": 0.0})
    day_rows = []
    approved = 0

    for d in _days(a, b):
        cr = aggregate_cr(fetch_cr(db, d), d)
        day_rows.append([f"{d:%a %d %b}", _n(cr["total_crs"]), _kg(cr["total_kg"]),
                         _inr(cr["total_value"]), _n(cr["approved"])])
        approved += cr["approved"]
        for r in cr["rows"]:
            # Raised on one day, approved on another — one return, not two.
            if r["cr_id"] in seen:
                continue
            seen.add(r["cr_id"])
            rows.append(r)
            s = by_site[r["site"]]
            s["crs"] += 1; s["kg"] += r["kg"]; s["qty"] += r["qty"]; s["value"] += r["value"]
            by_status[r["status"]] += 1
            c = by_cust[r["customer"]]
            c["crs"] += 1; c["kg"] += r["kg"]; c["value"] += r["value"]
        for (cat, sub), v in cr["by_category"].items():
            k = by_cat[(cat, sub)]
            k["kg"] += v["kg"]; k["qty"] += v["qty"]; k["value"] += v["value"]

    tot_kg = sum(r["kg"] for r in rows)
    tot_val = sum(r["value"] for r in rows)

    tiles = [
        ("Returns", _n(len(rows)), f"{approved} approved this week"),
        ("Returned weight", _kg(tot_kg) + " kg", None),
        ("Value", "Rs. " + _inr(tot_val), None),
        ("Customers", _n(len(by_cust)), f'{len(by_site)} factories'),
    ]

    pending = sum(n for s, n in by_status.items()
                  if str(s).strip().lower() in ("pending", "submitted", "on hold"))
    flags = []
    if pending:
        flags.append((f'{pending} of {len(rows)} returns raised this week are '
                      f'still awaiting approval.', "warn"))

    tables = [
        _table("Factory-wise", ["Factory", "Returns", "Net kg", "Qty", "Value (Rs.)"],
               [[s, _n(v["crs"]), _kg(v["kg"]), _n(v["qty"]), _inr(v["value"])]
                for s, v in sorted(by_site.items(), key=lambda kv: -kv[1]["kg"])],
               ["left"] + ["right"] * 4, keep=[0, 1, 2, 4],
               total=["TOTAL", _n(len(rows)), _kg(tot_kg),
                      _n(sum(r["qty"] for r in rows)), _inr(tot_val)]),
        _table("Status", ["Status", "Returns"],
               [[s, _n(n)] for s, n in sorted(by_status.items(), key=lambda x: -x[1])],
               ["left", "right"]),
        _table("Customers", ["Customer", "Returns", "Net kg", "Value (Rs.)"],
               [[c, _n(v["crs"]), _kg(v["kg"]), _inr(v["value"])]
                for c, v in sorted(by_cust.items(), key=lambda kv: -kv[1]["kg"])],
               ["left", "right", "right", "right"], cap=15, keep=[0, 1, 2]),
        _table("Category × sub-group",
               ["Category", "Sub-group", "Net kg", "Qty", "Value (Rs.)"],
               [[c, s, _kg(v["kg"]), _n(v["qty"]), _inr(v["value"])]
                for (c, s), v in sorted(by_cat.items(), key=lambda kv: -kv[1]["kg"])],
               ["left", "left", "right", "right", "right"], cap=15, keep=[0, 1, 2]),
        _table("Returns raised or approved this week",
               ["CR", "Factory", "Customer", "Net kg", "Value (Rs.)", "Status",
                "Raised by"],
               [[r["cr_id"], r["site"], r["customer"], _kg(r["kg"]),
                 _inr(r["value"]), r["status"], r["by"]]
                for r in sorted(rows, key=lambda r: -r["kg"])],
               ["left", "left", "left", "right", "right", "left", "left"],
               widths=[17, 10, 24, 12, 14, 12, 11], keep=[0, 2, 3, 5], cap=25),
    ]

    return {"tiles": tiles, "flags": flags,
            "days": {"headers": ["Day", "Returns", "Net kg", "Value (Rs.)",
                                 "Approved"],
                     "aligns": ["left"] + ["right"] * 4, "rows": day_rows,
                     "keep": [0, 1, 2, 4]},
            "tables": tables,
            "fingerprint": f'{len(rows)}:{tot_kg:.2f}:{tot_val:.2f}:{approved}'}


# ═════════════════════════════════════════════════════════════════════════
#  STREAM 4 — job work
# ═════════════════════════════════════════════════════════════════════════
def build_job_work(db: Session, a: date, b: date) -> dict:
    from services.ims_service.daily_report_ops import aggregate_jobwork, fetch_jobwork

    out_rows: dict[str, dict] = {}
    in_rows: dict[str, dict] = {}
    by_party = defaultdict(lambda: {"out": 0, "out_kg": 0.0, "in": 0,
                                    "fg": 0.0, "waste": 0.0, "rej": 0.0})
    day_rows = []

    for d in _days(a, b):
        jw = aggregate_jobwork(fetch_jobwork(db, d))
        day_rows.append([f"{d:%a %d %b}", _n(jw["out_challans"]), _kg(jw["out_kg"]),
                         _n(jw["in_receipts"]), _kg(jw["in_fg_kg"]),
                         _kg(jw["in_waste_kg"] + jw["in_rej_kg"])])
        for r in jw["out_rows"]:
            out_rows.setdefault(r["challan_no"], r)
            p = by_party[r["party"]]
            p["out"] += 1; p["out_kg"] += r["kg"]
        for r in jw["in_rows"]:
            in_rows.setdefault(r["ir_number"], r)
            p = by_party[r["party"]]
            p["in"] += 1; p["fg"] += r["fg_kg"]
            p["waste"] += r["waste_kg"]; p["rej"] += r["rej_kg"]

    out_kg = sum(r["kg"] for r in out_rows.values())
    fg_kg = sum(r["fg_kg"] for r in in_rows.values())
    waste_kg = sum(r["waste_kg"] for r in in_rows.values())
    rej_kg = sum(r["rej_kg"] for r in in_rows.values())
    boxes = sum(r["boxes"] for r in out_rows.values())

    # Yield is only meaningful against what came back, not against what was sent
    # this week — material sent on Friday usually returns the following week.
    returned = fg_kg + waste_kg + rej_kg
    yield_pct = (100.0 * fg_kg / returned) if returned else 0.0

    tiles = [
        ("Sent out", _n(len(out_rows)) + " challans", _kg(out_kg) + f" kg · {boxes:,} boxes"),
        ("Received back", _n(len(in_rows)) + " receipts", _kg(fg_kg) + " kg FG"),
        ("Waste + rejection", _kg(waste_kg + rej_kg) + " kg",
         f'waste {_kg(waste_kg)} · rejection {_kg(rej_kg)}'),
        ("FG share of returns", f"{yield_pct:.1f}%" if returned else "–",
         "of what came back this week"),
    ]

    flags = []
    if rej_kg and returned and (100.0 * rej_kg / returned) > 5:
        flags.append((f'Rejection is {100.0 * rej_kg / returned:.1f}% of everything '
                      f'received back this week ({_kg(rej_kg)} kg).', "warn"))

    tables = [
        _table("Party-wise",
               ["Party", "Challans out", "Kg out", "Receipts", "FG kg",
                "Waste kg", "Rejection kg"],
               [[p, _n(v["out"]), _kg(v["out_kg"]), _n(v["in"]), _kg(v["fg"]),
                 _kg(v["waste"]), _kg(v["rej"])]
                for p, v in sorted(by_party.items(), key=lambda kv: -kv[1]["out_kg"])],
               ["left"] + ["right"] * 6, keep=[0, 1, 2, 4],
               total=["TOTAL", _n(len(out_rows)), _kg(out_kg), _n(len(in_rows)),
                      _kg(fg_kg), _kg(waste_kg), _kg(rej_kg)]),
        _table("Material out", ["Challan", "From", "Party", "Kg", "Boxes", "Status"],
               [[r["challan_no"], r["site"], r["party"], _kg(r["kg"]),
                 _n(r["boxes"]), r["status"]]
                for r in sorted(out_rows.values(), key=lambda r: -r["kg"])],
               ["left", "left", "left", "right", "right", "left"],
               widths=[18, 12, 30, 14, 11, 15], keep=[0, 2, 3], cap=25),
        _table("Material in",
               ["Receipt", "Challan", "Party", "FG kg", "Waste kg", "Rejection kg"],
               [[r["ir_number"], r["challan_no"], r["party"], _kg(r["fg_kg"]),
                 _kg(r["waste_kg"]), _kg(r["rej_kg"])]
                for r in sorted(in_rows.values(), key=lambda r: -r["fg_kg"])],
               ["left", "left", "left", "right", "right", "right"],
               widths=[20, 16, 26, 13, 12, 13], keep=[0, 2, 3], cap=25,
               note="FG is what the party returned as finished goods; waste and "
                    "rejection are the rest. Material sent late in the week is "
                    "usually received back in the next one, so out and in will "
                    "not reconcile within a single week."),
    ]

    return {"tiles": tiles, "flags": flags,
            "days": {"headers": ["Day", "Challans out", "Kg out", "Receipts",
                                 "FG kg", "Waste + rej kg"],
                     "aligns": ["left"] + ["right"] * 5, "rows": day_rows,
                     "keep": [0, 1, 2, 4]},
            "tables": tables,
            "fingerprint": f'{len(out_rows)}:{out_kg:.2f}:{len(in_rows)}:'
                           f'{fg_kg:.2f}:{waste_kg:.2f}:{rej_kg:.2f}'}


# ═════════════════════════════════════════════════════════════════════════
#  REGISTRY
# ═════════════════════════════════════════════════════════════════════════
def _daily_recipients() -> list[str]:
    from services.ims_service.daily_report import REPORT_CC, REPORT_TO
    return REPORT_TO + REPORT_CC


def _stock_take_recipients() -> list[str]:
    from services.ims_service.stock_take_report import REPORT_CC, REPORT_TO
    return REPORT_TO + REPORT_CC


def _cr_recipients() -> list[str]:
    from shared.email_notifier import RTV_CC_CONSTANT, RTV_NOTIFY_TO
    return [RTV_NOTIFY_TO] + list(RTV_CC_CONSTANT)


def _job_work_recipients() -> list[str]:
    from shared.email_notifier import JOB_WORK_CC, JOB_WORK_TO, WEEKLY_DIGEST_TO
    return [JOB_WORK_TO] + list(JOB_WORK_CC) + list(WEEKLY_DIGEST_TO)


STREAMS: dict[str, dict] = {
    "daily": {
        "title": "Weekly Inward & Transfer Report",
        "subtitle": "Inward, transfers, customer returns, job cards and samples",
        "tone": {"deep": "#1E3A6E", "mid": "#2F5FA8", "tint": "#EAF0FA"},
        "recipients": _daily_recipients,
        "build": build_daily,
        "source": "the daily inward & transfer report",
    },
    "stock_take": {
        "title": "Weekly Stock Take Report",
        "subtitle": "Physical counts by warehouse, floor, item group and counter",
        "tone": {"deep": "#0F5132", "mid": "#1A7F4B", "tint": "#E7F5EC"},
        "recipients": _stock_take_recipients,
        "build": build_stock_take,
        "source": "the daily stock take report",
    },
    "cr": {
        "title": "Weekly Customer Returns Report",
        "subtitle": "Every return raised or approved during the week",
        "tone": {"deep": "#8A2E4D", "mid": "#B8446B", "tint": "#FBEAF0"},
        "recipients": _cr_recipients,
        "build": build_cr,
        "source": "the customer return notifications",
    },
    "job_work": {
        "title": "Weekly Job Work Report",
        "subtitle": "Material sent to processing parties and what came back",
        "tone": {"deep": "#7A4310", "mid": "#B4661A", "tint": "#FBEFE1"},
        "recipients": _job_work_recipients,
        "build": build_job_work,
        "source": "the job work notifications",
    },
}

LEDGER = Ledger("weekly_report_log")


def week_anchor(key: str, start: date) -> str:
    return f"weekly-{key}-{start:%Y-%m-%d}@candorfoods.in"


def week_subject(key: str, start: date, end: date) -> str:
    title = STREAMS[key]["title"]
    if start.month == end.month:
        span = f"{start:%d}–{end:%d %b %Y}"
    else:
        span = f"{start:%d %b}–{end:%d %b %Y}"
    return f"{title} — {span}"


def view_url(key: str, start: date) -> str | None:
    base = (settings.BACKEND_URL or "").rstrip("/")
    return f"{base}/weekly-report/view?stream={key}&week={start:%Y-%m-%d}" if base else None


# ═════════════════════════════════════════════════════════════════════════
#  SEND
# ═════════════════════════════════════════════════════════════════════════
def _send_mail(subject: str, html_body: str, plain_body: str, to: list[str], *,
               key: str, start: date, message_id: str | None) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = settings.SMTP_EMAIL
    msg["To"] = ", ".join(to)
    msg.set_content(plain_body)
    msg.add_alternative(html_body, subtype="html")
    if message_id:
        msg["Message-ID"] = f"<{message_id}>"

    stamp(msg, module=Module.REPORTS, policy=SubjectPolicy.ANCHOR,
          entity_type="WeeklyReport", entity_id=f"{key}:{start:%Y-%m-%d}",
          event="WEEKLY_REPORT", status="report", sender=settings.SMTP_EMAIL)

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=90) as server:
        server.starttls()
        server.login(settings.SMTP_EMAIL, settings.SMTP_APP_PASSWORD)
        server.send_message(msg, to_addrs=to)


def build_stream(db: Session, key: str, start: date, end: date) -> dict:
    """The normalised report structure for one stream and one week."""
    spec = STREAMS[key]
    rep = spec["build"](db, start, end)
    return {**rep, "key": key, "title": spec["title"],
            "subtitle": spec["subtitle"], "tone": spec["tone"],
            "source": spec["source"], "start": start, "end": end}


def send_stream(key: str, start: date, end: date, *, kind: str = "weekly",
                db: Session | None = None,
                to_override: list[str] | None = None) -> dict:
    """Build and email one weekly stream. Never raises."""
    from services.ims_service.weekly_report_html import render_email, render_plain

    if key not in STREAMS:
        return {"stream": key, "status": "unknown_stream", "sent": False}

    own_db = db is None
    db = db or SessionLocal()
    try:
        LEDGER.ensure(db)
        rep = build_stream(db, key, start, end)

        to, dropped = internal_only(
            to_override if to_override is not None else STREAMS[key]["recipients"]())
        if dropped:
            logger.warning("Weekly %s: external recipients dropped from the "
                           "roll-up — %s", key, ", ".join(dropped))
        if not to:
            LEDGER.log(db, start, f"{kind}:{key}", "no_recipients")
            return {"stream": key, "status": "no_recipients", "sent": False}

        # The fingerprint carries the stream, so four streams can claim the same
        # week without colliding on the ledger's (day, kind, fingerprint) index.
        fp = f'{key}|{rep["fingerprint"]}'
        claim_id = None
        if kind == "weekly":
            claim_id = LEDGER.claim(db, start, "weekly", fp)
            if claim_id is None:
                logger.info("Weekly %s for %s: an identical send is already "
                            "recorded — skipping this duplicate", key, start)
                return {"stream": key, "status": "skipped_duplicate", "sent": False}

        generated = now_ist()
        html = render_email(rep, generated, view_url=view_url(key, start))
        plain = render_plain(rep, generated)
        try:
            _send_mail(week_subject(key, start, end), html, plain, to,
                       key=key, start=start, message_id=week_anchor(key, start))
        except Exception:
            if claim_id is not None:
                LEDGER.release(db, claim_id, "SMTP send failed")
            raise

        if claim_id is None:
            LEDGER.log(db, start, f"{kind}:{key}", "sent", fp)

        logger.info("Weekly %s (%s to %s) sent to %d recipients",
                    key, start, end, len(to))
        return {"stream": key, "status": "sent", "sent": True,
                "week": f"{start} to {end}", "recipients": to,
                "dropped_external": dropped, "fingerprint": fp}
    except Exception as exc:                                       # noqa: BLE001
        logger.error("Weekly %s (%s to %s) FAILED: %s", key, start, end, exc)
        try:
            db.rollback()
            LEDGER.log(db, start, f"{kind}:{key}", "failed", error=str(exc)[:1000])
        except Exception:                                          # noqa: BLE001
            logger.error("Could not record the weekly failure for %s", key)
        return {"stream": key, "status": "failed", "sent": False, "error": str(exc)}
    finally:
        if own_db:
            db.close()


def run_weekly_reports(today: date | None = None) -> list[dict]:
    """Monday 10:00 IST — every stream, each to its own recipients.

    One stream failing must not cost the other three: each is sent and recorded
    independently, and the caller gets a result per stream.
    """
    start, end = week_bounds(today or now_ist().date())
    results = []
    for key in STREAMS:
        try:
            results.append(send_stream(key, start, end))
        except Exception as exc:                                   # noqa: BLE001
            logger.error("Weekly stream %s raised: %s", key, exc)
            results.append({"stream": key, "status": "failed", "sent": False,
                            "error": str(exc)})
    sent = sum(1 for r in results if r.get("sent"))
    logger.info("Weekly reports for %s to %s: %d of %d streams sent",
                start, end, sent, len(STREAMS))
    return results
