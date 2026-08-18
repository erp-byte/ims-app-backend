"""Stock take — the day's physical count, aggregated for the 7:00 PM mail.

WHAT IT REPORTS
    Entries keyed into the Stock Take app (`stocktake_entries`): how much was
    counted, where, by whom — and, just as deliberately, who was rostered to
    count and did not.

COUNTING IS A CAMPAIGN, NOT A DAILY HABIT
    31 counting days between 24 Jan and 18 Aug 2026, in bursts: 1,006 entries on
    14 Aug, then nothing for four days; nothing at all between 13 Jul and 10 Aug.
    A mail that only reported the day's tally would therefore be blank most of
    the time, and a blank mail that arrives every evening trains people to stop
    opening it — by the time a real count lands, nobody looks.

    So a day with no counting still carries the standing position: how long each
    warehouse has gone uncounted, and the verification backlog sitting in the
    app. That is the "outstanding" view, and it is the reason the quiet-day mail
    is worth sending at all.

MEASURE
    `total_weight` is the reported figure. The app computes it as
    total_quantity x unit_uom on save, so it is already the reconciled number
    and re-deriving it here would only invent a second answer. Quantity is
    carried alongside because packaging is counted in units, not kilos.

WHO COUNTED
    `entered_by` is the Stock Take login ('SUMITBAIKAR'), not a name, and it is
    matched against `stocktake_users` to decide who is missing. The match is on
    the squashed spelling so 'R M PATIL', 'rmpatil' and 'R.M.Patil' all find the
    same row; display names come from the entry's own `authority` field, which
    is where the human name is actually written out.

FLOORS
    Free text, and it shows: 'TERRACE' and 'TERRACE ', 'UPPER BASEMENT' twice,
    '1ST FLOOR' / '1 ST FLOOR' / 'FIRST FLOOR'. Whitespace collapses on its own;
    the rest needs the alias table below, because a floor reported twice under
    two spellings is two half-counted floors to anyone reading the mail.
"""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger
from services.ims_service.daily_report_users import (
    base_fold, squash, name_directory, titlecase,
)

logger = get_logger("stock_take")

DASH = "–"

# Counting roles — the people a count is actually expected from. Everyone else
# on the roster (managers, superusers) can key entries and sometimes does, but
# "the inventory manager did not count today" is not a finding.
COUNTING_ROLES = {"floorhead", "floor_manager"}

# Rows that are somebody testing the app rather than counting stock. They are
# real rows and they would otherwise sit in the floor table as if they were a
# location, and in the SKU table as if they were goods.
TEST_FLOORS = {"TEST", "TEST FLOOR", "TESTINGG", "TEESTTTT", "TESTING"}

_FLOOR_ALIASES = {
    "1 ST FLOOR": "1ST FLOOR",
    "FIRST FLOOR": "1ST FLOOR",
    "1ST FLOOR MAZZE": "1ST MEZZANINE",
    "1ST - MAZZO": "1ST MEZZANINE",
    "MAZERNINE": "MEZZANINE",
    "COLD2": "COLD 2",
    "PACKING": "PACKING FLOOR",
    "DOCK": "DOCK AREA",
    "RACK": "RACK AREA",
    "PRODUCTION": "PRODUCTION FLOOR",
    "PRODUCTION - ROASTING": "ROASTING AREA",
}


def canon_floor(raw) -> str:
    s = re.sub(r"\s+", " ", str(raw or "").strip()).upper()
    if not s:
        return "(No floor named)"
    return _FLOOR_ALIASES.get(s, s)


# Counting happens at W-202 and A-185. Everywhere else is either dormant (F53
# was last counted in April) or was never in scope (A101), and a warehouse that
# appears every evening with nothing against it is a row people learn to skip.
PRIMARY_WAREHOUSES = ("W202", "A185")

_WH_ALIASES = {"W-202": "W202", "W 202": "W202", "A-185": "A185",
               "A 185": "A185", "F-53": "F53", "A-101": "A101", "A-68": "A68"}


def canon_wh(raw) -> str:
    s = re.sub(r"\s+", " ", str(raw or "").strip()).upper()
    if not s:
        return "(No warehouse)"
    return _WH_ALIASES.get(s, s)


def scope_warehouses(agg: dict) -> list[str]:
    """Which warehouses this mail shows: the two primaries, plus any that moved.

    The primaries are always present so that a zero at W-202 is visible as a
    zero rather than as an absent row — which is the whole point of reporting
    them. Anywhere else earns its row by having been counted.
    """
    extra = sorted(w for w, b in agg["wh"].items()
                   if b["n"] and w not in PRIMARY_WAREHOUSES)
    return list(PRIMARY_WAREHOUSES) + extra


def in_scope(warehouse: str, scope) -> bool:
    """True for a warehouse on show, and for staff not tied to one.

    A counter assigned to 'All' or to no warehouse belongs to whatever was
    counted, so they are never filtered out by a warehouse rule.
    """
    w = canon_wh(warehouse)
    return w in set(scope) or w.upper() in ("ALL", "(NO WAREHOUSE)", DASH)


_GROUP_ACRONYMS = {"RM", "PM", "FG"}


def canon_group(raw) -> str:
    """'SEEDS'/'seeds' -> 'Seeds'. Mirrors the daily report's grouping."""
    s = re.sub(r"\s+", " ", str(raw or "").strip())
    if not s:
        return "(Uncategorised)"
    return " ".join(w.upper() if w.upper() in _GROUP_ACRONYMS else w.capitalize()
                    for w in s.split())


def _f(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


# `stocktake_users.name` holds the four-character string 'null' for nine of the
# thirty-nine rows — not SQL NULL, the word. Printed straight through it becomes
# a floor head called "Null", which is how the roster block first read.
_NON_NAMES = {"", "-", "null", "none", "nil", "na", "n/a", "undefined"}


def _clean_name(v) -> str:
    s = re.sub(r"\s+", " ", str(v or "").strip())
    return "" if s.lower() in _NON_NAMES else s


def best_name(directory: dict[str, str], *candidates) -> str:
    """The fullest usable spelling among the candidates and the directory.

    'shabanasayyed' has a `name` of 'Shabana' on its roster row and a full
    'SHABANA SAYYED' in the directory; the roster row is not wrong, it is just
    less useful, and word count is what separates the two.
    """
    from services.ims_service.daily_report_users import fold, rank

    pool = [_clean_name(c) for c in candidates]
    for c in list(pool):
        if not c:
            continue
        # The alias table is itself a source of full names: 'madhurishewale'
        # resolves to MADHURI SHEWALE without the directory needing to hold it.
        aliased = fold(c)
        if " " in aliased:
            pool.append(aliased)
        # Try both the raw and the alias-resolved spelling, so a login the alias
        # table knows ('arbaazshaikh' -> ARBAJ SHAIKH) still finds its name.
        for key in (squash(base_fold(c)), squash(aliased)):
            hit = directory.get(key)
            if hit:
                pool.append(hit)
    pool = [c for c in pool if c]
    return titlecase(max(pool, key=rank)) if pool else ""


# ═════════════════════════════════════════════════════════════════════════
#  SQL
# ═════════════════════════════════════════════════════════════════════════
DAY_SQL = """
SELECT id, entry_id, item_name, item_type, item_category, item_subcategory,
       floor_name, warehouse, total_quantity, unit_uom, total_weight,
       entered_by, entered_by_email, authority, stock_type, status,
       is_checked, verified, verified_by, created_at
FROM stocktake_entries
WHERE created_at::date = :d
"""

# The standing position, independent of whether anything was counted today.
# Drafts and unverified rows are what the next person to open the app has to
# deal with, so they belong in every mail, not only the busy ones.
#
# Bounded at `:d` because a report for a past day must read as it did on that
# day. Unbounded, a re-send of 14 Aug picks up the 18 Aug count and reports the
# warehouse as counted -4 days ago.
BACKLOG_SQL = """
SELECT warehouse,
       COUNT(*)                                                        AS total,
       COUNT(*) FILTER (WHERE status = 'draft')                        AS drafts,
       COUNT(*) FILTER (WHERE status <> 'draft'
                          AND NOT COALESCE(verified, false))           AS unverified,
       COUNT(*) FILTER (WHERE NOT COALESCE(is_checked, false))         AS unchecked,
       MAX(created_at)::date                                           AS last_count,
       SUM(COALESCE(total_weight, 0))                                  AS kg
FROM stocktake_entries
WHERE created_at::date <= :d
GROUP BY warehouse
"""

ROSTER_SQL = """
SELECT id, username, name, email, role, warehouse, is_active
FROM stocktake_users
WHERE is_active
ORDER BY id
"""


def fetch(db: Session, day: date) -> dict:
    def run(label, sql, **p):
        try:
            return [dict(r._mapping) for r in db.execute(text(sql), p)]
        except Exception as exc:                                   # noqa: BLE001
            logger.error("Stock take %s fetch failed for %s: %s", label, day, exc)
            db.rollback()
            return []

    return {
        "rows": run("entries", DAY_SQL, d=day),
        "backlog": run("backlog", BACKLOG_SQL, d=day),
        "roster": run("roster", ROSTER_SQL),
    }


# ═════════════════════════════════════════════════════════════════════════
#  AGGREGATION
# ═════════════════════════════════════════════════════════════════════════
def _bucket():
    return {"n": 0, "kg": 0.0, "qty": 0.0, "floors": set(), "users": set(),
            "skus": set(), "fresh_kg": 0.0, "off_kg": 0.0,
            "drafts": 0, "unverified": 0}


def aggregate(data: dict, day: date, directory: dict[str, str] | None = None) -> dict:
    """Everything the mail prints, from one pass over the day's entries."""
    rows = data["rows"]
    directory = directory or {}

    head = _bucket()
    wh = defaultdict(_bucket)
    floor = defaultdict(_bucket)
    cat = defaultdict(_bucket)
    sku = defaultdict(_bucket)
    user = defaultdict(_bucket)
    by_type = defaultdict(_bucket)
    by_stock = defaultdict(_bucket)
    batches: set[str] = set()
    test_rows = 0

    for r in rows:
        f = canon_floor(r["floor_name"])
        if f in TEST_FLOORS:
            test_rows += 1
            continue

        w = canon_wh(r["warehouse"])
        g = canon_group(r["item_category"])
        item = re.sub(r"\s+", " ", str(r["item_name"] or "").strip()) or "(Unnamed)"
        ukey = squash(base_fold(r["entered_by"]))
        kg, qty = _f(r["total_weight"]), _f(r["total_quantity"])
        fresh = str(r["stock_type"] or "").strip().lower() != "off grade/rejection"
        draft = str(r["status"] or "").strip().lower() == "draft"
        unver = not draft and not bool(r["verified"])
        if r["entry_id"]:
            batches.add(str(r["entry_id"]))

        for b, key in ((head, None), (wh[w], w), (floor[(w, f)], f),
                       (cat[g], g), (sku[(item, g)], item), (user[ukey], ukey),
                       (by_type[str(r["item_type"] or "").strip().upper() or "(Blank)"], None),
                       (by_stock["Fresh stock" if fresh else "Off grade / rejection"], None)):
            b["n"] += 1
            b["kg"] += kg
            b["qty"] += qty
            b["floors"].add((w, f))
            b["users"].add(ukey)
            b["skus"].add(item)
            b["drafts"] += 1 if draft else 0
            b["unverified"] += 1 if unver else 0
            if fresh:
                b["fresh_kg"] += kg
            else:
                b["off_kg"] += kg

        # The human name lives on the entry, not on the login row — but only
        # when the authority IS the person who keyed it. Sumit Baikar keys
        # entries under Swapnil Raikar's authority, and taking that name would
        # credit one man's counting to another.
        auth = _clean_name(r["authority"])
        if auth and squash(base_fold(auth)) == ukey and \
                len(auth) > len(user[ukey].get("name", "")):
            user[ukey]["name"] = auth

    for key, b in user.items():
        b["name"] = best_name(directory, b.get("name"), key) or titlecase(key)

    return {
        "head": head, "wh": dict(wh), "floor": dict(floor), "cat": dict(cat),
        "sku": dict(sku), "user": dict(user), "by_type": dict(by_type),
        "by_stock": dict(by_stock), "batches": batches,
        "test_rows": test_rows, "day": day,
        "empty": head["n"] == 0,
    }


def top_skus(agg: dict, n: int = 10) -> list[dict]:
    """The n heaviest SKUs counted, with their share of the day's weight."""
    total = agg["head"]["kg"] or 1.0
    items = sorted(agg["sku"].items(), key=lambda kv: -kv[1]["kg"])[:n]
    return [{"item": item, "group": group, "kg": b["kg"], "qty": b["qty"],
             "lines": b["n"], "share": 100.0 * b["kg"] / total}
            for (item, group), b in items]


# ═════════════════════════════════════════════════════════════════════════
#  WHO COUNTED, AND WHO DID NOT
# ═════════════════════════════════════════════════════════════════════════
def roster_split(data: dict, agg: dict, directory: dict[str, str],
                 scope=None) -> dict:
    """Rostered counters who entered today, against those who did not.

    A login is matched on its squashed spelling, and on every spelling the row
    carries — username, name and email all point at the same person, and only
    one of them is what the app happened to write into `entered_by`.
    """
    counted = set(agg["user"])
    entered, missing, other = [], [], []

    for u in data["roster"]:
        # `name` is 'null' on nine roster rows, and its squashed form would
        # otherwise match any entry keyed under a login of the same shape.
        keys = {squash(base_fold(v)) for v in
                (u["username"], _clean_name(u["name"]), u["email"]) if v}
        keys.discard("")
        hit = next((k for k in keys if k in counted), None)
        name = best_name(directory, u["name"], u["username"],
                         agg["user"][hit]["name"] if hit else None) \
            or titlecase(u["username"])
        row = {"name": name, "warehouse": canon_wh(u["warehouse"]) if u["warehouse"] else DASH,
               "role": (u["role"] or "").strip(),
               "counts": bool(hit)}
        if hit:
            b = agg["user"][hit]
            row.update(n=b["n"], kg=b["kg"], qty=b["qty"],
                       floors=len(b["floors"]), key=hit)
            entered.append(row)
        elif scope is not None and not in_scope(u["warehouse"], scope):
            # Rostered to a warehouse this mail is not reporting on. Chasing
            # F53's floor heads on a day F53 was never in scope is noise that
            # buries the two warehouses that matter.
            continue
        elif row["role"].lower() in COUNTING_ROLES:
            missing.append(row)
        else:
            other.append(row)

    # Anyone who counted but is not on the roster at all — a login that still
    # works after the person left, or a manager helping out. Reported rather
    # than dropped: the day's totals include their entries either way.
    known = {squash(base_fold(v)) for u in data["roster"]
             for v in (u["username"], _clean_name(u["name"]), u["email"]) if v}
    unrostered = [{"name": agg["user"][k]["name"], "n": agg["user"][k]["n"],
                   "kg": agg["user"][k]["kg"], "qty": agg["user"][k]["qty"],
                   "floors": len(agg["user"][k]["floors"]), "key": k}
                  for k in sorted(counted - known)]

    entered.sort(key=lambda r: -r["n"])
    unrostered.sort(key=lambda r: -r["n"])
    missing.sort(key=lambda r: (r["warehouse"], r["name"]))
    other.sort(key=lambda r: (r["warehouse"], r["name"]))
    return {"entered": entered, "missing": missing, "other": other,
            "unrostered": unrostered,
            "expected": len(entered) + len(missing)}


# ═════════════════════════════════════════════════════════════════════════
#  OUTSTANDING  (what the quiet-day mail is for)
# ═════════════════════════════════════════════════════════════════════════
def outstanding(data: dict, day: date, scope=None) -> dict:
    """The standing position: uncounted time and the verification backlog.

    Restricted to the warehouses on show, so the totals underneath the table
    are the totals OF the table. A grand total that silently included F53's 282
    unverified entries while F53 had no row would not add up on the page.
    """
    rows = []
    tot = {"total": 0, "drafts": 0, "unverified": 0, "unchecked": 0}
    for r in data["backlog"]:
        if scope is not None and not in_scope(r["warehouse"], scope):
            continue
        last = r["last_count"]
        rows.append({
            "warehouse": canon_wh(r["warehouse"]),
            "last_count": last,
            "days_since": (day - last).days if last else None,
            "total": int(r["total"] or 0),
            "drafts": int(r["drafts"] or 0),
            "unverified": int(r["unverified"] or 0),
            "unchecked": int(r["unchecked"] or 0),
            "kg": _f(r["kg"]),
        })
        for k in tot:
            tot[k] += int(r[k] or 0) if k != "total" else int(r["total"] or 0)

    rows.sort(key=lambda r: (-(r["days_since"] or 0), r["warehouse"]))
    return {"rows": rows, **tot,
            "stale": [r for r in rows if (r["days_since"] or 0) >= 30]}


def fingerprint(agg: dict, roster: dict, out: dict) -> str:
    """Stable signature of the figures, so a re-send can be told from a repeat."""
    h = agg["head"]
    return "|".join([
        f"n:{h['n']}:{h['kg']:.2f}:{h['qty']:.2f}",
        f"w:{sorted((k, round(v['kg'], 2)) for k, v in agg['wh'].items())}",
        f"f:{len(agg['floor'])}:b:{len(agg['batches'])}",
        f"u:{roster['expected']}:{len(roster['entered'])}:{len(roster['missing'])}",
        f"o:{out['drafts']}:{out['unverified']}:{out['unchecked']}",
    ])


def build(db: Session, day: date) -> dict:
    """Everything the mail needs for `day`, in one call."""
    data = fetch(db, day)
    _, dir_by_squash = name_directory(db)
    agg = aggregate(data, day, dir_by_squash)
    scope = scope_warehouses(agg)
    roster = roster_split(data, agg, dir_by_squash, scope)
    out = outstanding(data, day, scope)
    return {"day": day, "agg": agg, "roster": roster, "outstanding": out,
            "scope": scope, "top_skus": top_skus(agg),
            "fingerprint": fingerprint(agg, roster, out)}


RANGE_SQL = DAY_SQL.replace("created_at::date = :d", "created_at::date BETWEEN :a AND :b")

# One row per counting day, for the weekly trend line.
BY_DAY_SQL = """
SELECT created_at::date            AS d,
       COUNT(*)                    AS entries,
       COUNT(DISTINCT entered_by)  AS counters,
       COUNT(DISTINCT warehouse)   AS warehouses,
       COUNT(DISTINCT warehouse || '/' || COALESCE(floor_name, '')) AS floors,
       SUM(COALESCE(total_weight, 0))   AS kg,
       SUM(COALESCE(total_quantity, 0)) AS qty
FROM stocktake_entries
WHERE created_at::date BETWEEN :a AND :b
GROUP BY 1 ORDER BY 1
"""


def build_range(db: Session, a: date, b: date) -> dict:
    """The same aggregation over a date range, for the weekly mail.

    `aggregate` only ever reads `data["rows"]`, so a range is a different WHERE
    clause and nothing else — the weekly and the daily cannot disagree about
    what a floor or an item group is, because it is one function.
    """
    def run(label, sql, **p):
        try:
            return [dict(r._mapping) for r in db.execute(text(sql), p)]
        except Exception as exc:                                   # noqa: BLE001
            logger.error("Stock take %s fetch failed for %s..%s: %s", label, a, b, exc)
            db.rollback()
            return []

    data = {"rows": run("entries", RANGE_SQL, a=a, b=b),
            "backlog": run("backlog", BACKLOG_SQL, d=b),
            "roster": run("roster", ROSTER_SQL)}
    _, dir_by_squash = name_directory(db)
    agg = aggregate(data, b, dir_by_squash)
    scope = scope_warehouses(agg)
    roster = roster_split(data, agg, dir_by_squash, scope)
    out = outstanding(data, b, scope)
    return {"from": a, "to": b, "agg": agg, "roster": roster, "outstanding": out,
            "scope": scope, "top_skus": top_skus(agg),
            "by_day": run("by day", BY_DAY_SQL, a=a, b=b),
            "fingerprint": fingerprint(agg, roster, out)}
