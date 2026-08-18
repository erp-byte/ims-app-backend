"""HTML for the stock take mail — the 7:00 PM count report.

BUILT ON THE DAILY REPORT'S PRIMITIVES
    `table`, `tiles`, `h3`, `badge` and `flag` are imported from
    daily_report_html rather than restated. Two mails land in the same inbox
    from the same address an hour apart; if they are built from two sets of
    table code they drift, and the pair starts looking like it came from two
    different companies. It also means the Gmail lessons already paid for —
    tabular figures set once on the table, striping on <tr bgcolor>, no
    fixed widths — apply here without being rediscovered.

TWO SHAPES, ONE FUNCTION
    A counting day gets the full report. A day with no count gets a short note
    and the outstanding position only — no floor tables, no SKU tables, nothing
    that would be a page of dashes. Both are the same mail with the same
    subject and the same thread, because "did anyone count today" is exactly
    the question the mail exists to answer and a silent day is an answer.
"""
from __future__ import annotations

from datetime import date, datetime

from services.ims_service.daily_report_html import (
    BAND, GREY, INK, RULE, FS_H3, FS_SECTION,
    _tone_helpers, badge, e, flag, h3, tiles,
)

# Teal-green: this is a counting report, and it must not be mistaken at a glance
# for the inward/transfer mail that arrives from the same address minutes later.
ST = {"deep": "#0F5132", "mid": "#1A7F4B", "tint": "#E7F5EC", "name": "Stock take"}
ALERT = "#C62828"
ALERT_BG = "#FDECEA"
AMBER = "#B45309"
AMBER_BG = "#FFF7ED"

MAIL_ROW_CAP = 12
MAIL_SAFE_BYTES = 92_000
MAIL_MIN_ROW_CAP = 4

STALE_DAYS = 30          # a warehouse uncounted this long is called out by name


def _kg(v) -> str:
    try:
        f = float(v or 0)
    except (TypeError, ValueError):
        return "–"
    return f"{f:,.2f}" if f else "–"


def _n(v) -> str:
    try:
        i = int(v or 0)
    except (TypeError, ValueError):
        return "–"
    return f"{i:,}" if i else "–"


def _pct(v) -> str:
    return f"{float(v or 0):.1f}%" if v else "–"


def _helpers(slim: bool):
    """Table/heading/tile builders bound to this report's colour and width.

    `slim` is the mail. A phone gives a table about 390px, which at a readable
    size is roughly four columns, so wide tables keep only the columns that
    answer the question and `keep=` names them — the hosted page still renders
    every column. That trimming lives in `_tone_helpers`, which is why these are
    borrowed from the daily report rather than calling `table` directly.
    """
    return _tone_helpers(ST, slim)


def _H(t) -> str:
    return h3(t, tone=ST)


def _section(title: str, inner: str) -> str:
    return (
        f'<div style="margin:0 0 24px;">'
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>'
        f'<td style="background:{ST["deep"]};color:#fff;padding:16px 20px;'
        f'border-radius:10px 10px 0 0;'
        f'font:700 {FS_SECTION}px Arial,Helvetica,sans-serif;">{e(title)}</td></tr></table>'
        f'<div style="border:3px solid {ST["deep"]};border-top:0;'
        f'border-radius:0 0 10px 10px;background:#fff;padding:18px 20px 22px;">'
        f'{inner}</div></div>'
    )


# ═════════════════════════════════════════════════════════════════════════
#  BLOCKS
# ═════════════════════════════════════════════════════════════════════════
def _banner(agg, out) -> str:
    """The one-line verdict that opens the mail.

    Green when the day was counted, red when it was not — and the red version
    carries the reason it matters, which is how long the silence has run, not
    the silence itself.
    """
    if not agg["empty"]:
        n, kg = agg["head"]["n"], agg["head"]["kg"]
        return (
            f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
            f'style="border-collapse:separate;margin:0 0 14px;"><tr>'
            f'<td style="background:{ST["tint"]};border:3px solid {ST["deep"]};'
            f'border-radius:10px;padding:13px 16px;">'
            f'<div style="font:700 22px/1.35 Arial,Helvetica,sans-serif;'
            f'color:{ST["deep"]};">'
            f'Counted today &mdash; {_n(n)} entries, {_kg(kg)} kg across '
            f'{len(agg["floor"])} floor{"" if len(agg["floor"]) == 1 else "s"}'
            f'</div></td></tr></table>'
        )

    stale = out["stale"]
    tail = ""
    if stale:
        tail = (" &nbsp;&middot;&nbsp; longest uncounted: "
                + ", ".join(f'{e(r["warehouse"])} {r["days_since"]}d'
                            for r in stale[:3]))
    return (
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'style="border-collapse:separate;margin:0 0 14px;"><tr>'
        f'<td style="background:{ALERT};border:3px solid {ALERT};'
        f'border-radius:10px;padding:13px 16px;">'
        f'<div style="font:700 22px/1.35 Arial,Helvetica,sans-serif;color:#fff;">'
        f'No stock take entries today{tail}</div></td></tr></table>'
    )


def _at_a_glance(agg, roster, out) -> str:
    h = agg["head"]
    return tiles([
        ("Entries", _n(h["n"]), f'{len(agg["batches"])} batches'),
        ("Total weight", _kg(h["kg"]) + " kg", _n(h["qty"]) + " units"),
        ("Floors counted", _n(len(agg["floor"])),
         f'{len(agg["wh"])} warehouse{"" if len(agg["wh"]) == 1 else "s"}'),
        ("Who counted", _n(len(roster["entered"])),
         f'{len(roster["missing"])} rostered did not'),
    ], tone=ST)


def _warehouse_table(T, agg, cap, scope=None) -> str:
    """One row per warehouse on show — the two primaries always, others if they moved.

    A primary with nothing against it still gets its row: 'W202 — 0 entries' is
    the finding on a day nobody counted there, and an absent row says nothing at
    all.
    """
    scope = scope or sorted(agg["wh"], key=lambda w: -agg["wh"][w]["kg"])
    empty = {"n": 0, "kg": 0.0, "qty": 0.0, "off_kg": 0.0,
             "floors": (), "users": ()}
    rows, t = [], {"n": 0, "kg": 0.0, "qty": 0.0, "off": 0.0}
    for w in sorted(scope, key=lambda w: -agg["wh"].get(w, empty)["kg"]):
        b = agg["wh"].get(w, empty)
        rows.append([w, _n(b["n"]), _n(len(b["floors"])), _n(len(b["users"])),
                     _kg(b["kg"]), _n(b["qty"]), _kg(b["off_kg"])])
        t["n"] += b["n"]; t["kg"] += b["kg"]
        t["qty"] += b["qty"]; t["off"] += b["off_kg"]
    return T(["Warehouse", "Entries", "Floors", "Counters", "Net kg", "Units",
              "Off grade kg"], rows,
             ["left", "right", "right", "right", "right", "right", "right"],
             cap=cap, keep=[0, 1, 2, 4],
             total=["TOTAL", _n(t["n"]), _n(len(agg["floor"])),
                    _n(len(agg["user"])), _kg(t["kg"]), _n(t["qty"]),
                    _kg(t["off"])],
             note="W-202 and A-185 are always shown. Any other warehouse "
                  "appears only when something was counted there.",
             empty="No stock take entries for this day")


def _floor_table(T, agg, cap) -> str:
    rows = []
    for (w, f), b in sorted(agg["floor"].items(), key=lambda kv: -kv[1]["kg"]):
        rows.append([w, f, _n(b["n"]), _kg(b["kg"]), _n(b["qty"]),
                     _n(len(b["skus"]))])
    return T(["Warehouse", "Floor", "Entries", "Net kg", "Units", "SKUs"], rows,
              ["left", "left", "right", "right", "right", "right"],
              cap=cap, widths=[15, 30, 12, 17, 14, 12], keep=[0, 1, 3],
              total=["TOTAL", "", _n(agg["head"]["n"]), _kg(agg["head"]["kg"]),
                     _n(agg["head"]["qty"]), _n(len(agg["sku"]))],
              note="Floor names are typed by hand in the app; near-duplicates "
                   "('1ST FLOOR' / 'FIRST FLOOR') are merged before counting.",
              empty="No floor was counted on this day")


def _people_tables(T, roster, cap) -> str:
    rows = [[u["name"], u["warehouse"], _n(u["n"]), _kg(u["kg"]),
             _n(u["qty"]), _n(u["floors"])] for u in roster["entered"]]
    out = _H(f'Who counted ({len(roster["entered"])})')
    out += T(["User", "Warehouse", "Entries", "Net kg", "Units", "Floors"], rows,
              ["left", "left", "right", "right", "right", "right"],
              cap=cap, keep=[0, 1, 2, 3],
              empty="Nobody keyed a stock take entry on this day")

    if roster["unrostered"]:
        rows = [[u["name"], _n(u["n"]), _kg(u["kg"]), _n(u["floors"])]
                for u in roster["unrostered"]]
        out += _H("Counted but not on the Stock Take roster")
        out += T(["User", "Entries", "Net kg", "Floors"], rows,
                  ["left", "right", "right", "right"], cap=cap,
                  note="These logins keyed entries but have no active row in "
                       "stocktake_users. Their entries are included in every "
                       "total above.")

    miss = roster["missing"]
    out += _H(f"Rostered to count and did not ({len(miss)})")
    if miss:
        by_wh: dict[str, list[str]] = {}
        for u in miss:
            by_wh.setdefault(u["warehouse"], []).append(u["name"])
        rows = [[w, _n(len(names)), ", ".join(sorted(names))]
                for w, names in sorted(by_wh.items())]
        out += T(["Warehouse", "Count", "Users with no entry today"], rows,
                  ["left", "right", "left"], cap=None, widths=[16, 10, 74],
                  keep=[0, 2],
                  note="Floor heads and floor managers only. Managers and "
                       "superusers can key entries but are not expected to "
                       "count, so they are not listed as missing.")
    else:
        out += T(["Warehouse", "Users"], [],
                  empty="Every rostered counter made an entry today")
    return out


def _category_tables(T, agg, tops, cap) -> str:
    total = agg["head"]["kg"] or 1.0
    rows = []
    for g, b in sorted(agg["cat"].items(), key=lambda kv: -kv[1]["kg"]):
        rows.append([g, _n(b["n"]), _kg(b["kg"]), _n(b["qty"]),
                     _pct(100.0 * b["kg"] / total), _n(len(b["skus"]))])
    out = _H("Item group")
    out += T(["Item group", "Entries", "Net kg", "Units", "Share", "SKUs"], rows,
              ["left", "right", "right", "right", "right", "right"],
              cap=cap, keep=[0, 1, 2, 4],
              total=["TOTAL", _n(agg["head"]["n"]), _kg(agg["head"]["kg"]),
                     _n(agg["head"]["qty"]), "100.0%", _n(len(agg["sku"]))],
              empty="No item group counted on this day")

    rows = [[str(i), s["item"], s["group"], _kg(s["kg"]), _n(s["qty"]),
             _pct(s["share"]), _n(s["lines"])]
            for i, s in enumerate(tops, 1)]
    out += _H("Top 10 SKUs by weight")
    out += T(["#", "SKU", "Group", "Net kg", "Units", "Share", "Lines"], rows,
              ["right", "left", "left", "right", "right", "right", "right"],
              cap=None, widths=[5, 34, 15, 15, 12, 10, 9], keep=[0, 1, 3, 5],
              note="Share is of the day's counted weight, not of stock on hand.",
              empty="No SKU counted on this day")
    return out


def _mix_table(T, agg) -> str:
    rows = []
    for label, src in (("Stock type", agg["by_stock"]), ("Item type", agg["by_type"])):
        for k, b in sorted(src.items(), key=lambda kv: -kv[1]["kg"]):
            rows.append([label, k, _n(b["n"]), _kg(b["kg"]), _n(b["qty"])])
    return T(["", "Split", "Entries", "Net kg", "Units"], rows,
              ["left", "left", "right", "right", "right"],
              widths=[16, 30, 16, 22, 16], keep=[1, 2, 3],
              note="Off grade / rejection is counted separately from fresh "
                   "stock so a rejection pile is never read as sellable stock.",
              empty="Nothing counted on this day")


def _outstanding_table(T, out, day) -> str:
    rows = []
    for r in out["rows"]:
        since = (badge(f'{r["days_since"]}d', "bad")
                 if (r["days_since"] or 0) >= STALE_DAYS
                 else _n(r["days_since"]) if r["days_since"] else "today")
        rows.append([r["warehouse"],
                     f'{r["last_count"]:%d %b %Y}' if r["last_count"] else "never",
                     since, _n(r["total"]), _n(r["drafts"]),
                     _n(r["unverified"]), _n(r["unchecked"])])
    body = T(["Warehouse", "Last counted", "Days", "Entries held",
               "Drafts", "Unverified", "Unchecked"], rows,
              ["left", "left", "right", "right", "right", "right", "right"],
              widths=[15, 17, 9, 15, 14, 15, 15], keep=[0, 1, 2, 5],
              total=["TOTAL", "", "", _n(out["total"]), _n(out["drafts"]),
                     _n(out["unverified"]), _n(out["unchecked"])],
              note="Standing position across all counts to date, not just "
                   "today's. Drafts were started and never submitted; "
                   "unverified entries are submitted and still awaiting a "
                   "verifier.",
              empty="No stock take entry has ever been recorded")

    warn = ""
    if out["stale"]:
        warn = flag("Not counted in over 30 days: " + ", ".join(
            f'{e(r["warehouse"])} ({r["days_since"]} days, last '
            f'{r["last_count"]:%d %b %Y})' for r in out["stale"]))
    if out["drafts"]:
        warn += flag(f'{out["drafts"]} entries are still drafts — keyed but '
                     f'never submitted, so they are in no count.')
    return warn + body


# ═════════════════════════════════════════════════════════════════════════
#  MAIL
# ═════════════════════════════════════════════════════════════════════════
def render_email(day: date, rep: dict, generated: datetime, *,
                 revised: bool = False, view_url: str | None = None,
                 _cap: int | None = None) -> str:
    cap = MAIL_ROW_CAP if _cap is None else _cap
    agg, roster, out = rep["agg"], rep["roster"], rep["outstanding"]
    quiet = agg["empty"]
    T, _, _TI = _helpers(True)

    body = _banner(agg, out)
    if revised:
        body += flag("Revised — entries were recorded for this day after the "
                     "7:00 PM cut-off. These figures supersede the mail sent "
                     "earlier.")

    if quiet:
        # Deliberately short. A quiet day is one sentence plus the standing
        # position; printing eight empty tables to prove nothing happened is
        # how a daily mail teaches people to stop opening it.
        body += _section(
            "Nothing counted today",
            (f'<div style="font:{FS_H3}px/1.6 Arial,Helvetica,sans-serif;'
             f'color:{INK};margin:0 0 14px;">'
             f'No stock take entry was keyed on {day:%A, %d %B %Y}. '
             f'Counting runs as a campaign rather than daily, so this is normal '
             f'between drives &mdash; what follows is what is still open.'
             f'</div>')
            + _outstanding_table(T, out, day))
        body += _section(
            f'Rostered counters ({roster["expected"]})',
            T(["Warehouse", "Count", "Rostered counters"],
              [[w, _n(len(v)), ", ".join(sorted(v))] for w, v in
               sorted(_by_wh(roster["missing"]).items())],
              ["left", "right", "left"], widths=[16, 10, 74], keep=[0, 2],
              note="Nobody on this list counted today.",
              empty="No counting roles on the Stock Take roster"))
    else:
        body += _section("At a glance", _at_a_glance(agg, roster, out)
                         + _H("Warehouse-wise") + _warehouse_table(T, agg, cap, rep.get("scope"))
                         + _H("Stock and item type") + _mix_table(T, agg))
        body += _section("Floor-wise", _floor_table(T, agg, cap))
        body += _section("People", _people_tables(T, roster, cap))
        body += _section("Item group and SKUs",
                         _category_tables(T, agg, rep["top_skus"], cap))
        body += _section("Still open", _outstanding_table(T, out, day))

    if agg["test_rows"]:
        body += flag(f'{agg["test_rows"]} entries were keyed onto a test floor '
                     f'and are excluded from every figure above.', "warn")

    cta = ""
    if view_url:
        cta = (
            f'<div style="margin:0 0 16px;">'
            f'<a href="{e(view_url)}" style="display:inline-block;padding:13px 22px;'
            f'background:{ST["deep"]};color:#fff;text-decoration:none;border-radius:8px;'
            f'font:700 15px Arial,Helvetica,sans-serif;">Open the full view &rarr;</a>'
            f'</div>'
        )

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock Take Report — {day:%d %b %Y}</title>
<style>
  body {{ margin:0; padding:0; background:#F1F4F9;
          -webkit-text-size-adjust:100%; -ms-text-size-adjust:100%; }}
  a {{ color:{ST["deep"]}; }}
  .wrap {{ max-width:920px; margin:0 auto; }}
  table {{ mso-table-lspace:0; mso-table-rspace:0; }}
  @media only screen and (max-width:600px) {{
    .pad {{ padding:14px !important; }}
    .tiles td.tile {{ display:block !important; width:auto !important; margin-bottom:8px; }}
    .tiles td {{ width:auto !important; }}
    .hdr h1 {{ font-size:19px !important; }}
    .scroll table {{ font-size:20px !important; }}
    .scroll td, .scroll th {{ padding:11px 7px !important; }}
    .wrap {{ padding:8px 4px !important; }}
  }}
</style></head>
<body>
<div class="wrap" style="padding:16px 10px;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0"
         style="background:#fff;border-radius:10px;overflow:hidden;
                box-shadow:0 1px 4px rgba(0,0,0,.09);">
    <tr><td class="hdr" style="background:{ST["deep"]};padding:16px 18px;">
      <h1 style="margin:0;font:700 18px Arial,Helvetica,sans-serif;color:#fff;">
        Stock Take Report{' — REVISED' if revised else ''}</h1>
      <div style="font:12px Arial,Helvetica,sans-serif;color:#C8E2D3;margin-top:3px;">
        {day:%A, %d %B %Y} &nbsp;·&nbsp; generated {generated:%d %b %Y, %I:%M %p} IST
      </div>
    </td></tr>
    <tr><td class="pad" style="padding:16px 18px 20px;">{cta}{body}</td></tr>
    <tr><td style="background:{BAND};padding:11px 18px;text-align:center;
                   font:11px Arial,Helvetica,sans-serif;color:{GREY};">
      Candor Foods — Stock Take automated daily report. Weight is the app's own
      total (units × UOM) as recorded on the entry.
      <br>{'Revised send' if revised else 'Evening send'} &nbsp;·&nbsp;
      generated {generated:%d %b %Y, %I:%M:%S %p} IST
    </td></tr>
  </table>
</div>
</body></html>"""

    # Same Gmail cliff as the daily report: past ~102 KB the tail of the message
    # is hidden behind "View entire message", which here would be the whole
    # outstanding section.
    if len(html.encode("utf-8")) > MAIL_SAFE_BYTES and cap > MAIL_MIN_ROW_CAP:
        return render_email(day, rep, generated, revised=revised,
                            view_url=view_url,
                            _cap=max(MAIL_MIN_ROW_CAP, cap - 3))
    return html


def _by_wh(rows) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for r in rows:
        out.setdefault(r["warehouse"], []).append(r["name"])
    return out


def render_page(day: date, rep: dict, generated: datetime) -> str:
    """The hosted version — every row, no caps, no Gmail size ceiling."""
    agg, roster, out = rep["agg"], rep["roster"], rep["outstanding"]
    T, _, _TI = _helpers(False)          # no column trimming on the hosted page
    body = _banner(agg, out)
    if agg["empty"]:
        body += _section("Nothing counted today", _outstanding_table(T, out, day))
    else:
        body += _section("At a glance", _at_a_glance(agg, roster, out)
                         + _H("Warehouse-wise") + _warehouse_table(T, agg, None, rep.get("scope"))
                         + _H("Stock and item type") + _mix_table(T, agg))
        body += _section("Floor-wise", _floor_table(T, agg, None))
        body += _section("People", _people_tables(T, roster, None))
        body += _section("Item group and SKUs",
                         _category_tables(T, agg, rep["top_skus"], None))
        body += _section("Still open", _outstanding_table(T, out, day))

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock Take — {day:%d %b %Y}</title>
<style>
  *,*::before,*::after{{box-sizing:border-box}}
  body{{margin:0;background:#F1F4F9;color:{INK};
       font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif}}
  header{{background:{ST["deep"]};color:#fff;padding:18px 0}}
  header .in{{max-width:1120px;margin:0 auto;padding:0 12px}}
  h1{{margin:0;font-size:20px}}
  .sub{{color:#C8E2D3;font-size:13px;margin-top:3px}}
  .wrap{{max-width:1120px;margin:0 auto;padding:16px 12px 48px}}
  .scroll{{overflow-x:auto;-webkit-overflow-scrolling:touch}}
  table{{width:100%}}
  @media (prefers-color-scheme:dark){{
    body{{background:#10151F;color:#E5E9F0}}
  }}
</style></head>
<body>
<header><div class="in">
  <h1>Stock Take Report</h1>
  <div class="sub">{day:%A, %d %B %Y} &nbsp;·&nbsp; generated
       {generated:%d %b %Y, %I:%M %p} IST</div>
</div></header>
<div class="wrap">{body}</div>
</body></html>"""


def render_plain(day: date, rep: dict, revised: bool = False) -> str:
    """Text alternative — clients that refuse HTML still get the day's answer."""
    agg, roster, out = rep["agg"], rep["roster"], rep["outstanding"]
    h = agg["head"]
    lines = [f"Stock Take Report{' - REVISED' if revised else ''}: "
             f"{day:%A, %d %B %Y}", ""]

    if agg["empty"]:
        lines += ["NO STOCK TAKE ENTRIES TODAY.", "",
                  "Counting runs as a campaign rather than daily. Still open:"]
    else:
        lines += [
            f"COUNTED: {h['n']:,} entries | {h['kg']:,.2f} kg | {h['qty']:,.0f} units",
            f"  {len(agg['wh'])} warehouses | {len(agg['floor'])} floors | "
            f"{len(agg['sku'])} SKUs | {len(agg['batches'])} batches",
            "",
            "WAREHOUSE-WISE",
        ]
        for w, b in sorted(agg["wh"].items(), key=lambda kv: -kv[1]["kg"]):
            lines.append(f"  {w}: {b['n']:,} entries | {b['kg']:,.2f} kg | "
                         f"{len(b['floors'])} floors | {len(b['users'])} counters")
        lines += ["", "FLOOR-WISE"]
        for (w, f), b in sorted(agg["floor"].items(), key=lambda kv: -kv[1]["kg"]):
            lines.append(f"  {w} / {f}: {b['n']:,} entries | {b['kg']:,.2f} kg")
        lines += ["", "ITEM GROUP"]
        for g, b in sorted(agg["cat"].items(), key=lambda kv: -kv[1]["kg"])[:15]:
            lines.append(f"  {g}: {b['n']:,} entries | {b['kg']:,.2f} kg")
        lines += ["", "TOP 10 SKUs"]
        for i, s in enumerate(rep["top_skus"], 1):
            lines.append(f"  {i}. {s['item']} ({s['group']}): "
                         f"{s['kg']:,.2f} kg | {s['share']:.1f}%")
        lines += ["", f"WHO COUNTED ({len(roster['entered'])})"]
        for u in roster["entered"]:
            lines.append(f"  {u['name']} [{u['warehouse']}]: {u['n']:,} entries | "
                         f"{u['kg']:,.2f} kg | {u['floors']} floors")

    lines += ["", f"ROSTERED AND DID NOT COUNT ({len(roster['missing'])})"]
    if roster["missing"]:
        for w, names in sorted(_by_wh(roster["missing"]).items()):
            lines.append(f"  {w}: " + ", ".join(sorted(names)))
    else:
        lines.append("  None - every rostered counter made an entry.")

    lines += ["", "STILL OPEN",
              f"  {out['drafts']:,} drafts | {out['unverified']:,} unverified | "
              f"{out['unchecked']:,} unchecked"]
    for r in out["rows"]:
        last = f"{r['last_count']:%d %b %Y}" if r["last_count"] else "never"
        lines.append(f"  {r['warehouse']}: last counted {last} "
                     f"({r['days_since']}d) | {r['unverified']:,} unverified")
    return "\n".join(lines)
