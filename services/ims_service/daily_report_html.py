"""HTML rendering for the daily report — mail body and hosted interactive page.

TWO RENDERINGS, ONE SET OF SECTIONS
    `render_email` produces the mail body. `render_page` produces a full web page.
    Both build their tables from the same `_section_*` functions, so the two can
    not drift apart.

WHY THE MAIL IS NOT TABBED
    Mail clients strip <script>, and Gmail — which is what this company uses —
    also drops the <input>/:checked "CSS tab" trick and <details>/<summary>.
    A tabbed container in the mail body would therefore render for some people
    as every panel stacked at once and for others as nothing at all. So the mail
    gets a jump-nav plus stacked sections (works everywhere, degrades to a plain
    scroll), and the real tabbed, paginated, searchable view is the hosted page
    the mail links to at the top.

MAIL SIZE
    Gmail clips a message past ~102 KB and hides the rest behind "View entire
    message", which would bury the last section. Detail tables in the mail are
    therefore capped per section, each capped table says how many rows it is
    hiding, and the full set lives on the hosted page.
"""
from __future__ import annotations

from datetime import date, datetime
from html import escape

from services.ims_service.daily_report_gaps import ALL_SITES, group_by_site

NAVY = "#29417A"
NAVY_D = "#1F3260"
NAVY_L = "#E8EDF5"
INK = "#1F2937"
GREY = "#6B7280"
RULE = "#D5DBE5"
BAND = "#F7F9FC"
AMBER = "#B45309"
AMBER_BG = "#FFF7ED"
GREEN = "#1E7E44"
GREEN_BG = "#EAFAF1"
RED = "#B3261E"

# The exception panel gets its own palette. It has to out-shout four module
# colours that are all, deliberately, calm.
ALERT = "#C62828"
ALERT_DEEP = "#8C1D18"
ALERT_RULE = "#F5CFCB"

MAIL_ROW_CAP = 12          # detail rows per table in the mail body
GMAIL_CLIP_BYTES = 102_400  # Gmail hides everything past this behind "View entire message"
MAIL_SAFE_BYTES = 92_000    # rebuild with fewer rows before we get near the cliff
MAIL_MIN_ROW_CAP = 4        # never strip a table below this

TABS = [
    ("inward", "Inward"),
    ("transfers", "Transfers"),
    ("jobcards", "Job Cards"),
    ("samples", "Samples / NPD"),
]

# One colour per module, so a section is identifiable before a word is read.
# Every section used the same navy, which made four different kinds of work look
# like one continuous table. (deep, mid, tint, header-text-on-tint)
SECTION = {
    "inward":    {"deep": "#1E3A6E", "mid": "#2F5FA8", "tint": "#EAF0FA", "name": "Inward"},
    "transfers": {"deep": "#0E5A54", "mid": "#12857B", "tint": "#E4F4F2", "name": "Transfers"},
    "jobcards":  {"deep": "#7A4310", "mid": "#B4661A", "tint": "#FBEFE1", "name": "Job Cards"},
    "samples":   {"deep": "#553087", "mid": "#7B4FBF", "tint": "#F1EAFB", "name": "Samples / NPD"},
}

# Type scale. The previous 12–13px body was unreadable on a phone; this roughly
# doubles the small text and lifts everything else with it. Table cells stop
# short of a literal 2x because a 27px cell forces horizontal scrolling on any
# table wider than four columns, which costs more legibility than it buys.
FS_CELL = 21        # was 12  -> 1.75x
FS_HEAD = 18        # was 12  -> 1.5x
FS_TILE = 38        # was 17
FS_TILE_LABEL = 16  # was 10
FS_SECTION = 27     # was 14
FS_H3 = 23          # was 13
FS_NOTE = 16        # was 11
FS_BADGE = 16       # was 11
FS_SUB = 15         # gross sub-line inside a cell

# The exception panel sits just above the body text in size — enough to read
# first, not enough to become the mail. It opens the message, so every pixel it
# takes is a pixel of the first real table pushed below the fold on a phone.
FS_GAP_HEAD = 22    # "Not entered today — N items"
FS_GAP = 20         # one bullet per warehouse


def e(v) -> str:
    return escape("" if v is None else str(v))


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
    try:
        f = float(v or 0)
    except (TypeError, ValueError):
        return "–"
    return f"{f:.2f}%" if f else "–"


# ── table builder ────────────────────────────────────────────────────────
def _sub(cell_html: str) -> str:
    """Render the gross sub-line marker as a small second line in the same cell.

    Net is the reported figure; gross sits under it in smaller, lighter type so
    it can inform without ever being mistaken for the headline number.
    """
    if "␟" not in cell_html:
        return cell_html.replace("\n", "<br>")
    main, _, sub = cell_html.partition("␟")
    return (main.replace("\n", "<br>")
            + f'<div style="font-size:{FS_SUB}px;color:{GREY};font-weight:400;'
              f'margin-top:4px;">{sub.strip()}</div>')


def table(headers, rows, aligns=None, *, cap=None, total=None, note=None,
          widths=None, tone=None, empty="Nothing recorded for this day") -> str:
    """One responsive table. `cap` limits rows and appends a "+N more" line.

    Columns are sized by what they hold rather than split evenly: a right-aligned
    column is treated as numeric — tabular figures, never wrapped, only as wide as
    its digits — which leaves the remaining width for the descriptive columns that
    actually need it. `widths` (percentages, one per column) overrides that.
    """
    aligns = aligns or (["left"] + ["right"] * (len(headers) - 1))
    hidden = 0
    if cap is not None and len(rows) > cap:
        hidden = len(rows) - cap
        rows = rows[:cap]

    cols = ""
    if widths:
        cols = "<colgroup>" + "".join(f'<col style="width:{w}%;">' for w in widths) + "</colgroup>"

    hbg = tone["mid"] if tone else NAVY
    hcell = (f'padding:13px 10px;background:{hbg};color:#fff;font-weight:700;'
             f'font-size:{FS_HEAD}px;line-height:1.25;'
             f'border:1px solid {hbg};text-align:')
    th = "".join(f'<th style="{hcell}{a};">{e(h)}</th>' for h, a in zip(headers, aligns))

    if not rows:
        body = (f'<tr><td colspan="{len(headers)}" style="padding:24px 14px;text-align:center;'
                f'color:{GREY};border:1px solid {RULE};font-style:italic;font-size:{FS_CELL}px;">'
                f'{e(empty)}</td></tr>')
    else:
        # Row striping goes on <tr bgcolor>, and text colour / tabular figures are
        # set once on the <table>. Repeating any of them per cell inflates the
        # message by ~20 KB on a busy day, which is what pushes Gmail into clipping
        # the last section away entirely.
        stripe = tone["tint"] if tone else BAND
        body = ""
        txt = f'padding:12px 10px;border:1px solid {RULE};text-align:left;word-break:break-word;'
        num = f'padding:12px 10px;border:1px solid {RULE};text-align:right;word-break:break-word;'
        for i, r in enumerate(rows):
            tds = "".join(
                f'<td style="{num if a == "right" else txt}">'
                f'{c if isinstance(c, _Raw) else _sub(e(c))}</td>'
                for c, a in zip(r, aligns)
            )
            body += f'<tr bgcolor="{"#ffffff" if i % 2 == 0 else stripe}">{tds}</tr>'

    if hidden:
        body += (f'<tr><td colspan="{len(headers)}" style="padding:12px 14px;text-align:center;'
                 f'color:{AMBER};background:{AMBER_BG};border:1px solid {RULE};'
                 f'font-size:{FS_NOTE}px;font-weight:600;">'
                 f'+ {hidden} more row{"s" if hidden != 1 else ""} — open the full view for all'
                 f'</td></tr>')

    if total:
        tdeep = tone["deep"] if tone else NAVY
        tcell = (f'padding:14px 14px;border:1px solid {RULE};border-top:3px solid {tdeep};'
                 f'font-weight:700;font-size:{FS_CELL + 1}px;color:{tdeep};text-align:')
        tds = "".join(f'<td style="{tcell}{a};">{c if isinstance(c, _Raw) else _sub(e(c))}</td>'
                      for c, a in zip(total, aligns))
        body += f'<tr bgcolor="{tone["tint"] if tone else NAVY_L}">{tds}</tr>'

    n = (f'<div style="font-size:{FS_NOTE}px;line-height:1.55;color:{GREY};'
         f'margin:9px 2px 0;">{note}</div>' if note else "")

    return (
        f'<div class="scroll" style="margin:0 0 8px;">'
        f'<table role="presentation" cellspacing="0" cellpadding="0" '
        f'style="border-collapse:collapse;width:100%;table-layout:fixed;'
        f'font-size:{FS_CELL}px;line-height:1.45;font-variant-numeric:tabular-nums;'
        f'color:{INK};font-family:Arial,Helvetica,sans-serif;">'
        f"{cols}<thead><tr>{th}</tr></thead><tbody>{body}</tbody></table></div>{n}"
    )


class _Raw(str):
    """Marks a cell whose HTML should not be escaped (badges)."""


def badge(txt, kind="neutral") -> _Raw:
    colors = {
        "good": (GREEN, "#EAFAF1"), "bad": (RED, "#FDECEA"),
        "warn": (AMBER, AMBER_BG), "neutral": (NAVY_D, NAVY_L),
    }
    fg, bg = colors.get(kind, colors["neutral"])
    return _Raw(f'<span style="display:inline-block;padding:5px 13px;border-radius:13px;'
                f'background:{bg};color:{fg};font-size:{FS_BADGE}px;font-weight:700;'
                f'">{escape(str(txt))}</span>')


def h3(txt, tone=None) -> str:
    c = tone["deep"] if tone else NAVY
    u = tone["tint"] if tone else NAVY_L
    return (f'<div style="font:700 {FS_H3}px/1.3 Arial,Helvetica,sans-serif;color:{c};'
            f'margin:28px 0 10px;padding-bottom:7px;'
            f'border-bottom:3px solid {u};">{e(txt)}</div>')


def flag(txt, kind="warn") -> str:
    """A callout that has to be read, not skimmed past."""
    fg, bg = (AMBER, AMBER_BG) if kind == "warn" else (NAVY_D, NAVY_L)
    return (f'<div style="margin:16px 0;padding:15px 18px;background:{bg};'
            f'border-left:6px solid {fg};color:{fg};'
            f'font:700 {FS_NOTE + 2}px/1.55 Arial,Helvetica,sans-serif;'
            f'border-radius:0 7px 7px 0;">{txt}</div>')


# ── exception panel ──────────────────────────────────────────────────────
# Built from <div>/<span> rather than a table on purpose. A table needs column
# widths, and a fixed width is what made the mail unreadable on Gmail mobile
# before: the client scales the whole message down to fit the widest element, so
# one wide row shrinks the type everywhere. Flowing text wraps instead.
#
# It is a SUMMARY, and summaries earn their place by being short. One bullet per
# warehouse, its gaps run together on that line, each phrase in its own module's
# colour so you can still tell inward from job cards without a label taking up a
# row. An earlier version gave every gap its own line and repeated the whole
# panel inside each section; it was accurate and nobody could see past it — on a
# phone it pushed the first real table off the screen entirely.
def gaps_panel(gaps: list[dict]) -> str:
    """The warehouse-wise "what is missing" summary that opens the report.

    Grouped by site rather than by module because that is how it gets read: a
    supervisor looks for their own warehouse first and wants everything wrong
    with it in one place, whichever part of the business it came from.

    Rendered even when there is nothing to report — a panel that only appears on
    bad days is indistinguishable from one that failed to render.
    """
    if not gaps:
        return (
            f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
            f'style="border-collapse:separate;margin:0 0 14px;"><tr>'
            f'<td style="background:{GREEN_BG};border:3px solid {GREEN};border-radius:10px;'
            f'padding:12px 16px;">'
            f'<div class="gap-head" style="font:700 {FS_GAP_HEAD}px/1.35 '
            f'Arial,Helvetica,sans-serif;color:{GREEN};">'
            f'Nothing missing &mdash; every site that normally reports has entered'
            f'</div></td></tr></table>'
        )

    dot = f'<span style="color:{GREY};font-weight:400;"> &middot; </span>'
    rows_html = ""
    for i, (site, rows) in enumerate(group_by_site(gaps)):
        named = site != ALL_SITES
        phrases = dot.join(
            f'<span style="color:{(SECTION.get(g["module"]) or {}).get("deep", INK)};">'
            f'{e(g["text"])}</span>' for g in rows
        )
        rows_html += (
            f'<div class="gap-line" style="padding:8px 0;'
            + (f'border-top:1px solid {ALERT_RULE};' if i else '')
            + f'font:700 {FS_GAP}px/1.45 Arial,Helvetica,sans-serif;color:{INK};">'
            f'<span style="color:{ALERT};">&bull;&nbsp;</span>'
            f'<span style="color:{ALERT_DEEP if named else GREY};">'
            f'{e(site if named else "All sites")}</span>'
            f'<span style="color:{GREY};font-weight:400;"> &mdash; </span>'
            f'{phrases}</div>'
        )

    n = len(gaps)
    return (
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'style="border-collapse:separate;margin:0 0 14px;"><tr>'
        f'<td style="background:{ALERT};border:3px solid {ALERT};'
        f'border-radius:10px 10px 0 0;padding:11px 16px;">'
        f'<div class="gap-head" style="font:700 {FS_GAP_HEAD}px/1.3 '
        f'Arial,Helvetica,sans-serif;color:#fff;">'
        f'Not entered today &mdash; {n} item{"" if n == 1 else "s"}</div>'
        f'</td></tr><tr>'
        f'<td style="background:#fff;border:3px solid {ALERT};border-top:0;'
        f'border-radius:0 0 10px 10px;padding:3px 16px 9px;">{rows_html}</td>'
        f'</tr></table>'
    )


def gap_counts(gaps: list[dict]) -> dict[str, int]:
    out = {k: 0 for k, _ in TABS}
    for g in gaps or ():
        if g["module"] in out:
            out[g["module"]] += 1
    return out


def tiles(items, tone=None) -> str:
    """Headline numbers — the largest type on the page, because they are what the
    mail is scanned for. A table, not flexbox: Outlook ignores flex."""
    deep = tone["deep"] if tone else NAVY
    tint = tone["tint"] if tone else BAND
    cells = ""
    for label, value, sub in items:
        cells += (
            f'<td class="tile" style="padding:18px 20px;background:{tint};'
            f'border:2px solid {deep};border-top:6px solid {deep};'
            f'border-radius:10px;vertical-align:top;">'
            f'<div style="font:700 {FS_TILE}px/1.12 Arial,Helvetica,sans-serif;color:{deep};'
            f'font-variant-numeric:tabular-nums;">{e(value)}</div>'
            f'<div style="font:700 {FS_TILE_LABEL}px/1.3 Arial,Helvetica,sans-serif;color:{GREY};'
            f'text-transform:uppercase;letter-spacing:.6px;margin-top:8px;">{e(label)}</div>'
            + (f'<div style="font:{FS_NOTE}px/1.45 Arial,Helvetica,sans-serif;color:{INK};'
               f'margin-top:6px;">{e(sub)}</div>' if sub else "")
            + '</td><td style="width:12px;"></td>'
        )
    return (f'<table role="presentation" cellspacing="0" cellpadding="0" class="tiles" '
            f'style="width:100%;border-collapse:separate;margin:4px 0 8px;"><tr>{cells}</tr></table>')


# ═════════════════════════════════════════════════════════════════════════
#  SECTIONS  (shared by the mail body and the hosted page)
# ═════════════════════════════════════════════════════════════════════════
def _gross_sub(m) -> str | None:
    """Gross as a tile sub-line. Net stays the headline; gross only informs."""
    return f"gross {m.gross:,.2f} kg" if getattr(m, "gross", 0) > 0 else None


def _gsuffix(m) -> str:
    return f" · gross {m.gross:,.2f} kg" if getattr(m, "gross", 0) > 0 else ""


def _tone_helpers(tone, slim=False):
    """Bind a section's colour once, so every table/heading/tile in that section
    carries it without threading `tone=` through dozens of call sites.

    `slim` is the mail. A phone gives a table about 390px; at a readable 21px
    that is roughly four columns. Wide tables therefore keep only the columns
    that answer "what happened", and `keep=` names them — the hosted page still
    renders every column. Squeezing nine columns onto a phone is what made the
    text unreadable, not the font size.
    """
    def T(*a, **k):
        k.setdefault("tone", tone)
        keep = k.pop("keep", None)
        if slim and keep:
            headers, rows = a[0], a[1]
            aligns = a[2] if len(a) > 2 else (["left"] + ["right"] * (len(headers) - 1))
            a = ([headers[i] for i in keep],
                 [[r[i] for i in keep] for r in rows],
                 [aligns[i] for i in keep]) + a[3:]
            k.pop("widths", None)
            if k.get("total"):
                k["total"] = [k["total"][i] for i in keep]
        else:
            k.pop("keep", None)
        return table(*a, **k)

    def H(txt):
        return h3(txt, tone=tone)

    def TI(items):
        return tiles(items, tone=tone)

    return T, H, TI


def _section_inward(agg, cap, scap, tone=None, slim=False) -> str:
    T, H, TI = _tone_helpers(tone, slim)
    h, vg = agg["head"], agg["val_gap"]
    out = TI([
        ("Transactions", _n(h["inw_txns"]), None),
        ("Total received (net)", h["inw_m"].phrase(), _gross_sub(h["inw_m"])),
        ("Value entered", "Rs. " + _inr(h["inw_val"]), f"{vg['missing']} of {vg['lines']} lines blank"),
    ])
    if vg["missing"]:
        out += flag(f"Value not entered on {vg['missing']} of {vg['lines']} inward lines "
                    f"({e(vg['missing_m'].phrase())}). The value column is understated.")

    rows, tot_tx, tot_val = [], 0, 0.0
    for name in sorted(agg["wh"], key=lambda k: -agg["wh"][k]["im"].kg):
        w = agg["wh"][name]
        if not w["itx"]:
            continue
        rows.append([name, _n(len(w["itx"])), w["im"].cell().replace("\n", " + "),
                     badge("not entered", "warn") if not w["ival"] else _inr(w["ival"])])
        tot_tx += len(w["itx"]); tot_val += w["ival"]
    out += H("Warehouse-wise")
    out += T(["Warehouse", "Transactions", "Kgs / Qty", "Value (Rs.)"], rows,
                 ["left", "right", "right", "right"], cap=scap,
                 total=["TOTAL", _n(tot_tx), h["inw_m"].cell().replace("\n", " + "), _inr(tot_val)],
                 empty="No inward recorded for this day")

    rows = []
    for name in sorted(agg["usr"], key=lambda k: -len(agg["usr"][k]["itx"])):
        u = agg["usr"][name]
        if u["itx"]:
            rows.append([name, _n(len(u["itx"])), u["im"].cell().replace("\n", " + ")])
    out += H("Who keyed it")
    out += T(["User", "Transactions", "Kgs / Qty"], rows, ["left", "right", "right"],
                 cap=scap,
                 note="The IMS login that saved the GRN — not the purchase head or approval authority.",
                 empty="No inward recorded for this day")

    rows = []
    for (w, g), v in sorted(agg["inw_wg"].items(), key=lambda kv: -kv[1]["m"].kg):
        if v["m"].is_empty and not v["val"]:
            continue
        rows.append([w, g, v["m"].cell().replace("\n", " + "),
                     badge("not entered", "warn") if not v["val"] else _inr(v["val"])])
    out += H("Warehouse × category")
    out += T(["Warehouse", "Category / Group", "Kgs / Qty", "Value (Rs.)"], rows,
                 ["left", "left", "right", "right"], cap=cap, widths=[18, 32, 24, 26],
                 empty="No inward recorded for this day")
    return out


def _section_transfers(agg, cap, scap, tone=None, slim=False) -> str:
    T, H, TI = _tone_helpers(tone, slim)
    h = agg["head"]
    out = TI([
        ("Dispatched (net)", _n(h["out_chl"]) + " challans", h["out_m"].phrase() + _gsuffix(h["out_m"])),
        ("Received (net)", _n(h["in_grn"]) + " GRNs", h["in_m"].phrase() + _gsuffix(h["in_m"])),
        ("Routes", _n(len(agg["route"])), None),
    ])
    out += (f'<div style="font-size:11px;color:{GREY};font-style:italic;margin:2px 2px 0;">'
            f'Transfers move existing stock between our own locations and carry no rate in IMS, '
            f'so they are reported by weight/count only.</div>')

    rows, tc = [], 0
    tm = None
    for (f_, t_), v in sorted(agg["route"].items(), key=lambda kv: -kv[1]["m"].kg):
        rows.append([f_, t_, _n(len(v["ch"])), v["m"].cell().replace("\n", " + ")])
        tc += len(v["ch"])
    out += H("Routes")
    out += T(["From", "To", "Challans", "Kgs / Qty"], rows,
                 ["left", "left", "right", "right"], cap=scap,
                 total=["TOTAL", "", _n(tc), h["out_m"].cell().replace("\n", " + ")],
                 empty="No transfers dispatched on this day")

    rows = []
    for name in sorted(agg["usr"], key=lambda k: -(len(agg["usr"][k]["och"]) + len(agg["usr"][k]["igrn"]))):
        u = agg["usr"][name]
        if u["och"] or u["igrn"]:
            rows.append([name, _n(len(u["och"])), u["om"].cell().replace("\n", " + "),
                         _n(len(u["igrn"])), u["inm"].cell().replace("\n", " + ")])
    out += H("Who keyed it")
    out += T(["User", "Out challans", "Out Kgs", "In GRNs", "In Kgs"], rows,
                 ["left", "right", "right", "right", "right"], cap=scap, keep=[0, 1, 2],
                 empty="No transfer activity for this day")

    rows = []
    keys = set(agg["out_rg"]) | set(agg["in_rg"])
    for (f_, t_, g) in sorted(keys, key=lambda k: -(agg["out_rg"][k].kg if k in agg["out_rg"] else 0)):
        o = agg["out_rg"].get((f_, t_, g))
        i = agg["in_rg"].get((f_, t_, g))
        rows.append([f"{f_} → {t_}", g,
                     o.cell().replace("\n", " + ") if o and not o.is_empty else "–",
                     i.cell().replace("\n", " + ") if i and not i.is_empty else "–"])
    out += H("Route × category")
    out += T(["Route", "Category / Group", "Out Kgs / Qty", "In Kgs / Qty"], rows,
                 ["left", "left", "right", "right"], cap=cap, widths=[26, 28, 23, 23],
                 note="Out and In will not match — a consignment is usually received a day or more "
                      "after it is dispatched.",
                 empty="No transfers recorded for this day")
    return out


_STATUS_KIND = {
    "completed": "good", "closed": "good", "bh_approved": "good", "approved": "good",
    "in_progress": "warn", "assigned": "warn", "submitted": "warn", "draft": "neutral",
    "locked": "neutral", "unlocked": "neutral",
    "cancelled": "bad", "bh_rejected": "bad", "rejected": "bad",
}


def _status_badge(s) -> _Raw:
    key = str(s or "").strip().lower()
    return badge(str(s or "-").replace("_", " ").title(), _STATUS_KIND.get(key, "neutral"))


def _section_jobcards(jc, cap, scap, tone=None, slim=False) -> str:
    T, H, TI = _tone_helpers(tone, slim)
    if jc["empty"]:
        return (TI([("Job cards", "0", "none active"), ("Users", "0", None), ("FG items", "0", None)])
                + T(["Job card", "Factory", "FG item", "Status"], [],
                        empty="No job card activity for this day"))

    loss = jc["loss"]
    out = TI([
        ("Job cards", _n(jc["total_cards"]), "active on the day"),
        ("Users", _n(jc["total_users"]), "team leaders"),
        ("FG items", _n(jc["total_fg"]), None),
        ("Planned qty", _kg(jc["total_kg"]) + " kg", _n(jc["total_units"]) + " units"),
    ])

    rows = []
    for w, v in sorted(jc["wh"].items(), key=lambda kv: -len(kv[1]["cards"])):
        st = ", ".join(f"{k.replace('_', ' ')} {n}" for k, n in
                       sorted(v["status"].items(), key=lambda x: -x[1]))
        rows.append([w, _n(len(v["cards"])), _n(len(v["users"])), _n(len(v["fg"])),
                     _kg(v["kg"]), _n(v["units"]), st])
    out += H("Warehouse-wise")
    out += T(["Factory", "Job cards", "Users", "FG items", "Planned kg", "Units", "Status mix"],
                 rows, ["left", "right", "right", "right", "right", "right", "left"],
                 cap=scap, keep=[0, 1, 2, 4],
                 total=["TOTAL", _n(jc["total_cards"]), _n(jc["total_users"]), _n(jc["total_fg"]),
                        _kg(jc["total_kg"]), _n(jc["total_units"]), ""])

    out += H("Status update")
    out += T(["Status", "Job cards"],
                 [[_status_badge(k), _n(v)] for k, v in
                  sorted(jc["status"].items(), key=lambda x: -x[1])],
                 ["left", "right"], cap=scap)

    out += H("Loss metrics (accounting summary)")
    if loss.get("rows"):
        bal_kind = "good" if loss["unbalanced"] == 0 else "warn"
        if loss["unbalanced"]:
            out += flag(f"{loss['unbalanced']} of {loss['rows']} accounted job cards do not "
                        f"balance — input, output and losses do not reconcile on those cards.")
        out += T(
            ["Measure", "Quantity", "Measure", "Quantity"],
            [
                ["Total input", _kg(loss["input"]) + " kg", "Off-grade", _kg(loss["offgrade"]) + " kg"],
                ["Output", _kg(loss["output"]) + " kg", "Control sample", _kg(loss["control"]) + " kg"],
                ["Process loss", _kg(loss["process"]) + " kg", "Rejection", _kg(loss["rejection"]) + " kg"],
                ["Wastage", _kg(loss["wastage"]) + " kg", "Balance difference", _kg(loss["balance_diff"]) + " kg"],
                ["Avg process loss", _pct(loss["avg_process_pct"]), "Avg total loss", _pct(loss["avg_total_pct"])],
                ["Accounted cards", _n(loss["rows"]), "Balanced",
                 badge(f"{loss['balanced']} of {loss['rows']}", bal_kind)],
            ],
            ["left", "right", "left", "right"],
            note="From job card accounting. Cards without an accounting entry are excluded from these totals.")
    else:
        out += T(["Measure", "Quantity"], [],
                     empty="No accounting entries recorded against these job cards")

    rows = []
    for r in sorted(jc["rows"], key=lambda x: -(float(x["planned_qty_kg"] or 0))):
        rows.append([r["job_card_number"], canon := r["factory"] or "-",
                     r["fg_sku_name"] or "-", r["customer_name"] or "-",
                     _kg(r["planned_qty_kg"]), _status_badge(r["status"]),
                     (r["assigned_to_team_leader"] or "–")])
    out += H("Job cards")
    out += T(["Job card", "Factory", "FG item", "Customer", "Planned kg", "Status", "Team leader"],
                 rows, ["left", "left", "left", "left", "right", "left", "left"], cap=cap,
                 widths=[18, 8, 22, 20, 11, 11, 10], keep=[0, 2, 4, 5])
    return out


def _section_samples(sm, cap, scap, tone=None, slim=False) -> str:
    T, H, TI = _tone_helpers(tone, slim)
    out = TI([
        ("Requisitions", _n(len(sm["requisitions"])), "raised today"),
        ("Actions", _n(len(sm["actions"])), "approvals / status changes"),
        ("NPD job cards", _n(len(sm["npd_jobcards"])), None),
        ("Customers", _n(len(sm["customers"])), None),
    ])

    if sm["empty"]:
        return out + T(["Requisition", "Type", "Status"], [],
                           empty="No sample or NPD activity for this day")

    rows = []
    for r in sm["requisitions"]:
        qty = _kg(r["quantity"])
        if r["pcs"]:
            qty += f" ({_n(r['pcs'])} pcs)"
        rows.append([f"#{r['id']}", badge(r["sample_type"] or "-"), _status_badge(r["status"]),
                     r["npd_target_name"] or r["customer_name"] or "-",
                     (r["customer_name"] or r["company_name"] or "-"),
                     (r["sale_groups"] or "not mapped").upper(), qty, r["requestor"]])
    out += H("Requisitions raised")
    out += T(["Req", "Type", "Status", "Target / item", "Customer", "Sales group",
                  "Quantity", "Requested by"], rows,
                 ["left", "left", "left", "left", "left", "left", "right", "left"], cap=cap,
                 widths=[6, 9, 13, 20, 17, 12, 11, 12], keep=[0, 2, 3, 6],
                 note="Sales group is taken from all_sku via the requisition's articles.",
                 empty="No requisitions raised on this day")

    rows = [[g, _n(v["reqs"]), _kg(v["qty"])] for g, v in
            sorted(sm["by_sale_group"].items(), key=lambda kv: -kv[1]["reqs"])]
    out += H("By sales group")
    out += T(["Sales group", "Requisitions", "Quantity"], rows,
                 ["left", "right", "right"], cap=scap)

    rows = []
    for a in sm["actions"]:
        move = f"{a['from']} → {a['to']}" if a["from"] and a["to"] else (a["to"] or a["from"] or "-")
        rows.append([f"#{a['req']}", a["event"], move, a["target"] or "-",
                     a["actor"], a["role"] or "-", a["remarks"] or "-"])
    out += H("Actions taken")
    out += T(["Req", "Action", "Movement", "Target", "By", "Role", "Remarks"], rows,
                 ["left", "left", "left", "left", "left", "left", "left"], cap=cap,
                 widths=[6, 13, 22, 17, 15, 11, 16], keep=[0, 1, 2, 4],
                 empty="No sample actions on this day")

    rows = []
    for n in sm["npd_jobcards"]:
        rows.append([f"#{n['id']}", n["title"] or "-", n["fg_sku_name"] or "-",
                     f"{_kg(n['target_qty'])} {e(n['uom'] or '')}".strip(),
                     _kg(n["output_qty"]), _pct(n["yield_pct"]),
                     _status_badge(n["status"]),
                     (n["customer_name"] or n["company_name"] or "-"), n["created_by_name"]])
    out += H("NPD development job cards")
    out += T(["Card", "Title", "FG item", "Target", "Output", "Yield", "Status",
                  "Customer", "By"], rows,
                 ["left", "left", "left", "right", "right", "right", "left", "left", "left"],
                 cap=cap, keep=[0, 1, 4, 6], empty="No NPD development job cards on this day")
    return out


def _inr(v) -> str:
    import re
    if v is None or float(v) == 0:
        return "–"
    n = float(v)
    sign = "-" if n < 0 else ""
    whole, frac = f"{abs(n):.2f}".split(".")
    if len(whole) > 3:
        head, tail = whole[:-3], whole[-3:]
        whole = re.sub(r"(?<=\d)(?=(\d\d)+$)", ",", head) + "," + tail
    return f"{sign}{whole}.{frac}"


def build_sections(agg, ops, cap, slim: bool = False) -> dict[str, str]:
    """`cap` bounds detail tables; summary tables get a looser bound.

    Both are bounded, because the size guard can only shrink the mail if every
    table responds to it — an uncapped summary table would grow with the data and
    push the message past Gmail's clip limit no matter how far the detail tables
    were trimmed. In practice the summary bound is never reached: there are ~11
    warehouses and ~20 routes in the business.
    """
    scap = None if cap is None else max(cap * 3, 12)
    return {
        "inward": _section_inward(agg, cap, scap, SECTION["inward"], slim),
        "transfers": _section_transfers(agg, cap, scap, SECTION["transfers"], slim),
        "jobcards": _section_jobcards(ops["jobcards"], cap, scap, SECTION["jobcards"], slim),
        "samples": _section_samples(ops["samples"], cap, scap, SECTION["samples"], slim),
    }


def _counts(agg, ops) -> dict[str, str]:
    h = agg["head"]
    return {
        "inward": f"{h['inw_txns']}",
        "transfers": f"{h['out_chl'] + h['in_grn']}",
        "jobcards": f"{ops['jobcards']['total_cards']}",
        "samples": f"{len(ops['samples']['requisitions']) + len(ops['samples']['npd_jobcards'])}",
    }


# ═════════════════════════════════════════════════════════════════════════
#  MAIL BODY
# ═════════════════════════════════════════════════════════════════════════
def render_email(day: date, agg, ops, generated: datetime, *,
                 revised: bool = False, view_url: str | None = None,
                 gaps: list[dict] | None = None, _cap: int | None = None) -> str:
    """The mail body.

    Re-renders with fewer detail rows if the result approaches Gmail's clip limit.
    Clipping is not a cosmetic failure — Gmail hides everything past the cut behind
    "View entire message", so the Samples section would simply not be in the mail
    for most readers. Capping rows costs a few lines that the linked view still has;
    clipping costs a whole section.
    """
    cap = MAIL_ROW_CAP if _cap is None else _cap
    sections = build_sections(agg, ops, cap, slim=True)
    counts = _counts(agg, ops)
    gcount = gap_counts(gaps)

    nav = ""
    for key, label in TABS:
        t = SECTION[key]
        # A jump link that says how much is wrong behind it, so the reader can
        # go straight to the section that needs them.
        warn = (f'<span style="display:inline-block;margin-left:7px;padding:1px 9px;'
                f'border-radius:11px;background:{ALERT};color:#fff;font-size:{FS_NOTE}px;'
                f'font-weight:700;">{gcount[key]}</span>') if gcount[key] else ""
        nav += (
            f'<td style="padding:0 6px 9px 0;">'
            f'<a href="#sec-{key}" style="display:block;padding:14px 14px;'
            f'background:{t["tint"]};border:2px solid {t["deep"]};border-radius:9px;'
            f'text-decoration:none;color:{t["deep"]};'
            f'font:700 {FS_NOTE + 3}px Arial,Helvetica,sans-serif;text-align:center;">'
            f'{e(label)} <span style="font-weight:400;">({e(counts[key])})</span>{warn}'
            f'</a></td>'
        )

    cta = ""
    if view_url:
        cta = (
            f'<div style="margin:0 0 18px;">'
            f'<a href="{e(view_url)}" style="display:inline-block;padding:14px 24px;'
            f'background:{NAVY};color:#fff;text-decoration:none;border-radius:8px;'
            f'font:700 15px Arial,Helvetica,sans-serif;">Open the interactive view &rarr;</a>'
            f'<div style="font:12.5px/1.5 Arial,Helvetica,sans-serif;color:{GREY};margin-top:7px;">'
            f'Tabbed, searchable and paginated — every row, on phone or desktop.</div></div>'
        )

    body = ""
    for key, label in TABS:
        t = SECTION[key]
        body += (
            f'<a name="sec-{key}"></a>'
            f'<div id="sec-{key}" style="margin:0 0 26px;">'
            f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>'
            f'<td style="background:{t["deep"]};color:#fff;padding:18px 20px;'
            f'border-radius:10px 10px 0 0;'
            f'font:700 {FS_SECTION}px Arial,Helvetica,sans-serif;">{e(label)}'
            f'<span style="float:right;font-weight:400;font-size:{FS_H3}px;opacity:.92;">'
            f'{e(counts[key])}</span></td></tr></table>'
            f'<div style="border:3px solid {t["deep"]};border-top:0;'
            f'border-radius:0 0 10px 10px;background:#fff;'
            f'padding:20px 20px 24px;">{sections[key]}</div>'
            + (f'<div style="text-align:right;margin:7px 2px 0;">'
               f'<a href="#top" style="font:12.5px Arial,Helvetica,sans-serif;color:{GREY};'
               f'text-decoration:none;">&uarr; back to top</a></div>')
            + '</div>'
        )

    rev = ""
    if revised:
        rev = flag("Revised — entries were recorded for this day after the 7:00 PM cut-off. "
                   "These figures supersede the report sent yesterday evening.")

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Daily Inward &amp; Transfer Report — {day:%d %b %Y}</title>
<style>
  body {{ margin:0; padding:0; background:#F1F4F9;
          -webkit-text-size-adjust:100%; -ms-text-size-adjust:100%; }}
  a {{ color:{NAVY_D}; }}
  .wrap {{ max-width:920px; margin:0 auto; }}
  table {{ mso-table-lspace:0; mso-table-rspace:0; }}
  @media only screen and (max-width:600px) {{
    .pad {{ padding:14px !important; }}
    /* Tiles stack rather than shrink — a 24px figure squeezed into a quarter of a
       phone screen is the thing that was unreadable before. */
    .tiles td.tile {{ display:block !important; width:auto !important; margin-bottom:8px; }}
    .tiles td {{ width:auto !important; }}
    .nav td {{ display:inline-block !important; width:47% !important; }}
    .hdr h1 {{ font-size:19px !important; }}
    .scroll table {{ font-size:20px !important; }}
    .scroll td, .scroll th {{ padding:11px 7px !important; }}
    .wrap {{ padding:8px 4px !important; }}
    /* The summary stays legible but must not eat the fold — the first table
       has to be visible on the same screen. */
    .gap-head {{ font-size:21px !important; }}
    .gap-line {{ font-size:19px !important; }}
  }}
</style></head>
<body>
<a name="top"></a>
<div class="wrap" style="padding:16px 10px;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0"
         style="background:#fff;border-radius:10px;overflow:hidden;
                box-shadow:0 1px 4px rgba(0,0,0,.09);">
    <tr><td class="hdr" style="background:{NAVY};padding:16px 18px;">
      <h1 style="margin:0;font:700 18px Arial,Helvetica,sans-serif;color:#fff;">
        Daily Inward &amp; Transfer Report{' — REVISED' if revised else ''}</h1>
      <div style="font:12px Arial,Helvetica,sans-serif;color:#C7D2E6;margin-top:3px;">
        {day:%A, %d %B %Y} &nbsp;·&nbsp; generated {generated:%d %b %Y, %I:%M %p} IST
        &nbsp;·&nbsp; CFPL + CDPL
      </div>
    </td></tr>
    <tr><td class="pad" style="padding:16px 18px 20px;">
      {rev}
      {gaps_panel(gaps or [])}
      {cta}
      <table role="presentation" class="nav" width="100%" cellspacing="0" cellpadding="0"
             style="margin:0 0 12px;"><tr>{nav}</tr></table>
      {body}
    </td></tr>
    <tr><td style="background:{BAND};padding:11px 18px;text-align:center;
                   font:11px Arial,Helvetica,sans-serif;color:{GREY};">
      Candor Foods — IMS automated daily report. Weights are net kg as recorded in IMS;
      count-keyed lines (packaging) are shown in their own unit.
    </td></tr>
  </table>
</div>
</body></html>"""

    if len(html.encode("utf-8")) > MAIL_SAFE_BYTES and cap > MAIL_MIN_ROW_CAP:
        return render_email(day, agg, ops, generated, revised=revised,
                            view_url=view_url, gaps=gaps,
                            _cap=max(MAIL_MIN_ROW_CAP, cap - 3))
    return html


# ═════════════════════════════════════════════════════════════════════════
#  HOSTED PAGE  (real tabs + search + pagination; JS is fine here)
# ═════════════════════════════════════════════════════════════════════════
def render_page(day: date, agg, ops, generated: datetime, *, revised: bool = False,
                gaps: list[dict] | None = None) -> str:
    sections = build_sections(agg, ops, None)      # no cap — every row
    counts = _counts(agg, ops)
    gcount = gap_counts(gaps)

    tabs = "".join(
        f'<button class="tab" data-t="{k}" onclick="show(\'{k}\')">{e(l)}'
        f'<span class="cnt">{e(counts[k])}</span>'
        + (f'<span class="warn">{gcount[k]}</span>' if gcount[k] else "")
        + '</button>' for k, l in TABS
    )
    panels = "".join(
        f'<section class="panel" id="p-{k}"><div class="tools">'
        f'<input class="find" type="search" placeholder="Filter {e(l).lower()}…" '
        f'oninput="filt(this,\'{k}\')" aria-label="Filter {e(l)}">'
        f'</div>{sections[k]}</section>' for k, l in TABS
    )

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Daily Report — {day:%d %b %Y}</title>
<style>
  *,*::before,*::after{{box-sizing:border-box}}
  body{{margin:0;background:#F1F4F9;color:{INK};
       font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif}}
  .wrap{{max-width:1120px;margin:0 auto;padding:0 12px 48px}}
  header{{background:{NAVY};color:#fff;padding:18px 0 0}}
  header .in{{max-width:1120px;margin:0 auto;padding:0 12px}}
  h1{{margin:0;font-size:20px}}
  .sub{{color:#C7D2E6;font-size:13px;margin-top:3px}}
  .tabs{{display:flex;gap:6px;overflow-x:auto;margin-top:14px;padding-bottom:0;
         -webkit-overflow-scrolling:touch;scrollbar-width:none}}
  .tabs::-webkit-scrollbar{{display:none}}
  .tab{{flex:0 0 auto;border:0;background:rgba(255,255,255,.13);color:#fff;cursor:pointer;
        padding:10px 15px;border-radius:9px 9px 0 0;font:600 13px inherit;
        display:flex;align-items:center;gap:7px}}
  .tab:hover{{background:rgba(255,255,255,.22)}}
  .tab[aria-selected=true]{{background:#F1F4F9;color:{NAVY_D}}}
  .cnt{{background:rgba(0,0,0,.16);border-radius:9px;padding:1px 7px;font-size:11px}}
  .tab[aria-selected=true] .cnt{{background:{NAVY_L};color:{NAVY_D}}}
  .warn{{background:{ALERT};color:#fff;border-radius:9px;padding:1px 7px;font-size:11px;
         font-weight:700}}
  .alert{{margin:16px 0 0}}
  .panel{{display:none;background:#fff;border-radius:0 10px 10px 10px;padding:16px;
          box-shadow:0 1px 4px rgba(0,0,0,.09);margin-top:0}}
  .panel.on{{display:block;animation:fade .18s ease}}
  @keyframes fade{{from{{opacity:0;transform:translateY(4px)}}to{{opacity:1;transform:none}}}}
  .tools{{display:flex;justify-content:flex-end;margin-bottom:6px}}
  .find{{width:100%;max-width:290px;padding:8px 11px;border:1px solid {RULE};
         border-radius:8px;font:13px inherit}}
  .find:focus{{outline:2px solid {NAVY};outline-offset:1px}}
  .scroll{{overflow-x:auto;-webkit-overflow-scrolling:touch}}
  table{{width:100%}}
  .pager{{display:flex;gap:6px;align-items:center;justify-content:flex-end;
          margin:7px 2px 14px;font-size:12px;color:{GREY};flex-wrap:wrap}}
  .pager button{{border:1px solid {RULE};background:#fff;border-radius:7px;padding:5px 11px;
                 cursor:pointer;font:600 12px inherit;color:{NAVY_D}}}
  .pager button:disabled{{opacity:.4;cursor:default}}
  .none{{padding:18px;text-align:center;color:{GREY};font-style:italic}}
  @media(max-width:640px){{
    .panel{{padding:11px;border-radius:10px}}
    h1{{font-size:17px}} .find{{max-width:none}}
  }}
  @media (prefers-color-scheme:dark){{
    body{{background:#10151F;color:#E5E9F0}}
    .panel{{background:#18202E;box-shadow:none}}
    .tab[aria-selected=true]{{background:#18202E;color:#fff}}
    .find{{background:#111827;color:#E5E9F0;border-color:#2B3648}}
    .pager button{{background:#111827;color:#E5E9F0;border-color:#2B3648}}
  }}
</style></head>
<body>
<header><div class="in">
  <h1>Daily Inward &amp; Transfer Report{' — REVISED' if revised else ''}</h1>
  <div class="sub">{day:%A, %d %B %Y} &nbsp;·&nbsp; generated {generated:%d %b %Y, %I:%M %p} IST
       &nbsp;·&nbsp; CFPL + CDPL</div>
  <div class="alert">{gaps_panel(gaps or [])}</div>
  <div class="tabs" role="tablist">{tabs}</div>
</div></header>
<div class="wrap">{panels}</div>
<script>
  var PAGE = 25;
  function show(k){{
    document.querySelectorAll('.panel').forEach(function(p){{p.classList.remove('on')}});
    document.querySelectorAll('.tab').forEach(function(t){{
      t.setAttribute('aria-selected', t.dataset.t === k ? 'true' : 'false');
    }});
    var p = document.getElementById('p-' + k);
    if (p) p.classList.add('on');
    if (history.replaceState) history.replaceState(null, '', '#' + k);
    window.scrollTo({{top:0, behavior:'smooth'}});
  }}
  // Client-side pagination: long tables get pager controls, short ones do not.
  function paginate(tb){{
    var rows = Array.prototype.slice.call(tb.tBodies[0].rows);
    if (rows.length <= PAGE) return;
    var page = 0;
    var bar = document.createElement('div');
    bar.className = 'pager';
    var prev = document.createElement('button'); prev.textContent = 'Prev';
    var next = document.createElement('button'); next.textContent = 'Next';
    var lbl = document.createElement('span');
    bar.appendChild(lbl); bar.appendChild(prev); bar.appendChild(next);
    tb.parentNode.parentNode.insertBefore(bar, tb.parentNode.nextSibling);
    function draw(){{
      var vis = rows.filter(function(r){{ return r.dataset.hid !== '1'; }});
      var pages = Math.max(1, Math.ceil(vis.length / PAGE));
      if (page >= pages) page = pages - 1;
      rows.forEach(function(r){{ r.style.display = 'none'; }});
      vis.slice(page * PAGE, page * PAGE + PAGE).forEach(function(r){{ r.style.display = ''; }});
      lbl.textContent = vis.length ? ('Showing ' + (page * PAGE + 1) + '–' +
        Math.min(vis.length, page * PAGE + PAGE) + ' of ' + vis.length) : 'No matching rows';
      prev.disabled = page === 0; next.disabled = page >= pages - 1;
      bar.style.display = vis.length > PAGE ? 'flex' : 'none';
    }}
    prev.onclick = function(){{ if (page > 0) {{ page--; draw(); }} }};
    next.onclick = function(){{ page++; draw(); }};
    tb._redraw = draw;
    draw();
  }}
  function filt(inp, k){{
    var q = inp.value.toLowerCase().trim();
    document.querySelectorAll('#p-' + k + ' table').forEach(function(tb){{
      Array.prototype.slice.call(tb.tBodies[0].rows).forEach(function(r){{
        var hit = !q || r.textContent.toLowerCase().indexOf(q) !== -1;
        r.dataset.hid = hit ? '0' : '1';
        r.style.display = hit ? '' : 'none';
      }});
      if (tb._redraw) tb._redraw();
    }});
  }}
  document.querySelectorAll('.panel table').forEach(paginate);
  show((location.hash || '').replace('#', '') || '{TABS[0][0]}');
</script>
</body></html>"""
