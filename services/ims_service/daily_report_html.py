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
RED = "#B3261E"

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
def table(headers, rows, aligns=None, *, cap=None, total=None, note=None,
          widths=None, empty="Nothing recorded for this day") -> str:
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

    hcell = (f'padding:9px 11px;background:{NAVY};color:#fff;font-weight:700;'
             f'font-size:12.5px;line-height:1.3;white-space:nowrap;'
             f'border:1px solid {NAVY};text-align:')
    th = "".join(f'<th style="{hcell}{a};">{e(h)}</th>' for h, a in zip(headers, aligns))

    if not rows:
        body = (f'<tr><td colspan="{len(headers)}" style="padding:18px 12px;text-align:center;'
                f'color:{GREY};border:1px solid {RULE};font-style:italic;font-size:13px;">'
                f'{e(empty)}</td></tr>')
    else:
        # Row striping goes on <tr bgcolor>, and the text colour is set once on the
        # table. Repeating either per cell inflates the message by ~20 KB on a busy
        # day, which is what pushes Gmail into clipping the last section away.
        body = ""
        # tabular-nums is declared once on the <table> and inherited — repeating it
        # per cell cost ~20 KB on a busy day, enough to trip Gmail's clip limit and
        # drop the last section out of the mail entirely.
        txt = f'padding:9px 11px;border:1px solid {RULE};text-align:left;'
        num = f'padding:9px 11px;border:1px solid {RULE};text-align:right;white-space:nowrap;'
        for i, r in enumerate(rows):
            tds = "".join(
                f'<td style="{num if a == "right" else txt}">'
                f'{c if isinstance(c, _Raw) else e(c)}</td>'
                for c, a in zip(r, aligns)
            )
            body += f'<tr bgcolor="{"#ffffff" if i % 2 == 0 else BAND}">{tds}</tr>'

    if hidden:
        body += (f'<tr><td colspan="{len(headers)}" style="padding:9px 11px;text-align:center;'
                 f'color:{AMBER};background:{AMBER_BG};border:1px solid {RULE};'
                 f'font-size:12px;font-weight:600;">'
                 f'+ {hidden} more row{"s" if hidden != 1 else ""} — open the full view for all'
                 f'</td></tr>')

    if total:
        tcell = (f'padding:10px 11px;border:1px solid {RULE};border-top:2px solid {NAVY};'
                 f'font-weight:700;font-size:13.5px;color:{NAVY_D};text-align:')
        tds = "".join(f'<td style="{tcell}{a};">{c if isinstance(c, _Raw) else e(c)}</td>'
                      for c, a in zip(total, aligns))
        body += f'<tr bgcolor="{NAVY_L}">{tds}</tr>'

    n = (f'<div style="font-size:12px;line-height:1.5;color:{GREY};margin:7px 2px 0;">{note}</div>'
         if note else "")

    return (
        f'<div class="scroll" style="overflow-x:auto;-webkit-overflow-scrolling:touch;'
        f'margin:0 0 6px;border-radius:6px;">'
        f'<table role="presentation" cellspacing="0" cellpadding="0" '
        f'style="border-collapse:collapse;width:100%;min-width:{min(len(headers) * 96, 640)}px;'
        f'font-size:13.5px;line-height:1.4;font-variant-numeric:tabular-nums;'
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
    return _Raw(f'<span style="display:inline-block;padding:3px 10px;border-radius:11px;'
                f'background:{bg};color:{fg};font-size:12px;font-weight:700;'
                f'white-space:nowrap;">{escape(str(txt))}</span>')


def h3(txt) -> str:
    return (f'<div style="font:700 15px/1.3 Arial,Helvetica,sans-serif;color:{NAVY};'
            f'margin:22px 0 8px;padding-bottom:5px;'
            f'border-bottom:2px solid {NAVY_L};">{e(txt)}</div>')


def flag(txt, kind="warn") -> str:
    """A callout that has to be read, not skimmed past."""
    fg, bg = (AMBER, AMBER_BG) if kind == "warn" else (NAVY_D, NAVY_L)
    return (f'<div style="margin:12px 0;padding:12px 15px;background:{bg};'
            f'border-left:5px solid {fg};color:{fg};'
            f'font:700 13.5px/1.5 Arial,Helvetica,sans-serif;'
            f'border-radius:0 6px 6px 0;">{txt}</div>')


def tiles(items) -> str:
    """Headline numbers — the largest type on the page, because they are what the
    mail is scanned for. A table, not flexbox: Outlook ignores flex."""
    cells = ""
    for label, value, sub in items:
        cells += (
            f'<td class="tile" style="padding:13px 15px;background:{BAND};'
            f'border:1px solid {RULE};border-top:3px solid {NAVY};'
            f'border-radius:8px;vertical-align:top;">'
            f'<div style="font:700 24px/1.15 Arial,Helvetica,sans-serif;color:{NAVY_D};'
            f'font-variant-numeric:tabular-nums;">{e(value)}</div>'
            f'<div style="font:700 11.5px/1.3 Arial,Helvetica,sans-serif;color:{GREY};'
            f'text-transform:uppercase;letter-spacing:.5px;margin-top:5px;">{e(label)}</div>'
            + (f'<div style="font:12.5px/1.4 Arial,Helvetica,sans-serif;color:{INK};'
               f'margin-top:4px;">{e(sub)}</div>' if sub else "")
            + '</td><td style="width:9px;"></td>'
        )
    return (f'<table role="presentation" cellspacing="0" cellpadding="0" class="tiles" '
            f'style="width:100%;border-collapse:separate;margin:4px 0 8px;"><tr>{cells}</tr></table>')


# ═════════════════════════════════════════════════════════════════════════
#  SECTIONS  (shared by the mail body and the hosted page)
# ═════════════════════════════════════════════════════════════════════════
def _section_inward(agg, cap, scap) -> str:
    h, vg = agg["head"], agg["val_gap"]
    out = tiles([
        ("Transactions", _n(h["inw_txns"]), None),
        ("Total received", h["inw_m"].phrase(), None),
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
    out += h3("Warehouse-wise")
    out += table(["Warehouse", "Transactions", "Kgs / Qty", "Value (Rs.)"], rows,
                 ["left", "right", "right", "right"], cap=scap,
                 total=["TOTAL", _n(tot_tx), h["inw_m"].cell().replace("\n", " + "), _inr(tot_val)],
                 empty="No inward recorded for this day")

    rows = []
    for name in sorted(agg["usr"], key=lambda k: -len(agg["usr"][k]["itx"])):
        u = agg["usr"][name]
        if u["itx"]:
            rows.append([name, _n(len(u["itx"])), u["im"].cell().replace("\n", " + ")])
    out += h3("Who keyed it")
    out += table(["User", "Transactions", "Kgs / Qty"], rows, ["left", "right", "right"],
                 cap=scap,
                 note="The IMS login that saved the GRN — not the purchase head or approval authority.",
                 empty="No inward recorded for this day")

    rows = []
    for (w, g), v in sorted(agg["inw_wg"].items(), key=lambda kv: -kv[1]["m"].kg):
        if v["m"].is_empty and not v["val"]:
            continue
        rows.append([w, g, v["m"].cell().replace("\n", " + "),
                     badge("not entered", "warn") if not v["val"] else _inr(v["val"])])
    out += h3("Warehouse × category")
    out += table(["Warehouse", "Category / Group", "Kgs / Qty", "Value (Rs.)"], rows,
                 ["left", "left", "right", "right"], cap=cap, widths=[18, 32, 24, 26],
                 empty="No inward recorded for this day")
    return out


def _section_transfers(agg, cap, scap) -> str:
    h = agg["head"]
    out = tiles([
        ("Dispatched", _n(h["out_chl"]) + " challans", h["out_m"].phrase()),
        ("Received", _n(h["in_grn"]) + " GRNs", h["in_m"].phrase()),
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
    out += h3("Routes")
    out += table(["From", "To", "Challans", "Kgs / Qty"], rows,
                 ["left", "left", "right", "right"], cap=scap,
                 total=["TOTAL", "", _n(tc), h["out_m"].cell().replace("\n", " + ")],
                 empty="No transfers dispatched on this day")

    rows = []
    for name in sorted(agg["usr"], key=lambda k: -(len(agg["usr"][k]["och"]) + len(agg["usr"][k]["igrn"]))):
        u = agg["usr"][name]
        if u["och"] or u["igrn"]:
            rows.append([name, _n(len(u["och"])), u["om"].cell().replace("\n", " + "),
                         _n(len(u["igrn"])), u["inm"].cell().replace("\n", " + ")])
    out += h3("Who keyed it")
    out += table(["User", "Out challans", "Out Kgs", "In GRNs", "In Kgs"], rows,
                 ["left", "right", "right", "right", "right"], cap=scap,
                 empty="No transfer activity for this day")

    rows = []
    keys = set(agg["out_rg"]) | set(agg["in_rg"])
    for (f_, t_, g) in sorted(keys, key=lambda k: -(agg["out_rg"][k].kg if k in agg["out_rg"] else 0)):
        o = agg["out_rg"].get((f_, t_, g))
        i = agg["in_rg"].get((f_, t_, g))
        rows.append([f"{f_} → {t_}", g,
                     o.cell().replace("\n", " + ") if o and not o.is_empty else "–",
                     i.cell().replace("\n", " + ") if i and not i.is_empty else "–"])
    out += h3("Route × category")
    out += table(["Route", "Category / Group", "Out Kgs / Qty", "In Kgs / Qty"], rows,
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


def _section_jobcards(jc, cap, scap) -> str:
    if jc["empty"]:
        return (tiles([("Job cards", "0", "none active"), ("Users", "0", None), ("FG items", "0", None)])
                + table(["Job card", "Factory", "FG item", "Status"], [],
                        empty="No job card activity for this day"))

    loss = jc["loss"]
    out = tiles([
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
    out += h3("Warehouse-wise")
    out += table(["Factory", "Job cards", "Users", "FG items", "Planned kg", "Units", "Status mix"],
                 rows, ["left", "right", "right", "right", "right", "right", "left"],
                 cap=scap,
                 total=["TOTAL", _n(jc["total_cards"]), _n(jc["total_users"]), _n(jc["total_fg"]),
                        _kg(jc["total_kg"]), _n(jc["total_units"]), ""])

    out += h3("Status update")
    out += table(["Status", "Job cards"],
                 [[_status_badge(k), _n(v)] for k, v in
                  sorted(jc["status"].items(), key=lambda x: -x[1])],
                 ["left", "right"], cap=scap)

    out += h3("Loss metrics (accounting summary)")
    if loss.get("rows"):
        bal_kind = "good" if loss["unbalanced"] == 0 else "warn"
        if loss["unbalanced"]:
            out += flag(f"{loss['unbalanced']} of {loss['rows']} accounted job cards do not "
                        f"balance — input, output and losses do not reconcile on those cards.")
        out += table(
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
        out += table(["Measure", "Quantity"], [],
                     empty="No accounting entries recorded against these job cards")

    rows = []
    for r in sorted(jc["rows"], key=lambda x: -(float(x["planned_qty_kg"] or 0))):
        rows.append([r["job_card_number"], canon := r["factory"] or "-",
                     r["fg_sku_name"] or "-", r["customer_name"] or "-",
                     _kg(r["planned_qty_kg"]), _status_badge(r["status"]),
                     (r["assigned_to_team_leader"] or "–")])
    out += h3("Job cards")
    out += table(["Job card", "Factory", "FG item", "Customer", "Planned kg", "Status", "Team leader"],
                 rows, ["left", "left", "left", "left", "right", "left", "left"], cap=cap,
                 widths=[18, 8, 22, 20, 11, 11, 10])
    return out


def _section_samples(sm, cap, scap) -> str:
    out = tiles([
        ("Requisitions", _n(len(sm["requisitions"])), "raised today"),
        ("Actions", _n(len(sm["actions"])), "approvals / status changes"),
        ("NPD job cards", _n(len(sm["npd_jobcards"])), None),
        ("Customers", _n(len(sm["customers"])), None),
    ])

    if sm["empty"]:
        return out + table(["Requisition", "Type", "Status"], [],
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
    out += h3("Requisitions raised")
    out += table(["Req", "Type", "Status", "Target / item", "Customer", "Sales group",
                  "Quantity", "Requested by"], rows,
                 ["left", "left", "left", "left", "left", "left", "right", "left"], cap=cap,
                 widths=[6, 9, 13, 20, 17, 12, 11, 12],
                 note="Sales group is taken from all_sku via the requisition's articles.",
                 empty="No requisitions raised on this day")

    rows = [[g, _n(v["reqs"]), _kg(v["qty"])] for g, v in
            sorted(sm["by_sale_group"].items(), key=lambda kv: -kv[1]["reqs"])]
    out += h3("By sales group")
    out += table(["Sales group", "Requisitions", "Quantity"], rows,
                 ["left", "right", "right"], cap=scap)

    rows = []
    for a in sm["actions"]:
        move = f"{a['from']} → {a['to']}" if a["from"] and a["to"] else (a["to"] or a["from"] or "-")
        rows.append([f"#{a['req']}", a["event"], move, a["target"] or "-",
                     a["actor"], a["role"] or "-", a["remarks"] or "-"])
    out += h3("Actions taken")
    out += table(["Req", "Action", "Movement", "Target", "By", "Role", "Remarks"], rows,
                 ["left", "left", "left", "left", "left", "left", "left"], cap=cap,
                 widths=[6, 13, 22, 17, 15, 11, 16],
                 empty="No sample actions on this day")

    rows = []
    for n in sm["npd_jobcards"]:
        rows.append([f"#{n['id']}", n["title"] or "-", n["fg_sku_name"] or "-",
                     f"{_kg(n['target_qty'])} {e(n['uom'] or '')}".strip(),
                     _kg(n["output_qty"]), _pct(n["yield_pct"]),
                     _status_badge(n["status"]),
                     (n["customer_name"] or n["company_name"] or "-"), n["created_by_name"]])
    out += h3("NPD development job cards")
    out += table(["Card", "Title", "FG item", "Target", "Output", "Yield", "Status",
                  "Customer", "By"], rows,
                 ["left", "left", "left", "right", "right", "right", "left", "left", "left"],
                 cap=cap, empty="No NPD development job cards on this day")
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


def build_sections(agg, ops, cap) -> dict[str, str]:
    """`cap` bounds detail tables; summary tables get a looser bound.

    Both are bounded, because the size guard can only shrink the mail if every
    table responds to it — an uncapped summary table would grow with the data and
    push the message past Gmail's clip limit no matter how far the detail tables
    were trimmed. In practice the summary bound is never reached: there are ~11
    warehouses and ~20 routes in the business.
    """
    scap = None if cap is None else max(cap * 3, 12)
    return {
        "inward": _section_inward(agg, cap, scap),
        "transfers": _section_transfers(agg, cap, scap),
        "jobcards": _section_jobcards(ops["jobcards"], cap, scap),
        "samples": _section_samples(ops["samples"], cap, scap),
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
                 _cap: int | None = None) -> str:
    """The mail body.

    Re-renders with fewer detail rows if the result approaches Gmail's clip limit.
    Clipping is not a cosmetic failure — Gmail hides everything past the cut behind
    "View entire message", so the Samples section would simply not be in the mail
    for most readers. Capping rows costs a few lines that the linked view still has;
    clipping costs a whole section.
    """
    cap = MAIL_ROW_CAP if _cap is None else _cap
    sections = build_sections(agg, ops, cap)
    counts = _counts(agg, ops)

    nav = ""
    for key, label in TABS:
        nav += (
            f'<td style="padding:0 5px 7px 0;">'
            f'<a href="#sec-{key}" style="display:block;padding:11px 12px;background:{NAVY_L};'
            f'border:1px solid {RULE};border-radius:8px;text-decoration:none;color:{NAVY_D};'
            f'font:700 14px Arial,Helvetica,sans-serif;text-align:center;white-space:nowrap;">'
            f'{e(label)} <span style="color:{GREY};font-weight:400;">({e(counts[key])})</span>'
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
        body += (
            f'<a name="sec-{key}"></a>'
            f'<div id="sec-{key}" style="margin:0 0 14px;">'
            f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>'
            f'<td style="background:{NAVY};color:#fff;padding:13px 16px;border-radius:8px 8px 0 0;'
            f'font:700 17px Arial,Helvetica,sans-serif;">{e(label)}'
            f'<span style="float:right;font-weight:400;font-size:14px;opacity:.9;">'
            f'{e(counts[key])}</span></td></tr></table>'
            f'<div style="border:1px solid {RULE};border-top:0;border-radius:0 0 8px 8px;'
            f'padding:15px 16px 18px;">{sections[key]}</div>'
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
    .scroll table {{ font-size:14px !important; }}
    .scroll td, .scroll th {{ padding:10px 11px !important; }}
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
                            view_url=view_url, _cap=max(MAIL_MIN_ROW_CAP, cap - 3))
    return html


# ═════════════════════════════════════════════════════════════════════════
#  HOSTED PAGE  (real tabs + search + pagination; JS is fine here)
# ═════════════════════════════════════════════════════════════════════════
def render_page(day: date, agg, ops, generated: datetime, *, revised: bool = False) -> str:
    sections = build_sections(agg, ops, None)      # no cap — every row
    counts = _counts(agg, ops)

    tabs = "".join(
        f'<button class="tab" data-t="{k}" onclick="show(\'{k}\')">{e(l)}'
        f'<span class="cnt">{e(counts[k])}</span></button>' for k, l in TABS
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
        padding:10px 15px;border-radius:9px 9px 0 0;font:600 13px inherit;white-space:nowrap;
        display:flex;align-items:center;gap:7px}}
  .tab:hover{{background:rgba(255,255,255,.22)}}
  .tab[aria-selected=true]{{background:#F1F4F9;color:{NAVY_D}}}
  .cnt{{background:rgba(0,0,0,.16);border-radius:9px;padding:1px 7px;font-size:11px}}
  .tab[aria-selected=true] .cnt{{background:{NAVY_L};color:{NAVY_D}}}
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
