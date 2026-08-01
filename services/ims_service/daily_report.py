"""Daily Inward & Transfer report — 2-page PDF, emailed at 19:00 IST.

WHAT IT REPORTS
    Page 1  headline totals, warehouse-wise, user-wise, transfer routes
    Page 2  inward warehouse x category, transfers route x category

ATTRIBUTION — "user" is whoever KEYED the entry (the logged-in IMS user), never
the purchase head / business head / approval authority:
    inward        {cfpl,cdpl}_transactions_v2.approved_by  (the frontend writes
                  `user?.name || user?.email` when the entry is saved)
    transfer out  interunit_transfers_header.created_by
    transfer in   interunit_transfer_in_header.received_by
`Stores A185` is a SHARED store login, so those entries are credited to the
person named in the entry's approval authority (Sumit Baikar by default).

MEASURE — weight is the primary measure, but packaging / PM lines are keyed by
count, not weight. A line therefore reports, in order of preference:
    1. article net_weight        2. sum of its box net weights        3. quantity + UOM
so a packaging line shows "100 BOX" rather than a misleading 0.00 kg.

VALUE — inward only. Transfers move existing stock between our own locations
and carry no rate anywhere in IMS, so they are reported by weight/count alone.
Inward value comes from the GRN line; where no rate was entered the report says
"not entered" rather than silently showing zero.

SCHEDULE (registered in main.py)
    19:00 IST daily   — the day's report. Mon-Sat always; Sunday only if the day
                        had activity.
    10:30 IST daily   — yesterday's report re-sent ONLY if it changed after the
                        19:00 cut-off (late entries), or if the 19:00 send was
                        missed entirely (server asleep / redeploying).
Every attempt is recorded in `daily_report_log`, which is also what makes the
"no silent misses" guarantee testable.
"""
from __future__ import annotations

import re
import smtplib
import threading
from collections import defaultdict
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from html import escape
from io import BytesIO

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.config_loader import settings
from shared.database import SessionLocal
from shared.logger import get_logger
from shared.timezone import IST, now_ist
from shared.mail_identity import Module, SubjectPolicy, stamp
from services.ims_service.daily_report_ops import fetch_and_aggregate as ops_data
from services.ims_service.daily_report_html import render_email, render_page

logger = get_logger("daily_report")


def view_url(day: date) -> str | None:
    """Public link to the interactive version, or None when no base URL is set."""
    base = (settings.BACKEND_URL or "").rstrip("/")
    return f"{base}/daily-report/view?day={day:%Y-%m-%d}" if base else None


def day_anchor(day: date) -> str:
    """Deterministic thread root for a business day.

    A revision is the same day's report restated, so it belongs in the same
    conversation as the original rather than as a second inbox row. Deriving the
    anchor from the date alone means no stored state: the evening send claims it as
    its Message-ID, and every later revision or catch-up references it.
    """
    return f"daily-report-{day:%Y-%m-%d}@candorfoods.in"


def day_subject(day: date) -> str:
    """One subject per business day — identical for the original and its revisions.

    Gmail splits a conversation the moment the subject changes, so "REVISED" must
    NOT appear here; it is called out in the body banner and the page header instead.
    """
    return f"Daily Report — {day:%a, %d %b %Y}"

# ── recipients ───────────────────────────────────────────────────────────
REPORT_TO = [
    "b.hrithik@candorfoods.in",
    "yash@candorfoods.in",
    "sunil.jasoria@candorfoods.in",
    "stores@candorfoods.in",
]
REPORT_CC = [
    "vaibhav.kumkar@candorfoods.in",
    "stores-a185@candorfoods.in",
    "pankaj.ranga@candorfoods.in",
    "rmpatil@candorfoods.in",
    "satyendra@candorfoods.in",
    "aman.yadav@candorfoods.in",
]

EVENING_HOUR, EVENING_MIN = 19, 0     # IST
MORNING_HOUR, MORNING_MIN = 10, 30    # IST

DASH = "–"
ARROW = "→"

# ── warehouse canonicalisation (mirrors shared/canonicalize.py) ──────────
WAREHOUSE_ALIASES = {
    "savla d-39": "Savla D-39", "savla d39": "Savla D-39", "d-39": "Savla D-39",
    "d39": "Savla D-39", "savla bond": "Savla D-39", "old savla": "Savla D-39",
    "savla d-514": "Savla D-514", "savla d514": "Savla D-514", "d-514": "Savla D-514",
    "d514": "Savla D-514", "new savla": "Savla D-514",
    "rishi": "Rishi", "rishi cold": "Rishi", "rishi cold storage": "Rishi",
    "supreme": "Supreme", "supreme cold": "Supreme",
    "eskimo": "Eskimo", "eskimo cold": "Eskimo",
    "w202": "W202", "a101": "A101", "a185": "A185", "a68": "A68",
    "f53": "F53", "dev int": "Dev Int",
}


def canon_wh(raw: str | None) -> str:
    if not raw or not raw.strip():
        return "(Unassigned)"
    return WAREHOUSE_ALIASES.get(raw.strip().lower().replace("_", " "), raw.strip())


# ── user canonicalisation ────────────────────────────────────────────────
# The report is limited to the ten data-entry operators; anyone else rolls up
# into "Other users" so the column totals still tie to the headline figures.
ALLOWED_USERS = [
    "Vaibhav Kumkar", "Samal Kumar", "Pankaj Ranga", "Vaishali Dhuri",
    "Sumit Baikar", "Naresh Jadhav", "Mayuresh Mahadik", "B Hrithik",
    "Yash Gawdi", "Mahesh Taparia",
]
OTHERS = "Other users"
NOT_RECORDED = "(Not recorded)"

USER_ALIASES = {
    "VAIBHAV": "Vaibhav Kumkar", "VAIBHAV KUMKAR": "Vaibhav Kumkar",
    "VAIBHAV KUMKAR CANDORFOODS IN": "Vaibhav Kumkar",
    "SAMAL": "Samal Kumar", "SAMAL KUMAR": "Samal Kumar",
    "SAMAL KUMAR CANDORFOODS IN": "Samal Kumar",
    "PANKAJ": "Pankaj Ranga", "PANKAJ RANGA": "Pankaj Ranga",
    "PANKAJ RANGA CANDORFOODS IN": "Pankaj Ranga",
    "VAISHALI": "Vaishali Dhuri", "VAISHALI DHURI": "Vaishali Dhuri",
    "VAISHALI DHURI CANDORFOODS IN": "Vaishali Dhuri",
    "SUMIT": "Sumit Baikar", "SUMIT BAIKAR": "Sumit Baikar",
    "NARESH": "Naresh Jadhav", "NARESH JADHAV": "Naresh Jadhav",
    "NARESH JADHAV CANDORFOODS IN": "Naresh Jadhav",
    "MAYURESH": "Mayuresh Mahadik", "MAYURESH DATES": "Mayuresh Mahadik",
    "MAYURESH MAHADIK": "Mayuresh Mahadik",
    "MAYURESH CANDORFOODS IN": "Mayuresh Mahadik",
    "HRITHIK": "B Hrithik", "B HRITHIK": "B Hrithik", "HRITHIK B": "B Hrithik",
    "B HRITHIK CANDORFOODS IN": "B Hrithik",
    "YASH": "Yash Gawdi", "YASH GAWDI": "Yash Gawdi",
    "YASH CANDORFOODS IN": "Yash Gawdi",
    "MAHESH": "Mahesh Taparia", "MAHESH TAPARIA": "Mahesh Taparia",
    "MAHESH TAPARIYA": "Mahesh Taparia",
    "MAHESH TAPARIYA CANDORFOODS IN": "Mahesh Taparia",
}
_HONORIFIC = re.compile(r"\b(SIR|MADAM|MAM|JI)\b")


def _fold(raw: str | None) -> str:
    if not raw or not raw.strip():
        return ""
    s = raw.strip()
    if "@" in s:                                    # email -> local part
        s = s.split("@", 1)[0].replace(".", " ").replace("_", " ")
    s = _HONORIFIC.sub(" ", s.upper())
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def canon_user(raw: str | None, authority: str | None = None) -> str:
    """Resolve the keying user to one of the ten operators.

    The A185 store login is shared between staff, so the human named on the
    entry (its approval authority) is preferred; where that is blank the
    store's owner is assumed — Sumit Baikar keyed 129 of 139 such entries.
    """
    key = _fold(raw)
    if key.startswith("STORES"):
        alt = USER_ALIASES.get(_fold(authority))
        return alt if alt in ALLOWED_USERS else "Sumit Baikar"
    if not key:
        alt = USER_ALIASES.get(_fold(authority))
        return alt if alt in ALLOWED_USERS else NOT_RECORDED
    name = USER_ALIASES.get(key)
    return name if name in ALLOWED_USERS else OTHERS


def user_sort_key(name: str) -> tuple:
    if name in ALLOWED_USERS:
        return (0, ALLOWED_USERS.index(name))
    return (1 if name == OTHERS else 2, 0)


_GROUP_ACRONYMS = {"RM", "PM", "FG"}


def canon_group(raw: str | None) -> str:
    """Collapse case variants: 'DATES'/'dates' -> 'Dates', 'TRAIL MIX' -> 'Trail Mix'."""
    if not raw or not raw.strip():
        return "(Uncategorised)"
    s = re.sub(r"\s+", " ", raw.strip())
    return " ".join(w.upper() if w.upper() in _GROUP_ACRONYMS else w.capitalize()
                    for w in s.split())


# ── measures ─────────────────────────────────────────────────────────────
class Measure:
    """Weight plus any count-keyed quantity, kept in separate units.

    Packaging / PM lines are entered as a count (e.g. 100 BOX) with no weight.
    Reporting those as 0.00 kg hides real receipts, so the count is carried
    alongside the weight and printed in its own unit.
    """

    __slots__ = ("kg", "qty")

    def __init__(self):
        self.kg = 0.0
        self.qty: dict[str, float] = defaultdict(float)

    def add_line(self, net_weight, box_kg, qty, uom):
        kg = float(net_weight or 0)
        if kg <= 0:
            kg = float(box_kg or 0)          # boxes are authoritative when the line is blank
        if kg > 0:
            self.kg += kg
            return
        q = float(qty or 0)
        if q > 0:
            self.qty[(uom or "NOS").strip().upper() or "NOS"] += q

    def merge(self, other: "Measure"):
        self.kg += other.kg
        for u, q in other.qty.items():
            self.qty[u] += q

    @property
    def is_empty(self) -> bool:
        return self.kg <= 0 and not any(v > 0 for v in self.qty.values())

    def cell(self, *, unit: bool = False) -> str:
        """Render as 'kg' and/or 'n UOM', one per line. Never a bare 0.00.

        `unit=True` spells out "kg" for prose, where there is no column header
        to say what the number is.
        """
        parts = []
        if self.kg > 0:
            parts.append(f"{self.kg:,.2f}" + (" kg" if unit else ""))
        for u, q in sorted(self.qty.items()):
            if q > 0:
                parts.append(f"{q:,.0f} {u}")
        return "\n".join(parts) if parts else DASH

    def phrase(self) -> str:
        """One-line form for sentences: '33,031.39 kg + 103 BOX'."""
        return self.cell(unit=True).replace("\n", " + ")


def kg(v) -> str:
    return DASH if v is None else f"{float(v):,.2f}"


_LAKH_SEP = re.compile(r"(?<=\d)(?=(\d\d)+$)")


def inr(v) -> str:
    """Indian digit grouping: 5569548.75 -> 55,69,548.75"""
    if v is None or float(v) == 0:
        return DASH
    n = float(v)
    sign = "-" if n < 0 else ""
    whole, frac = f"{abs(n):.2f}".split(".")
    if len(whole) > 3:
        head, tail = whole[:-3], whole[-3:]
        whole = _LAKH_SEP.sub(",", head) + "," + tail
    return f"{sign}{whole}.{frac}"


def num(v) -> str:
    return DASH if not v else f"{int(v):,}"


# ═════════════════════════════════════════════════════════════════════════
#  DATA
# ═════════════════════════════════════════════════════════════════════════
INWARD_SQL = """
SELECT t.transaction_no, t.warehouse, t.status,
       t.approved_by, t.approval_authority,
       a.item_category, a.net_weight, a.quantity_units, a.uom, a.total_amount,
       COALESCE(bx.box_kg, 0) AS box_kg
FROM {p}_transactions_v2 t
LEFT JOIN {p}_articles_v2 a USING (transaction_no)
LEFT JOIN LATERAL (
    SELECT SUM(COALESCE(b.net_weight, 0)) AS box_kg
    FROM {p}_boxes_v2 b
    WHERE b.transaction_no = a.transaction_no
      AND b.article_description = a.item_description
) bx ON TRUE
WHERE t.entry_date::date = :d
UNION ALL
SELECT t.transaction_no, t.warehouse, t.status,
       t.approved_by, t.approval_authority,
       a.item_category, a.net_weight, a.quantity_units, a.uom, a.total_amount,
       0 AS box_kg
FROM {p}_bulk_entry_transactions t
LEFT JOIN {p}_bulk_entry_articles a USING (transaction_no)
WHERE NULLIF(t.entry_date::text, '')::date = :d
"""

TRF_OUT_SQL = """
SELECT h.id, h.from_site, h.to_site, h.from_cold_unit,
       h.created_by, h.approved_by,
       l.item_category, l.net_weight, l.qty, l.uom
FROM interunit_transfers_header h
JOIN interunit_transfers_lines l ON l.header_id = h.id
WHERE h.stock_trf_date = :d
"""

TRF_IN_SQL = """
SELECT i.id, o.from_site, o.from_cold_unit,
       COALESCE(NULLIF(TRIM(i.receiving_warehouse), ''), o.to_site) AS to_site,
       i.received_by, b.article, b.net_weight
FROM interunit_transfer_in_header i
LEFT JOIN interunit_transfer_in_boxes b ON b.header_id = i.id
LEFT JOIN interunit_transfers_header o ON o.id = i.transfer_out_id
WHERE i.grn_date::date = :d
"""

# item_category lives on the transfer-OUT line, not on the received box
IN_CAT_SQL = """
SELECT LOWER(TRIM(COALESCE(item_desc_raw, ''))) AS item,
       MIN(COALESCE(item_category, '')) AS cat
FROM interunit_transfers_lines
WHERE COALESCE(item_category, '') <> ''
GROUP BY 1
"""


def fetch(db: Session, day: date) -> dict:
    inward = []
    for p in ("cfpl", "cdpl"):
        inward += [dict(r._mapping) for r in
                   db.execute(text(INWARD_SQL.format(p=p)), {"d": day})]
    out = [dict(r._mapping) for r in db.execute(text(TRF_OUT_SQL), {"d": day})]
    tin = [dict(r._mapping) for r in db.execute(text(TRF_IN_SQL), {"d": day})]
    cat_map = {r.item: r.cat for r in db.execute(text(IN_CAT_SQL))}
    return {"inward": inward, "out": out, "in": tin, "cat_map": cat_map}


def src_site(row) -> str:
    """'Cold Storage' is a generic from_site; from_cold_unit names the real one."""
    fs = (row.get("from_site") or "").strip()
    if fs.lower().startswith("cold") and row.get("from_cold_unit"):
        return canon_wh(row["from_cold_unit"])
    return canon_wh(fs)


def _mk():
    return {"itx": set(), "im": Measure(), "ival": 0.0, "imiss": 0,
            "och": set(), "om": Measure(), "igrn": set(), "inm": Measure()}


def aggregate(data: dict) -> dict:
    inward, out, tin, cat_map = data["inward"], data["out"], data["in"], data["cat_map"]

    head_i, head_o, head_n = Measure(), Measure(), Measure()
    inw_txns, out_chl, in_grn = set(), set(), set()
    inw_val = 0.0

    wh = defaultdict(_mk)
    usr = defaultdict(_mk)
    route = defaultdict(lambda: {"ch": set(), "m": Measure()})
    inw_wg = defaultdict(lambda: {"m": Measure(), "val": 0.0, "miss": 0})
    out_rg = defaultdict(Measure)
    in_rg = defaultdict(Measure)

    art_lines = 0
    miss_lines = 0
    miss_m = Measure()

    for r in inward:
        inw_txns.add(r["transaction_no"])
        has_line = any(r[k] is not None for k in ("item_category", "net_weight", "quantity_units"))
        args = (r["net_weight"], r["box_kg"], r["quantity_units"], r["uom"])
        val = float(r["total_amount"] or 0)
        inw_val += val
        head_i.add_line(*args)

        w = wh[canon_wh(r["warehouse"])]
        w["itx"].add(r["transaction_no"]); w["im"].add_line(*args); w["ival"] += val

        u = usr[canon_user(r["approved_by"], r["approval_authority"])]
        u["itx"].add(r["transaction_no"]); u["im"].add_line(*args)

        g = inw_wg[(canon_wh(r["warehouse"]), canon_group(r["item_category"]))]
        g["m"].add_line(*args); g["val"] += val

        if has_line:
            art_lines += 1
            if not val:
                miss_lines += 1
                w["imiss"] += 1
                g["miss"] += 1
                miss_m.add_line(*args)

    for r in out:
        out_chl.add(r["id"])
        args = (r["net_weight"], 0, r["qty"], r["uom"])
        head_o.add_line(*args)
        w = wh[src_site(r)]
        w["och"].add(r["id"]); w["om"].add_line(*args)
        u = usr[canon_user(r["created_by"], r["approved_by"])]
        u["och"].add(r["id"]); u["om"].add_line(*args)
        k = (src_site(r), canon_wh(r["to_site"]))
        route[k]["ch"].add(r["id"]); route[k]["m"].add_line(*args)
        out_rg[(k[0], k[1], canon_group(r["item_category"]))].add_line(*args)

    for r in tin:
        in_grn.add(r["id"])
        # a received box with no weight still counts as one physical box
        args = (r["net_weight"], 0, 0 if r["net_weight"] else (1 if r.get("article") else 0), "BOX")
        head_n.add_line(*args)
        w = wh[canon_wh(r["to_site"])]
        w["igrn"].add(r["id"]); w["inm"].add_line(*args)
        u = usr[canon_user(r["received_by"])]
        u["igrn"].add(r["id"]); u["inm"].add_line(*args)
        cat = cat_map.get((r.get("article") or "").strip().lower(), "")
        in_rg[(src_site(r), canon_wh(r["to_site"]), canon_group(cat))].add_line(*args)

    return {
        "head": {"inw_txns": len(inw_txns), "inw_m": head_i, "inw_val": inw_val,
                 "out_chl": len(out_chl), "out_m": head_o,
                 "in_grn": len(in_grn), "in_m": head_n},
        "val_gap": {"lines": art_lines, "missing": miss_lines, "missing_m": miss_m},
        "wh": wh, "usr": usr, "route": route,
        "inw_wg": inw_wg, "out_rg": out_rg, "in_rg": in_rg,
        "empty": not (inw_txns or out_chl or in_grn),
    }


def fingerprint(agg: dict, ops: dict | None = None) -> str:
    """Stable signature of the reported figures.

    The morning job re-runs the day and compares this to what was sent at 19:00;
    a difference means entries landed (or were edited) after the cut-off. This
    catches back-dated edits too, which a created_at>19:00 filter would miss.

    Job cards and samples are included, so a late job card or sample approval
    triggers the revision just as a late GRN would.
    """
    h = agg["head"]
    parts = [
        f"i:{h['inw_txns']}:{h['inw_m'].kg:.2f}:{sorted(h['inw_m'].qty.items())}:{h['inw_val']:.2f}",
        f"o:{h['out_chl']}:{h['out_m'].kg:.2f}:{sorted(h['out_m'].qty.items())}",
        f"n:{h['in_grn']}:{h['in_m'].kg:.2f}:{sorted(h['in_m'].qty.items())}",
    ]
    if ops:
        jc, sm = ops["jobcards"], ops["samples"]
        parts.append(f"j:{jc['total_cards']}:{jc['total_kg']:.2f}:"
                     f"{sorted(jc['status'].items())}:{jc['loss'].get('rows', 0)}")
        parts.append(f"s:{len(sm['requisitions'])}:{len(sm['actions'])}:"
                     f"{len(sm['npd_jobcards'])}:{sm['total_qty']:.3f}")
    return "|".join(parts)


# ═════════════════════════════════════════════════════════════════════════
#  PDF
# ═════════════════════════════════════════════════════════════════════════
def build_pdf(day: date, agg: dict, generated: datetime, *, revised: bool = False) -> bytes:
    # imported lazily so a deploy without reportlab still boots the API
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas as pdfcanvas
    from reportlab.platypus import (
        BaseDocTemplate, Frame, PageTemplate, Paragraph, Spacer, Table,
        TableStyle, PageBreak,
    )

    NAVY = colors.HexColor("#29417A")
    NAVY_LT = colors.HexColor("#E8EDF5")
    GREY = colors.HexColor("#6B7280")
    RULE = colors.HexColor("#D5DBE5")
    BAND = colors.HexColor("#F7F9FC")
    AMBER = colors.HexColor("#B45309")
    AMBER_BG = colors.HexColor("#FFF7ED")

    S_TITLE = ParagraphStyle("t", fontName="Helvetica-Bold", fontSize=13, leading=16,
                             textColor=colors.white)
    S_SUB = ParagraphStyle("s", fontName="Helvetica", fontSize=8.5, leading=11,
                           textColor=colors.HexColor("#C7D2E6"))
    S_H = ParagraphStyle("h", fontName="Helvetica-Bold", fontSize=9.5, leading=11,
                         textColor=NAVY, spaceBefore=6, spaceAfter=3)
    S_NOTE = ParagraphStyle("n", fontName="Helvetica-Oblique", fontSize=6.5, leading=8,
                            textColor=GREY, spaceBefore=2)
    S_FLAG = ParagraphStyle("f", fontName="Helvetica-Bold", fontSize=7.2, leading=9,
                            textColor=AMBER, spaceBefore=3, backColor=AMBER_BG,
                            borderPadding=3, leftIndent=2)

    W = 178 * mm

    def hdr_band():
        tag = " &nbsp;&mdash;&nbsp; REVISED" if revised else ""
        left = [Paragraph(f"CANDOR FOODS &nbsp;&middot;&nbsp; Daily Inward &amp; Transfer Report{tag}", S_TITLE),
                Paragraph(f"Business day: <b>{day:%A, %d %B %Y}</b>", S_SUB)]
        right = [Paragraph(f"<para align=right>Generated {generated:%d %b %Y, %I:%M %p} IST</para>", S_SUB),
                 Paragraph("<para align=right>Companies: CFPL + CDPL &nbsp;&middot;&nbsp; Source: IMS</para>", S_SUB)]
        t = Table([[left, right]], colWidths=[112 * mm, 66 * mm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), NAVY),
            ("LEFTPADDING", (0, 0), (-1, -1), 10), ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 8), ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        return t

    def grid(data, widths, aligns, *, total_row=False, band_rows=(), amber_cells=(), fs=7.2):
        t = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
        st = [
            ("BACKGROUND", (0, 0), (-1, 0), NAVY),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), fs),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("GRID", (0, 0), (-1, -1), 0.4, RULE),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 2.4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2.4),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, BAND]),
        ]
        for i, a in enumerate(aligns):
            st.append(("ALIGN", (i, 0), (i, -1), a))
        for (r, col) in amber_cells:
            st += [("TEXTCOLOR", (col, r), (col, r), AMBER),
                   ("BACKGROUND", (col, r), (col, r), AMBER_BG),
                   ("FONTNAME", (col, r), (col, r), "Helvetica-Oblique")]
        if total_row:
            st += [("BACKGROUND", (0, -1), (-1, -1), NAVY_LT),
                   ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                   ("LINEABOVE", (0, -1), (-1, -1), 0.9, NAVY)]
        for r in band_rows:
            st += [("BACKGROUND", (0, r), (-1, r), NAVY_LT),
                   ("FONTNAME", (0, r), (-1, r), "Helvetica-Bold")]
        t.setStyle(TableStyle(st))
        return t

    # The page count is written on a second pass: how many pages the tables need
    # depends on how many warehouses / categories moved that day, so "of 2"
    # cannot be hardcoded without eventually lying.
    class NumberedCanvas(pdfcanvas.Canvas):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self._saved_states = []

        def showPage(self):
            self._saved_states.append(dict(self.__dict__))
            self._startPage()

        def save(self):
            total = len(self._saved_states)
            for state in self._saved_states:
                self.__dict__.update(state)
                self._footer(total)
                super().showPage()
            super().save()

        def _footer(self, total):
            self.saveState()
            self.setStrokeColor(RULE); self.setLineWidth(0.5)
            self.line(16 * mm, 12 * mm, A4[0] - 16 * mm, 12 * mm)
            self.setFont("Helvetica", 6.5); self.setFillColor(GREY)
            self.drawString(16 * mm, 8 * mm,
                            "Candor Foods - IMS automated daily report. Weights are net kg as recorded in "
                            "IMS; count-keyed lines (packaging) are shown in their own unit.")
            self.drawRightString(A4[0] - 16 * mm, 8 * mm,
                                 f"Page {self.getPageNumber()} of {total}")
            self.restoreState()

    buf = BytesIO()
    doc = BaseDocTemplate(buf, pagesize=A4,
                          leftMargin=16 * mm, rightMargin=16 * mm,
                          topMargin=12 * mm, bottomMargin=14 * mm,
                          title=f"Daily Inward & Transfer Report {day:%d %b %Y}",
                          author="Candor Foods IMS")
    frame = Frame(doc.leftMargin, doc.bottomMargin, W, A4[1] - 26 * mm, id="f")
    doc.addPageTemplates([PageTemplate(id="p", frames=[frame])])

    F = []
    h, vg = agg["head"], agg["val_gap"]

    # ── PAGE 1 ──
    F.append(hdr_band())
    F.append(Spacer(1, 7))

    if revised:
        F.append(Paragraph(
            "Revised report — entries were recorded for this day after the 7:00 PM cut-off. "
            "These figures supersede the report sent yesterday evening.", S_FLAG))

    F.append(Paragraph("1 &nbsp;&middot;&nbsp; At a glance", S_H))
    total_m = Measure()
    for m in (h["inw_m"], h["out_m"], h["in_m"]):
        total_m.merge(m)
    rows = [["Activity", "Transactions", "Total Kgs / Qty", "Value (Rs.)"],
            ["Inward (GRN)", num(h["inw_txns"]), h["inw_m"].cell(), inr(h["inw_val"])],
            ["Transfer - Out (dispatched)", num(h["out_chl"]), h["out_m"].cell(), DASH],
            ["Transfer - In (received)", num(h["in_grn"]), h["in_m"].cell(), DASH],
            ["TOTAL", num(h["inw_txns"] + h["out_chl"] + h["in_grn"]),
             total_m.cell(), inr(h["inw_val"])]]
    F.append(grid(rows, [64 * mm, 30 * mm, 40 * mm, 44 * mm],
                  ["LEFT", "RIGHT", "RIGHT", "RIGHT"], total_row=True, fs=7.8))
    F.append(Paragraph(
        "Transfers move existing stock between our own locations and carry no rate in IMS, so they are "
        "reported by weight/count only. Lines keyed by count rather than weight (packaging, PM) are shown "
        "in their own unit (BOX / CARTON / BAG) rather than as 0 kg.", S_NOTE))

    if vg["missing"]:
        F.append(Paragraph(
            f"Value not entered on {vg['missing']} of {vg['lines']} inward lines "
            f"({vg['missing_m'].phrase()}). Inward value below is therefore understated.",
            S_FLAG))

    F.append(Paragraph("2 &nbsp;&middot;&nbsp; Warehouse-wise summary", S_H))
    rows = [["Warehouse", "Inward\nTxns", "Inward\nKgs / Qty", "Inward\nValue (Rs.)",
             "Trf-Out\nChallans", "Trf-Out\nKgs / Qty", "Trf-In\nGRNs", "Trf-In\nKgs / Qty"]]
    amber = []
    t_itx = t_och = t_igrn = 0
    t_val = 0.0
    t_im, t_om, t_in = Measure(), Measure(), Measure()
    for name in sorted(agg["wh"], key=lambda k: -(agg["wh"][k]["im"].kg + agg["wh"][k]["om"].kg
                                                  + agg["wh"][k]["inm"].kg)):
        w = agg["wh"][name]
        rows.append([name, num(len(w["itx"])), w["im"].cell(),
                     "not entered" if (w["itx"] and not w["ival"]) else inr(w["ival"]),
                     num(len(w["och"])), w["om"].cell(),
                     num(len(w["igrn"])), w["inm"].cell()])
        if w["itx"] and not w["ival"]:
            amber.append((len(rows) - 1, 3))
        t_itx += len(w["itx"]); t_och += len(w["och"]); t_igrn += len(w["igrn"])
        t_val += w["ival"]
        t_im.merge(w["im"]); t_om.merge(w["om"]); t_in.merge(w["inm"])
    rows.append(["TOTAL", num(t_itx), t_im.cell(), inr(t_val),
                 num(t_och), t_om.cell(), num(t_igrn), t_in.cell()])
    F.append(grid(rows, [28 * mm, 15 * mm, 27 * mm, 32 * mm, 18 * mm, 26 * mm, 14 * mm, 18 * mm],
                  ["LEFT"] + ["RIGHT"] * 7, total_row=True, amber_cells=amber, fs=6.8))

    F.append(Paragraph("3 &nbsp;&middot;&nbsp; User-wise summary (who keyed the entry)", S_H))
    rows = [["User", "Inward\nTxns", "Inward\nKgs / Qty", "Trf-Out\nChallans", "Trf-Out\nKgs / Qty",
             "Trf-In\nGRNs", "Trf-In\nKgs / Qty", "Total\nTxns"]]
    t_itx = t_och = t_igrn = t_n = 0
    t_im, t_om, t_in = Measure(), Measure(), Measure()
    for name in sorted(agg["usr"], key=user_sort_key):
        u = agg["usr"][name]
        n = len(u["itx"]) + len(u["och"]) + len(u["igrn"])
        rows.append([name, num(len(u["itx"])), u["im"].cell(), num(len(u["och"])),
                     u["om"].cell(), num(len(u["igrn"])), u["inm"].cell(), num(n)])
        t_itx += len(u["itx"]); t_och += len(u["och"]); t_igrn += len(u["igrn"]); t_n += n
        t_im.merge(u["im"]); t_om.merge(u["om"]); t_in.merge(u["inm"])
    rows.append(["TOTAL", num(t_itx), t_im.cell(), num(t_och), t_om.cell(),
                 num(t_igrn), t_in.cell(), num(t_n)])
    F.append(grid(rows, [38 * mm, 15 * mm, 26 * mm, 18 * mm, 26 * mm, 14 * mm, 24 * mm, 15 * mm],
                  ["LEFT"] + ["RIGHT"] * 7, total_row=True, fs=6.8))
    F.append(Paragraph(
        "User = the IMS login that saved the record (inward: entry submitter; transfer-out: challan creator; "
        "transfer-in: receiver) - not the purchase head, business head or approval authority. The A185 store "
        "login is shared, so those entries are credited to the person named on the entry (Sumit Baikar unless "
        "stated otherwise). Anyone outside the ten operators rolls up into 'Other users' so the totals still tie.",
        S_NOTE))

    F.append(Paragraph("4 &nbsp;&middot;&nbsp; Transfer routes (warehouse to warehouse)", S_H))
    rows = [["From", "To", "Challans", "Total Kgs / Qty"]]
    tc = 0
    tm = Measure()
    for (f, t_), v in sorted(agg["route"].items(), key=lambda kv: -kv[1]["m"].kg):
        rows.append([f, t_, num(len(v["ch"])), v["m"].cell()])
        tc += len(v["ch"]); tm.merge(v["m"])
    if len(rows) == 1:
        rows.append(["-", "-", DASH, DASH])
    rows.append(["TOTAL", "", num(tc), tm.cell()])
    F.append(grid(rows, [50 * mm, 50 * mm, 33 * mm, 45 * mm],
                  ["LEFT", "LEFT", "RIGHT", "RIGHT"], total_row=True, fs=7.2))

    # ── PAGE 2 ──
    F.append(PageBreak())
    F.append(hdr_band())
    F.append(Spacer(1, 7))
    F.append(Paragraph("5 &nbsp;&middot;&nbsp; INWARD - warehouse-wise, category-wise", S_H))

    rows = [["Warehouse", "Category / Group", "Total Kgs / Qty", "Value (Rs.)"]]
    bands, amber = [], []
    g_m, g_val = Measure(), 0.0
    by_wh = defaultdict(list)
    for (w, g), v in agg["inw_wg"].items():
        by_wh[w].append((g, v))
    for w in sorted(by_wh, key=lambda k: -sum(x[1]["m"].kg for x in by_wh[k])):
        items = sorted(by_wh[w], key=lambda x: -x[1]["m"].kg)
        sub_m, sub_val = Measure(), 0.0
        for i, (g, v) in enumerate(items):
            rows.append([w if i == 0 else "", g, v["m"].cell(),
                         "not entered" if not v["val"] else inr(v["val"])])
            if not v["val"]:
                amber.append((len(rows) - 1, 3))
            sub_m.merge(v["m"]); sub_val += v["val"]
        rows.append([f"{w} subtotal", "", sub_m.cell(), inr(sub_val)])
        bands.append(len(rows) - 1)
        g_m.merge(sub_m); g_val += sub_val
    if len(rows) == 1:
        rows.append(["-", "No inward recorded", DASH, DASH])
    rows.append(["GRAND TOTAL", "", g_m.cell(), inr(g_val)])
    F.append(grid(rows, [40 * mm, 60 * mm, 36 * mm, 42 * mm],
                  ["LEFT", "LEFT", "RIGHT", "RIGHT"], total_row=True,
                  band_rows=bands, amber_cells=amber, fs=7.0))
    F.append(Paragraph(
        "'not entered' = no rate was captured on that GRN line, so it contributes 0 to the value column. "
        "The weight or count is still counted in full.", S_NOTE))

    F.append(Paragraph("6 &nbsp;&middot;&nbsp; TRANSFER between warehouses - route-wise, category-wise", S_H))
    rows = [["From", "To", "Category / Group", "Out Kgs / Qty", "In Kgs / Qty"]]
    bands = []
    keys = set(agg["out_rg"]) | set(agg["in_rg"])
    by_route = defaultdict(list)
    for (f, t_, g) in keys:
        by_route[(f, t_)].append(g)
    go, gi = Measure(), Measure()
    for (f, t_) in sorted(by_route,
                          key=lambda k: -sum(agg["out_rg"][(k[0], k[1], g)].kg
                                             for g in by_route[k] if (k[0], k[1], g) in agg["out_rg"])):
        groups = sorted(by_route[(f, t_)],
                        key=lambda g: -(agg["out_rg"][(f, t_, g)].kg if (f, t_, g) in agg["out_rg"] else 0))
        so, si = Measure(), Measure()
        for i, g in enumerate(groups):
            o = agg["out_rg"].get((f, t_, g))
            n = agg["in_rg"].get((f, t_, g))
            rows.append([f if i == 0 else "", t_ if i == 0 else "", g,
                         o.cell() if o and not o.is_empty else DASH,
                         n.cell() if n and not n.is_empty else DASH])
            if o:
                so.merge(o)
            if n:
                si.merge(n)
        rows.append([f"{f} {ARROW} {t_} subtotal", "", "", so.cell(), si.cell()])
        bands.append(len(rows) - 1)
        go.merge(so); gi.merge(si)
    if len(rows) == 1:
        rows.append(["-", "-", "No transfers recorded", DASH, DASH])
    rows.append(["GRAND TOTAL", "", "", go.cell(), gi.cell()])
    F.append(grid(rows, [34 * mm, 30 * mm, 46 * mm, 34 * mm, 34 * mm],
                  ["LEFT", "LEFT", "LEFT", "RIGHT", "RIGHT"], total_row=True,
                  band_rows=bands, fs=7.0))
    F.append(Paragraph(
        "Out Kgs = dispatched on this day; In Kgs = received on this day. They will not match, because a "
        "consignment is usually received a day or more after it is dispatched.", S_NOTE))

    doc.build(F, canvasmaker=NumberedCanvas)
    return buf.getvalue()


# ═════════════════════════════════════════════════════════════════════════
#  EMAIL
# ═════════════════════════════════════════════════════════════════════════
def _plain_summary(day: date, agg: dict, ops: dict, revised: bool) -> str:
    """Text alternative. Clients that refuse HTML still get the day's numbers."""
    h, vg = agg["head"], agg["val_gap"]
    jc, sm = ops["jobcards"], ops["samples"]
    banner = ("REVISED - entries were recorded for this day after the 7:00 PM cut-off. "
              "These figures supersede yesterday evening's report."
              if revised else
              "Daily activity across inward, transfers, job cards and samples.")

    lines = [
        f"Daily Report{' - REVISED' if revised else ''}: {day:%A, %d %B %Y}",
        "", banner, "",
        "INWARD",
        f"  {h['inw_txns']} transactions | {h['inw_m'].phrase()} | Rs. {inr(h['inw_val'])}",
    ]
    if vg["missing"]:
        lines.append(f"  Value not entered on {vg['missing']} of {vg['lines']} lines "
                     f"({vg['missing_m'].phrase()}) - value is understated.")
    lines += [
        "", "TRANSFERS",
        f"  Out: {h['out_chl']} challans | {h['out_m'].phrase()}",
        f"  In : {h['in_grn']} GRNs | {h['in_m'].phrase()}",
        "", "JOB CARDS",
        f"  {jc['total_cards']} active | {jc['total_users']} users | "
        f"{jc['total_fg']} FG items | {jc['total_kg']:,.2f} kg planned",
    ]
    if jc["loss"].get("rows"):
        L = jc["loss"]
        lines.append(f"  Loss: process {L['process']:,.2f} kg, wastage {L['wastage']:,.2f} kg, "
                     f"rejection {L['rejection']:,.2f} kg | avg total loss {L['avg_total_pct']:.2f}% | "
                     f"{L['balanced']} of {L['rows']} balanced")
    lines += [
        "", "SAMPLES / NPD",
        f"  {len(sm['requisitions'])} requisitions | {len(sm['actions'])} actions | "
        f"{len(sm['npd_jobcards'])} NPD job cards | {len(sm['customers'])} customers",
    ]
    url = view_url(day)
    if url:
        lines += ["", f"Full interactive view: {url}"]
    return "\n".join(lines)


def _send_mail(subject: str, html_body: str, plain_body: str,
               to: list[str], cc: list[str],
               pdf: bytes | None = None, filename: str | None = None,
               *, day: date | None = None, revised: bool = False,
               message_id: str | None = None, in_reply_to: str | None = None,
               verbatim_subject: bool = False) -> None:
    """Send synchronously and raise on failure — the caller records the outcome.

    The report lives in the message body; the PDF is optional and off by default
    (it is still available on demand from /daily-report/preview).
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = settings.SMTP_EMAIL
    msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg.set_content(plain_body)
    msg.add_alternative(html_body, subtype="html")
    if pdf and filename:
        msg.add_attachment(pdf, maintype="application", subtype="pdf", filename=filename)

    # One conversation per business day: the evening send claims the anchor, and a
    # revision replies into it rather than opening a second row.
    if message_id:
        msg["Message-ID"] = f"<{message_id}>"
    if in_reply_to:
        msg["In-Reply-To"] = f"<{in_reply_to}>"
        msg["References"] = f"<{in_reply_to}>"

    # ANCHOR, not EVENT: the subject must stay identical between the original and
    # its revisions or Gmail files them as two unrelated mails. "REVISED" lives in
    # the body banner.
    stamp(msg, module=Module.REPORTS,
          policy=SubjectPolicy.VERBATIM if verbatim_subject else SubjectPolicy.ANCHOR,
          entity_type="DailyReport",
          entity_id=f"{day:%Y-%m-%d}" if day else None,
          event="DAILY_REPORT_REVISED" if revised else "DAILY_REPORT",
          status="updated" if revised else "report",
          sender=settings.SMTP_EMAIL)

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=60) as server:
        server.starttls()
        server.login(settings.SMTP_EMAIL, settings.SMTP_APP_PASSWORD)
        server.send_message(msg, to_addrs=to + cc)


# ═════════════════════════════════════════════════════════════════════════
#  DELIVERY LOG
# ═════════════════════════════════════════════════════════════════════════
def ensure_log_table(db: Session) -> None:
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS daily_report_log (
            id            SERIAL PRIMARY KEY,
            business_day  DATE        NOT NULL,
            kind          VARCHAR(20) NOT NULL,
            fingerprint   TEXT        NOT NULL DEFAULT '',
            recipients    TEXT        NOT NULL DEFAULT '',
            status        VARCHAR(20) NOT NULL,
            error         TEXT,
            sent_at       TIMESTAMP   NOT NULL DEFAULT NOW()
        )
    """))
    db.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_daily_report_log_day ON daily_report_log(business_day)"))
    db.commit()


def _log(db: Session, day: date, kind: str, status: str,
         fp: str = "", error: str | None = None) -> None:
    db.execute(text("""
        INSERT INTO daily_report_log (business_day, kind, fingerprint, recipients, status, error)
        VALUES (:d, :k, :f, :r, :s, :e)
    """), {"d": day, "k": kind, "f": fp,
           "r": ", ".join(REPORT_TO + REPORT_CC), "s": status, "e": error})
    db.commit()


def _last_sent(db: Session, day: date) -> tuple[str, datetime] | None:
    row = db.execute(text("""
        SELECT fingerprint, sent_at FROM daily_report_log
        WHERE business_day = :d AND status = 'sent'
        ORDER BY sent_at DESC LIMIT 1
    """), {"d": day}).fetchone()
    return (row.fingerprint, row.sent_at) if row else None


# ═════════════════════════════════════════════════════════════════════════
#  ORCHESTRATION
# ═════════════════════════════════════════════════════════════════════════
def send_report(day: date, *, kind: str, revised: bool = False,
                skip_if_empty: bool = False, db: Session | None = None,
                subject_override: str | None = None,
                thread_onto: str | None = None) -> dict:
    """Build and email the report for `day`. Returns a result dict; never raises.

    `subject_override` / `thread_onto` exist for one narrow case: replying into a
    conversation that was started before this module owned the subject line, where
    the only way to land in the right thread is to reproduce the old subject exactly.
    Normal sends leave both unset.
    """
    own_db = db is None
    db = db or SessionLocal()
    try:
        ensure_log_table(db)
        agg = aggregate(fetch(db, day))
        ops = ops_data(db, day)
        fp = fingerprint(agg, ops)

        # "Empty" must consider every section — a day with no GRN but a job card
        # or a sample approval is not a quiet day.
        nothing_happened = agg["empty"] and ops["jobcards"]["empty"] and ops["samples"]["empty"]
        if skip_if_empty and nothing_happened:
            _log(db, day, kind, "skipped_empty", fp)
            logger.info("Daily report %s: no activity in any section, nothing sent", day)
            return {"day": str(day), "status": "skipped_empty", "sent": False}

        generated = now_ist()
        html = render_email(day, agg, ops, generated,
                            revised=revised, view_url=view_url(day))
        plain = _plain_summary(day, agg, ops, revised)
        # The first send of a day owns the thread anchor; anything after it replies
        # into that same conversation.
        anchor = day_anchor(day)
        first_send = _last_sent(db, day) is None
        _send_mail(
            subject_override or day_subject(day), html, plain, REPORT_TO, REPORT_CC,
            day=day, revised=revised,
            message_id=anchor if first_send else None,
            in_reply_to=None if first_send else (thread_onto or anchor),
            verbatim_subject=bool(subject_override),
        )
        _log(db, day, kind, "sent", fp)
        h = agg["head"]
        logger.info("Daily report %s (%s) sent to %d recipients — "
                    "inward %d, trf-out %d, trf-in %d",
                    day, kind, len(REPORT_TO) + len(REPORT_CC),
                    h["inw_txns"], h["out_chl"], h["in_grn"])
        return {"day": str(day), "status": "sent", "sent": True, "kind": kind,
                "revised": revised, "fingerprint": fp,
                "inward_txns": h["inw_txns"], "transfer_out": h["out_chl"],
                "transfer_in": h["in_grn"]}
    except Exception as exc:                                  # noqa: BLE001
        logger.error("Daily report %s (%s) FAILED: %s", day, kind, exc)
        try:
            db.rollback()      # the session may be poisoned; clear it before logging
            _log(db, day, kind, "failed", error=str(exc)[:1000])
        except Exception:                                     # noqa: BLE001
            logger.error("Could not record the failure for %s in daily_report_log", day)
        return {"day": str(day), "status": "failed", "sent": False, "error": str(exc)}
    finally:
        if own_db:
            db.close()


def run_evening_report() -> dict:
    """19:00 IST. Mon-Sat unconditionally; Sunday only when the day had activity."""
    day = now_ist().date()
    is_sunday = day.weekday() == 6
    return send_report(day, kind="evening", skip_if_empty=is_sunday)


def run_morning_revision() -> dict:
    """10:30 IST. Re-send yesterday only if it changed after the 19:00 cut-off,
    or if yesterday's evening send never happened (server asleep / redeploy)."""
    day = now_ist().date() - timedelta(days=1)
    db = SessionLocal()
    try:
        ensure_log_table(db)
        prev = _last_sent(db, day)
        agg = aggregate(fetch(db, day))
        ops = ops_data(db, day)
        fp = fingerprint(agg, ops)
        nothing_happened = agg["empty"] and ops["jobcards"]["empty"] and ops["samples"]["empty"]

        if prev is None:
            if nothing_happened and day.weekday() == 6:
                _log(db, day, "catchup", "skipped_empty", fp)
                return {"day": str(day), "status": "skipped_empty", "sent": False}
            logger.warning("No evening report was sent for %s — sending catch-up now", day)
            return send_report(day, kind="catchup", db=db)

        if prev[0] != fp:
            logger.info("Figures for %s changed after the 19:00 cut-off — sending revision", day)
            return send_report(day, kind="revision", revised=True, db=db)

        _log(db, day, "revision", "skipped_unchanged", fp)
        logger.info("No late entries for %s — no revision sent", day)
        return {"day": str(day), "status": "skipped_unchanged", "sent": False}
    except Exception as exc:                                  # noqa: BLE001
        logger.error("Morning revision check for %s failed: %s", day, exc)
        return {"day": str(day), "status": "failed", "sent": False, "error": str(exc)}
    finally:
        db.close()


def run_startup_catchup(max_days: int = 7) -> None:
    """On boot, send any business day in the recent past that never went out.

    Guards the "without fail" requirement against the host idling the service
    through 19:00. Runs in a background thread so startup is not delayed.

    Only days from the first logged send onwards are considered — otherwise the
    very first deploy would mail out a week of back-dated reports nobody asked
    for. An empty log means nothing was ever owed.
    """
    def _work():
        db = SessionLocal()
        try:
            ensure_log_table(db)
            since = db.execute(
                text("SELECT MIN(business_day) FROM daily_report_log")
            ).scalar()
            if since is None:
                logger.info("Daily report catch-up: no send history yet, nothing owed")
                return

            today = now_ist().date()
            for back in range(1, max_days + 1):
                day = today - timedelta(days=back)
                if day < since or _last_sent(db, day):
                    continue
                agg = aggregate(fetch(db, day))
                ops = ops_data(db, day)
                if agg["empty"] and ops["jobcards"]["empty"] and ops["samples"]["empty"]:
                    continue          # nothing happened; nothing owed
                logger.warning("Catch-up: %s was never sent — sending now", day)
                send_report(day, kind="catchup", db=db)
        except Exception as exc:                              # noqa: BLE001
            logger.error("Startup catch-up failed: %s", exc)
        finally:
            db.close()

    threading.Thread(target=_work, daemon=True).start()
