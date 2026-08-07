"""Jobcard, Sample/NPD, Job work and Customer Return sections of the daily report.

Kept apart from `daily_report.py` (inward + transfers) because these read a
different part of the business and would otherwise push that module past the
point where it can be reasoned about in one sitting.

WHICH DATABASE — both live in the IMS database (`warehouse_db`), NOT in the ERP
replica. The ERP `.env` points at a near-empty dev copy (13 job cards vs 889
here), so reporting from there would silently under-report almost everything.

JOB CARDS (`job_card_v2` + `job_card_accounting_v2`)
    "Active on the day" means created, updated, started or ended that day —
    not merely created. Job cards are raised in batches and worked over the
    following days: 31 Jul 2026 had 0 created but 10 started, so a
    created_at-only view would have reported a blank day that was actually busy.
    Warehouses come through as "W-202"/"A-185" here vs "W202"/"A185" in inward,
    so both spellings canonicalise to one name.

SAMPLES / NPD (`sample_requisitions`, `sample_approvals`, `sample_audit_log`,
`npd_dev_job_cards`)
    Requestors and actors are integer ids resolved against `auth_user`.
    Sales group comes from `all_sku.sale_group` via the requisition's articles.

JOB WORK (`jb_materialout_*`, `jb_work_inward_*`)
    Material sent to a processing party and what comes back from them. Reported
    inside the Transfers section, because that is what it is: stock leaving and
    re-entering our own books, just via an outside party rather than another of
    our warehouses.

CUSTOMER RETURNS (`{cfpl,cdpl}_rtv_header` + `_lines` + `_boxes`)
    Goods coming back from a customer. Its own section — a return is neither an
    inward purchase nor a transfer, and folding it into either would overstate
    that one. A CR counts for the day it was raised OR the day it was approved,
    so an approval that lands days later is still reported when it happens.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger

logger = get_logger("daily_report_ops")

DASH = "–"

# Job cards spell the factories with a hyphen; inward does not.
_FACTORY_ALIASES = {
    "w-202": "W202", "w202": "W202",
    "a-185": "A185", "a185": "A185",
    "a-68": "A68", "a68": "A68",
    "a-101": "A101", "a101": "A101",
    "f-53": "F53", "f53": "F53",
}


def canon_factory(raw: str | None) -> str:
    if not raw or not raw.strip():
        return "(Unassigned)"
    return _FACTORY_ALIASES.get(raw.strip().lower().replace(" ", ""), raw.strip())


_GROUP_ACRONYMS = {"RM", "PM", "FG"}


def canon_group_name(raw: str | None) -> str:
    """'PISTA'/'pista' -> 'Pista'. Mirrors `daily_report.canon_group`.

    Restated rather than imported: `daily_report` imports this module, so reaching
    back the other way would make the pair circular for the sake of four lines.
    """
    if not raw or not str(raw).strip():
        return "(Uncategorised)"
    return " ".join(w.upper() if w.upper() in _GROUP_ACRONYMS else w.capitalize()
                    for w in " ".join(str(raw).split()).split())


# The only two states that mean a card is finished. Checked against the live
# table rather than assumed: every `completed`/`closed` card has both a
# start_time and an end_time, while all 430 `locked`/`unlocked`/`assigned` cards
# have neither and carry no accounting row — those are planned, not worked.
JC_CLOSED_STATES = {"completed", "closed"}


def _clean_person(raw: str | None) -> str:
    """Team-leader names are free text ('Monika'/'MONIKA'/'monika')."""
    if not raw or not raw.strip():
        return "(Unassigned)"
    return " ".join(w.capitalize() for w in raw.strip().split())


def _who(raw: str | None) -> str:
    """`created_by` on the job work and CR tables is the login email, not a name.

    'stores-a185@candorfoods.in' printed verbatim in a report column is both
    unreadable and wider than the column, so the local part becomes the name.
    """
    s = (raw or "").strip()
    if not s:
        return "(Not recorded)"
    if "@" in s:
        s = s.split("@", 1)[0].replace(".", " ").replace("_", " ").replace("-", " ")
    return _clean_person(s)


def _f(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


# ═════════════════════════════════════════════════════════════════════════
#  JOB CARDS
# ═════════════════════════════════════════════════════════════════════════
JOBCARD_SQL = """
SELECT j.job_card_id, j.job_card_number, j.factory, j.floor, j.process_name,
       j.stage, j.fg_sku_name, j.customer_name, j.batch_number,
       j.planned_qty_kg, j.planned_qty_units, j.uom, j.status,
       j.assigned_to_team_leader, j.updated_by,
       j.created_at::date  AS created_on,
       j.start_time::date  AS started_on,
       j.end_time::date    AS ended_on,
       a.total_input_qty, a.output_qty, a.output_qty_units, a.output_kind,
       a.process_loss_qty, a.rejection_qty, a.wastage_qty, a.offgrade_total_qty,
       a.control_sample_qty, a.balance_difference_qty, a.is_balanced,
       a.process_loss_pct, a.total_loss_pct, a.invisible_loss_pct
FROM job_card_v2 j
LEFT JOIN job_card_accounting_v2 a ON a.job_card_id = j.job_card_id
WHERE j.deleted_at IS NULL
  AND (j.created_at::date = :d OR j.updated_at::date = :d
       OR j.start_time::date = :d OR j.end_time::date = :d)
"""


def fetch_jobcards(db: Session, day: date) -> list[dict]:
    try:
        return [dict(r._mapping) for r in db.execute(text(JOBCARD_SQL), {"d": day})]
    except Exception as exc:                                   # noqa: BLE001
        logger.error("Job card fetch failed for %s: %s", day, exc)
        return []


def aggregate_jobcards(rows: list[dict], day: date | None = None) -> dict:
    """Warehouse-wise cards/users/FG/qty, status counts and the loss accounting.

    Also records, per factory, what did NOT happen — cards never started, cards
    worked on but not closed, cards with no accounting entry. The exception panel
    is built from these; counting them here rather than re-querying keeps the two
    views of a day from disagreeing.
    """
    # One job card can have several accounting rows; keep card-level facts unique.
    cards: dict[object, dict] = {}
    acct_seen: set[tuple] = set()
    acct_cards: set[object] = set()
    loss = defaultdict(float)
    acct_rows = balanced = 0
    pct_proc: list[float] = []
    pct_total: list[float] = []

    wh = defaultdict(lambda: {"cards": set(), "users": set(), "fg": set(),
                              "kg": 0.0, "units": 0.0, "status": defaultdict(int),
                              "closed": set(), "closed_today": set(), "started": set(),
                              "no_fg": 0, "no_acct": 0,
                              "acct_rows": 0, "unbalanced": 0})
    status_all = defaultdict(int)
    users_all: set[str] = set()
    fg_all: set[str] = set()

    for r in rows:
        jid = r["job_card_id"]
        if jid not in cards:
            cards[jid] = r
            w = canon_factory(r["factory"])
            person = _clean_person(r["assigned_to_team_leader"])
            b = wh[w]
            b["cards"].add(jid)
            b["status"][r["status"] or "(blank)"] += 1
            b["kg"] += _f(r["planned_qty_kg"])
            b["units"] += _f(r["planned_qty_units"])
            if r["fg_sku_name"]:
                b["fg"].add(r["fg_sku_name"])
                fg_all.add(r["fg_sku_name"])
            else:
                b["no_fg"] += 1
            if person != "(Unassigned)":
                b["users"].add(person)
                users_all.add(person)
            if str(r["status"] or "").strip().lower() in JC_CLOSED_STATES:
                b["closed"].add(jid)
            if r["started_on"]:
                b["started"].add(jid)
            if day is not None and r["ended_on"] == day:
                b["closed_today"].add(jid)
            status_all[r["status"] or "(blank)"] += 1

        # accounting is per (card, output row); dedupe on the measured tuple
        if r["total_input_qty"] is None and r["output_qty"] is None:
            continue
        acct_cards.add(jid)
        key = (jid, r["total_input_qty"], r["output_qty"], r["process_loss_qty"],
               r["balance_difference_qty"])
        if key in acct_seen:
            continue
        acct_seen.add(key)
        acct_rows += 1
        balanced += 1 if r["is_balanced"] else 0
        # ...and again per factory, so "does not balance" can name a warehouse
        # instead of landing in a company-wide total nobody owns.
        fb = wh[canon_factory(r["factory"])]
        fb["acct_rows"] += 1
        if not r["is_balanced"]:
            fb["unbalanced"] += 1
        loss["input"] += _f(r["total_input_qty"])
        loss["output"] += _f(r["output_qty"])
        loss["process"] += _f(r["process_loss_qty"])
        loss["rejection"] += _f(r["rejection_qty"])
        loss["wastage"] += _f(r["wastage_qty"])
        loss["offgrade"] += _f(r["offgrade_total_qty"])
        loss["control"] += _f(r["control_sample_qty"])
        loss["balance_diff"] += _f(r["balance_difference_qty"])
        if r["process_loss_pct"] is not None:
            pct_proc.append(_f(r["process_loss_pct"]))
        if r["total_loss_pct"] is not None:
            pct_total.append(_f(r["total_loss_pct"]))

    loss["avg_process_pct"] = sum(pct_proc) / len(pct_proc) if pct_proc else 0.0
    loss["avg_total_pct"] = sum(pct_total) / len(pct_total) if pct_total else 0.0
    loss["rows"] = acct_rows
    loss["balanced"] = balanced
    loss["unbalanced"] = acct_rows - balanced

    for b in wh.values():
        b["no_acct"] = len(b["cards"] - acct_cards)

    return {
        "rows": list(cards.values()),
        "acct_cards": acct_cards,
        "no_acct": len(set(cards) - acct_cards),
        "wh": wh,
        "status": dict(status_all),
        "loss": dict(loss),
        "total_cards": len(cards),
        "total_users": len(users_all),
        "total_fg": len(fg_all),
        "total_kg": sum(_f(c["planned_qty_kg"]) for c in cards.values()),
        "total_units": sum(_f(c["planned_qty_units"]) for c in cards.values()),
        "empty": not cards,
    }


# ═════════════════════════════════════════════════════════════════════════
#  SAMPLES / NPD
# ═════════════════════════════════════════════════════════════════════════
# Requisitions raised on the day. Sales group comes from the requisition's
# articles via all_sku; a requisition can span groups, hence the aggregation.
SAMPLE_REQ_SQL = """
SELECT r.id, r.sample_type, r.status, r.warehouse, r.customer_name, r.company_name,
       r.npd_target_name, r.quantity, r.pcs, r.weight_per_piece, r.purpose_tag,
       r.expected_dispatch_date, r.returnable, r.paid, r.amount,
       COALESCE(NULLIF(TRIM(u.full_name), ''), 'User ' || r.created_by::text) AS requestor,
       grp.sale_groups, grp.item_groups, grp.articles
FROM sample_requisitions r
LEFT JOIN auth_user u ON u.user_id::text = r.created_by::text
LEFT JOIN LATERAL (
    SELECT STRING_AGG(DISTINCT NULLIF(TRIM(s.sale_group), ''), ', ') AS sale_groups,
           STRING_AGG(DISTINCT NULLIF(TRIM(s.item_group), ''), ', ') AS item_groups,
           COUNT(*) AS articles
    FROM sample_requisition_articles ra
    LEFT JOIN all_sku s ON s.sku_id = ra.sku_id
    WHERE ra.requisition_id = r.id
) grp ON TRUE
WHERE r.deleted_at IS NULL AND r.created_at::date = :d
ORDER BY r.id DESC
"""

# Every action taken on any requisition that day (create, submit, approve,
# reject, cancel) — this is the "actions" view, not just same-day requisitions.
SAMPLE_ACTION_SQL = """
SELECT l.requisition_id, l.event_type, l.old_value, l.new_value, l.remarks,
       l.actor_role, l.created_at,
       COALESCE(NULLIF(TRIM(u.full_name), ''), 'User ' || l.actor_user_id::text) AS actor,
       r.sample_type, r.customer_name, r.npd_target_name
FROM sample_audit_log l
LEFT JOIN auth_user u ON u.user_id::text = l.actor_user_id::text
LEFT JOIN sample_requisitions r ON r.id = l.requisition_id
WHERE l.created_at::date = :d
ORDER BY l.created_at DESC
"""

NPD_JC_SQL = """
SELECT n.id, n.title, n.warehouse, n.fg_sku_name, n.target_qty, n.uom, n.status,
       n.output_qty, n.output_uom, n.yield_pct, n.customer_name, n.company_name,
       n.rm_consumed_qty, n.wastage_qty, n.dispatch_qty,
       COALESCE(NULLIF(TRIM(u.full_name), ''), 'User ' || n.created_by::text) AS created_by_name
FROM npd_dev_job_cards n
LEFT JOIN auth_user u ON u.user_id::text = n.created_by::text
WHERE n.created_at::date = :d OR n.closed_at::date = :d OR n.started_at::date = :d
ORDER BY n.id DESC
"""


def fetch_samples(db: Session, day: date) -> dict:
    def _run(label, sql):
        try:
            return [dict(r._mapping) for r in db.execute(text(sql), {"d": day})]
        except Exception as exc:                               # noqa: BLE001
            logger.error("Sample %s fetch failed for %s: %s", label, day, exc)
            return []

    return {
        "requisitions": _run("requisitions", SAMPLE_REQ_SQL),
        "actions": _run("actions", SAMPLE_ACTION_SQL),
        "npd_jobcards": _run("npd jobcards", NPD_JC_SQL),
    }


def _status_of(val) -> str:
    """audit old_value/new_value are jsonb blobs like {'status': 'SUBMITTED'}."""
    if isinstance(val, dict):
        return str(val.get("status") or val.get("action") or "").strip()
    return ""


def aggregate_samples(data: dict) -> dict:
    reqs, actions, npd = data["requisitions"], data["actions"], data["npd_jobcards"]

    by_type = defaultdict(int)
    by_status = defaultdict(int)
    by_sale_group = defaultdict(lambda: {"reqs": 0, "qty": 0.0})
    customers: set[str] = set()
    total_qty = total_pcs = 0.0

    for r in reqs:
        by_type[r["sample_type"] or "(blank)"] += 1
        by_status[r["status"] or "(blank)"] += 1
        total_qty += _f(r["quantity"])
        total_pcs += _f(r["pcs"])
        cust = (r["customer_name"] or r["company_name"] or "").strip()
        if cust and cust != "-":
            customers.add(cust)
        groups = [g.strip() for g in (r["sale_groups"] or "").split(",") if g.strip()]
        for g in (groups or ["(not mapped)"]):
            by_sale_group[g.upper()]["reqs"] += 1
            by_sale_group[g.upper()]["qty"] += _f(r["quantity"]) / max(len(groups), 1)

    act_rows = []
    for a in actions:
        frm, to = _status_of(a["old_value"]), _status_of(a["new_value"])
        act_rows.append({
            "req": a["requisition_id"],
            "event": (a["event_type"] or "").replace("_", " ").title(),
            "from": frm, "to": to,
            "actor": a["actor"], "role": (a["actor_role"] or "").replace("_", " ").title(),
            "remarks": a["remarks"] or "",
            "target": a["npd_target_name"] or a["customer_name"] or "",
            "at": a["created_at"],
        })

    npd_qty = sum(_f(n["target_qty"]) for n in npd)
    npd_out = sum(_f(n["output_qty"]) for n in npd)
    npd_status = defaultdict(int)
    for n in npd:
        npd_status[n["status"] or "(blank)"] += 1
        cust = (n["customer_name"] or n["company_name"] or "").strip()
        if cust and cust != "-":
            customers.add(cust)

    return {
        "requisitions": reqs,
        "actions": act_rows,
        "npd_jobcards": npd,
        "by_type": dict(by_type),
        "by_status": dict(by_status),
        "by_sale_group": dict(by_sale_group),
        "customers": sorted(customers),
        "total_qty": total_qty,
        "total_pcs": total_pcs,
        "npd_target_qty": npd_qty,
        "npd_output_qty": npd_out,
        "npd_status": dict(npd_status),
        "empty": not (reqs or actions or npd),
    }


# ═════════════════════════════════════════════════════════════════════════
#  JOB WORK
# ═════════════════════════════════════════════════════════════════════════
# Both business-date columns are VARCHAR and they do NOT agree on a format:
# material-out writes DD-MM-YYYY, the inward receipt writes YYYY-MM-DD. Checked
# against the live tables — every row matches its own format and none is blank —
# so the day is matched as a formatted string rather than cast. A cast is the
# tempting version and the fragile one: a single stray value would raise and take
# the whole day's report down with it, where a string compare simply misses that
# row. Should the app ever start writing a second format, that is a fix here, not
# a report-wide outage.
JW_OUT_SQL = """
SELECT h.id, h.challan_no, h.from_warehouse, h.to_party, h.status, h.created_by,
       h.expected_return_date, h.purpose_of_work,
       l.item_description, l.item_category, l.quantity_kgs, l.quantity_boxes, l.uom
FROM jb_materialout_header h
LEFT JOIN jb_materialout_lines l ON l.header_id = h.id
WHERE h.job_work_date = TO_CHAR(CAST(:d AS date), 'DD-MM-YYYY')
"""

JW_IN_SQL = """
SELECT r.id, r.ir_number, r.challan_no, r.receipt_type, r.inward_warehouse,
       r.created_by, h.to_party, h.from_warehouse,
       l.item_description, l.sent_kgs, l.finished_goods_kgs, l.finished_goods_boxes,
       l.waste_kgs, l.rejection_kgs, l.process_type
FROM jb_work_inward_receipt r
LEFT JOIN jb_materialout_header h ON h.id = r.header_id
LEFT JOIN jb_work_inward_lines l ON l.inward_receipt_id = r.id
WHERE r.receipt_date = TO_CHAR(CAST(:d AS date), 'YYYY-MM-DD')
"""


def fetch_jobwork(db: Session, day: date) -> dict:
    def _run(label, sql):
        try:
            return [dict(r._mapping) for r in db.execute(text(sql), {"d": day})]
        except Exception as exc:                               # noqa: BLE001
            logger.error("Job work %s fetch failed for %s: %s", label, day, exc)
            db.rollback()
            return []

    return {"out": _run("material out", JW_OUT_SQL), "in": _run("material in", JW_IN_SQL)}


def aggregate_jobwork(data: dict) -> dict:
    """Roll the line-level rows up to one row per challan / receipt.

    A challan is keyed line by line — one line per box on a cold dispatch, so a
    single day is 200 rows for two challans. Reporting those raw would fill the
    whole mail with one party's boxes; the challan is the unit anyone works with.
    """
    out_h: dict[object, dict] = {}
    in_h: dict[object, dict] = {}
    parties: set[str] = set()

    for r in data["out"]:
        c = out_h.get(r["id"])
        if c is None:
            party = (r["to_party"] or "").strip() or "(No party named)"
            parties.add(party)
            c = out_h[r["id"]] = {
                "challan_no": r["challan_no"] or "-",
                "site": canon_factory(r["from_warehouse"]),
                "party": party,
                "status": r["status"] or "-",
                "by": _who(r["created_by"]),
                "kg": 0.0, "boxes": 0, "lines": 0,
            }
        if r["item_description"] is None and r["quantity_kgs"] is None:
            continue                                   # header with no lines yet
        c["lines"] += 1
        c["kg"] += _f(r["quantity_kgs"])
        c["boxes"] += int(_f(r["quantity_boxes"]))

    for r in data["in"]:
        c = in_h.get(r["id"])
        if c is None:
            party = (r["to_party"] or "").strip() or "(No party named)"
            parties.add(party)
            c = in_h[r["id"]] = {
                "ir_number": r["ir_number"] or "-",
                "challan_no": r["challan_no"] or "-",
                "site": canon_factory(r["inward_warehouse"]),
                "party": party,
                "kind": (r["receipt_type"] or "-").title(),
                "by": _who(r["created_by"]),
                "sent_kg": 0.0, "fg_kg": 0.0, "waste_kg": 0.0, "rej_kg": 0.0,
                "boxes": 0, "lines": 0,
            }
        if r["item_description"] is None and r["finished_goods_kgs"] is None:
            continue
        c["lines"] += 1
        c["sent_kg"] += _f(r["sent_kgs"])
        c["fg_kg"] += _f(r["finished_goods_kgs"])
        c["waste_kg"] += _f(r["waste_kgs"])
        c["rej_kg"] += _f(r["rejection_kgs"])
        c["boxes"] += int(_f(r["finished_goods_boxes"]))

    return {
        "out_rows": sorted(out_h.values(), key=lambda x: -x["kg"]),
        "in_rows": sorted(in_h.values(), key=lambda x: -x["fg_kg"]),
        "out_challans": len(out_h),
        "in_receipts": len(in_h),
        "out_kg": sum(c["kg"] for c in out_h.values()),
        "out_boxes": sum(c["boxes"] for c in out_h.values()),
        "in_sent_kg": sum(c["sent_kg"] for c in in_h.values()),
        "in_fg_kg": sum(c["fg_kg"] for c in in_h.values()),
        "in_waste_kg": sum(c["waste_kg"] for c in in_h.values()),
        "in_rej_kg": sum(c["rej_kg"] for c in in_h.values()),
        "parties": sorted(parties),
        "empty": not (out_h or in_h),
    }


# ═════════════════════════════════════════════════════════════════════════
#  CUSTOMER RETURNS  (CR)
# ═════════════════════════════════════════════════════════════════════════
# The two companies keep separate tables, as everywhere else in IMS.
#
# MEASURE — the line's `net_weight` is the reported weight, not the sum of the
# box rows. Where a CR has both (56 of them), they agree except when boxing is
# still part-done: CR-20260719122215 declares 400 kg on its lines and has 80 of
# its boxes scanned at 155 kg. The declared return is what came back; the box
# total is how far the scanning has got. Reporting the box figure would show a
# return shrinking as a data-entry job that has nothing to do with the goods.
CR_HDR_SQL = """
SELECT h.id, h.rtv_id, h.factory_unit, h.customer, h.status, h.created_by,
       h.approved_by,
       h.rtv_date::date     AS raised_on,
       h.approved_at::date  AS approved_on,
       COALESCE(ln.qty, 0)    AS qty,
       COALESCE(ln.value, 0)  AS value,
       COALESCE(ln.net_kg, 0) AS net_kg
FROM {p}_rtv_header h
LEFT JOIN LATERAL (
    SELECT SUM(COALESCE(l.qty, 0)) AS qty,
           SUM(COALESCE(l.value, 0)) AS value,
           SUM(COALESCE(l.net_weight, 0)) AS net_kg
    FROM {p}_rtv_lines l WHERE l.header_id = h.id
) ln ON TRUE
WHERE h.rtv_date::date = :d OR h.approved_at::date = :d
"""

CR_CAT_SQL = """
SELECT l.item_category, l.sub_category, COUNT(*) AS lines,
       SUM(COALESCE(l.qty, 0)) AS qty, SUM(COALESCE(l.value, 0)) AS value,
       SUM(COALESCE(l.net_weight, 0)) AS net_kg
FROM {p}_rtv_header h
JOIN {p}_rtv_lines l ON l.header_id = h.id
WHERE h.rtv_date::date = :d OR h.approved_at::date = :d
GROUP BY 1, 2
"""


def fetch_cr(db: Session, day: date) -> dict:
    headers, cats = [], []
    for company, p in (("CFPL", "cfpl"), ("CDPL", "cdpl")):
        try:
            headers += [dict(r._mapping, company=company) for r in
                        db.execute(text(CR_HDR_SQL.format(p=p)), {"d": day})]
            cats += [dict(r._mapping, company=company) for r in
                     db.execute(text(CR_CAT_SQL.format(p=p)), {"d": day})]
        except Exception as exc:                               # noqa: BLE001
            logger.error("CR fetch failed for %s (%s): %s", day, company, exc)
            db.rollback()
    return {"headers": headers, "categories": cats}


def aggregate_cr(data: dict, day: date | None = None) -> dict:
    rows: list[dict] = []
    by_site = defaultdict(lambda: {"crs": 0, "kg": 0.0, "qty": 0.0, "value": 0.0})
    by_status = defaultdict(int)
    customers: set[str] = set()
    approved = 0

    for h in data["headers"]:
        site = canon_factory(h["factory_unit"])
        cust = (h["customer"] or "").strip() or "(No customer named)"
        customers.add(cust)
        kg, qty, val = _f(h["net_kg"]), _f(h["qty"]), _f(h["value"])
        rows.append({
            "cr_id": h["rtv_id"] or f"#{h['id']}",
            "company": h["company"], "site": site, "customer": cust,
            "status": h["status"] or "(blank)",
            "by": _who(h["created_by"]),
            "approver": _who(h["approved_by"]) if h["approved_by"] else "–",
            "kg": kg, "qty": qty, "value": val,
        })
        b = by_site[site]
        b["crs"] += 1; b["kg"] += kg; b["qty"] += qty; b["value"] += val
        by_status[h["status"] or "(blank)"] += 1
        approved += 1 if (day is not None and h["approved_on"] == day) else 0

    cats = defaultdict(lambda: {"lines": 0, "kg": 0.0, "qty": 0.0, "value": 0.0})
    for c in data["categories"]:
        k = (canon_group_name(c["item_category"]), canon_group_name(c["sub_category"]))
        v = cats[k]
        v["lines"] += int(_f(c["lines"]))
        v["kg"] += _f(c["net_kg"])
        v["qty"] += _f(c["qty"])
        v["value"] += _f(c["value"])

    return {
        "rows": sorted(rows, key=lambda r: -r["kg"]),
        "by_site": dict(by_site),
        "by_status": dict(by_status),
        "by_category": dict(cats),
        "customers": sorted(customers),
        "total_crs": len(rows),
        "total_kg": sum(r["kg"] for r in rows),
        "total_qty": sum(r["qty"] for r in rows),
        "total_value": sum(r["value"] for r in rows),
        "approved": approved,
        "empty": not rows,
    }


def fetch_and_aggregate(db: Session, day: date) -> dict:
    """All four extra sections in one call, so callers stay simple."""
    jc = aggregate_jobcards(fetch_jobcards(db, day), day)
    sm = aggregate_samples(fetch_samples(db, day))
    jw = aggregate_jobwork(fetch_jobwork(db, day))
    cr = aggregate_cr(fetch_cr(db, day), day)
    return {"jobcards": jc, "samples": sm, "jobwork": jw, "cr": cr}
