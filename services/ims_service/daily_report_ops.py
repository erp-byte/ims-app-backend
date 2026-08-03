"""Jobcard and Sample/NPD sections of the daily report.

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


def fetch_and_aggregate(db: Session, day: date) -> dict:
    """Both extra sections in one call, so callers stay simple."""
    jc = aggregate_jobcards(fetch_jobcards(db, day), day)
    sm = aggregate_samples(fetch_samples(db, day))
    return {"jobcards": jc, "samples": sm}
