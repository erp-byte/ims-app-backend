"""Manual controls for the daily inward & transfer report.

The report normally sends itself (7:00 PM IST, plus a 10:30 AM revision check).
These endpoints exist for the cases automation cannot cover: previewing the PDF
before it goes out, re-sending a specific date, and checking what was actually
delivered.

Its own router/prefix on purpose — `/inward` already owns `/{company}/{txn}`,
which would swallow a path like `/inward/daily-report`.
"""
from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from io import BytesIO
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.database import get_db
from shared.logger import get_logger
from shared.timezone import now_ist
from services.ims_service.daily_report import (
    REPORT_CC,
    REPORT_TO,
    aggregate,
    build_pdf,
    ensure_log_table,
    fetch,
    send_report,
)
from services.ims_service.daily_report_ops import fetch_and_aggregate as ops_data
from services.ims_service.daily_report_html import render_email, render_page

logger = get_logger("daily_report_server")

router = APIRouter(prefix="/daily-report", tags=["daily-report"])


def _parse_day(day: str | None) -> date:
    if not day:
        return now_ist().date()
    try:
        return date.fromisoformat(day)
    except ValueError:
        raise HTTPException(status_code=400, detail="day must be YYYY-MM-DD")


@router.get("/preview")
def preview(day: str | None = Query(default=None, description="YYYY-MM-DD, defaults to today"),
            db: Session = Depends(get_db)):
    """Render the PDF for a date and return it inline. Sends no email."""
    d = _parse_day(day)
    agg = aggregate(fetch(db, d))
    pdf = build_pdf(d, agg, now_ist())
    return StreamingResponse(
        BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="Daily_Inward_Transfer_{d:%Y-%m-%d}.pdf"'},
    )


@router.get("/view", response_class=HTMLResponse)
def view(day: str | None = Query(default=None, description="YYYY-MM-DD, defaults to today"),
         db: Session = Depends(get_db)):
    """The interactive report: tabbed, filterable, paginated, mobile-friendly.

    This is what the mail links to. Mail clients strip JavaScript, so the real
    tab/pagination behaviour has to live on a page rather than in the message.
    """
    d = _parse_day(day)
    agg = aggregate(fetch(db, d))
    ops = ops_data(db, d)
    return HTMLResponse(render_page(d, agg, ops, now_ist()))


@router.get("/email-preview", response_class=HTMLResponse)
def email_preview(day: str | None = Query(default=None),
                  revised: bool = Query(default=False),
                  db: Session = Depends(get_db)):
    """Exactly what the mail body will look like, without sending anything."""
    d = _parse_day(day)
    agg = aggregate(fetch(db, d))
    ops = ops_data(db, d)
    from services.ims_service.daily_report import view_url
    return HTMLResponse(render_email(d, agg, ops, now_ist(),
                                     revised=revised, view_url=view_url(d)))


@router.get("/summary")
def summary(day: str | None = Query(default=None), db: Session = Depends(get_db)):
    """The report's headline figures as JSON — useful for a quick sanity check."""
    d = _parse_day(day)
    agg = aggregate(fetch(db, d))
    ops = ops_data(db, d)
    jc, sm = ops["jobcards"], ops["samples"]
    h = agg["head"]
    return {
        "day": str(d),
        "empty": agg["empty"] and jc["empty"] and sm["empty"],
        "jobcards": {"active": jc["total_cards"], "users": jc["total_users"],
                     "fg_items": jc["total_fg"], "planned_kg": round(jc["total_kg"], 3),
                     "status": jc["status"], "loss": jc["loss"]},
        "samples": {"requisitions": len(sm["requisitions"]), "actions": len(sm["actions"]),
                    "npd_jobcards": len(sm["npd_jobcards"]),
                    "customers": sm["customers"], "by_sale_group": sm["by_sale_group"],
                    "by_type": sm["by_type"], "by_status": sm["by_status"]},
        "inward": {"transactions": h["inw_txns"], "kg": round(h["inw_m"].kg, 3),
                   "qty": dict(h["inw_m"].qty), "value": round(h["inw_val"], 2)},
        "transfer_out": {"challans": h["out_chl"], "kg": round(h["out_m"].kg, 3),
                         "qty": dict(h["out_m"].qty)},
        "transfer_in": {"grns": h["in_grn"], "kg": round(h["in_m"].kg, 3),
                        "qty": dict(h["in_m"].qty)},
        "value_not_entered_lines": agg["val_gap"]["missing"],
        "total_lines": agg["val_gap"]["lines"],
        "warehouses": sorted(agg["wh"].keys()),
        "users": sorted(agg["usr"].keys()),
    }


@router.post("/send")
def send_now(day: str | None = Query(default=None),
             revised: bool = Query(default=False,
                                   description="Label the mail as a revision of an earlier send")):
    """Send the report for a date to the standing recipient list, right now."""
    d = _parse_day(day)
    result = send_report(d, kind="manual", revised=revised)
    if result.get("status") == "failed":
        raise HTTPException(status_code=502, detail=result.get("error", "send failed"))
    return {**result, "to": REPORT_TO, "cc": REPORT_CC}


@router.get("/log")
def delivery_log(days: int = Query(default=14, ge=1, le=90),
                 db: Session = Depends(get_db)):
    """Recent send attempts, so a missed day is visible rather than silent."""
    ensure_log_table(db)
    since = now_ist().date() - timedelta(days=days)
    rows = db.execute(text("""
        SELECT business_day, kind, status, error, sent_at
        FROM daily_report_log
        WHERE business_day >= :since
        ORDER BY business_day DESC, sent_at DESC
    """), {"since": since}).fetchall()
    return {
        "since": str(since),
        "entries": [
            {"business_day": str(r.business_day), "kind": r.kind, "status": r.status,
             "error": r.error, "sent_at": r.sent_at.isoformat() if r.sent_at else None}
            for r in rows
        ],
    }
