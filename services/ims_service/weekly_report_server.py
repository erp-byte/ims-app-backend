"""Manual controls for the weekly roll-ups.

The four streams send themselves on Monday at 10:00 AM IST. These endpoints
cover previewing a week before it goes out, re-sending one, and checking what
was delivered.
"""
from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from shared.database import get_db
from shared.logger import get_logger
from shared.timezone import now_ist
from services.ims_service.weekly_report import (
    LEDGER, STREAMS, build_stream, internal_only, run_weekly_reports,
    send_stream, view_url, week_bounds, week_subject,
)
from services.ims_service.weekly_report_html import (
    render_email, render_page, render_plain,
)

logger = get_logger("weekly_report_server")

router = APIRouter(prefix="/weekly-report", tags=["weekly-report"])


def _parse_week(week: str | None) -> tuple[date, date]:
    """`week` is any date inside the wanted week; default is the last full one."""
    if not week:
        return week_bounds(now_ist().date())
    try:
        d = date.fromisoformat(week)
    except ValueError:
        raise HTTPException(status_code=400, detail="week must be YYYY-MM-DD")
    monday = d - timedelta(days=d.weekday())
    return monday, monday + timedelta(days=6)


def _check(stream: str) -> None:
    if stream not in STREAMS:
        raise HTTPException(
            status_code=404,
            detail=f"unknown stream {stream!r}; expected one of {sorted(STREAMS)}")


@router.get("/streams")
def list_streams():
    """Every weekly stream, its recipients, and what it summarises."""
    out = []
    for key, spec in STREAMS.items():
        to, dropped = internal_only(spec["recipients"]())
        out.append({"stream": key, "title": spec["title"],
                    "summarises": spec["source"], "recipients": to,
                    "dropped_external": dropped})
    start, end = week_bounds(now_ist().date())
    return {"schedule": "Monday 10:00 AM IST",
            "current_week": {"from": str(start), "to": str(end)},
            "streams": out}


@router.get("/view", response_class=HTMLResponse)
def view(stream: str = Query(..., description="daily | stock_take | cr | job_work"),
         week: str | None = Query(default=None, description="any date in the week"),
         db: Session = Depends(get_db)):
    """The full week as a page — every row, no mail size ceiling."""
    _check(stream)
    a, b = _parse_week(week)
    return HTMLResponse(render_page(build_stream(db, stream, a, b), now_ist()))


@router.get("/email-preview", response_class=HTMLResponse)
def email_preview(stream: str = Query(...), week: str | None = Query(default=None),
                  db: Session = Depends(get_db)):
    """Exactly what the mail body will look like, without sending anything."""
    _check(stream)
    a, b = _parse_week(week)
    return HTMLResponse(render_email(build_stream(db, stream, a, b), now_ist(),
                                     view_url=view_url(stream, a)))


@router.get("/plain", response_class=HTMLResponse)
def plain_preview(stream: str = Query(...), week: str | None = Query(default=None),
                  db: Session = Depends(get_db)):
    _check(stream)
    a, b = _parse_week(week)
    return HTMLResponse(
        f"<pre>{render_plain(build_stream(db, stream, a, b), now_ist())}</pre>")


@router.get("/summary")
def summary(stream: str = Query(...), week: str | None = Query(default=None),
            db: Session = Depends(get_db)):
    """The week's headline figures as JSON."""
    _check(stream)
    a, b = _parse_week(week)
    rep = build_stream(db, stream, a, b)
    to, dropped = internal_only(STREAMS[stream]["recipients"]())
    return {
        "stream": stream, "title": rep["title"],
        "week": {"from": str(a), "to": str(b)},
        "subject": week_subject(stream, a, b),
        "recipients": to, "dropped_external": dropped,
        "fingerprint": rep["fingerprint"],
        "headline": [{"label": l, "value": v, "detail": s} for l, v, s in rep["tiles"]],
        "flags": [t for t, _ in rep["flags"]],
        "tables": [{"title": t["title"], "rows": len(t["rows"])}
                   for t in rep["tables"]],
    }


@router.post("/send")
def send_now(stream: str = Query(..., description="one stream, or 'all'"),
             week: str | None = Query(default=None)):
    """Send a weekly roll-up to its standing recipients, right now."""
    a, b = _parse_week(week)
    if stream == "all":
        return {"week": f"{a} to {b}", "results": run_weekly_reports(a + timedelta(days=7))}
    _check(stream)
    result = send_stream(stream, a, b, kind="manual")
    if result.get("status") == "failed":
        raise HTTPException(status_code=502, detail=result.get("error", "send failed"))
    return result


@router.get("/log")
def delivery_log(weeks: int = Query(default=8, ge=1, le=52),
                 db: Session = Depends(get_db)):
    """Recent weekly sends, so a skipped Monday is visible rather than silent."""
    LEDGER.ensure(db)
    since = now_ist().date() - timedelta(weeks=weeks)
    return {"since": str(since), "entries": LEDGER.recent(db, since)}
