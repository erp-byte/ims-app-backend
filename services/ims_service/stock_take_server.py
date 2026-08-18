"""Manual controls for the stock take report.

The report sends itself at 7:00 PM IST with a 10:30 AM revision check. These
endpoints cover what automation cannot: seeing the mail before it goes out,
re-sending a date, and checking what was actually delivered.

Its own router/prefix, for the same reason the daily report has one — a shared
prefix with a path like `/{company}/{id}` would swallow these routes.
"""
from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from shared.database import get_db
from shared.logger import get_logger
from shared.timezone import now_ist
from services.ims_service.stock_take import build
from services.ims_service.stock_take_html import (
    render_email, render_page, render_plain,
)
from services.ims_service.stock_take_report import (
    LEDGER, REPORT_CC, REPORT_TO, send_report, view_url,
)

logger = get_logger("stock_take_server")

router = APIRouter(prefix="/stock-take", tags=["stock-take"])


def _parse_day(day: str | None) -> date:
    if not day:
        return now_ist().date()
    try:
        return date.fromisoformat(day)
    except ValueError:
        raise HTTPException(status_code=400, detail="day must be YYYY-MM-DD")


@router.get("/view", response_class=HTMLResponse)
def view(day: str | None = Query(default=None, description="YYYY-MM-DD, defaults to today"),
         db: Session = Depends(get_db)):
    """The full report as a page — every row, no mail size ceiling."""
    d = _parse_day(day)
    return HTMLResponse(render_page(d, build(db, d), now_ist()))


@router.get("/email-preview", response_class=HTMLResponse)
def email_preview(day: str | None = Query(default=None),
                  revised: bool = Query(default=False),
                  db: Session = Depends(get_db)):
    """Exactly what the mail body will look like, without sending anything."""
    d = _parse_day(day)
    return HTMLResponse(render_email(d, build(db, d), now_ist(), revised=revised,
                                     view_url=view_url(d)))


@router.get("/plain", response_class=HTMLResponse)
def plain_preview(day: str | None = Query(default=None), db: Session = Depends(get_db)):
    """The text alternative, as sent to clients that refuse HTML."""
    d = _parse_day(day)
    return HTMLResponse(f"<pre>{render_plain(d, build(db, d))}</pre>")


@router.get("/summary")
def summary(day: str | None = Query(default=None), db: Session = Depends(get_db)):
    """The day's figures as JSON — a quick sanity check on what was counted."""
    d = _parse_day(day)
    rep = build(db, d)
    agg, roster, out = rep["agg"], rep["roster"], rep["outstanding"]
    h = agg["head"]
    return {
        "day": str(d),
        "empty": agg["empty"],
        "fingerprint": rep["fingerprint"],
        "totals": {"entries": h["n"], "kg": round(h["kg"], 2),
                   "units": round(h["qty"], 2), "batches": len(agg["batches"]),
                   "skus": len(agg["sku"]), "floors": len(agg["floor"]),
                   "off_grade_kg": round(h["off_kg"], 2),
                   "test_rows_excluded": agg["test_rows"]},
        "warehouses": {w: {"entries": b["n"], "kg": round(b["kg"], 2),
                           "units": round(b["qty"], 2), "floors": len(b["floors"]),
                           "counters": len(b["users"])}
                       for w, b in agg["wh"].items()},
        "floors": [{"warehouse": w, "floor": f, "entries": b["n"],
                    "kg": round(b["kg"], 2), "units": round(b["qty"], 2)}
                   for (w, f), b in sorted(agg["floor"].items(),
                                           key=lambda kv: -kv[1]["kg"])],
        "item_groups": [{"group": g, "entries": b["n"], "kg": round(b["kg"], 2),
                         "units": round(b["qty"], 2), "skus": len(b["skus"])}
                        for g, b in sorted(agg["cat"].items(),
                                           key=lambda kv: -kv[1]["kg"])],
        "top_skus": [{"item": s["item"], "group": s["group"],
                      "kg": round(s["kg"], 2), "units": round(s["qty"], 2),
                      "lines": s["lines"], "share_pct": round(s["share"], 2)}
                     for s in rep["top_skus"]],
        "counted_by": [{"name": u["name"], "warehouse": u["warehouse"],
                        "entries": u["n"], "kg": round(u["kg"], 2),
                        "floors": u["floors"]} for u in roster["entered"]],
        "did_not_count": [{"name": u["name"], "warehouse": u["warehouse"],
                           "role": u["role"]} for u in roster["missing"]],
        "not_on_roster": [{"name": u["name"], "entries": u["n"]}
                          for u in roster["unrostered"]],
        "outstanding": {"drafts": out["drafts"], "unverified": out["unverified"],
                        "unchecked": out["unchecked"], "warehouses": out["rows"]},
    }


@router.post("/send")
def send_now(day: str | None = Query(default=None),
             revised: bool = Query(default=False,
                                   description="Label the mail as a revision")):
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
    LEDGER.ensure(db)
    since = now_ist().date() - timedelta(days=days)
    return {"since": str(since), "entries": LEDGER.recent(db, since)}
