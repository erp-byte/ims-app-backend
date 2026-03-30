"""
Jobwork Dashboard API — Filter-based summary with KPIs, grouped aggregation,
drilldown to JWO detail, and lazy-loaded IR receipts.

Endpoints:
  GET /jobwork/dashboard/summary          — KPIs + grouped summary rows
  GET /jobwork/dashboard/filter-options   — dropdown options for filters
  GET /jobwork/dashboard/group-details    — expanded JWO rows within a group
  GET /jobwork/dashboard/jwo-receipts/{id}— IR receipts for a single JWO
  GET /jobwork/dashboard/export-excel     — Excel export of filtered data
"""

import math
from datetime import date, datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text, func, case, and_, or_, extract
from sqlalchemy.orm import Session
import io

from shared.database import get_db
from shared.logger import get_logger
from services.ims_service.jobwork_models import JobworkOrder, JobworkInwardReceipt

logger = get_logger("jobwork_dashboard")

router = APIRouter(
    prefix="/jobwork/dashboard",
    tags=["Jobwork Dashboard"],
)


def _f(v):
    """Round to 2 decimal places, default 0."""
    return round(float(v), 2) if v else 0.0


# ── Helpers: build filter conditions ─────────────────────────────

def _parse_csv(val: Optional[str]) -> List[str]:
    if not val:
        return []
    return [v.strip() for v in val.split(",") if v.strip()]


def _build_jwo_filters(
    company: str,
    date_from: Optional[str],
    date_to: Optional[str],
    months: Optional[str],
    vendors: Optional[str],
    items: Optional[str],
    process_types: Optional[str],
    jwo_statuses: Optional[str],
    loss_statuses: Optional[str],
):
    """Return a list of SQLAlchemy filter conditions for JobworkOrder."""
    conditions = [JobworkOrder.company == company.upper()]

    month_list = _parse_csv(months)
    if month_list:
        # months like "2026-01", "2026-02" — override date range
        month_conds = []
        for m in month_list:
            parts = m.split("-")
            if len(parts) == 2:
                yr, mo = int(parts[0]), int(parts[1])
                month_conds.append(
                    and_(
                        extract("year", JobworkOrder.dispatch_date) == yr,
                        extract("month", JobworkOrder.dispatch_date) == mo,
                    )
                )
        if month_conds:
            conditions.append(or_(*month_conds))
    else:
        if date_from:
            conditions.append(JobworkOrder.dispatch_date >= date_from)
        if date_to:
            conditions.append(JobworkOrder.dispatch_date <= date_to)

    vendor_list = _parse_csv(vendors)
    if vendor_list:
        conditions.append(JobworkOrder.vendor_name.in_(vendor_list))

    item_list = _parse_csv(items)
    if item_list:
        conditions.append(JobworkOrder.item_name.in_(item_list))

    pt_list = _parse_csv(process_types)
    if pt_list:
        conditions.append(JobworkOrder.process_type.in_(pt_list))

    status_list = _parse_csv(jwo_statuses)
    if status_list:
        conditions.append(JobworkOrder.jwo_status.in_(status_list))

    # loss_statuses filter: needs subquery on inward receipts
    ls_list = _parse_csv(loss_statuses)
    # We handle loss_statuses at the summary level via a joined subquery
    return conditions, ls_list


def _count_active_filters(
    date_from, date_to, months, vendors, items, process_types, jwo_statuses, loss_statuses
) -> int:
    count = 0
    if date_from or date_to:
        count += 1
    if months:
        count += 1
    if vendors:
        count += 1
    if items:
        count += 1
    if process_types:
        count += 1
    if jwo_statuses:
        count += 1
    if loss_statuses:
        count += 1
    return count


def _determine_group_by(
    explicit: Optional[str],
    months: Optional[str],
    vendors: Optional[str],
    items: Optional[str],
    process_types: Optional[str],
) -> str:
    """Auto-determine grouping based on dominant filter."""
    if explicit:
        return explicit
    if months:
        return "month"
    if vendors:
        return "vendor"
    if items:
        return "item"
    if process_types:
        return "process_type"
    return "vendor"  # default


def _group_column(group_by: str):
    """Return the SQLAlchemy column expression for grouping."""
    if group_by == "month":
        return func.to_char(JobworkOrder.dispatch_date, 'YYYY-MM')
    elif group_by == "vendor":
        return JobworkOrder.vendor_name
    elif group_by == "item":
        return JobworkOrder.item_name
    elif group_by == "process_type":
        return JobworkOrder.process_type
    elif group_by == "jwo_status":
        return JobworkOrder.jwo_status
    return JobworkOrder.vendor_name


# ── Endpoint: Summary ────────────────────────────────────────────

@router.get("/summary")
async def get_dashboard_summary(
    company: str = Query("cdpl"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    months: Optional[str] = Query(None),
    vendors: Optional[str] = Query(None),
    items: Optional[str] = Query(None),
    process_types: Optional[str] = Query(None),
    jwo_statuses: Optional[str] = Query(None),
    loss_statuses: Optional[str] = Query(None),
    group_by: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Return KPI cards + grouped summary table based on filters."""
    conditions, ls_list = _build_jwo_filters(
        company, date_from, date_to, months, vendors, items,
        process_types, jwo_statuses, loss_statuses,
    )

    effective_group_by = _determine_group_by(group_by, months, vendors, items, process_types)
    grp_col = _group_column(effective_group_by)

    today = date.today()
    overdue_threshold = 30

    try:
        # ── Subquery: aggregated IR data per JWO ──
        ir_sub = (
            db.query(
                JobworkInwardReceipt.jwo_order_id,
                func.sum(JobworkInwardReceipt.fg_qty_received).label("total_fg"),
                func.sum(JobworkInwardReceipt.waste_qty_received).label("total_waste"),
                func.sum(JobworkInwardReceipt.rejection_qty).label("total_rejection"),
                func.max(JobworkInwardReceipt.ir_date).label("last_ir_date"),
                func.max(
                    case(
                        (JobworkInwardReceipt.loss_status == "Excess Loss", 1),
                        else_=0,
                    )
                ).label("has_excess_loss"),
            )
            .group_by(JobworkInwardReceipt.jwo_order_id)
            .subquery("ir_agg")
        )

        # ── Base query: JWO joined with IR aggregates ──
        base = (
            db.query(
                grp_col.label("group_label"),
                func.count(JobworkOrder.id).label("num_jwos"),
                func.sum(JobworkOrder.qty_dispatched).label("total_dispatched"),
                func.coalesce(func.sum(ir_sub.c.total_fg), 0).label("total_fg"),
                func.coalesce(func.sum(ir_sub.c.total_waste), 0).label("total_waste"),
                func.coalesce(func.sum(ir_sub.c.total_rejection), 0).label("total_rejection"),
                func.sum(
                    JobworkOrder.qty_dispatched
                    - func.coalesce(ir_sub.c.total_fg, 0)
                    - func.coalesce(ir_sub.c.total_waste, 0)
                    - func.coalesce(ir_sub.c.total_rejection, 0)
                ).label("unaccounted_balance"),
                # Avg loss % = avg of per-JWO loss
                func.avg(
                    case(
                        (
                            JobworkOrder.qty_dispatched > 0,
                            (
                                JobworkOrder.qty_dispatched
                                - func.coalesce(ir_sub.c.total_fg, 0)
                                - func.coalesce(ir_sub.c.total_waste, 0)
                                - func.coalesce(ir_sub.c.total_rejection, 0)
                            ) / JobworkOrder.qty_dispatched * 100,
                        ),
                        else_=0,
                    )
                ).label("avg_loss_pct"),
                # Open JWOs
                func.sum(
                    case(
                        (JobworkOrder.jwo_status.in_(["Open", "Partially Received"]), 1),
                        else_=0,
                    )
                ).label("open_jwos"),
                # Overdue JWOs
                func.sum(
                    case(
                        (
                            and_(
                                JobworkOrder.jwo_status.in_(["Open", "Partially Received"]),
                                func.current_date() - JobworkOrder.dispatch_date > overdue_threshold,
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("overdue_jwos"),
                # Excess loss flags
                func.sum(func.coalesce(ir_sub.c.has_excess_loss, 0)).label("excess_loss_flags"),
                # Avg turnaround days (for closed JWOs with a final receipt)
                func.avg(
                    case(
                        (
                            and_(
                                JobworkOrder.jwo_status.in_(["Fully Received", "Reconciled", "Closed"]),
                                ir_sub.c.last_ir_date.isnot(None),
                            ),
                            func.extract("day", ir_sub.c.last_ir_date - JobworkOrder.dispatch_date),
                        ),
                        else_=None,
                    )
                ).label("avg_turnaround_days"),
            )
            .outerjoin(ir_sub, ir_sub.c.jwo_order_id == JobworkOrder.id)
            .filter(*conditions)
        )

        # Apply loss_status filter if present
        if ls_list:
            base = base.filter(ir_sub.c.has_excess_loss == 1) if "Excess Loss" in ls_list else base

        rows = base.group_by(grp_col).order_by(grp_col).all()

        # ── Build summary rows ──
        summary = []
        kpi_total_jwos = 0
        kpi_total_dispatched = 0.0
        kpi_total_fg = 0.0
        kpi_open_pending = 0
        kpi_excess_flags = 0
        loss_pcts = []

        for r in rows:
            row_dict = {
                "group_label": str(r.group_label) if r.group_label else "Unknown",
                "num_jwos": int(r.num_jwos or 0),
                "total_dispatched_kgs": _f(r.total_dispatched),
                "total_fg_received_kgs": _f(r.total_fg),
                "total_waste_received_kgs": _f(r.total_waste),
                "total_rejection_kgs": _f(r.total_rejection),
                "unaccounted_balance_kgs": _f(r.unaccounted_balance),
                "avg_loss_pct": _f(r.avg_loss_pct),
                "open_jwos": int(r.open_jwos or 0),
                "overdue_jwos": int(r.overdue_jwos or 0),
                "excess_loss_flags": int(r.excess_loss_flags or 0),
                "avg_turnaround_days": _f(r.avg_turnaround_days) if r.avg_turnaround_days else 0,
            }
            summary.append(row_dict)

            kpi_total_jwos += row_dict["num_jwos"]
            kpi_total_dispatched += row_dict["total_dispatched_kgs"]
            kpi_total_fg += row_dict["total_fg_received_kgs"]
            kpi_open_pending += row_dict["open_jwos"]
            kpi_excess_flags += row_dict["excess_loss_flags"]
            if row_dict["avg_loss_pct"] > 0:
                loss_pcts.append(row_dict["avg_loss_pct"])

        avg_loss = round(sum(loss_pcts) / len(loss_pcts), 2) if loss_pcts else 0.0

        filters_count = _count_active_filters(
            date_from, date_to, months, vendors, items,
            process_types, jwo_statuses, loss_statuses,
        )

        return {
            "kpis": {
                "total_jwos": kpi_total_jwos,
                "total_dispatched_kgs": _f(kpi_total_dispatched),
                "total_fg_received_kgs": _f(kpi_total_fg),
                "avg_loss_pct": avg_loss,
                "open_pending_jwos": kpi_open_pending,
                "excess_loss_flags": kpi_excess_flags,
            },
            "summary": summary,
            "group_by": effective_group_by,
            "as_of_date": today.isoformat(),
            "filters_applied": filters_count,
        }
    except Exception as exc:
        logger.error("Dashboard summary error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Failed to load dashboard: {str(exc)}")


# ── Endpoint: Filter Options ────────────────────────────────────

@router.get("/filter-options")
async def get_filter_options(
    company: str = Query("cdpl"),
    db: Session = Depends(get_db),
):
    """Return distinct dropdown values for the filter panel."""
    try:
        comp = company.upper()

        # Vendors with active JWO count
        vendor_rows = (
            db.query(
                JobworkOrder.vendor_name,
                func.count(JobworkOrder.id).label("cnt"),
            )
            .filter(
                JobworkOrder.company == comp,
                JobworkOrder.jwo_status.in_(["Open", "Partially Received"]),
            )
            .group_by(JobworkOrder.vendor_name)
            .order_by(JobworkOrder.vendor_name)
            .all()
        )
        vendors = [{"name": v.vendor_name, "active_jwo_count": v.cnt} for v in vendor_rows]

        # All vendors (including those without active JWOs)
        all_vendor_rows = (
            db.query(JobworkOrder.vendor_name)
            .filter(JobworkOrder.company == comp)
            .distinct()
            .order_by(JobworkOrder.vendor_name)
            .all()
        )
        all_vendor_names = {v.vendor_name for v in all_vendor_rows}
        active_vendor_names = {v["name"] for v in vendors}
        for name in all_vendor_names - active_vendor_names:
            vendors.append({"name": name, "active_jwo_count": 0})
        vendors.sort(key=lambda x: x["name"])

        # Items
        item_rows = (
            db.query(JobworkOrder.item_name)
            .filter(JobworkOrder.company == comp)
            .distinct()
            .order_by(JobworkOrder.item_name)
            .all()
        )
        items_list = [r.item_name for r in item_rows]

        # Process types
        pt_rows = (
            db.query(JobworkOrder.process_type)
            .filter(JobworkOrder.company == comp)
            .distinct()
            .order_by(JobworkOrder.process_type)
            .all()
        )
        process_types = [r.process_type for r in pt_rows]

        return {
            "vendors": vendors,
            "items": items_list,
            "process_types": process_types,
        }
    except Exception as exc:
        logger.error("Filter options error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Failed to load filter options: {str(exc)}")


# ── Endpoint: Group Details (expand a group row) ────────────────

@router.get("/group-details")
async def get_group_details(
    company: str = Query("cdpl"),
    group_by: str = Query("vendor"),
    group_label: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    months: Optional[str] = Query(None),
    vendors: Optional[str] = Query(None),
    items: Optional[str] = Query(None),
    process_types: Optional[str] = Query(None),
    jwo_statuses: Optional[str] = Query(None),
    loss_statuses: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Return individual JWO rows within a specific group."""
    conditions, ls_list = _build_jwo_filters(
        company, date_from, date_to, months, vendors, items,
        process_types, jwo_statuses, loss_statuses,
    )

    # Add group-specific filter
    grp_col = _group_column(group_by)
    if group_by == "month":
        conditions.append(func.to_char(JobworkOrder.dispatch_date, 'YYYY-MM') == group_label)
    elif group_by == "vendor":
        conditions.append(JobworkOrder.vendor_name == group_label)
    elif group_by == "item":
        conditions.append(JobworkOrder.item_name == group_label)
    elif group_by == "process_type":
        conditions.append(JobworkOrder.process_type == group_label)
    elif group_by == "jwo_status":
        conditions.append(JobworkOrder.jwo_status == group_label)

    try:
        ir_sub = (
            db.query(
                JobworkInwardReceipt.jwo_order_id,
                func.sum(JobworkInwardReceipt.fg_qty_received).label("total_fg"),
                func.sum(JobworkInwardReceipt.waste_qty_received).label("total_waste"),
                func.sum(JobworkInwardReceipt.rejection_qty).label("total_rejection"),
                func.max(JobworkInwardReceipt.ir_date).label("last_ir_date"),
                func.max(
                    case(
                        (JobworkInwardReceipt.loss_status == "Excess Loss", 1),
                        else_=0,
                    )
                ).label("has_excess_loss"),
                # Determine overall loss status
                func.max(JobworkInwardReceipt.loss_status).label("overall_loss_status"),
            )
            .group_by(JobworkInwardReceipt.jwo_order_id)
            .subquery("ir_agg")
        )

        rows = (
            db.query(
                JobworkOrder.id,
                JobworkOrder.jwo_id,
                JobworkOrder.dispatch_date,
                JobworkOrder.vendor_name,
                JobworkOrder.item_name,
                JobworkOrder.process_type,
                JobworkOrder.qty_dispatched,
                JobworkOrder.jwo_status,
                func.coalesce(ir_sub.c.total_fg, 0).label("fg_received"),
                func.coalesce(ir_sub.c.total_waste, 0).label("waste_received"),
                func.coalesce(ir_sub.c.total_rejection, 0).label("rejection"),
                (
                    JobworkOrder.qty_dispatched
                    - func.coalesce(ir_sub.c.total_fg, 0)
                    - func.coalesce(ir_sub.c.total_waste, 0)
                    - func.coalesce(ir_sub.c.total_rejection, 0)
                ).label("unaccounted_balance"),
                case(
                    (
                        JobworkOrder.qty_dispatched > 0,
                        (
                            JobworkOrder.qty_dispatched
                            - func.coalesce(ir_sub.c.total_fg, 0)
                            - func.coalesce(ir_sub.c.total_waste, 0)
                            - func.coalesce(ir_sub.c.total_rejection, 0)
                        ) / JobworkOrder.qty_dispatched * 100,
                    ),
                    else_=0,
                ).label("actual_loss_pct"),
                func.coalesce(ir_sub.c.overall_loss_status, "Pending").label("loss_status"),
                case(
                    (
                        and_(
                            ir_sub.c.last_ir_date.isnot(None),
                            JobworkOrder.jwo_status.in_(["Fully Received", "Reconciled", "Closed"]),
                        ),
                        func.extract("day", ir_sub.c.last_ir_date - JobworkOrder.dispatch_date),
                    ),
                    else_=None,
                ).label("turnaround_days"),
            )
            .outerjoin(ir_sub, ir_sub.c.jwo_order_id == JobworkOrder.id)
            .filter(*conditions)
            .order_by(JobworkOrder.dispatch_date.desc())
            .all()
        )

        return [
            {
                "id": r.id,
                "jwo_id": r.jwo_id,
                "dispatch_date": r.dispatch_date.isoformat() if r.dispatch_date else None,
                "vendor_name": r.vendor_name,
                "item_name": r.item_name,
                "process_type": r.process_type,
                "qty_dispatched": _f(r.qty_dispatched),
                "fg_received": _f(r.fg_received),
                "waste_received": _f(r.waste_received),
                "rejection": _f(r.rejection),
                "unaccounted_balance": _f(r.unaccounted_balance),
                "actual_loss_pct": _f(r.actual_loss_pct),
                "loss_status": r.loss_status,
                "jwo_status": r.jwo_status,
                "turnaround_days": int(r.turnaround_days) if r.turnaround_days else None,
            }
            for r in rows
        ]
    except Exception as exc:
        logger.error("Group details error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Failed to load group details: {str(exc)}")


# ── Endpoint: JWO Receipts (lazy-loaded IR expansion) ───────────

@router.get("/jwo-receipts/{jwo_id}")
async def get_jwo_receipts(
    jwo_id: int,
    company: str = Query("cdpl"),
    db: Session = Depends(get_db),
):
    """Return all Inward Receipts for a specific JWO (lazy-loaded on row expand)."""
    try:
        receipts = (
            db.query(JobworkInwardReceipt)
            .join(JobworkOrder, JobworkOrder.id == JobworkInwardReceipt.jwo_order_id)
            .filter(
                JobworkInwardReceipt.jwo_order_id == jwo_id,
                JobworkOrder.company == company.upper(),
            )
            .order_by(JobworkInwardReceipt.ir_date.asc())
            .all()
        )

        # Calculate running loss %
        jwo = db.query(JobworkOrder).filter(JobworkOrder.id == jwo_id).first()
        dispatched = jwo.qty_dispatched if jwo else 0

        result = []
        running_fg = 0.0
        running_waste = 0.0
        running_rejection = 0.0

        for ir in receipts:
            running_fg += ir.fg_qty_received or 0
            running_waste += ir.waste_qty_received or 0
            running_rejection += ir.rejection_qty or 0
            running_unaccounted = dispatched - running_fg - running_waste - running_rejection
            running_loss_pct = (running_unaccounted / dispatched * 100) if dispatched > 0 else 0

            result.append({
                "id": ir.id,
                "ir_number": ir.ir_number,
                "ir_date": ir.ir_date.isoformat() if ir.ir_date else None,
                "receipt_type": ir.receipt_type,
                "fg_qty_received": _f(ir.fg_qty_received),
                "waste_qty_received": _f(ir.waste_qty_received),
                "rejection_qty": _f(ir.rejection_qty),
                "actual_loss_pct": _f(ir.actual_loss_pct),
                "running_loss_pct": _f(running_loss_pct),
                "loss_status": ir.loss_status,
                "remarks": ir.remarks or "",
            })

        return result
    except Exception as exc:
        logger.error("JWO receipts error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Failed to load receipts: {str(exc)}")


# ── Endpoint: Export Excel ──────────────────────────────────────

@router.get("/export-excel")
async def export_excel(
    company: str = Query("cdpl"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    months: Optional[str] = Query(None),
    vendors: Optional[str] = Query(None),
    items: Optional[str] = Query(None),
    process_types: Optional[str] = Query(None),
    jwo_statuses: Optional[str] = Query(None),
    loss_statuses: Optional[str] = Query(None),
    group_by: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Export filtered dashboard data as .xlsx file."""
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

        # Get summary data (reuse summary logic)
        summary_resp = await get_dashboard_summary(
            company, date_from, date_to, months, vendors, items,
            process_types, jwo_statuses, loss_statuses, group_by, db,
        )

        wb = openpyxl.Workbook()

        # ── Sheet 1: Summary ──
        ws = wb.active
        ws.title = "Summary"

        header_font = Font(bold=True, size=12)
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        header_font_white = Font(bold=True, size=11, color="FFFFFF")
        thin_border = Border(
            left=Side(style="thin"), right=Side(style="thin"),
            top=Side(style="thin"), bottom=Side(style="thin"),
        )

        # Title
        ws.merge_cells("A1:L1")
        ws["A1"] = f"Jobwork Summary — {company.upper()} — As of {summary_resp['as_of_date']}"
        ws["A1"].font = Font(bold=True, size=14)

        # KPIs
        kpis = summary_resp["kpis"]
        kpi_labels = ["Total JWOs", "Total Dispatched (Kgs)", "Total FG Received (Kgs)",
                       "Avg Loss %", "Open/Pending JWOs", "Excess Loss Flags"]
        kpi_values = [kpis["total_jwos"], kpis["total_dispatched_kgs"], kpis["total_fg_received_kgs"],
                      kpis["avg_loss_pct"], kpis["open_pending_jwos"], kpis["excess_loss_flags"]]

        for i, (label, val) in enumerate(zip(kpi_labels, kpi_values)):
            ws.cell(row=3, column=i * 2 + 1, value=label).font = Font(bold=True)
            ws.cell(row=3, column=i * 2 + 2, value=val)

        # Summary table headers
        headers = [
            "Group", "No. of JWOs", "Total Dispatched (Kgs)", "Total FG Received (Kgs)",
            "Total Waste (Kgs)", "Total Rejection (Kgs)", "Unaccounted Balance (Kgs)",
            "Avg Loss %", "Open JWOs", "Overdue JWOs", "Excess Loss Flags", "Avg Turnaround Days",
        ]
        for col, h in enumerate(headers, 1):
            cell = ws.cell(row=5, column=col, value=h)
            cell.font = header_font_white
            cell.fill = header_fill
            cell.border = thin_border
            cell.alignment = Alignment(horizontal="center")

        for row_idx, row_data in enumerate(summary_resp["summary"], 6):
            vals = [
                row_data["group_label"], row_data["num_jwos"],
                row_data["total_dispatched_kgs"], row_data["total_fg_received_kgs"],
                row_data["total_waste_received_kgs"], row_data["total_rejection_kgs"],
                row_data["unaccounted_balance_kgs"], row_data["avg_loss_pct"],
                row_data["open_jwos"], row_data["overdue_jwos"],
                row_data["excess_loss_flags"], row_data["avg_turnaround_days"],
            ]
            for col, v in enumerate(vals, 1):
                cell = ws.cell(row=row_idx, column=col, value=v)
                cell.border = thin_border

        # Auto-fit column widths
        for col in ws.columns:
            max_length = 0
            col_letter = col[0].column_letter
            for cell in col:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            ws.column_dimensions[col_letter].width = min(max_length + 4, 30)

        # Save to bytes
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        today_str = date.today().strftime("%d%b%Y")
        filename = f"Jobwork_Summary_{company.upper()}_{today_str}.xlsx"

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except ImportError:
        raise HTTPException(500, "openpyxl is not installed on the server. Run: pip install openpyxl")
    except Exception as exc:
        logger.error("Export Excel error: %s", exc, exc_info=True)
        raise HTTPException(500, f"Export failed: {str(exc)}")
