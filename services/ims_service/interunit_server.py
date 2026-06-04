from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.database import get_db
from services.ims_service.inward_models import Company
from services.ims_service.inward_tools import get_box_by_number, get_box_by_box_id
from services.ims_service.interunit_models import (
    RequestCreate,
    RequestUpdate,
    RequestWithLines,
    RequestResponse,
    RequestListResponse,
    WarehouseSiteResponse,
    DeleteResponse,
    TransferCreate,
    TransferWithLines,
    TransferListResponse,
    TransferDeleteResponse,
    TransferInCreate,
    TransferInDetail,
    TransferInListResponse,
    PendingTransferInCreate,
    PendingBoxAcknowledge,
    FinalizeTransferIn,
    TransferInEdit,
    CategorialSearchResponse,
    CategorialDropdownResponse,
)
from services.ims_service.interunit_tools import (
    get_warehouse_sites,
    create_request,
    list_requests,
    get_request,
    update_request,
    delete_request,
    create_transfer,
    update_transfer,
    list_transfers,
    get_transfer,
    delete_transfer,
    create_transfer_in,
    list_transfer_ins,
    get_transfer_in,
    create_pending_transfer_in,
    acknowledge_pending_box,
    unacknowledge_pending_box,
    acknowledge_pending_boxes_batch,
    finalize_transfer_in,
    get_pending_by_transfer_out,
    categorial_global_search,
    categorial_dropdown,
    get_bulk_entry_box,
    generate_transfer_in_qrs,
)
from services.ims_service.pending_stock_tools import (
    list_pending_transfers,
    pending_by_lot,
    in_transit_by_lot,
    backfill_pending_from_existing_transfers,
)

router = APIRouter(prefix="/interunit", tags=["interunit"])

AUTHORIZED_DELETE_EMAILS = {"yash@candorfoods.in", "b.hrithik@candorfoods.in"}
ADMIN_ROLES = {"admin", "developer"}


def _check_delete_permission(user_email: str, user_role: str = ""):
    if (user_email or "").lower() in AUTHORIZED_DELETE_EMAILS:
        return
    if (user_role or "").lower() in ADMIN_ROLES:
        return
    raise HTTPException(403, "You are not authorized to delete records")


@router.get("/dropdowns/warehouse-sites", response_model=List[WarehouseSiteResponse])
def get_warehouse_sites_endpoint(
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
):
    return get_warehouse_sites(active_only, db)


@router.get("/pending-stock")
def list_pending_stock_endpoint(
    from_site: Optional[str] = Query(None),
    to_site: Optional[str] = Query(None),
    company: Optional[str] = Query(None, description="cfpl or cdpl"),
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    search: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """List in-transit transfers grouped per transfer_out for the Pending modal."""
    return list_pending_transfers(
        db=db,
        from_site=from_site,
        to_site=to_site,
        company=company,
        from_date=from_date,
        to_date=to_date,
        search=search,
    )


@router.get("/pending-stock/by-lot")
def pending_stock_by_lot_endpoint(
    lot_no: Optional[str] = Query(None),
    item_description: Optional[str] = Query(None),
    from_site: Optional[str] = Query(None),
    from_company: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Sum pending cartons/kg by (lot_no, item_description, from_site)
    for inventory display deduction. Also returns box-level transactions."""
    return pending_by_lot(
        db=db,
        lot_no=lot_no,
        item_description=item_description,
        from_site=from_site,
        from_company=from_company,
    )


@router.get("/pending-stock/in-transit-by-lot")
def in_transit_by_lot_endpoint(
    company: Optional[str] = Query(None, description="cfpl or cdpl"),
    db: Session = Depends(get_db),
):
    """Batched map of lot_no -> in-transit {cartons, kg, box_count} for dashboard
    overlays. Display context only — these boxes are already removed from cold_stocks,
    so do NOT subtract this from displayed stock."""
    return in_transit_by_lot(db=db, company=company)


@router.post("/pending-stock/backfill")
def backfill_pending_stock_endpoint(
    user_email: str = Query(..., description="Email of user triggering backfill"),
    user_role: str = Query("", description="Role (admin/developer bypass allowlist)"),
    dry_run: bool = Query(False, description="Preview only — reconcile report, no DB writes"),
    db: Session = Depends(get_db),
):
    """Reconcile in-transit transfers into pending_transfer_stock: park any unparked
    boxes and top up each transfer to its ordered qty by matching shortfall stock
    BY LOT from the main sheet. Idempotent. Pass dry_run=true to preview without
    writing (returns a per-transfer reconcile report)."""
    _check_delete_permission(user_email, user_role)
    return backfill_pending_from_existing_transfers(db, dry_run=dry_run)


@router.get("/box-lookup/{company}")
def box_lookup_endpoint(
    company: Company,
    box_number: int = Query(..., description="Box number"),
    transaction_no: str = Query(..., description="Transaction number"),
    db: Session = Depends(get_db),
):
    """Look up a single box by box_number and transaction_no from company boxes table."""
    return get_box_by_number(company, box_number, transaction_no, db)


@router.get("/box-lookup-by-id/{company}")
def box_lookup_by_id_endpoint(
    company: Company,
    box_id: str = Query(..., description="Box ID (e.g. 34888867-2)"),
    transaction_no: str = Query(..., description="Transaction number (e.g. TR-20260224173440)"),
    db: Session = Depends(get_db),
):
    """Look up a single box by box_id and transaction_no from company boxes table."""
    return get_box_by_box_id(company, box_id, transaction_no, db)


@router.get("/bulk-entry-box-lookup/{company}")
def bulk_entry_box_lookup_endpoint(
    company: Company,
    box_id: str = Query(..., description="Box ID from QR code"),
    transaction_no: str = Query(..., description="Transaction number starting with BE-"),
    db: Session = Depends(get_db),
):
    """Look up a single box from bulk entry boxes table (for BE- prefix transactions)."""
    return get_bulk_entry_box(company, box_id, transaction_no, db)


@router.post("/requests", response_model=RequestWithLines, status_code=201)
def create_request_endpoint(
    request_data: RequestCreate,
    created_by: str = Query("system"),
    db: Session = Depends(get_db),
):
    return create_request(request_data, created_by, db)


@router.get("/requests", response_model=RequestListResponse)
def list_requests_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=1000),
    status: Optional[str] = Query(None),
    from_warehouse: Optional[str] = Query(None),
    to_warehouse: Optional[str] = Query(None),
    created_by: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    return list_requests(page, per_page, status, from_warehouse, to_warehouse, created_by, db)


@router.get("/requests/{request_id}", response_model=RequestWithLines)
def get_request_endpoint(
    request_id: int,
    db: Session = Depends(get_db),
):
    return get_request(request_id, db)


@router.put("/requests/{request_id}", response_model=RequestResponse)
def update_request_endpoint(
    request_id: int,
    update_data: RequestUpdate,
    db: Session = Depends(get_db),
):
    return update_request(request_id, update_data, db)


@router.delete("/requests/{request_id}", response_model=DeleteResponse)
def delete_request_endpoint(
    request_id: int,
    user_email: str = Query(..., description="Email of user performing delete"),
    db: Session = Depends(get_db),
):
    _check_delete_permission(user_email)
    return delete_request(request_id, db)


# ── Transfer endpoints (Phase B) ──


@router.post("/transfers", status_code=201)
def create_transfer_endpoint(
    transfer_data: TransferCreate,
    created_by: str = Query("system"),
    db: Session = Depends(get_db),
):
    return create_transfer(transfer_data, created_by, db)


@router.get("/transfers", response_model=TransferListResponse)
def list_transfers_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=1000),
    status: Optional[str] = Query(None),
    from_site: Optional[str] = Query(None),
    to_site: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    challan_no: Optional[str] = Query(None),
    sort_by: str = Query("created_ts"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
):
    return list_transfers(
        page, per_page, status, from_site, to_site,
        from_date, to_date, challan_no, sort_by, sort_order, db,
    )


@router.get("/transfers/{transfer_id}", response_model=TransferWithLines)
def get_transfer_endpoint(
    transfer_id: int,
    db: Session = Depends(get_db),
):
    return get_transfer(transfer_id, db)


@router.put("/transfers/{transfer_id}")
def update_transfer_endpoint(
    transfer_id: int,
    transfer_data: TransferCreate,
    db: Session = Depends(get_db),
):
    return update_transfer(transfer_id, transfer_data, db)


@router.delete("/transfers/{transfer_id}", response_model=TransferDeleteResponse)
def delete_transfer_endpoint(
    transfer_id: int,
    user_email: str = Query(..., description="Email of user performing delete"),
    user_role: str = Query("", description="Role of user (admin/developer bypass allowlist)"),
    db: Session = Depends(get_db),
):
    _check_delete_permission(user_email, user_role)
    return delete_transfer(transfer_id, db)


# ── Transfer IN endpoints (Phase C) ──


@router.post("/transfer-in", status_code=201)
def create_transfer_in_endpoint(
    transfer_in_data: TransferInCreate,
    db: Session = Depends(get_db),
):
    return create_transfer_in(transfer_in_data, db)


@router.get("/transfer-in", response_model=TransferInListResponse)
def list_transfer_ins_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=1000),
    receiving_warehouse: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    sort_by: str = Query("created_at"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
):
    return list_transfer_ins(
        page, per_page, receiving_warehouse,
        from_date, to_date, sort_by, sort_order, db,
    )


# ── Pending Transfer IN (real-time acknowledge) ──


@router.post("/transfer-in/pending", status_code=201)
def create_pending_transfer_in_endpoint(
    data: PendingTransferInCreate,
    db: Session = Depends(get_db),
):
    return create_pending_transfer_in(data, db)


@router.get("/transfer-in/pending/by-transfer-out/{transfer_out_id}")
def get_pending_by_transfer_out_endpoint(
    transfer_out_id: int,
    db: Session = Depends(get_db),
):
    return get_pending_by_transfer_out(transfer_out_id, db)


@router.post("/transfer-in/{header_id}/acknowledge")
def acknowledge_box_endpoint(
    header_id: int,
    data: PendingBoxAcknowledge,
    db: Session = Depends(get_db),
):
    return acknowledge_pending_box(header_id, data, db)


@router.post("/transfer-in/{header_id}/acknowledge-batch")
def acknowledge_batch_endpoint(
    header_id: int,
    boxes: List[PendingBoxAcknowledge],
    db: Session = Depends(get_db),
):
    return acknowledge_pending_boxes_batch(header_id, boxes, db)


@router.delete("/transfer-in/{header_id}/acknowledge/{box_id:path}")
def unacknowledge_box_endpoint(
    header_id: int,
    box_id: str,
    db: Session = Depends(get_db),
):
    return unacknowledge_pending_box(header_id, box_id, db)


@router.post("/transfer-in/{header_id}/finalize")
def finalize_transfer_in_endpoint(
    header_id: int,
    data: FinalizeTransferIn,
    db: Session = Depends(get_db),
):
    return finalize_transfer_in(header_id, data, db)


@router.post("/transfer-in/{header_id}/close-with-shortage")
def close_transfer_in_with_shortage_endpoint(
    header_id: int,
    closed_by: Optional[str] = Query(None),
    shortage_reason: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Close a Pending transfer-in that has a genuine shortage: write off the
    unreceived boxes and mark it Received with a shortage note."""
    from services.ims_service.interunit_tools import close_transfer_in_with_shortage
    return close_transfer_in_with_shortage(header_id, shortage_reason, closed_by, db)


@router.post("/transfer-in/{header_id}/generate-qrs")
def generate_transfer_in_qrs_endpoint(
    header_id: int,
    db: Session = Depends(get_db),
):
    return generate_transfer_in_qrs(header_id, db)


# ── Transfer IN detail/delete (must come after /pending and /{id}/acknowledge routes) ──


@router.get("/transfer-in/{transfer_in_id}/reconciliation")
def get_transfer_in_reconciliation(
    transfer_in_id: int,
    db: Session = Depends(get_db),
):
    """STBR audit report for one Transfer IN — all box-id reconciliations
    (matched / overridden / propagated / conflict) with original vs actual
    box IDs, source of scan, who scanned, when.

    Returns:
        {
          "transfer_in_id": int,
          "total":          int,
          "by_status":      { matched: N, overridden: N, ... },
          "rows":           [ {id, lot_no, transaction_no, original_box_id,
                               actual_box_id, reconciliation_status, scan_source,
                               scanned_by, scanned_at, propagated_from_id, ... } ]
        }
    """
    # Verify the table exists; self-heal init writes it on first STBR call.
    exists = db.execute(
        text("SELECT to_regclass('public.transfer_box_reconciliation')")
    ).scalar()
    if not exists:
        return {"transfer_in_id": transfer_in_id, "total": 0, "by_status": {}, "rows": []}

    rows = db.execute(
        text("""
            SELECT id, transfer_in_id, transfer_out_id, lot_no, transaction_no,
                   original_box_id, actual_box_id, reconciliation_status,
                   conflict_reason, scan_source, scanned_by, scanned_at,
                   from_company, to_company, from_site, to_site,
                   propagated_from_id
            FROM transfer_box_reconciliation
            WHERE transfer_in_id = :tid
            ORDER BY scanned_at ASC, id ASC
        """),
        {"tid": transfer_in_id},
    ).fetchall()

    by_status: dict = {}
    out_rows = []
    for r in rows:
        s = r.reconciliation_status or "unknown"
        by_status[s] = by_status.get(s, 0) + 1
        out_rows.append({
            "id": r.id,
            "transfer_in_id": r.transfer_in_id,
            "transfer_out_id": r.transfer_out_id,
            "lot_no": r.lot_no,
            "transaction_no": r.transaction_no,
            "original_box_id": r.original_box_id,
            "actual_box_id": r.actual_box_id,
            "reconciliation_status": r.reconciliation_status,
            "conflict_reason": r.conflict_reason,
            "scan_source": r.scan_source,
            "scanned_by": r.scanned_by,
            "scanned_at": r.scanned_at.isoformat() if r.scanned_at else None,
            "from_company": r.from_company,
            "to_company": r.to_company,
            "from_site": r.from_site,
            "to_site": r.to_site,
            "propagated_from_id": r.propagated_from_id,
        })

    return {
        "transfer_in_id": transfer_in_id,
        "total": len(out_rows),
        "by_status": by_status,
        "rows": out_rows,
    }


@router.get("/box-history/{box_id}")
def get_box_history(
    box_id: str,
    transaction_no: Optional[str] = Query(None, alias="txn"),
    db: Session = Depends(get_db),
):
    """Trace a single box across all four ledgers:

      1. cold_stocks (cfpl/cdpl) — is it currently sitting in cold storage?
      2. boxes_v2     (cfpl/cdpl) — is it currently in warehouse inventory?
      3. pending_transfer_stock   — is it parked in transit right now?
      4. cold_stock_disposition   — every "left source" event (Direct Out,
                                    Job Work, Transfer Out, fungible relabel).
      5. transfer_box_reconciliation — every STBR scan that touched this box.

    `txn` is optional but strongly recommended — a single box_id label may
    repeat across different inward batches, and txn pins us to one pool.

    Returns one consolidated audit dossier per (box_id, [txn]).
    """
    txn = (transaction_no or "").strip() or None

    def _scan_table(tbl_name: str) -> list:
        exists = db.execute(text("SELECT to_regclass(:t)"), {"t": tbl_name}).scalar()
        if not exists:
            return []
        where = "box_id = :b"
        params = {"b": box_id}
        if txn:
            where += " AND transaction_no = :t"
            params["t"] = txn
        try:
            rows = db.execute(
                text(f"SELECT * FROM {tbl_name} WHERE {where} ORDER BY id ASC"),
                params,
            ).mappings().all()
            return [dict(r) for r in rows]
        except Exception:
            return []

    cold_stocks = []
    for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
        for r in _scan_table(tbl):
            r["_table"] = tbl
            cold_stocks.append(r)

    warehouse_boxes = []
    for tbl in ("cfpl_boxes_v2", "cdpl_boxes_v2"):
        for r in _scan_table(tbl):
            r["_table"] = tbl
            warehouse_boxes.append(r)

    pending = _scan_table("pending_transfer_stock")

    # Disposition ledger — include reverted rows so we see history.
    dispositions = []
    exists = db.execute(text("SELECT to_regclass('public.cold_stock_disposition')")).scalar()
    if exists:
        where = "(box_id = :b OR (notes LIKE '%' || :b || '%'))"
        params = {"b": box_id}
        if txn:
            where = "box_id = :b AND transaction_no = :t"
            params["t"] = txn
        try:
            disp_rows = db.execute(
                text(f"""
                    SELECT id, box_id, transaction_no, lot_no, item_description,
                           from_company, unit, from_site, source_table,
                           disposition_type, disposition_ref_table,
                           disposition_ref_id, disposition_ref_no,
                           disposed_by, disposed_at, reverted, reverted_at,
                           reverted_reason, snapshot_data, notes
                    FROM cold_stock_disposition
                    WHERE {where}
                    ORDER BY disposed_at ASC, id ASC
                """),
                params,
            ).mappings().all()
            dispositions = [dict(r) for r in disp_rows]
        except Exception:
            dispositions = []

    # Reconciliation ledger — STBR audit
    reconciliations = []
    exists = db.execute(text("SELECT to_regclass('public.transfer_box_reconciliation')")).scalar()
    if exists:
        where_clauses = ["(original_box_id = :b OR actual_box_id = :b)"]
        params = {"b": box_id}
        if txn:
            where_clauses.append("transaction_no = :t")
            params["t"] = txn
        try:
            rec_rows = db.execute(
                text(f"""
                    SELECT id, transfer_in_id, transfer_out_id, lot_no, transaction_no,
                           original_box_id, actual_box_id, reconciliation_status,
                           conflict_reason, scan_source, scanned_by, scanned_at,
                           from_company, to_company, from_site, to_site,
                           propagated_from_id
                    FROM transfer_box_reconciliation
                    WHERE {' AND '.join(where_clauses)}
                    ORDER BY scanned_at ASC, id ASC
                """),
                params,
            ).mappings().all()
            reconciliations = [dict(r) for r in rec_rows]
        except Exception:
            reconciliations = []

    # Build a flat unified timeline (best-effort sort by timestamp).
    def _ts(row, *keys):
        for k in keys:
            v = row.get(k)
            if v:
                return v.isoformat() if hasattr(v, "isoformat") else str(v)
        return ""

    timeline = []
    for d in dispositions:
        timeline.append({
            "kind": "disposition",
            "when": _ts(d, "disposed_at"),
            "summary": f"{d.get('disposition_type')} → ref={d.get('disposition_ref_no')}"
                       + (" (REVERTED)" if d.get("reverted") else ""),
            "row_id": d.get("id"),
            "raw": d,
        })
    for r in reconciliations:
        timeline.append({
            "kind": "reconciliation",
            "when": _ts(r, "scanned_at"),
            "summary": f"{r.get('reconciliation_status')}: "
                       f"{r.get('original_box_id')} → {r.get('actual_box_id')}",
            "row_id": r.get("id"),
            "raw": r,
        })
    timeline.sort(key=lambda x: x["when"] or "")

    return {
        "box_id": box_id,
        "transaction_no": txn,
        "current_state": {
            "cold_stocks": cold_stocks,
            "warehouse_boxes": warehouse_boxes,
            "pending_transit": pending,
        },
        "dispositions": dispositions,
        "reconciliations": reconciliations,
        "timeline": timeline,
        "summary": {
            "in_cold_stocks": len(cold_stocks),
            "in_warehouse": len(warehouse_boxes),
            "in_transit": len(pending),
            "disposition_events": len(dispositions),
            "active_dispositions": sum(1 for d in dispositions if not d.get("reverted")),
            "reconciliation_events": len(reconciliations),
        },
    }


@router.get("/transfer-in/{transfer_in_id}", response_model=TransferInDetail)
def get_transfer_in_endpoint(
    transfer_in_id: int,
    db: Session = Depends(get_db),
):
    return get_transfer_in(transfer_in_id, db)


@router.delete("/transfer-in/{transfer_in_id}")
def delete_transfer_in_endpoint(
    transfer_in_id: int,
    user_email: str = Query(..., description="Email of the user performing the delete"),
    db: Session = Depends(get_db),
):
    from services.ims_service.interunit_tools import delete_transfer_in
    return delete_transfer_in(transfer_in_id, user_email, db)


@router.post("/transfer-in/reopen-by-transfer-out/{transfer_out_id}")
def reopen_transfer_in_endpoint(
    transfer_out_id: int,
    user_email: str = Query(..., description="Email of the user performing the re-open"),
    db: Session = Depends(get_db),
):
    from services.ims_service.interunit_tools import reopen_transfer_in
    return reopen_transfer_in(transfer_out_id, user_email, db)


@router.get("/transfer-in/by-transfer-out/{transfer_out_id}")
def get_transfer_in_by_transfer_out_endpoint(
    transfer_out_id: int,
    db: Session = Depends(get_db),
):
    """Fetch a transfer-in (header + boxes), any status, to pre-fill the edit form."""
    from services.ims_service.interunit_tools import get_transfer_in_by_transfer_out
    return get_transfer_in_by_transfer_out(transfer_out_id, db)


@router.put("/transfer-in/by-transfer-out/{transfer_out_id}/edit")
def edit_transfer_in_endpoint(
    transfer_out_id: int,
    data: TransferInEdit,
    user_email: str = Query(..., description="Email of the user performing the edit"),
    db: Session = Depends(get_db),
):
    from services.ims_service.interunit_tools import edit_transfer_in
    return edit_transfer_in(transfer_out_id, data, user_email, db)


# ── All SKU lookup endpoints ──


@router.get("/categorial-search", response_model=CategorialSearchResponse)
def categorial_search_endpoint(
    search: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Global search on all_sku.particulars for transfer/request article lookup."""
    return categorial_global_search(search, limit, offset, db)


@router.get("/categorial-dropdown", response_model=CategorialDropdownResponse)
def categorial_dropdown_endpoint(
    material_type: Optional[str] = Query(None),
    item_category: Optional[str] = Query(None),
    sub_category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Cascading dropdown on all_sku for transfer/request article lookup."""
    return categorial_dropdown(
        material_type, item_category, sub_category,
        search, limit, offset, db,
    )
