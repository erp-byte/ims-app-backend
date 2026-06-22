"""Cold Transfer-In dedicated endpoints.

POST /interunit/cold-transfer-in/create
POST /interunit/cold-transfer-in/{header_id}/finalize
GET  /interunit/cold-transfer-in/{header_id}

These endpoints write ONLY to cold_transfer_in_headers, cold_transfer_inboxes,
and <company>_cold_stocks (with pending_transfer_stock cleanup). They DO NOT
write to interunit_transfer_in_header / interunit_transfer_in_boxes.

Cold destinations: Savla D-39, Savla D-514, Rishi, Supreme.
Company table routing:
  Savla D-39, Savla D-514 -> cfpl_cold_stocks
  Rishi, Supreme          -> cdpl_cold_stocks
"""
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple

from fastapi import HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session


# ── Cold-destination routing ────────────────────────────────────────────
COLD_DESTINATIONS = {"savla d-39", "savla d-514", "rishi", "supreme", "eskimo"}
CFPL_COLD_DESTS = {"savla d-39", "savla d-514"}
CDPL_COLD_DESTS = {"rishi", "supreme", "eskimo"}


def _is_cold_destination(name: Optional[str]) -> bool:
    return bool(name) and name.strip().lower() in COLD_DESTINATIONS


def _cold_stocks_table(dest: str) -> str:
    d = dest.strip().lower()
    if d in CFPL_COLD_DESTS:
        return "cfpl_cold_stocks"
    if d in CDPL_COLD_DESTS:
        return "cdpl_cold_stocks"
    raise HTTPException(status_code=400, detail=f"Destination '{dest}' is not a cold warehouse")


# ── Request / response models ───────────────────────────────────────────
class ColdTransferInBoxInput(BaseModel):
    box_id: str
    transaction_no: str
    lot_no: Optional[str] = None
    item_description: Optional[str] = None
    weight_kg: Optional[float] = None
    no_of_cartons: Optional[float] = 1
    unit: Optional[str] = None
    # Optional override fields; usually populated from pending_transfer_stock.cold_storage_data
    cold_storage_data: Optional[Dict[str, Any]] = None


# Header-level cold-storage detail fields (from the receive form's "Cold Storage Details").
class _ColdHeaderDetails(BaseModel):
    item_description: Optional[str] = None
    inward_dt: Optional[str] = None
    vakkal: Optional[str] = None
    lot_no: Optional[str] = None
    item_mark: Optional[str] = None
    group_name: Optional[str] = None
    item_subgroup: Optional[str] = None
    storage_location: Optional[str] = None
    exporter: Optional[str] = None
    rate: Optional[float] = None
    value: Optional[float] = None
    spl_remarks: Optional[str] = None


class ColdTransferInCreate(_ColdHeaderDetails):
    transfer_out_id: int
    transfer_out_no: str
    from_site: str
    to_site: str
    received_by: str
    grn_number: Optional[str] = None
    grn_date: Optional[datetime] = None
    inward_transaction_no: Optional[str] = None
    box_condition: Optional[str] = "Good"
    condition_remarks: Optional[str] = None
    to_company: Optional[str] = None
    boxes: List[ColdTransferInBoxInput] = Field(default_factory=list)


class ColdTransferInFinalize(_ColdHeaderDetails):
    to_company: Optional[str] = None
    boxes: List[ColdTransferInBoxInput] = Field(default_factory=list)


class ColdTransferInBoxOut(BaseModel):
    id: int
    box_id: Optional[str]
    transaction_no: Optional[str]
    lot_no: Optional[str]
    item_description: Optional[str]
    weight_kg: Optional[float]
    no_of_cartons: Optional[float]
    unit: Optional[str]


class ColdTransferInDetail(BaseModel):
    id: int
    transfer_out_id: Optional[int]
    transfer_out_no: Optional[str]
    grn_number: Optional[str]
    grn_date: Optional[datetime]
    from_site: Optional[str]
    to_site: Optional[str]
    received_by: Optional[str]
    received_at: Optional[datetime]
    status: Optional[str]
    box_condition: Optional[str]
    condition_remarks: Optional[str]
    to_company: Optional[str]
    boxes: List[ColdTransferInBoxOut]


class ColdTransferInCreateResponse(BaseModel):
    header_id: int
    boxes_inserted: int
    status: str
    out_status: str


# ── Handlers ────────────────────────────────────────────────────────────
def create_cold_transfer_in(db: Session, data: ColdTransferInCreate) -> ColdTransferInCreateResponse:
    """Create a new cold transfer-in header + boxes + cold_stocks rows.

    All writes happen inside a single transaction. ROLLBACK on any error.
    """
    if not _is_cold_destination(data.to_site):
        raise HTTPException(
            status_code=400,
            detail=f"Destination '{data.to_site}' is not a cold warehouse",
        )
    if not data.boxes:
        raise HTTPException(status_code=400, detail="At least one box required")

    cold_stocks_table = _cold_stocks_table(data.to_site)

    # Verify transfer_out_id exists.
    out_row = db.execute(text(
        "SELECT id, from_site, to_site, status FROM interunit_transfers_header WHERE id = :oid"
    ), {"oid": data.transfer_out_id}).fetchone()
    if not out_row:
        raise HTTPException(
            status_code=404,
            detail=f"transfer_out_id {data.transfer_out_id} not found",
        )

    # INSERT header (sequence-allocated id).
    header_id = db.execute(text("""
        INSERT INTO cold_transfer_in_headers
            (from_site, to_site, transfer_out_id, transfer_out_no, grn_number, grn_date,
             inward_transaction_no, received_by, received_at, box_condition, condition_remarks,
             status, created_at, updated_at, to_company,
             item_description, inward_dt, vakkal, lot_no, item_mark, group_name, item_subgroup,
             storage_location, exporter, rate, value, spl_remarks)
        VALUES
            (:from_site, :to_site, :transfer_out_id, :transfer_out_no, :grn_number,
             COALESCE(:grn_date, NOW()), :inward_transaction_no, :received_by, NOW(),
             COALESCE(:box_condition, 'Good'), :condition_remarks, 'Pending',
             NOW(), NOW(), :to_company,
             :item_description, :inward_dt, :vakkal, :lot_no, :item_mark, :group_name, :item_subgroup,
             :storage_location, :exporter, :rate, :value, :spl_remarks)
        RETURNING id
    """), {
        "from_site": data.from_site,
        "to_site": data.to_site,
        "transfer_out_id": data.transfer_out_id,
        "transfer_out_no": data.transfer_out_no,
        "grn_number": data.grn_number,
        "grn_date": data.grn_date,
        "inward_transaction_no": data.inward_transaction_no,
        "received_by": data.received_by,
        "box_condition": data.box_condition,
        "condition_remarks": data.condition_remarks,
        "to_company": data.to_company,
        "item_description": data.item_description,
        "inward_dt": data.inward_dt,
        "vakkal": data.vakkal,
        "lot_no": data.lot_no,
        "item_mark": data.item_mark,
        "group_name": data.group_name,
        "item_subgroup": data.item_subgroup,
        "storage_location": data.storage_location,
        "exporter": data.exporter,
        "rate": data.rate,
        "value": data.value,
        "spl_remarks": data.spl_remarks,
    }).scalar()

    boxes_inserted = _process_box_loop(
        db, header_id, data.boxes, cold_stocks_table,
        transfer_out_id=data.transfer_out_id,
    )

    # Reconcile statuses.
    out_status, in_status = _reconcile_statuses(db, data.transfer_out_id, header_id)

    db.commit()
    return ColdTransferInCreateResponse(
        header_id=header_id,
        boxes_inserted=boxes_inserted,
        status=in_status,
        out_status=out_status,
    )


def finalize_cold_transfer_in(
    db: Session, header_id: int, data: ColdTransferInFinalize
) -> ColdTransferInCreateResponse:
    """Append boxes to a cold transfer-in and fill the header-level cold-storage details.

    If no cold_transfer_in_headers row exists for header_id yet (the receive used the
    interunit pending header for acknowledge/STBR), CREATE it here with id = header_id,
    sourced from the interunit pending header + its transfer-out. Idempotent on resume:
    an existing cold header is UPDATEd (kept values when the new submit omits a field).
    """
    if not data.boxes:
        raise HTTPException(status_code=400, detail="At least one box required")

    cold_details = {
        "item_description": data.item_description,
        "inward_dt": data.inward_dt,
        "vakkal": data.vakkal,
        "lot_no": data.lot_no,
        "item_mark": data.item_mark,
        "group_name": data.group_name,
        "item_subgroup": data.item_subgroup,
        "storage_location": data.storage_location,
        "exporter": data.exporter,
        "rate": data.rate,
        "value": data.value,
        "spl_remarks": data.spl_remarks,
    }

    hdr = db.execute(text(
        "SELECT id, transfer_out_id, to_site FROM cold_transfer_in_headers WHERE id = :hid"
    ), {"hid": header_id}).fetchone()

    if hdr:
        transfer_out_id = hdr._mapping["transfer_out_id"]
        to_site = hdr._mapping["to_site"]
        # Keep existing values when the new submit omits a field (resume-safe).
        db.execute(text("""
            UPDATE cold_transfer_in_headers SET
                item_description = COALESCE(:item_description, item_description),
                inward_dt        = COALESCE(:inward_dt, inward_dt),
                vakkal           = COALESCE(:vakkal, vakkal),
                lot_no           = COALESCE(:lot_no, lot_no),
                item_mark        = COALESCE(:item_mark, item_mark),
                group_name       = COALESCE(:group_name, group_name),
                item_subgroup    = COALESCE(:item_subgroup, item_subgroup),
                storage_location = COALESCE(:storage_location, storage_location),
                exporter         = COALESCE(:exporter, exporter),
                rate             = COALESCE(:rate, rate),
                value            = COALESCE(:value, value),
                spl_remarks      = COALESCE(:spl_remarks, spl_remarks),
                updated_at       = NOW()
            WHERE id = :hid
        """), {**cold_details, "hid": header_id})
    else:
        # No cold header yet — build it from the interunit pending header + transfer-out.
        in_hdr = db.execute(text("""
            SELECT transfer_out_id, transfer_out_no, grn_number, grn_date, receiving_warehouse,
                   received_by, box_condition, condition_remarks, inward_transaction_no
            FROM interunit_transfer_in_header WHERE id = :hid
        """), {"hid": header_id}).fetchone()
        if not in_hdr:
            raise HTTPException(
                status_code=404,
                detail=f"No cold_transfer_in_headers or interunit_transfer_in_header id {header_id}",
            )
        transfer_out_id = in_hdr._mapping["transfer_out_id"]
        out_hdr = db.execute(text(
            "SELECT from_site, to_site FROM interunit_transfers_header WHERE id = :oid"
        ), {"oid": transfer_out_id}).fetchone()
        to_site = in_hdr._mapping["receiving_warehouse"] or (
            out_hdr._mapping["to_site"] if out_hdr else None
        )
        if not _is_cold_destination(to_site):
            raise HTTPException(
                status_code=400, detail=f"Destination '{to_site}' is not a cold warehouse"
            )
        to_company = data.to_company or (
            "cfpl" if to_site.strip().lower() in CFPL_COLD_DESTS else "cdpl"
        )
        from_site = out_hdr._mapping["from_site"] if out_hdr else None

        db.execute(text("""
            INSERT INTO cold_transfer_in_headers
                (id, from_site, to_site, transfer_out_id, transfer_out_no, grn_number, grn_date,
                 inward_transaction_no, received_by, received_at, box_condition, condition_remarks,
                 status, created_at, updated_at, to_company,
                 item_description, inward_dt, vakkal, lot_no, item_mark, group_name, item_subgroup,
                 storage_location, exporter, rate, value, spl_remarks)
            VALUES
                (:id, :from_site, :to_site, :transfer_out_id, :transfer_out_no, :grn_number,
                 COALESCE(:grn_date, NOW()), :inward_transaction_no, :received_by, NOW(),
                 COALESCE(:box_condition, 'Good'), :condition_remarks, 'Pending',
                 NOW(), NOW(), :to_company,
                 :item_description, :inward_dt, :vakkal, :lot_no, :item_mark, :group_name, :item_subgroup,
                 :storage_location, :exporter, :rate, :value, :spl_remarks)
            ON CONFLICT (id) DO NOTHING
        """), {
            "id": header_id,
            "from_site": from_site,
            "to_site": to_site,
            "transfer_out_id": transfer_out_id,
            "transfer_out_no": in_hdr._mapping["transfer_out_no"],
            "grn_number": in_hdr._mapping["grn_number"],
            "grn_date": in_hdr._mapping["grn_date"],
            "inward_transaction_no": in_hdr._mapping["inward_transaction_no"],
            "received_by": in_hdr._mapping["received_by"],
            "box_condition": in_hdr._mapping["box_condition"],
            "condition_remarks": in_hdr._mapping["condition_remarks"],
            "to_company": to_company,
            **cold_details,
        })
        # Keep the sequence ahead of explicitly-inserted ids to avoid future PK collisions.
        db.execute(text(
            "SELECT setval(pg_get_serial_sequence('cold_transfer_in_headers','id'), "
            "(SELECT GREATEST(MAX(id), :hid) FROM cold_transfer_in_headers))"
        ), {"hid": header_id})

    cold_stocks_table = _cold_stocks_table(to_site)
    boxes_inserted = _process_box_loop(
        db, header_id, data.boxes, cold_stocks_table,
        transfer_out_id=transfer_out_id,
    )
    out_status, in_status = _reconcile_statuses(db, transfer_out_id, header_id)

    # Once the cold receipt is fully received, the cold_transfer_in_headers row
    # (created above with id = header_id) is the system of record. Purge the
    # interunit staging header + its acknowledged boxes that the scan/acknowledge
    # phase created under the same id — otherwise the completed cold receipt also
    # lingers in interunit_transfer_in_header, polluting the interunit Transfer-IN
    # views and tripping stuck-receipt checks (mirrors the purge in
    # delete_cold_transfer_in). Gate on 'Received': while still 'Pending' (boxes in
    # transit, multi-session receive) the staging MUST stay so the next session
    # resumes on the same id instead of spawning a duplicate cold header. Guard on
    # transfer_out_id so a standalone cold header (create path, cold-sequence id)
    # can't wipe an unrelated interunit receipt that happens to share the id.
    if in_status == "Received":
        _staging = db.execute(text(
            "SELECT id FROM interunit_transfer_in_header WHERE id = :hid AND transfer_out_id = :oid"
        ), {"hid": header_id, "oid": transfer_out_id}).fetchone()
        if _staging is not None:
            db.execute(text(
                "DELETE FROM interunit_transfer_in_boxes WHERE header_id = :hid"
            ), {"hid": header_id})
            db.execute(text(
                "DELETE FROM interunit_transfer_in_header WHERE id = :hid"
            ), {"hid": header_id})

    db.commit()
    return ColdTransferInCreateResponse(
        header_id=header_id,
        boxes_inserted=boxes_inserted,
        status=in_status,
        out_status=out_status,
    )


def get_cold_transfer_in_by_id(db: Session, header_id: int) -> Dict[str, Any]:
    """Full detail for the cold Transfer-In view page: EVERY column from
    cold_transfer_in_headers + cold_transfer_inboxes, plus frontend-friendly aliases
    (from_warehouse/receiving_warehouse on the header; article/lot_number/net_weight on
    each box). Returned as a plain dict so the view sees all cold-storage detail fields
    (vakkal, item_mark, group/subgroup, storage_location, exporter, rate, value, …)."""
    hdr = db.execute(text("""
        SELECT id, transfer_out_id, transfer_out_no, grn_number, grn_date,
               from_site, to_site, received_by, received_at, status, box_condition,
               condition_remarks, to_company, inward_transaction_no, created_at, updated_at,
               item_description, inward_dt, vakkal, lot_no, item_mark, group_name,
               item_subgroup, storage_location, exporter, rate, value, spl_remarks
        FROM cold_transfer_in_headers WHERE id = :hid
    """), {"hid": header_id}).fetchone()
    if not hdr:
        raise HTTPException(
            status_code=404,
            detail=f"cold_transfer_in_headers id {header_id} not found",
        )

    box_rows = db.execute(text("""
        SELECT id, box_id, transaction_no, lot_no, item_description, weight_kg,
               no_of_cartons, unit, inward_dt, vakkal, item_mark, group_name,
               item_subgroup, storage_location, exporter, rate, value, spl_remarks, created_at
        FROM cold_transfer_inboxes WHERE header_id = :hid ORDER BY id
    """), {"hid": header_id}).fetchall()

    result = dict(hdr._mapping)
    # Aliases the view page reads (mirrors the interunit transfer-in shape).
    result["from_warehouse"] = result.get("from_site")
    result["receiving_warehouse"] = result.get("to_site")

    boxes = []
    for r in box_rows:
        b = dict(r._mapping)
        b["article"] = b.get("item_description")   # grouping key on the view page
        b["lot_number"] = b.get("lot_no")
        b["net_weight"] = b.get("weight_kg")       # totals on the view page
        boxes.append(b)
    result["boxes"] = boxes
    return result


def delete_cold_transfer_in(db: Session, header_id: int, user_email: str) -> Dict[str, Any]:
    """Delete a cold transfer-IN receipt and undo its effects.

    Mirrors the interunit `delete_transfer_in` semantics so cold receipts can be
    removed without leaking rows or losing stock:
      1. Reverse each box from the routed <company>_cold_stocks table.
      2. If the transfer-out still exists → re-park the box to pending_transfer_stock
         ('In Transit') and revert the transfer-out to 'Dispatch' so it can be
         re-received. If the transfer-out is already gone (pure orphan, e.g. the OUT
         was deleted earlier), skip re-park — just purge.
      3. Delete cold_transfer_inboxes + cold_transfer_in_headers.

    This is the ONLY correct way to delete a cold receipt — the legacy
    /interunit/transfer-in delete never touches the cold tables, which is what left
    orphaned cold headers showing in the cold Transfer-In records.
    """
    import json as _json
    from services.ims_service.pending_stock_tools import (
        _cold_row_to_json, _table_exists, _is_cold_site,
    )
    from services.ims_service.interunit_tools import TRANSFER_IN_DELETE_ALLOWED_EMAILS

    if user_email not in TRANSFER_IN_DELETE_ALLOWED_EMAILS:
        raise HTTPException(403, "You are not authorized to delete transfer-in records.")

    hdr = db.execute(text("""
        SELECT id, transfer_out_id, transfer_out_no, grn_number, from_site, to_site, to_company
        FROM cold_transfer_in_headers WHERE id = :hid
    """), {"hid": header_id}).fetchone()
    if not hdr:
        raise HTTPException(404, "Cold transfer-in not found")

    transfer_out_id = hdr._mapping["transfer_out_id"]
    to_site = hdr._mapping["to_site"]
    grn_number = hdr._mapping["grn_number"]

    # Routed cold-stocks table (cfpl/cdpl). Unknown/non-cold dest → skip stock reversal
    # but still purge the header + inboxes.
    try:
        cold_table = _cold_stocks_table(to_site)
    except HTTPException:
        cold_table = None
    to_company = (hdr._mapping["to_company"]
                  or ("cdpl" if (cold_table or "").startswith("cdpl") else "cfpl"))

    # Is the transfer-out still around? Controls re-park + status revert.
    out = None
    if transfer_out_id:
        out = db.execute(text(
            "SELECT id, challan_no, from_site, to_site, created_by "
            "FROM interunit_transfers_header WHERE id = :oid"
        ), {"oid": transfer_out_id}).fetchone()

    from_site = (out._mapping["from_site"] if out else None) or hdr._mapping["from_site"]
    from_storage_type = "cold" if (from_site and _is_cold_site(from_site)) else "warehouse"
    source_table_guess = (
        (f"{to_company}_cold_stocks") if from_storage_type == "cold"
        else f"{to_company}_bulk_entry_boxes"
    )
    destination_table_keep = cold_table or f"{to_company}_cold_stocks"

    inboxes = db.execute(text("""
        SELECT box_id, transaction_no, lot_no, item_description, weight_kg, no_of_cartons
        FROM cold_transfer_inboxes WHERE header_id = :hid
    """), {"hid": header_id}).fetchall()

    reversed_n = 0
    reparked_n = 0
    now = datetime.now()
    for b in inboxes:
        box_id = b._mapping["box_id"]
        tno = b._mapping["transaction_no"]
        if not box_id or not tno:
            continue

        cold_data = None
        if cold_table and _table_exists(db, cold_table):
            row = db.execute(text(
                f"SELECT * FROM {cold_table} WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"
            ), {"bid": box_id, "tno": tno}).fetchone()
            if row is not None:
                cold_data = _cold_row_to_json(row)
                db.execute(text(
                    f"DELETE FROM {cold_table} WHERE box_id = :bid AND transaction_no = :tno"
                ), {"bid": box_id, "tno": tno})
                reversed_n += 1

        # Re-park only when the transfer-out is still live (otherwise nothing to receive into).
        if out is not None:
            db.execute(text("""
                INSERT INTO pending_transfer_stock
                    (transfer_type, transfer_out_id, transfer_out_challan_no,
                     box_id, transaction_no,
                     from_company, to_company, from_site, to_site,
                     from_storage_type, to_storage_type,
                     source_table, source_row_id, destination_table,
                     item_description, lot_no, batch_number, weight_kg, no_of_cartons,
                     cold_storage_data,
                     gross_weight, net_weight, article,
                     status, dispatched_at, dispatched_by)
                VALUES
                    ('INTERUNIT', :transfer_out_id, :challan_no,
                     :box_id, :transaction_no,
                     :from_company, :to_company, :from_site, :to_site,
                     :from_storage_type, 'cold',
                     :source_table, NULL, :destination_table,
                     :item_description, :lot_no, NULL, :weight_kg, :no_of_cartons,
                     CAST(:cold_storage_data AS JSONB),
                     NULL, :net_weight, :article,
                     'In Transit', :dispatched_at, :dispatched_by)
                ON CONFLICT (box_id, transaction_no) DO NOTHING
            """), {
                "transfer_out_id": transfer_out_id,
                "challan_no": out._mapping["challan_no"],
                "box_id": box_id,
                "transaction_no": tno,
                "from_company": to_company,
                "to_company": to_company,
                "from_site": from_site,
                "to_site": to_site,
                "from_storage_type": from_storage_type,
                "source_table": source_table_guess,
                "destination_table": destination_table_keep,
                "item_description": b._mapping["item_description"],
                "lot_no": b._mapping["lot_no"],
                "weight_kg": b._mapping["weight_kg"],
                "no_of_cartons": b._mapping["no_of_cartons"] or 1,
                "cold_storage_data": _json.dumps(cold_data) if cold_data else None,
                "net_weight": b._mapping["weight_kg"],
                "article": b._mapping["item_description"],
                "dispatched_at": now,
                "dispatched_by": out._mapping["created_by"] or "system",
            })
            reparked_n += 1

    db.execute(text("DELETE FROM cold_transfer_inboxes WHERE header_id = :hid"), {"hid": header_id})
    db.execute(text("DELETE FROM cold_transfer_in_headers WHERE id = :hid"), {"hid": header_id})

    # Also purge the interunit pending/staging rows for this receipt. A cold receipt created
    # via finalize shares the interunit pending-header id (finalize set id = header_id), so
    # leaving the interunit_transfer_in_header + its acknowledged boxes behind makes a
    # re-receive "resume" the now-deleted acknowledgements (and leaves an orphan that trips
    # stuck-receipt checks). Guard on transfer_out_id so a standalone cold header (create
    # path, cold-sequence id) can't wipe an unrelated interunit receipt that happens to share
    # the numeric id.
    _staging = db.execute(text(
        "SELECT id FROM interunit_transfer_in_header WHERE id = :hid AND transfer_out_id = :oid"
    ), {"hid": header_id, "oid": transfer_out_id}).fetchone()
    if _staging is not None:
        db.execute(text("DELETE FROM interunit_transfer_in_boxes WHERE header_id = :hid"), {"hid": header_id})
        db.execute(text("DELETE FROM interunit_transfer_in_header WHERE id = :hid"), {"hid": header_id})

    if out is not None:
        db.execute(text(
            "UPDATE interunit_transfers_header SET status='Dispatch' WHERE id=:oid"
        ), {"oid": transfer_out_id})

    db.commit()
    return {
        "status": "success",
        "message": f"Cold transfer-in {grn_number or header_id} deleted.",
        "boxes_reversed": reversed_n,
        "reparked": reparked_n,
    }


# ── Internal helpers ────────────────────────────────────────────────────
def _process_box_loop(
    db: Session,
    header_id: int,
    boxes: List[ColdTransferInBoxInput],
    cold_stocks_table: str,
    transfer_out_id: Optional[int] = None,
) -> int:
    """Per-box: pull pending row, INSERT cold_transfer_inboxes + <company>_cold_stocks, DELETE pending row.

    After the per-box loop, sweeps any leftover `LINE-%` sentinel rows for this
    transfer_out_id (mirrors `pick_from_pending` Phase 1: LINE rows are
    tracking-only placeholders written by `park_in_pending` for box-less line
    transfers; they're always cleared when any ack box exists). The exact-match
    SELECT above never touches them because the TX-In page regenerates real
    `{epoch}-{n}` box_ids before submit.
    """
    # Reject boxes that haven't been through "Generate QR" — they carry no unique
    # identity (blank box_id / transaction_no), which collides on the cold_stocks
    # UNIQUE (transaction_no, box_id). Fail fast with an actionable message instead
    # of a 500 deep in the INSERT.
    _ungenerated = [
        b for b in boxes
        if not (b.box_id or "").strip() or not (b.transaction_no or "").strip()
    ]
    if _ungenerated:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{len(_ungenerated)} box(es) have no generated ID. "
                "Click 'Generate QR' to assign unique box IDs before finalizing."
            ),
        )

    inserted = 0
    for b in boxes:
        # Match the pending row by the PHYSICAL box identity (box_id + transaction_no) —
        # those uniquely identify a scanned box. lot_no is only a tiebreaker, NOT a hard
        # filter: the cold receive lets the user set a different cold lot per item, so the
        # finalize payload's lot_no often differs from the parked box's original lot. The
        # old exact-lot WHERE then found no row, so the box landed in cold_stocks but its
        # pending_transfer_stock 'In Transit' row was never deleted (stuck in transit).
        pending = None
        if (b.box_id or "").strip() and (b.transaction_no or "").strip():
            pending = db.execute(text("""
                SELECT id, cold_storage_data, weight_kg, item_description, no_of_cartons
                FROM pending_transfer_stock
                WHERE box_id = :box_id AND transaction_no = :transaction_no
                  AND status = 'In Transit'
                ORDER BY (CASE WHEN lot_no IS NOT DISTINCT FROM :lot_no THEN 0 ELSE 1 END), id
                LIMIT 1
            """), {
                "box_id": b.box_id,
                "transaction_no": b.transaction_no,
                "lot_no": b.lot_no,
            }).fetchone()

        cs_data: Dict[str, Any] = {}
        if pending and pending._mapping.get("cold_storage_data"):
            raw = pending._mapping["cold_storage_data"]
            cs_data = dict(raw) if isinstance(raw, dict) else {}
        if b.cold_storage_data:
            cs_data.update(b.cold_storage_data)

        weight_kg = b.weight_kg if b.weight_kg is not None else (
            float(pending._mapping["weight_kg"]) if pending and pending._mapping["weight_kg"] is not None else None
        )
        item_description = b.item_description or (
            pending._mapping["item_description"] if pending else None
        )
        no_of_cartons = b.no_of_cartons if b.no_of_cartons is not None else (
            float(pending._mapping["no_of_cartons"]) if pending and pending._mapping["no_of_cartons"] is not None else 1.0
        )

        # INSERT cold_transfer_inboxes
        db.execute(text("""
            INSERT INTO cold_transfer_inboxes
                (header_id, item_description, inward_dt, unit, vakkal, lot_no, item_mark,
                 no_of_cartons, weight_kg, group_name, item_subgroup, storage_location,
                 exporter, rate, value, spl_remarks, created_at, box_id, transaction_no)
            VALUES
                (:header_id, :item_description, :inward_dt, :unit, :vakkal, :lot_no, :item_mark,
                 :no_of_cartons, :weight_kg, :group_name, :item_subgroup, :storage_location,
                 :exporter, :rate, :value, :spl_remarks, NOW(), :box_id, :transaction_no)
        """), {
            "header_id": header_id,
            "box_id": b.box_id,
            "transaction_no": b.transaction_no,
            "lot_no": b.lot_no,
            "item_description": item_description,
            "weight_kg": weight_kg,
            "no_of_cartons": no_of_cartons,
            "unit": b.unit or cs_data.get("unit"),
            "inward_dt": cs_data.get("inward_dt"),
            "vakkal": cs_data.get("vakkal"),
            "item_mark": cs_data.get("item_mark"),
            "group_name": cs_data.get("group_name"),
            "item_subgroup": cs_data.get("item_subgroup"),
            "storage_location": cs_data.get("storage_location"),
            "exporter": cs_data.get("exporter"),
            "rate": cs_data.get("rate"),
            "value": cs_data.get("value"),
            "spl_remarks": cs_data.get("spl_remarks"),
        })

        # INSERT <company>_cold_stocks (column names: last_purchase_rate, not rate;
        # canonical_warehouse/group/subgroup auto-populate via trigger)
        db.execute(text(f"""
            INSERT INTO {cold_stocks_table}
                (item_description, inward_dt, unit, vakkal, lot_no, item_mark, no_of_cartons,
                 weight_kg, group_name, item_subgroup, storage_location, exporter,
                 last_purchase_rate, value, spl_remarks, created_at, box_id, transaction_no,
                 inward_transaction_no, cold_item_mark)
            VALUES
                (:item_description, :inward_dt, :unit, :vakkal, :lot_no, :item_mark, :no_of_cartons,
                 :weight_kg, :group_name, :item_subgroup, :storage_location, :exporter,
                 :last_purchase_rate, :value, :spl_remarks, NOW(), :box_id, :transaction_no,
                 :inward_transaction_no, :cold_item_mark)
        """), {
            "item_description": item_description,
            "weight_kg": weight_kg,
            "no_of_cartons": no_of_cartons,
            "lot_no": b.lot_no,
            "box_id": b.box_id,
            "transaction_no": b.transaction_no,
            "unit": b.unit or cs_data.get("unit"),
            "inward_dt": cs_data.get("inward_dt"),
            "vakkal": cs_data.get("vakkal"),
            "item_mark": cs_data.get("item_mark"),
            "group_name": cs_data.get("group_name"),
            "item_subgroup": cs_data.get("item_subgroup"),
            "storage_location": cs_data.get("storage_location"),
            "exporter": cs_data.get("exporter"),
            "last_purchase_rate": cs_data.get("rate") or cs_data.get("last_purchase_rate"),
            "value": cs_data.get("value"),
            "spl_remarks": cs_data.get("spl_remarks"),
            "inward_transaction_no": cs_data.get("inward_transaction_no"),
            "cold_item_mark": cs_data.get("cold_item_mark"),
        })

        # DELETE the pending row if it existed.
        if pending:
            db.execute(
                text("DELETE FROM pending_transfer_stock WHERE id = :pid"),
                {"pid": pending._mapping["id"]},
            )

        inserted += 1

    # Sweep LINE-% tracking sentinels for this transfer_out_id. These were
    # never going to match the exact-WHERE above (TX-In assigns fresh box_ids
    # on Generate-QR click) but they still occupy `pending_transfer_stock`,
    # which would freeze the header at "Partially Received". Mirrors
    # pending_stock_tools.pick_from_pending Phase 1.
    if inserted > 0 and transfer_out_id is not None:
        db.execute(text("""
            DELETE FROM pending_transfer_stock
            WHERE transfer_out_id = :oid
              AND status = 'In Transit'
              AND COALESCE(box_id, '') LIKE 'LINE-%'
        """), {"oid": transfer_out_id})

    return inserted


def _reconcile_statuses(
    db: Session, transfer_out_id: int, header_id: int
) -> Tuple[str, str]:
    """Two-state model only: 'Pending' (boxes still in transit) or 'Received' (all in).
    No 'Partially Received' — the cold IN header is 'Pending' until every dispatched box
    is received, matching the interunit Pending/Received convention.
    """
    pending_remaining = db.execute(text(
        "SELECT COUNT(*) FROM pending_transfer_stock "
        "WHERE transfer_out_id = :oid AND status = 'In Transit'"
    ), {"oid": transfer_out_id}).scalar() or 0

    if pending_remaining == 0:
        db.execute(text(
            "UPDATE interunit_transfers_header SET status='Received' WHERE id=:oid"
        ), {"oid": transfer_out_id})
        db.execute(text(
            "UPDATE cold_transfer_in_headers SET status='Received', updated_at=NOW() WHERE id=:hid"
        ), {"hid": header_id})
        return "Received", "Received"

    # Boxes still in transit → keep both at 'Pending' (never 'Partially Received').
    db.execute(text(
        "UPDATE cold_transfer_in_headers SET status='Pending', updated_at=NOW() WHERE id=:hid"
    ), {"hid": header_id})
    return "Pending", "Pending"
