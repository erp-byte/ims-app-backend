"""Cold-source transfer-OUT handler.

Owns the create/edit/delete lifecycle for transfers whose SOURCE is a cold
warehouse (Cold Storage / Savla D-39 / Savla D-514 / Rishi / Supreme).

These endpoints write to the SHARED OUT-side tables:
  - interunit_transfers_header
  - interunit_transfer_boxes
  - interunit_transfers_lines (line metadata for reconciliation/UI)

They also deduct source rows from `<company>_cold_stocks` and park each box
into `pending_transfer_stock` via the shared middleware (`park_in_pending`).

Warehouse-source dispatches remain in interunit_tools.py (`create_transfer`).
"""
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from services.ims_service.pending_stock_tools import (
    _is_cold_site,
    park_in_pending,
    park_lines_in_pending,
    restore_to_source,
    unpick_to_pending,
)
from shared.logger import get_logger

logger = get_logger("ims.cold_transfer_out")


def _box_for_park(b: "ColdOutBoxInput") -> SimpleNamespace:
    """Adapt a ColdOutBoxInput (uses lot_no / weight_kg) to the attribute names
    park_in_pending reads via getattr (lot_number / net_weight / article / batch_number)."""
    return SimpleNamespace(
        box_id=b.box_id,
        transaction_no=b.transaction_no,
        lot_number=b.lot_no,
        article=b.item_description,
        net_weight=b.weight_kg,
        gross_weight=b.weight_kg,
        batch_number=None,
    )


class ColdOutBoxInput(BaseModel):
    box_id: str
    transaction_no: str
    lot_no: Optional[str] = None
    item_description: Optional[str] = None
    no_of_cartons: Optional[float] = 1.0
    weight_kg: Optional[float] = None
    unit: Optional[str] = None
    cold_storage_data: Optional[Dict[str, Any]] = None


class ColdOutLineInput(BaseModel):
    item_desc_raw: str
    qty: float
    uom: Optional[str] = None
    net_weight: Optional[float] = 0.0
    total_weight: Optional[float] = 0.0
    lot_number: Optional[str] = None
    batch_number: Optional[str] = ""
    # NOT NULL on the table — kept as overridable but defaulted.
    rm_pm_fg_type: Optional[str] = "RM"
    item_category: Optional[str] = ""
    sub_category: Optional[str] = ""
    pack_size: Optional[float] = 0.0
    unit_pack_size: Optional[float] = 0.0


class ColdTransferOutCreate(BaseModel):
    challan_no: str
    request_no: Optional[str] = None
    stock_trf_date: Optional[str] = None
    from_warehouse: str  # "Cold Storage" or specific cold unit
    to_warehouse: str    # destination — warehouse OR another cold unit
    reason_code: Optional[str] = None
    remark: Optional[str] = None
    vehicle_no: Optional[str] = None
    driver_name: Optional[str] = None
    approved_by: Optional[str] = None
    created_by: Optional[str] = None
    lines: List[ColdOutLineInput] = Field(default_factory=list)
    boxes: List[ColdOutBoxInput] = Field(default_factory=list)


class ColdTransferOutEdit(BaseModel):
    challan_no: Optional[str] = None
    stock_trf_date: Optional[str] = None
    from_warehouse: Optional[str] = None
    to_warehouse: Optional[str] = None
    reason_code: Optional[str] = None
    remark: Optional[str] = None
    vehicle_no: Optional[str] = None
    driver_name: Optional[str] = None
    approved_by: Optional[str] = None
    lines: List[ColdOutLineInput] = Field(default_factory=list)
    boxes: List[ColdOutBoxInput] = Field(default_factory=list)


class ColdTransferOutCreateResponse(BaseModel):
    id: int
    challan_no: str
    status: str
    boxes_parked: int


def _parse_trf_date(raw: Optional[str]):
    """Convert the form's DD-MM-YYYY (or ISO) stock-transfer date into a real date
    object. Inserting the raw 'DD-MM-YYYY' string lets Postgres misparse it under the
    default MDY DateStyle (09-06-2026 -> Sep 6), corrupting the stored/displayed date
    — the regular create_transfer path avoids this via _convert_date."""
    s = (raw or "").strip()
    if s:
        for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    return datetime.now().date()


def create_cold_transfer_out(
    db: Session,
    payload: ColdTransferOutCreate,
) -> ColdTransferOutCreateResponse:
    """Create a cold-source dispatch.

    Inserts header + lines + boxes into the shared OUT tables, then deducts
    each box from <company>_cold_stocks and parks into pending_transfer_stock
    via the shared park_in_pending helper.
    """
    if not _is_cold_site(payload.from_warehouse):
        raise HTTPException(
            status_code=400,
            detail=(
                f"create_cold_transfer_out requires a cold from_warehouse "
                f"(got {payload.from_warehouse!r}). Warehouse-source dispatches "
                f"must use POST /interunit/transfers (the regular endpoint)."
            ),
        )

    # Defaults for NOT NULL columns (stock_trf_date, vehicle_no, remark, reason_code).
    stock_trf_date = _parse_trf_date(payload.stock_trf_date)
    vehicle_no = payload.vehicle_no or ""
    remark = payload.remark or ""
    reason_code = payload.reason_code or ""

    header_row = db.execute(
        text("""
            INSERT INTO interunit_transfers_header
                (challan_no, stock_trf_date, from_site, to_site,
                 vehicle_no, driver_name, approved_by, remark, reason_code,
                 status, created_by, created_ts)
            VALUES
                (:challan_no, :stock_trf_date, :from_site, :to_site,
                 :vehicle_no, :driver_name, :approved_by, :remark, :reason_code,
                 'Dispatch', :created_by, NOW())
            RETURNING id, challan_no, status
        """),
        {
            "challan_no": payload.challan_no,
            "stock_trf_date": stock_trf_date,
            "from_site": payload.from_warehouse,
            "to_site": payload.to_warehouse,
            "vehicle_no": vehicle_no,
            "driver_name": payload.driver_name,
            "approved_by": payload.approved_by,
            "remark": remark,
            "reason_code": reason_code,
            "created_by": payload.created_by,
        },
    ).fetchone()
    header_id = header_row.id

    # Auto-derive lines from boxes when caller omitted them (smoke test / minimal
    # callers). Group by (item_description, lot_no), qty = box count, weights = sum.
    derived_lines: List[ColdOutLineInput] = list(payload.lines)
    if not derived_lines and payload.boxes:
        groups: Dict[tuple, Dict[str, Any]] = {}
        for b in payload.boxes:
            key = ((b.item_description or "").strip(), (b.lot_no or "").strip())
            g = groups.setdefault(key, {"qty": 0, "net_weight": 0.0})
            g["qty"] += 1
            g["net_weight"] += float(b.weight_kg or 0)
        for (item_desc, lot_no), agg in groups.items():
            derived_lines.append(ColdOutLineInput(
                item_desc_raw=item_desc or "Cold Storage Item",
                qty=float(agg["qty"]),
                net_weight=agg["net_weight"],
                total_weight=agg["net_weight"],
                lot_number=lot_no or None,
            ))

    # Insert lines + remember (item, lot) → line_id so boxes can FK back.
    line_id_by_key: Dict[tuple, int] = {}
    for line in derived_lines:
        row = db.execute(
            text("""
                INSERT INTO interunit_transfers_lines
                    (header_id, rm_pm_fg_type, item_category, sub_category,
                     item_desc_raw, pack_size, unit_pack_size, qty, uom,
                     net_weight, total_weight, lot_number, batch_number)
                VALUES
                    (:header_id, :rm_pm_fg_type, :item_category, :sub_category,
                     :item_desc_raw, :pack_size, :unit_pack_size, :qty, :uom,
                     :net_weight, :total_weight, :lot_number, :batch_number)
                RETURNING id
            """),
            {
                "header_id": header_id,
                "rm_pm_fg_type": line.rm_pm_fg_type or "RM",
                "item_category": line.item_category or "",
                "sub_category": line.sub_category or "",
                "item_desc_raw": line.item_desc_raw,
                "pack_size": float(line.pack_size or 0),
                "unit_pack_size": float(line.unit_pack_size or 0),
                "qty": line.qty,
                "uom": line.uom,
                "net_weight": float(line.net_weight or 0),
                "total_weight": float(line.total_weight or 0),
                "lot_number": line.lot_number,
                "batch_number": line.batch_number or "",
            },
        ).fetchone()
        line_id_by_key[(
            (line.item_desc_raw or "").strip(),
            (line.lot_number or "").strip(),
        )] = row.id

    fallback_line_id = next(iter(line_id_by_key.values()), None)

    seen: set = set()
    box_idx = 0
    for box in payload.boxes:
        key = ((box.box_id or "").strip(), (box.transaction_no or "").strip())
        if key in seen:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Duplicate box_id '{box.box_id}' for transaction "
                    f"'{box.transaction_no}'. Each physical box must carry a "
                    f"unique box_id — re-pick via FIFO."
                ),
            )
        seen.add(key)
        line_key = (
            (box.item_description or "").strip(),
            (box.lot_no or "").strip(),
        )
        line_id = line_id_by_key.get(line_key, fallback_line_id)
        box_idx += 1
        db.execute(
            text("""
                INSERT INTO interunit_transfer_boxes
                    (header_id, transfer_line_id, box_number, box_id, transaction_no, article,
                     lot_number, batch_number, net_weight, gross_weight)
                VALUES
                    (:header_id, :transfer_line_id, :box_number, :box_id, :transaction_no, :article,
                     :lot_number, :batch_number, :net_weight, :gross_weight)
            """),
            {
                "header_id": header_id,
                "transfer_line_id": line_id,
                "box_number": box_idx,
                "box_id": box.box_id,
                "transaction_no": box.transaction_no,
                "article": box.item_description,
                "lot_number": box.lot_no,
                "batch_number": "",
                "net_weight": box.weight_kg,
                "gross_weight": box.weight_kg,
            },
        )

    # Park into pending_transfer_stock + deduct cold_stocks (shared middleware).
    parked = park_in_pending(
        transfer_out_id=header_id,
        challan_no=payload.challan_no,
        from_site=payload.from_warehouse,
        to_site=payload.to_warehouse,
        boxes=[_box_for_park(b) for b in payload.boxes],
        dispatched_by=payload.created_by or "system",
        db=db,
        transfer_type="INTERUNIT",
    )

    # Never-drop manual entries (mixed scan + manual): park the lines that have no
    # scanned box so manually-typed cold rows aren't lost. Tracking-only (LINE- sentinels;
    # no extra cold_stocks deduction beyond what the parked boxes already did).
    _covered: dict = {}
    for _b in payload.boxes:
        _k = ((_b.item_description or "").strip().upper(), (_b.lot_no or "").strip())
        _covered[_k] = _covered.get(_k, 0) + 1
    _uncovered = []
    for _l in derived_lines:
        _k = ((_l.item_desc_raw or "").strip().upper(), (_l.lot_number or "").strip())
        _qty = int(getattr(_l, "qty", 0) or 0)
        _take = min(_qty, _covered.get(_k, 0))
        _covered[_k] = _covered.get(_k, 0) - _take
        if _qty - _take > 0:
            _uncovered.append(SimpleNamespace(
                id=line_id_by_key.get(((_l.item_desc_raw or "").strip(), (_l.lot_number or "").strip())),
                item_desc_raw=_l.item_desc_raw, qty=_qty - _take,
                net_weight=getattr(_l, "net_weight", 0) or 0,
                total_weight=getattr(_l, "total_weight", 0) or 0,
                lot_number=_l.lot_number, batch_number=getattr(_l, "batch_number", "") or "",
                rm_pm_fg_type=getattr(_l, "rm_pm_fg_type", "") or "",
                item_category=getattr(_l, "item_category", "") or "",
                sub_category=getattr(_l, "sub_category", "") or "",
                pack_size=getattr(_l, "pack_size", 0) or 0,
                unit_pack_size=getattr(_l, "unit_pack_size", 0) or 0,
                uom=getattr(_l, "uom", "") or "",
            ))
    if _uncovered:
        park_lines_in_pending(
            transfer_out_id=header_id,
            challan_no=payload.challan_no,
            from_site=payload.from_warehouse,
            to_site=payload.to_warehouse,
            lines=_uncovered,
            dispatched_by=payload.created_by or "system",
            db=db,
        )

    db.commit()
    logger.info(
        "COLD_OUT: created header_id=%s challan=%s parked=%s",
        header_id, payload.challan_no, parked,
    )
    return ColdTransferOutCreateResponse(
        id=header_id,
        challan_no=header_row.challan_no,
        status=header_row.status,
        boxes_parked=parked,
    )


def edit_cold_transfer_out(
    db: Session,
    header_id: int,
    payload: ColdTransferOutEdit,
) -> Dict[str, Any]:
    """Edit an existing cold OUT header.

    Strategy: restore_to_source for all pending rows, DELETE old lines+boxes,
    UPDATE header, INSERT new lines+boxes, re-park.
    """
    existing = db.execute(
        text("""
            SELECT id, challan_no, from_site, to_site, status
            FROM interunit_transfers_header WHERE id = :hid
        """),
        {"hid": header_id},
    ).fetchone()
    if not existing:
        raise HTTPException(404, f"Transfer header {header_id} not found")

    if existing.status != "Dispatch":
        raise HTTPException(
            400,
            f"Cannot edit transfer in status '{existing.status}'. "
            f"Only 'Dispatch' transfers are editable.",
        )

    restore_to_source(transfer_out_id=header_id, db=db)

    db.execute(text("DELETE FROM interunit_transfer_boxes WHERE header_id = :hid"), {"hid": header_id})
    db.execute(text("DELETE FROM interunit_transfers_lines WHERE header_id = :hid"), {"hid": header_id})

    update_fields: Dict[str, Any] = {}
    for col, value in [
        ("challan_no", payload.challan_no),
        ("stock_trf_date", _parse_trf_date(payload.stock_trf_date) if payload.stock_trf_date else None),
        ("from_site", payload.from_warehouse),
        ("to_site", payload.to_warehouse),
        ("reason_code", payload.reason_code),
        ("remark", payload.remark),
        ("vehicle_no", payload.vehicle_no),
        ("driver_name", payload.driver_name),
        ("approved_by", payload.approved_by),
    ]:
        if value is not None:
            update_fields[col] = value
    if update_fields:
        sets = ", ".join(f"{c} = :{c}" for c in update_fields)
        update_params = dict(update_fields)
        update_params["hid"] = header_id
        db.execute(
            text(f"UPDATE interunit_transfers_header SET {sets}, updated_ts = NOW() WHERE id = :hid"),
            update_params,
        )

    from_site = payload.from_warehouse or existing.from_site
    if not _is_cold_site(from_site):
        db.rollback()
        raise HTTPException(
            400,
            f"edit_cold_transfer_out requires a cold from_warehouse (got {from_site!r}).",
        )
    to_site = payload.to_warehouse or existing.to_site

    # Same auto-derive + per-line/per-box insert as create.
    derived_lines: List[ColdOutLineInput] = list(payload.lines)
    if not derived_lines and payload.boxes:
        groups: Dict[tuple, Dict[str, Any]] = {}
        for b in payload.boxes:
            key = ((b.item_description or "").strip(), (b.lot_no or "").strip())
            g = groups.setdefault(key, {"qty": 0, "net_weight": 0.0})
            g["qty"] += 1
            g["net_weight"] += float(b.weight_kg or 0)
        for (item_desc, lot_no), agg in groups.items():
            derived_lines.append(ColdOutLineInput(
                item_desc_raw=item_desc or "Cold Storage Item",
                qty=float(agg["qty"]),
                net_weight=agg["net_weight"],
                total_weight=agg["net_weight"],
                lot_number=lot_no or None,
            ))

    line_id_by_key: Dict[tuple, int] = {}
    for line in derived_lines:
        row = db.execute(
            text("""
                INSERT INTO interunit_transfers_lines
                    (header_id, rm_pm_fg_type, item_category, sub_category,
                     item_desc_raw, pack_size, unit_pack_size, qty, uom,
                     net_weight, total_weight, lot_number, batch_number)
                VALUES
                    (:header_id, :rm_pm_fg_type, :item_category, :sub_category,
                     :item_desc_raw, :pack_size, :unit_pack_size, :qty, :uom,
                     :net_weight, :total_weight, :lot_number, :batch_number)
                RETURNING id
            """),
            {
                "header_id": header_id,
                "rm_pm_fg_type": line.rm_pm_fg_type or "RM",
                "item_category": line.item_category or "",
                "sub_category": line.sub_category or "",
                "item_desc_raw": line.item_desc_raw,
                "pack_size": float(line.pack_size or 0),
                "unit_pack_size": float(line.unit_pack_size or 0),
                "qty": line.qty,
                "uom": line.uom,
                "net_weight": float(line.net_weight or 0),
                "total_weight": float(line.total_weight or 0),
                "lot_number": line.lot_number,
                "batch_number": line.batch_number or "",
            },
        ).fetchone()
        line_id_by_key[(
            (line.item_desc_raw or "").strip(),
            (line.lot_number or "").strip(),
        )] = row.id

    fallback_line_id = next(iter(line_id_by_key.values()), None)

    seen: set = set()
    box_idx = 0
    for box in payload.boxes:
        key = ((box.box_id or "").strip(), (box.transaction_no or "").strip())
        if key in seen:
            db.rollback()
            raise HTTPException(
                400,
                f"Duplicate box_id '{box.box_id}' for transaction '{box.transaction_no}'.",
            )
        seen.add(key)
        line_key = (
            (box.item_description or "").strip(),
            (box.lot_no or "").strip(),
        )
        line_id = line_id_by_key.get(line_key, fallback_line_id)
        box_idx += 1
        db.execute(
            text("""
                INSERT INTO interunit_transfer_boxes
                    (header_id, transfer_line_id, box_number, box_id, transaction_no, article,
                     lot_number, batch_number, net_weight, gross_weight)
                VALUES
                    (:header_id, :transfer_line_id, :box_number, :box_id, :transaction_no, :article,
                     :lot_number, :batch_number, :net_weight, :gross_weight)
            """),
            {
                "header_id": header_id,
                "transfer_line_id": line_id,
                "box_number": box_idx,
                "box_id": box.box_id,
                "transaction_no": box.transaction_no,
                "article": box.item_description,
                "lot_number": box.lot_no,
                "batch_number": "",
                "net_weight": box.weight_kg,
                "gross_weight": box.weight_kg,
            },
        )

    parked = park_in_pending(
        transfer_out_id=header_id,
        challan_no=payload.challan_no or existing.challan_no,
        from_site=from_site,
        to_site=to_site,
        boxes=[_box_for_park(b) for b in payload.boxes],
        dispatched_by="system",
        db=db,
        transfer_type="INTERUNIT",
    )

    # Never-drop manual entries (mixed scan + manual) on edit too.
    _covered: dict = {}
    for _b in payload.boxes:
        _k = ((_b.item_description or "").strip().upper(), (_b.lot_no or "").strip())
        _covered[_k] = _covered.get(_k, 0) + 1
    _uncovered = []
    for _l in derived_lines:
        _k = ((_l.item_desc_raw or "").strip().upper(), (_l.lot_number or "").strip())
        _qty = int(getattr(_l, "qty", 0) or 0)
        _take = min(_qty, _covered.get(_k, 0))
        _covered[_k] = _covered.get(_k, 0) - _take
        if _qty - _take > 0:
            _uncovered.append(SimpleNamespace(
                id=line_id_by_key.get(((_l.item_desc_raw or "").strip(), (_l.lot_number or "").strip())),
                item_desc_raw=_l.item_desc_raw, qty=_qty - _take,
                net_weight=getattr(_l, "net_weight", 0) or 0,
                total_weight=getattr(_l, "total_weight", 0) or 0,
                lot_number=_l.lot_number, batch_number=getattr(_l, "batch_number", "") or "",
                rm_pm_fg_type=getattr(_l, "rm_pm_fg_type", "") or "",
                item_category=getattr(_l, "item_category", "") or "",
                sub_category=getattr(_l, "sub_category", "") or "",
                pack_size=getattr(_l, "pack_size", 0) or 0,
                unit_pack_size=getattr(_l, "unit_pack_size", 0) or 0,
                uom=getattr(_l, "uom", "") or "",
            ))
    if _uncovered:
        park_lines_in_pending(
            transfer_out_id=header_id,
            challan_no=payload.challan_no or existing.challan_no,
            from_site=from_site,
            to_site=to_site,
            lines=_uncovered,
            dispatched_by="system",
            db=db,
        )

    db.commit()
    return {"id": header_id, "boxes_parked": parked, "status": "Dispatch"}


def delete_cold_transfer_out(db: Session, header_id: int) -> Dict[str, Any]:
    """Delete a cold-source OUT.

    Reverses warehouse-dest interunit IN receives (unpick → pending),
    restores all pending rows to source cold_stocks, then deletes the OUT
    header + lines + boxes.
    """
    existing = db.execute(
        text("SELECT id, challan_no, from_site FROM interunit_transfers_header WHERE id = :hid"),
        {"hid": header_id},
    ).fetchone()
    if not existing:
        raise HTTPException(404, f"Transfer header {header_id} not found")
    if not _is_cold_site(existing.from_site):
        raise HTTPException(
            400,
            f"delete_cold_transfer_out only handles cold-source transfers "
            f"(this one is from {existing.from_site!r}). Use the regular delete endpoint.",
        )

    in_headers = db.execute(
        text("SELECT id FROM interunit_transfer_in_header WHERE transfer_out_id = :tid"),
        {"tid": header_id},
    ).fetchall()
    for ti in in_headers:
        unpick_to_pending(transfer_in_id=ti.id, transfer_out_id=header_id, db=db)
        db.execute(
            text("DELETE FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
            {"hid": ti.id},
        )
    db.execute(
        text("DELETE FROM interunit_transfer_in_header WHERE transfer_out_id = :tid"),
        {"tid": header_id},
    )

    restored = restore_to_source(transfer_out_id=header_id, db=db)

    db.execute(text("DELETE FROM interunit_transfer_boxes WHERE header_id = :hid"), {"hid": header_id})
    db.execute(text("DELETE FROM interunit_transfers_lines WHERE header_id = :hid"), {"hid": header_id})
    db.execute(text("DELETE FROM interunit_transfers_header WHERE id = :hid"), {"hid": header_id})

    db.commit()
    logger.info(
        "COLD_OUT: deleted header_id=%s challan=%s restored=%s",
        header_id, existing.challan_no, restored,
    )
    return {
        "success": True,
        "transfer_id": header_id,
        "challan_no": existing.challan_no,
        "restored_to_source": restored,
    }
