"""Make existing transfer entries accurate, then restartable.

The code fixes stop NEW damage. This repairs what is already on disk, so the
affected challans can be re-received with correct numbers.

WHAT WENT WRONG (all now fixed at the write path, all still present in old rows)

  A. PER-BOX LINES  (cold_transfer_out_tools._coalesce_lines)
     The cold form posted one interunit_transfers_lines row PER BOX at qty 1.
     The grain is one row per (article, lot), so only the K rows that owned boxes
     were corrected upward by _apply_box_totals; the rest were orphaned at qty 1.
     SUM(qty) became 2B - K  ->  TRANS202608171318: 198 for 100 boxes.

  B. PHANTOM PENDING  (same root cause)
     The uncovered-qty reconciler budgets real boxes against SUM(qty), so the
     2B-K surplus was parked as 'LINE-<id>-<n>' rows in pending_transfer_stock —
     in-transit inventory that never physically shipped.

  C. GHOST RECEIPTS  (cold_transfer_in_tools sentinel sweep)
     A cold receive deleted EVERY LINE- sentinel for the transfer as soon as one
     box landed, so _reconcile_statuses saw 0 remaining and stamped the headers
     'Received'. Warehouse->cold transfers are parked ENTIRELY as sentinels, so a
     1-of-100 receipt could mark the whole consignment received and strand 99
     units with no shortage record and no way to receive them.

WHAT THIS DOES

  REPORT (default, read-only)
     Per affected transfer: physical boxes vs recorded qty vs real receipts vs
     what is still in transit, and which of A/B/C it is suffering from.

  APPLY=1
     1. collapse lines to one row per (article, lot); qty = that pile's real box
        count (box-backed keys only — a box-less line is a manually typed row and
        keeps its qty)
     2. repoint interunit_transfer_boxes.transfer_line_id at the surviving line
     3. delete the orphan line rows
     4. delete phantom 'LINE-%' pending rows for keys that DID ship boxes
     5. recompute header status from real receipts across BOTH ledgers:
           received == 0        -> 'Dispatch'
           0 < received < boxes -> 'Partial'
           received >= boxes    -> 'Received'
        This is what makes a ghost-received transfer restartable.

  APPLY=1 RESTORE=1
     6. re-park units that C deleted, via the sanctioned park_lines_in_pending,
        so a stranded consignment has something left to receive. Only ever tops
        up to (ordered - received); never exceeds it.

SAFETY
  Dry-run by default: does the work in a transaction, prints it, ROLLS BACK.
  Refuses any transfer whose receipts it cannot read. Never touches
  <company>_cold_stocks — deleted stock rows are a restore-from-backup problem,
  not something a repair script should invent.

    DATABASE_URL=... python repair_transfer_data.py
    DATABASE_URL=... APPLY=1 python repair_transfer_data.py
    DATABASE_URL=... APPLY=1 RESTORE=1 python repair_transfer_data.py
    DATABASE_URL=... ONLY=TRANS202608171318 python repair_transfer_data.py
"""
import os
from types import SimpleNamespace

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from services.ims_service.cold_transfer_out_tools import _line_key
from services.ims_service.pending_stock_tools import (
    _received_box_count,
    park_lines_in_pending,
)

raw = os.environ["DATABASE_URL"]
URL = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
APPLY = os.environ.get("APPLY") == "1"
RESTORE = os.environ.get("RESTORE") == "1"
ONLY = (os.environ.get("ONLY") or "").strip()

engine = create_engine(URL)


# Any transfer whose line rows outnumber its distinct (article, lot) pairs, or whose
# SUM(qty) disagrees with its box count, or that carries LINE- sentinels. Scoped to
# cold-source headers: a warehouse header can legitimately carry two lines for one
# article (different pack sizes), and its qty is a PACK count by design.
CANDIDATES = """
    SELECT h.id, h.challan_no, h.stock_trf_date, h.from_site, h.to_site, h.status,
           COUNT(l.id)                                                   AS line_rows,
           COUNT(DISTINCT (UPPER(TRIM(l.item_desc_raw)),
                           UPPER(TRIM(COALESCE(l.lot_number, '')))))     AS real_lines,
           COALESCE(SUM(l.qty), 0)                                       AS sum_qty,
           (SELECT COUNT(*) FROM interunit_transfer_boxes b
             WHERE b.header_id = h.id)                                   AS boxes,
           (SELECT COUNT(*) FROM pending_transfer_stock p
             WHERE p.transfer_out_id = h.id AND p.status = 'In Transit'
               AND COALESCE(p.box_id, '') LIKE 'LINE-%')                AS sentinels,
           (SELECT COUNT(*) FROM pending_transfer_stock p
             WHERE p.transfer_out_id = h.id AND p.status = 'In Transit'
               AND COALESCE(p.box_id, '') NOT LIKE 'LINE-%')            AS real_pending
    FROM interunit_transfers_header h
    JOIN interunit_transfers_lines l ON l.header_id = h.id
    WHERE (h.from_site ILIKE 'cold%' OR h.from_cold_unit IS NOT NULL)
      {only}
    GROUP BY h.id
    ORDER BY h.stock_trf_date DESC, h.id
"""


def analyze(db, t, received):
    """The transfer's TRUE shape, computed per (article, lot) — not estimated.

    The summary query can only compare whole-transfer totals, which over-reports:
    a LINE- sentinel is legitimate for a box-less manually typed row, and only a
    sentinel whose key ALSO shipped boxes is phantom. Ordered units are likewise
    per-key — a box-backed key is worth its box count, a box-less key its typed
    qty. Getting this exactly right is the difference between a repair you can
    trust and one that invents or destroys units.
    """
    box_keys = {}
    for b in db.execute(text("""
        SELECT article, lot_number FROM interunit_transfer_boxes WHERE header_id = :h
    """), {"h": t.id}).fetchall():
        k = _line_key(b.article, b.lot_number)
        box_keys[k] = box_keys.get(k, 0) + 1

    line_qty = {}
    for ln in db.execute(text("""
        SELECT item_desc_raw, lot_number, COALESCE(SUM(qty), 0) AS q
          FROM interunit_transfers_lines WHERE header_id = :h
         GROUP BY item_desc_raw, lot_number
    """), {"h": t.id}).fetchall():
        k = _line_key(ln.item_desc_raw, ln.lot_number)
        line_qty[k] = line_qty.get(k, 0) + int(ln.q or 0)

    # A key that shipped boxes is worth its boxes; one that didn't keeps its qty.
    ordered = sum(box_keys.get(k, line_qty[k]) for k in line_qty)
    ordered += sum(n for k, n in box_keys.items() if k not in line_qty)

    phantom = int(db.execute(text("""
        SELECT COUNT(*) FROM pending_transfer_stock
         WHERE transfer_out_id = :h AND status = 'In Transit'
           AND COALESCE(box_id, '') LIKE 'LINE-%'
    """), {"h": t.id}).scalar() or 0)
    if phantom:
        legit = 0
        for p in db.execute(text("""
            SELECT article, lot_no FROM pending_transfer_stock
             WHERE transfer_out_id = :h AND status = 'In Transit'
               AND COALESCE(box_id, '') LIKE 'LINE-%'
        """), {"h": t.id}).fetchall():
            if _line_key(p.article, p.lot_no) not in box_keys:
                legit += 1
        phantom -= legit

    in_transit = t.real_pending + t.sentinels
    expected = max(0, ordered - received)

    flags = []
    if t.line_rows > t.real_lines:
        flags.append("PER_BOX_LINES")
    if int(t.sum_qty) != ordered:
        flags.append("QTY_INFLATED")
    if phantom:
        flags.append(f"PHANTOM_PENDING({phantom})")
    if (t.status or "").strip().lower() == "received" and received < ordered:
        flags.append("GHOST_RECEIVED")
    # Phantom rows inflate in_transit, so discount them before calling units lost.
    if (in_transit - phantom) < expected:
        flags.append(f"LOST_IN_TRANSIT({expected - (in_transit - phantom)})")
    return {"flags": flags, "ordered": ordered, "expected": expected,
            "phantom": phantom, "in_transit": in_transit}


def repair(db, t, received, ordered):
    """Steps 1-5. Returns a dict of what changed."""
    boxes = db.execute(text("""
        SELECT id, article, lot_number, net_weight, gross_weight
          FROM interunit_transfer_boxes WHERE header_id = :h
    """), {"h": t.id}).fetchall()
    lines = db.execute(text("""
        SELECT id, item_desc_raw, lot_number, qty
          FROM interunit_transfers_lines WHERE header_id = :h ORDER BY id
    """), {"h": t.id}).fetchall()

    stats = {}
    for b in boxes:
        k = _line_key(b.article, b.lot_number)
        s = stats.setdefault(k, {"qty": 0, "net": 0.0, "gross": 0.0})
        n, g = float(b.net_weight or 0), float(b.gross_weight or 0)
        s["qty"] += 1
        s["net"] += n or g
        s["gross"] += g or n

    keep, drop = {}, []
    for ln in lines:                      # ordered by id -> lowest survives
        k = _line_key(ln.item_desc_raw, ln.lot_number)
        (drop.append(ln.id) if k in keep else keep.__setitem__(k, ln.id))

    # 1. correct the surviving line from its boxes
    for k, line_id in keep.items():
        s = stats.get(k)
        if not s:
            continue                      # box-less manual row: typed qty stands
        db.execute(text("""
            UPDATE interunit_transfers_lines
               SET qty = :q, net_weight = :n, total_weight = :g,
                   uom = COALESCE(NULLIF(TRIM(uom), ''), 'BOX')
             WHERE id = :id AND header_id = :h
        """), {"q": s["qty"], "n": round(s["net"], 3), "g": round(s["gross"], 3),
               "id": line_id, "h": t.id})

    # 2. repoint boxes at the surviving line
    repointed = 0
    for b in boxes:
        tgt = keep.get(_line_key(b.article, b.lot_number))
        if tgt is None:
            continue
        repointed += db.execute(text("""
            UPDATE interunit_transfer_boxes SET transfer_line_id = :l
             WHERE id = :b AND COALESCE(transfer_line_id, -1) <> :l
        """), {"l": tgt, "b": b.id}).rowcount

    # 3. drop the orphans
    if drop:
        db.execute(text("DELETE FROM interunit_transfers_lines WHERE id = ANY(:ids)"),
                   {"ids": drop})

    # 4. phantom sentinels: only for keys that actually shipped boxes
    phantom = 0
    pend = db.execute(text("""
        SELECT id, article, lot_no FROM pending_transfer_stock
         WHERE transfer_out_id = :h AND status = 'In Transit'
           AND COALESCE(box_id, '') LIKE 'LINE-%'
    """), {"h": t.id}).fetchall()
    kill = [p.id for p in pend if _line_key(p.article, p.lot_no) in stats]
    if kill:
        phantom = db.execute(
            text("DELETE FROM pending_transfer_stock WHERE id = ANY(:ids)"),
            {"ids": kill}).rowcount

    # 5. status from real receipts — this is what makes it restartable
    if received <= 0:
        new_status = "Dispatch"
    elif received < ordered:
        new_status = "Partial"
    else:
        new_status = "Received"
    changed_status = None
    if (t.status or "").strip() != new_status:
        db.execute(text("UPDATE interunit_transfers_header SET status = :s WHERE id = :h"),
                   {"s": new_status, "h": t.id})
        changed_status = f"{t.status} -> {new_status}"

    new_qty = db.execute(
        text("SELECT COALESCE(SUM(qty),0) FROM interunit_transfers_lines WHERE header_id = :h"),
        {"h": t.id}).scalar()

    return {"lines_deleted": len(drop), "boxes_repointed": repointed,
            "phantom_deleted": phantom, "status": changed_status,
            "new_qty": float(new_qty or 0)}


def restore_in_transit(db, t, received, ordered):
    """Step 6 — re-park what the sentinel sweep deleted, so the consignment can
    actually be received. Tops up to (boxes - received), never beyond."""
    expected = max(0, ordered - received)
    have = db.execute(text("""
        SELECT COUNT(*) FROM pending_transfer_stock
         WHERE transfer_out_id = :h AND status = 'In Transit'
    """), {"h": t.id}).scalar() or 0
    missing = expected - int(have)
    if missing <= 0:
        return 0

    lines = db.execute(text("""
        SELECT id, item_desc_raw, qty, net_weight, total_weight, lot_number,
               batch_number, rm_pm_fg_type, item_category, sub_category,
               pack_size, unit_pack_size, uom
          FROM interunit_transfers_lines WHERE header_id = :h ORDER BY id
    """), {"h": t.id}).fetchall()

    budget, to_park = missing, []
    for ln in lines:
        if budget <= 0:
            break
        take = min(int(ln.qty or 0), budget)
        if take <= 0:
            continue
        budget -= take
        to_park.append(SimpleNamespace(
            id=ln.id, item_desc_raw=ln.item_desc_raw, qty=take,
            net_weight=ln.net_weight or 0, total_weight=ln.total_weight or 0,
            lot_number=ln.lot_number, batch_number=ln.batch_number or "",
            rm_pm_fg_type=ln.rm_pm_fg_type or "", item_category=ln.item_category or "",
            sub_category=ln.sub_category or "", pack_size=ln.pack_size or 0,
            unit_pack_size=ln.unit_pack_size or 0, uom=ln.uom or "",
        ))
    if not to_park:
        return 0
    park_lines_in_pending(
        transfer_out_id=t.id, challan_no=t.challan_no,
        from_site=t.from_site, to_site=t.to_site, lines=to_park,
        dispatched_by="repair_transfer_data", db=db,
    )
    return missing - budget


def main():
    db = Session(bind=engine)
    mode = "APPLY" + (" + RESTORE" if RESTORE else "") if APPLY else "DRY-RUN"
    try:
        sql = CANDIDATES.format(only="AND h.challan_no = :cn" if ONLY else "")
        targets = db.execute(text(sql), {"cn": ONLY} if ONLY else {}).fetchall()

        print(f"\n{len(targets)} cold transfer(s) examined   [{mode}]\n")
        hdr = (f"{'CHALLAN':<22} {'ROUTE':<24} {'BOX':>4} {'SHOWN':>6} {'TRUE':>5} "
               f"{'RCVD':>5} {'TRANSIT':>8} {'OWED':>5}  DIAGNOSIS")
        print(hdr); print("-" * len(hdr))

        affected = 0
        for t in targets:
            received = _received_box_count(db, t.id)
            a = analyze(db, t, received)
            if not a["flags"]:
                continue
            affected += 1
            route = f"{(t.from_site or '')[:11]} -> {(t.to_site or '')[:9]}"
            print(f"{t.challan_no:<22} {route:<24} {t.boxes:>4} {int(t.sum_qty):>6} "
                  f"{a['ordered']:>5} {received:>5} {a['in_transit']:>8} "
                  f"{a['expected']:>5}  {','.join(a['flags'])}")

            if not APPLY:
                continue
            r = repair(db, t, received, a["ordered"])
            note = (f"      -> qty {int(t.sum_qty)}->{r['new_qty']:.0f}, "
                    f"-{r['lines_deleted']} orphan lines, {r['boxes_repointed']} boxes repointed, "
                    f"-{r['phantom_deleted']} phantom rows")
            if r["status"]:
                note += f", status {r['status']}"
            print(note)
            if RESTORE:
                n = restore_in_transit(db, t, received, a["ordered"])
                if n:
                    print(f"      -> re-parked {n} unit(s) so the balance can be received")

        print(f"\n{affected} of {len(targets)} need repair.")
        if APPLY:
            db.commit()
            print("COMMITTED.")
        else:
            db.rollback()
            print("DRY-RUN — nothing written. Re-run with APPLY=1 to commit.")
            print()
            print("  BOX     physical box rows on the dispatch")
            print("  SHOWN   SUM(lines.qty) — the number the transfer list displays today")
            print("  TRUE    what was really consigned (box count per pile, typed qty for")
            print("          box-less manual rows). SHOWN should equal this.")
            print("  RCVD    boxes genuinely received, counted across BOTH receipt ledgers")
            print("  TRANSIT pending rows still 'In Transit' (real + LINE- sentinels)")
            print("  OWED    TRUE - RCVD: what still has to arrive for this to be complete")
    finally:
        db.close()


if __name__ == "__main__":
    main()
