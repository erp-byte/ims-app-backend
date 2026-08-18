"""One-time repair: collapse cold transfers written with one line PER BOX.

THE DAMAGE THIS REPAIRS
    The cold form posted `lines` as one row per box, each stamped with that box's
    `quantity_units` (bags per box) instead of a box count. `interunit_transfers_lines`
    is per (article, lot), so those rows collapsed onto one key: the last insert won
    the box mapping, the rest were orphaned, and `_apply_box_totals` — which only
    rewrites lines that own boxes — left the orphans holding their per-box qty.

    TRANS202608171318: 100 boxes, 2 articles, 100 line rows, SUM(qty) 198
    (= 100 correct on the 2 mapped lines + 98 stranded on orphans).

    The same 98 were parked into pending_transfer_stock as phantom 'LINE-%'
    sentinels — in-transit inventory that was never physically shipped.

    The write path is fixed (cold_transfer_out_tools._coalesce_lines); this only
    repairs rows already on disk.

WHAT IT DOES, PER AFFECTED HEADER
    1. keeps the LOWEST line id per (article, lot), sets its qty to that key's real
       box count and its weights to the boxes' summed net/gross — exactly what
       _apply_box_totals would have written;
    2. repoints interunit_transfer_boxes.transfer_line_id at the surviving line;
    3. deletes the orphan line rows;
    4. deletes phantom 'LINE-%' pending rows ONLY for keys that have real boxes.
       A key with no boxes is a genuine manual entry the 'never-drop' branch parks
       on purpose — those are left alone.

WHAT IT REFUSES TO TOUCH
    Headers not in status 'Dispatch', or with any pending row already acknowledged
    / received. Repointing lines under a completed receive is not safe to automate;
    those are listed for manual handling instead.

DRY-RUN by default (does the work, prints it, then ROLLS BACK).
Set APPLY=1 to commit.

    DATABASE_URL=... python repair_cold_per_box_lines.py
    DATABASE_URL=... APPLY=1 python repair_cold_per_box_lines.py
"""
import os

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from services.ims_service.cold_transfer_out_tools import _line_key

raw = os.environ["DATABASE_URL"]
URL = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
APPLY = os.environ.get("APPLY") == "1"

engine = create_engine(URL)

# The fingerprint of the bug: a cold-source header whose line rows outnumber its
# distinct (article, lot) pairs. A warehouse header can legitimately carry two
# lines for one article (different pack sizes), so this stays scoped to cold.
CANDIDATES = text("""
    SELECT h.id, h.challan_no, h.stock_trf_date, h.from_site, h.to_site, h.status,
           COUNT(l.id)                                                    AS line_rows,
           COUNT(DISTINCT (UPPER(TRIM(l.item_desc_raw)),
                           UPPER(TRIM(COALESCE(l.lot_number, '')))))      AS real_lines,
           SUM(l.qty)                                                     AS total_qty,
           (SELECT COUNT(*) FROM interunit_transfer_boxes b
             WHERE b.header_id = h.id)                                    AS boxes
    FROM interunit_transfers_header h
    JOIN interunit_transfers_lines l ON l.header_id = h.id
    WHERE h.from_site ILIKE 'cold%' OR h.from_cold_unit IS NOT NULL
    GROUP BY h.id
    HAVING COUNT(l.id) > COUNT(DISTINCT (UPPER(TRIM(l.item_desc_raw)),
                                         UPPER(TRIM(COALESCE(l.lot_number, '')))))
    ORDER BY h.stock_trf_date DESC, h.id
""")


def _unsafe_reason(db, header) -> str | None:
    if (header.status or "") != "Dispatch":
        return f"status is {header.status!r}, not 'Dispatch'"
    received = db.execute(text("""
        SELECT COUNT(*) FROM pending_transfer_stock
         WHERE transfer_out_id = :hid AND COALESCE(status, '') <> 'In Transit'
    """), {"hid": header.id}).scalar()
    if received:
        return f"{received} pending row(s) already past 'In Transit'"
    return None


def repair_header(db, header) -> dict:
    boxes = db.execute(text("""
        SELECT id, article, lot_number, net_weight, gross_weight
          FROM interunit_transfer_boxes WHERE header_id = :hid
    """), {"hid": header.id}).fetchall()
    lines = db.execute(text("""
        SELECT id, item_desc_raw, lot_number, qty
          FROM interunit_transfers_lines WHERE header_id = :hid ORDER BY id
    """), {"hid": header.id}).fetchall()

    box_stats: dict = {}
    for b in boxes:
        k = _line_key(b.article, b.lot_number)
        s = box_stats.setdefault(k, {"qty": 0, "net": 0.0, "gross": 0.0})
        n, g = float(b.net_weight or 0), float(b.gross_weight or 0)
        s["qty"] += 1
        s["net"] += n or g
        s["gross"] += g or n

    keep: dict = {}
    drop: list = []
    for l in lines:
        k = _line_key(l.item_desc_raw, l.lot_number)
        if k in keep:
            drop.append(l.id)          # lines are ordered by id: lowest survives
        else:
            keep[k] = l.id

    for k, line_id in keep.items():
        stats = box_stats.get(k)
        if not stats:
            continue                   # box-less manual line: its typed qty stands
        db.execute(text("""
            UPDATE interunit_transfers_lines
               SET qty = :qty, net_weight = :net, total_weight = :gross,
                   uom = COALESCE(NULLIF(TRIM(uom), ''), 'BOX')
             WHERE id = :id AND header_id = :hid
        """), {"qty": stats["qty"], "net": round(stats["net"], 3),
               "gross": round(stats["gross"], 3), "id": line_id, "hid": header.id})

    repointed = 0
    for b in boxes:
        target = keep.get(_line_key(b.article, b.lot_number))
        if target is None:
            continue
        repointed += db.execute(text("""
            UPDATE interunit_transfer_boxes SET transfer_line_id = :lid
             WHERE id = :bid AND COALESCE(transfer_line_id, -1) <> :lid
        """), {"lid": target, "bid": b.id}).rowcount

    if drop:
        db.execute(text("DELETE FROM interunit_transfers_lines WHERE id = ANY(:ids)"),
                   {"ids": drop})

    # Phantom sentinels: only for keys that actually shipped boxes.
    phantom = 0
    pending = db.execute(text("""
        SELECT id, article, lot_number FROM pending_transfer_stock
         WHERE transfer_out_id = :hid AND status = 'In Transit'
           AND COALESCE(box_id, '') LIKE 'LINE-%'
    """), {"hid": header.id}).fetchall()
    kill = [p.id for p in pending if _line_key(p.article, p.lot_number) in box_stats]
    if kill:
        phantom = db.execute(
            text("DELETE FROM pending_transfer_stock WHERE id = ANY(:ids)"),
            {"ids": kill}).rowcount

    new_qty = db.execute(text("SELECT COALESCE(SUM(qty),0) FROM interunit_transfers_lines "
                              "WHERE header_id = :hid"), {"hid": header.id}).scalar()
    return {"lines_deleted": len(drop), "boxes_repointed": repointed,
            "phantom_deleted": phantom, "new_total_qty": float(new_qty or 0)}


def main():
    db = Session(bind=engine)
    try:
        targets = db.execute(CANDIDATES).fetchall()
        print(f"{len(targets)} cold header(s) written with per-box lines "
              f"({'APPLY' if APPLY else 'DRY-RUN'}).\n")

        skipped, fixed = [], 0
        for h in targets:
            reason = _unsafe_reason(db, h)
            label = (f"  {h.challan_no}  {h.from_site} -> {h.to_site}  "
                     f"lines={h.line_rows} real={h.real_lines} "
                     f"qty={h.total_qty} boxes={h.boxes}")
            if reason:
                skipped.append((h.challan_no, reason))
                print(f"{label}\n      SKIP — {reason}")
                continue
            r = repair_header(db, h)
            fixed += 1
            print(f"{label}\n      qty {h.total_qty} -> {r['new_total_qty']:.0f} "
                  f"(boxes {h.boxes}) | -{r['lines_deleted']} lines, "
                  f"{r['boxes_repointed']} boxes repointed, "
                  f"-{r['phantom_deleted']} phantom pending rows")

        print(f"\n{fixed} repaired, {len(skipped)} skipped.")
        if skipped:
            print("\nNeed manual handling (already received / not in Dispatch):")
            for challan, reason in skipped:
                print(f"  {challan}: {reason}")

        if APPLY:
            db.commit()
            print("\nCOMMITTED.")
        else:
            db.rollback()
            print("\nDRY-RUN — rolled back. Re-run with APPLY=1 to commit.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
