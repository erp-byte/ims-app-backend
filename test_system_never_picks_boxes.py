"""The system must never choose a box. Only a scan chooses a box.  (C1)

THE RULE (owner, 2026-08-07)
    A box is recorded on a transfer because an operator scanned it. If nothing
    was scanned, the transfer carries the article and the quantity and NO boxes.
    The system does not go to the stock tables and pick some for you.

WHAT IT USED TO DO
    Article Entry sends lines but no boxes. create_transfer/update_transfer then
    ran _auto_derive_warehouse_boxes, which read the real box tables and took the
    OLDEST rows FIFO:

        if (not data.boxes) and lines and not _is_cold_site(...):
            data.boxes = _auto_derive_warehouse_boxes(...)

    Those boxes were never the ones on the truck. It is why 260 of 329
    (challan, GRN) groups are one contiguous run and 162 of them start at box #1
    — the fingerprint of an allocator, not of a person picking off a pallet. A
    July 16 kg lot was booked against March lot CF100326 at 10 kg this way.

WHAT HAPPENS INSTEAD
    Nothing is picked. The transfer falls through to park_lines_in_pending and
    is tracked at line level (the 'LINE-' paper-only rows) — which is already
    83% of transfer volume. No stock table is read, and none is written.

    Scanned transfers are untouched: boxes present -> boxes used, exactly as before.

Dependency-free:  python test_system_never_picks_boxes.py
"""
from datetime import date, datetime
from types import SimpleNamespace

from services.ims_service import interunit_tools as I
from services.ims_service.interunit_models import (
    BoxCreate, TransferCreate, TransferHeaderCreate, TransferLineCreate,
)


class Res:
    def __init__(self, rows=None, scalar=None):
        self._rows, self._scalar = rows or [], scalar
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar
    def __iter__(self): return iter(self._rows)   # _uncovered_lines iterates directly


HEADER_ROW = SimpleNamespace(
    id=1, challan_no="TRANS-TEST", stock_trf_date=date(2026, 8, 7),
    from_site="A185", to_site="W202", vehicle_no="MH-01-AB-1234",
    driver_name=None, approved_by=None, remark="", reason_code="",
    status="Dispatch", request_id=None, created_by="tester",
    created_ts=datetime(2026, 8, 7, 15, 52), approved_ts=None, has_variance=False,
)
LINE_ROW = SimpleNamespace(
    id=11, header_id=1, rm_pm_fg_type="RM", item_category="C", sub_category="S",
    item_desc_raw="DATES", pack_size=10.0, qty=20, uom="KG", unit_pack_size=1.0,
    net_weight=200.0, total_weight=200.0, batch_number="", lot_number="CF150726",
    vakkal="", created_at=None, updated_at=None,
)


STOCK_TABLES = ("boxes_v2", "bulk_entry_boxes", "cold_stocks")


def _db(box_inserts, stock_reads):
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if any(t in sql for t in STOCK_TABLES):
                stock_reads.append(sql.strip().split("\n")[0][:70])
            if "INSERT INTO interunit_transfers_header" in sql:
                return Res(rows=[HEADER_ROW])
            if "INSERT INTO interunit_transfers_lines" in sql:
                return Res(rows=[LINE_ROW])
            if "INSERT INTO interunit_transfer_boxes" in sql:
                box_inserts.append(params or {})
                return Res(rows=[SimpleNamespace(
                    id=len(box_inserts), header_id=1, transfer_line_id=11,
                    box_number=(params or {}).get("box_number"),
                    box_id=(params or {}).get("box_id"),
                    article=(params or {}).get("article"), lot_number=None,
                    batch_number=None, transaction_no=None, net_weight=0,
                    gross_weight=0, created_at=None, updated_at=None)])
            if "SELECT DISTINCT transfer_line_id FROM interunit_transfer_boxes" in sql:
                # Faithfully apply the same filter the real query asks for, so the
                # real _uncovered_lines is exercised rather than stubbed away.
                rows = box_inserts
                if "NOT LIKE 'ART-%'" in sql:
                    rows = [b for b in rows
                            if not str(b.get("box_id", "")).startswith("ART-")]
                return Res(rows=[(11,) for _ in rows])
            if "FROM interunit_transfers_header" in sql:
                return Res(rows=[HEADER_ROW])
            return Res()
    return DB()


def _payload(boxes):
    return TransferCreate(
        header=TransferHeaderCreate(
            stock_trf_date="07-08-2026", from_warehouse="A185",
            to_warehouse="W202", vehicle_no="MH-01-AB-1234",
        ),
        lines=[TransferLineCreate(
            material_type="RM", item_category="C", sub_category="S",
            item_description="DATES", quantity="20", pack_size="10",
            lot_number="CF150726",
        )],
        boxes=boxes,
    )


def _run(payload):
    """Drive create_transfer with every collaborator stubbed, recording calls."""
    calls = {"auto_derive": 0, "park_boxes": 0, "park_lines": 0,
             "box_inserts": [], "stock_reads": []}
    orig = {n: getattr(I, n) for n in (
        "unique_challan_no", "_boxes_authoritative",
        "park_in_pending", "park_lines_in_pending", "reconcile_transfer_to_order",
        "_auto_derive_warehouse_boxes",
    )}   # NOTE: _uncovered_lines is deliberately NOT stubbed — it is under test.

    def spy_auto_derive(db, from_site, lines):
        calls["auto_derive"] += 1
        return [BoxCreate(box_number=1, box_id="FIFO-OLDEST-1", article="DATES",
                          lot_number="CF100326", transaction_no="TR-MARCH")]

    def spy_park_boxes(**kw):
        calls["park_boxes"] += 1
        return 0

    def spy_park_lines(**kw):
        calls["park_lines"] += 1
        return 0

    I.unique_challan_no = lambda db, requested: "TRANS-TEST"
    I._boxes_authoritative = lambda db, hid, wh, boxes, lines: lines
    I.park_in_pending = spy_park_boxes
    I.park_lines_in_pending = spy_park_lines
    I.reconcile_transfer_to_order = lambda hid, db, dry_run=False: {}
    I._auto_derive_warehouse_boxes = spy_auto_derive
    try:
        I.create_transfer(payload, "tester",
                          _db(calls["box_inserts"], calls["stock_reads"]))
    finally:
        for n, fn in orig.items():
            setattr(I, n, fn)
    return calls


def test_article_entry_does_not_pick_boxes_from_stock():
    """THE BUG. No scan -> the system must not go and choose boxes itself."""
    calls = _run(_payload(boxes=None))
    assert calls["auto_derive"] == 0, (
        "system auto-picked boxes from the stock tables when the operator "
        "scanned nothing — only a scan may choose a box"
    )


def test_article_entry_is_recorded_box_wise_as_ART_ids():
    """The dispatch is still box-wise: qty 20 -> 20 rows, ART-1 .. ART-20.

    Not zero boxes. The operator asked for a box-wise document; what changed is
    only WHERE the ids come from — generated, not taken off the stock sheet.
    """
    calls = _run(_payload(boxes=None))
    ids = [b["box_id"] for b in calls["box_inserts"]]
    assert ids == [f"ART-{n}" for n in range(1, 21)], ids


def test_synthetic_boxes_carry_no_source_identity():
    """transaction_no must stay blank: these point at no real stock row, and a
    blank txn is what makes park_in_pending skip them instead of deducting."""
    calls = _run(_payload(boxes=None))
    assert all(not (b["transaction_no"] or "") for b in calls["box_inserts"])


def test_real_park_in_pending_refuses_a_synthetic_box():
    """Driving the REAL park_in_pending (not the spy): a blank-txn box parks
    nothing and issues no SQL, so a generated label can never move real stock."""
    from services.ims_service import pending_stock_tools as P

    issued = []

    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            issued.append(sql)
            if "FROM interunit_transfers_header" in sql:
                return Res(rows=[(date(2026, 8, 7), datetime(2026, 8, 7))])
            return Res()

    parked = P.park_in_pending(
        transfer_out_id=1, challan_no="TRANS-TEST", from_site="A185",
        to_site="W202",
        boxes=[BoxCreate(box_number=1, box_id="ART-1", article="DATES",
                         transaction_no="")],
        dispatched_by="tester", db=DB(),
    )
    assert parked == 0, parked
    assert not any("INSERT INTO pending_transfer_stock" in s for s in issued)
    assert not any("DELETE FROM" in s for s in issued)


def test_generating_boxes_reads_no_stock_table():
    """The whole point: box-wise display must not cost a single stock lookup."""
    calls = _run(_payload(boxes=None))
    assert calls["stock_reads"] == [], calls["stock_reads"]


def test_article_entry_still_tracked_at_line_level():
    """THE TRAP. _uncovered_lines counts ANY box row as coverage, so the new
    ART- rows would silently stop park_lines_in_pending and the transfer would
    vanish from the in-transit ledger. Synthetic rows must not count as coverage.
    """
    calls = _run(_payload(boxes=None))
    assert calls["park_lines"] == 1, (
        "line-level pending did not park — the ART- rows were mistaken for real "
        "box coverage, so this transfer has no in-transit record at all"
    )


def test_scanned_boxes_are_still_used():
    """Scanned transfers are untouched by this rule."""
    scanned = [BoxCreate(box_number=1, box_id="45293047-21", article="DATES",
                         lot_number="CF150726", transaction_no="TR-JULY")]
    calls = _run(_payload(boxes=scanned))
    assert calls["auto_derive"] == 0, calls
    assert calls["park_boxes"] == 1, calls


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
    print("\nOnly a scan chooses a box.")
