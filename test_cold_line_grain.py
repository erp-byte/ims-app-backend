"""A cold dispatch line is per (article, lot) — never per box.

THE BUG THIS PINS
    The cold form built its `lines` payload by mapping over scannedBoxes, which
    holds one entry per PHYSICAL BOX (`quantityUnits: '1'`), so it posted one
    line per box at qty 1. The DB grain is one row per (article, lot), which is
    what `line_id_by_key` and `_apply_box_totals` both assume.

    So N per-box rows collapsed onto a single key: the last insert won the
    mapping, every earlier row was orphaned, and `_apply_box_totals` — which only
    rewrites lines that own boxes — left the orphans holding their raw per-box
    qty forever.

    The inflation is exactly 2B - K (B boxes, K distinct (article, lot) piles):
    all B rows go in at qty 1, then `_apply_box_totals` rewrites the K rows that
    own boxes up to their real pile counts, leaving B - K orphans at qty 1.

    TRANS202608171318 (Cold Storage -> A185, 17 Aug 2026) shipped 100 boxes of 2
    articles and the transfer list showed "2 Items / Qty: 198":
        60 + 40    -> the 2 mapped lines, corrected by their boxes
        +  98      -> orphan rows still at the qty 1 they were posted with
        = 198      =  2(100) - 2
    Confirmed on the live row: 100 line rows, 2 distinct items, SUM(qty) 198.

    The same 98 leaked into pending_transfer_stock as phantom 'LINE-' sentinels,
    because the uncovered-qty reconciler budgets real boxes against SUM(qty) —
    inventory that was never physically shipped.

WHY THE GUARD LIVES IN THE BACKEND
    Fixing only the form would leave every older frontend build free to reproduce
    it. `_coalesce_lines` enforces the grain on the way in, for any caller.

Dependency-free:  python test_cold_line_grain.py
"""
from services.ims_service.cold_transfer_out_tools import (
    ColdOutBoxInput,
    ColdOutLineInput,
    _coalesce_lines,
    _line_key,
)

A = "PM24-D MART PREMIA DRIED BLUEBERRY STANDY POUCH 100 GM"
B = "PM24-D MART PREMIA NUTS AND SEED MIX 100 GM"


def _boxes(article, lot, n, start=0):
    return [
        ColdOutBoxInput(
            box_id=f"{article[:4]}-{lot}-{i}",
            transaction_no="TXN1",
            lot_no=lot,
            item_description=article,
            weight_kg=1.5,
        )
        for i in range(start, start + n)
    ]


def _per_box_lines(article, lot, n, qty=1.0):
    """What the cold form used to post: one line per physical box, qty 1 each."""
    return [
        ColdOutLineInput(
            item_desc_raw=article, qty=float(qty), lot_number=lot,
            net_weight=1.5, total_weight=1.5, uom="BOX",
        )
        for _ in range(n)
    ]


def _sum_qty_as_stored(lines, boxes):
    """SUM(qty) the old code left on disk: `_apply_box_totals` corrects only the
    lines that own boxes (one per key), every other row keeps its posted qty."""
    counts = {}
    for b in boxes:
        counts[_line_key(b.item_description, b.lot_no)] = (
            counts.get(_line_key(b.item_description, b.lot_no), 0) + 1
        )
    corrected, total = set(), 0.0
    for line in lines:
        k = _line_key(line.item_desc_raw, line.lot_number)
        if k in counts and k not in corrected:
            corrected.add(k)
            total += counts[k]          # rewritten to the real pile count
        else:
            total += float(line.qty or 0)
    return total


def test_the_198_case():
    """The exact reported shape: 100 boxes, 2 piles, posted as 100 lines of qty 1."""
    lines = _per_box_lines(A, "L1", 60) + _per_box_lines(B, "L2", 40)
    boxes = _boxes(A, "L1", 60) + _boxes(B, "L2", 40)    # 100 real boxes

    # Reproduce what the old path left on disk: 2B - K = 2(100) - 2 = 198.
    assert _sum_qty_as_stored(lines, boxes) == 198, "fixture must reproduce the bug"

    out = _coalesce_lines(lines, boxes)

    assert len(out) == 2, f"expected one row per (article, lot), got {len(out)}"
    assert sum(l.qty for l in out) == 100, (
        f"SUM(qty) must equal the box count, got {sum(l.qty for l in out)}"
    )
    assert _sum_qty_as_stored(out, boxes) == 100, "must stay 100 after _apply_box_totals"
    by_item = {l.item_desc_raw: l.qty for l in out}
    assert by_item[A] == 60, by_item
    assert by_item[B] == 40, by_item


def test_the_2b_minus_k_formula_holds_for_other_shapes():
    """One pile gives 2B-1, three piles give 2B-3 — and all collapse to B."""
    for piles in ([("L1", 100)], [("L1", 50), ("L2", 30), ("L3", 20)]):
        lines, boxes = [], []
        for lot, n in piles:
            lines += _per_box_lines(A, lot, n)
            boxes += _boxes(A, lot, n, start=len(boxes))
        assert _sum_qty_as_stored(lines, boxes) == 200 - len(piles)
        out = _coalesce_lines(lines, boxes)
        assert len(out) == len(piles)
        assert sum(l.qty for l in out) == 100


def test_no_phantom_pending_stock_remains():
    """Uncovered qty is what leaks into pending_transfer_stock as 'LINE-' rows."""
    lines = _per_box_lines(A, "L1", 60)
    boxes = _boxes(A, "L1", 60)
    out = _coalesce_lines(lines, boxes)
    covered = len(boxes)
    uncovered = sum(max(0, int(l.qty) - covered) for l in out)
    assert uncovered == 0, f"{uncovered} phantom units would be parked"


def test_a_correct_payload_is_left_alone():
    """One line per article with qty = box count must survive untouched."""
    lines = [
        ColdOutLineInput(item_desc_raw=A, qty=16, lot_number="L1", net_weight=207.08),
        ColdOutLineInput(item_desc_raw=B, qty=25, lot_number="L2", net_weight=389.0),
    ]
    boxes = _boxes(A, "L1", 16) + _boxes(B, "L2", 25)
    out = _coalesce_lines(lines, boxes)
    assert len(out) == 2
    assert [l.qty for l in out] == [16, 25]
    assert out[0].net_weight == 207.08, "weights must not be recomputed here"


def test_manual_rows_without_boxes_keep_their_typed_qty():
    """The 'never-drop manual entries' path: no boxes means nothing to defer to."""
    lines = [
        ColdOutLineInput(item_desc_raw=A, qty=12, lot_number="L1"),   # typed, no boxes
        ColdOutLineInput(item_desc_raw=B, qty=99, lot_number="L2"),   # boxes below
    ]
    out = _coalesce_lines(lines, _boxes(B, "L2", 40))
    by_item = {l.item_desc_raw: l.qty for l in out}
    assert by_item[A] == 12, "a box-less line must keep what the operator typed"
    assert by_item[B] == 40, "a box-backed line must defer to its boxes"


def test_lotless_stock_matches_across_the_na_sentinel():
    """Lines post a real null lot; boxes post the string 'N/A' for the same pile."""
    assert _line_key(A, None) == _line_key(A, "N/A") == _line_key(A, "")
    out = _coalesce_lines(
        _per_box_lines(A, None, 30),
        [
            ColdOutBoxInput(box_id=f"b{i}", transaction_no="T", lot_no="N/A",
                            item_description=A, weight_kg=1.0)
            for i in range(30)
        ],
    )
    assert len(out) == 1
    assert out[0].qty == 30, "lot-less boxes must still attach to their line"


def test_article_case_does_not_split_a_line():
    """line_id_by_key compared case-sensitively while the reconciler upper-cased."""
    lines = [
        ColdOutLineInput(item_desc_raw=A, qty=1, lot_number="L1"),
        ColdOutLineInput(item_desc_raw=A.lower(), qty=1, lot_number="L1"),
    ]
    out = _coalesce_lines(lines, _boxes(A, "L1", 2))
    assert len(out) == 1, "same article in different case must be one line"
    assert out[0].qty == 2


def test_weights_are_summed_when_rows_merge():
    lines = _per_box_lines(A, "L1", 4)   # 4 rows, 1.5 kg each
    out = _coalesce_lines(lines, _boxes(A, "L1", 4))
    assert out[0].net_weight == 6.0, out[0].net_weight
    assert out[0].total_weight == 6.0, out[0].total_weight


def test_descriptors_survive_the_merge():
    """The first non-empty value stands in — a later blank must not erase it."""
    lines = [
        ColdOutLineInput(item_desc_raw=A, qty=1, lot_number="L1",
                         uom="BOX", item_category="DRY FRUIT", pack_size=2000),
        ColdOutLineInput(item_desc_raw=A, qty=1, lot_number="L1",
                         uom=None, item_category="", pack_size=0),
    ]
    out = _coalesce_lines(lines, _boxes(A, "L1", 2))
    assert out[0].uom == "BOX"
    assert out[0].item_category == "DRY FRUIT"
    assert out[0].pack_size == 2000


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"  ok  {name}")
        except AssertionError as exc:
            failures += 1
            print(f"  FAIL {name}: {exc}")
    print()
    if failures:
        raise SystemExit(f"{failures} cold-line-grain test(s) failed.")
    print("All cold-line-grain tests passed.")
