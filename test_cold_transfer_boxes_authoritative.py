"""The cold dispatch line must report what its boxes actually weigh.

THE BUG THIS PINS
    A cold transfer's line carried the PER-BOX weight in net_weight while the
    boxes attached to it held the real total, so the delivery challan printed
    one box instead of the consignment. TRANS202608061358 went out on 6 Aug
    2026 as "10.00 kg" against 750 boxes weighing 7,500 kg; a 500 kg pick of
    ten 50 kg boxes printed as 50 kg. 661 of 924 box-backed lines were wrong,
    510,824 kg unreported, and it was still happening the day it was found.

WHY THE EXISTING GUARD MISSED IT
    `interunit_tools._boxes_authoritative` already enforces this for warehouse
    dispatches, but it returns early for cold sites and is never called from the
    cold path at all. It also groups boxes by article ALONE, which is why it had
    to be excluded: a cold line is per (article, lot), and collapsing by article
    would merge two lots into one line and lose the lot.

    So the cold path gets its own fold, keyed the same way the cold path already
    links boxes to lines — by transfer_line_id, which is exact.

Dependency-free:  python test_cold_transfer_boxes_authoritative.py
"""
from services.ims_service.cold_transfer_out_tools import totals_by_line


def test_qty_and_weight_come_from_the_boxes():
    """The reported failure: one line, many boxes, line shows a single box."""
    # 750 boxes of 10 kg — the real TRANS202608061358 shape.
    totals = totals_by_line([(11, 10.0, 10.6)] * 750)
    assert totals[11]["qty"] == 750, totals
    assert round(totals[11]["net"], 2) == 7500.00, totals
    assert round(totals[11]["gross"], 2) == 7950.00, totals


def test_the_500_shown_as_50_case():
    """Ten 50 kg boxes must report 500, not 50."""
    totals = totals_by_line([(7, 50.0, 51.0)] * 10)
    assert totals[7]["qty"] == 10
    assert round(totals[7]["net"], 3) == 500.0


def test_lots_stay_separate():
    """Two lots of the same article are two lines; folding must not merge them.
    This is exactly what stopped the warehouse helper being reused here."""
    rows = [(21, 10.0, 10.6)] * 3 + [(22, 25.0, 26.0)] * 4
    totals = totals_by_line(rows)
    # Rounded on comparison: the fold accumulates at full float precision and the
    # caller rounds once when it writes, so 3 x 10.6 is 31.799999999999997 here.
    assert (totals[21]["qty"], round(totals[21]["net"], 3),
            round(totals[21]["gross"], 3)) == (3, 30.0, 31.8), totals[21]
    assert (totals[22]["qty"], round(totals[22]["net"], 3),
            round(totals[22]["gross"], 3)) == (4, 100.0, 104.0), totals[22]


def test_pm_boxes_weigh_their_gross_when_net_is_blank():
    """PM boxes carry weight in gross only; a real box must never count as 0 kg."""
    totals = totals_by_line([(31, 0.0, 12.5), (31, 0.0, 12.5)])
    assert round(totals[31]["net"], 2) == 25.00, totals
    assert round(totals[31]["gross"], 2) == 25.00, totals


def test_net_falls_back_to_gross_and_vice_versa():
    totals = totals_by_line([(41, 9.0, 0.0)])
    assert totals[41]["net"] == 9.0 and totals[41]["gross"] == 9.0


def test_unlinked_boxes_are_ignored_not_crashed():
    """A box whose line could not be resolved must not fabricate a line."""
    totals = totals_by_line([(None, 10.0, 10.5), (5, 10.0, 10.5)])
    assert list(totals) == [5], totals


def test_no_boxes_means_no_correction():
    """Weight-only transfers carry no boxes; their typed figures must survive."""
    assert totals_by_line([]) == {}


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
    print("\nAll cold-transfer box-authority tests passed.")
