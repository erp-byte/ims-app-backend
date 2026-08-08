"""A pack size is a COUNT of packs per box, never a weight.

TWO RULES, PICKED BY MATERIAL TYPE (owner, 2026-08-07)

    PM / packaging   kg per box = pack_size
                     unit_pack_size is a piece count, irrelevant to weight.
                     So for PM, pack_size x qty IS the net weight.
    FG / RM          kg per box = pack_size x unit_pack_size
                     pack_size counts retail packs: 100 for a 100 g x 10 kg box,
                     16 for a 500 g x 8 kg box.

    Measured on 34,010 live box lines: the PM rule holds on 98.2% of PM lines,
    the FG/RM rule on 98.7% of the rest. Swapping them is useless — the FG/RM
    formula explains 1.9% of PM lines.

WHAT THIS PINS
    The first version of this check compared `pack_size x qty` against the
    scanned weight and reported 13,338 conflicts. Measured against the real
    formula, 25,343 of 34,010 lines are simply 100 g / 500 g packs and entirely
    correct — the check would have cried wolf on three quarters of the business
    and been switched off inside a week.

    Genuine conflicts are 189 lines over 38 challans. TRANS202608061552 is one:
    pack_size 16 x unit_pack_size 1 gives 16 kg per box, but the 20 scanned
    boxes weigh 10.011 kg each.

Dependency-free:  python test_pack_size_conflict.py
"""
from services.ims_service.interunit_tools import pack_size_conflict


# ── the formula holds: these must all stay silent ─────────────────────────
def test_100g_packs_are_not_a_conflict():
    """Carnival Festive Indian Raisin 100GM — 100 packs x 100 g = 10 kg/box."""
    assert pack_size_conflict(pack_size=100, qty=1, box_net=10.0,
                              unit_pack_size=0.1) is None


def test_500g_packs_are_not_a_conflict():
    """King Solomon Medjoul 500 GMS — 16 packs x 500 g = 8 kg/box, 50 boxes."""
    assert pack_size_conflict(pack_size=16, qty=50, box_net=400.0,
                              unit_pack_size=0.5) is None


def test_250g_packs_are_not_a_conflict():
    assert pack_size_conflict(pack_size=40, qty=10, box_net=100.0,
                              unit_pack_size=0.25) is None


def test_unit_pack_size_in_grams_is_accepted():
    """Some rows store the pack weight in grams. Same line, different unit."""
    assert pack_size_conflict(pack_size=100, qty=1, box_net=10.0,
                              unit_pack_size=100) is None


def test_pack_size_already_the_box_weight():
    """unit_pack_size 1 -> pack_size IS kg per box. 20 boxes x 10.011."""
    assert pack_size_conflict(pack_size=10.011, qty=20, box_net=200.22,
                              unit_pack_size=1) is None


def test_weighing_tolerance_is_not_a_conflict():
    """Real scales wobble; 20 boxes 40 g light is not worth an alarm."""
    assert pack_size_conflict(pack_size=10, qty=20, box_net=199.96,
                              unit_pack_size=1) is None


# ── genuine conflicts: these must be reported ─────────────────────────────
def test_pm_uses_pack_size_directly():
    """PM: pack_size IS kg per box. 28 cartons x 8.8 kg = 246.4 kg."""
    assert pack_size_conflict(pack_size=8.8, qty=28, box_net=246.4,
                              unit_pack_size=5000, material_type="PM") is None


def test_pm_ignores_unit_pack_size_entirely():
    """A piece count of 5000 must not touch the weight check."""
    assert pack_size_conflict(pack_size=12, qty=10, box_net=120.0,
                              unit_pack_size=5000, material_type="PM") is None
    assert pack_size_conflict(pack_size=12, qty=10, box_net=120.0,
                              unit_pack_size=1, material_type="PM") is None


def test_pm_by_category_not_just_material_type():
    assert pack_size_conflict(pack_size=8.8, qty=28, box_net=246.4,
                              unit_pack_size=2000,
                              item_category="PACKAGING") is None


def test_pm_gets_no_special_tolerance():
    """One tolerance for every material (owner, 2026-08-07). A 14.48 kg carton
    weighing 14.20 is out by 0.28 kg and IS reported — the tolerance absorbs
    scale rounding only, it does not excuse a wrong figure."""
    c = pack_size_conflict(pack_size=14.48, qty=1, box_net=14.20,
                           unit_pack_size=2000, material_type="PM")
    assert c is not None and c["material"] == "PM"
    assert c["implied_per_box"] == 14.48 and c["per_box_actual"] == 14.2


def test_pm_that_is_genuinely_wrong_is_still_flagged():
    """TRANS202607081105 — 39 boxes claimed 14.54 kg each, weighed 49.124."""
    c = pack_size_conflict(pack_size=14.54, qty=39, box_net=1915.82,
                           unit_pack_size=2000, material_type="PM")
    assert c is not None and c["material"] == "PM"
    assert c["implied_per_box"] == 14.54


def test_the_reported_challan_is_still_flagged():
    """TRANS202608061552 — 16 x 1 = 16 kg/box claimed, 10.011 kg scanned."""
    c = pack_size_conflict(pack_size=16.0, qty=20, box_net=200.22,
                           unit_pack_size=1.0, material_type="RM")
    assert c is not None, "the reported challan must survive every reading"
    assert c["implied_per_box"] == 16.0
    assert c["per_box_actual"] == 10.011
    assert c["implied_kg"] == 320.0
    assert c["actual_kg"] == 200.22


def test_grams_in_a_kilogram_field_is_flagged():
    """PM lines carrying unit_pack_size 5000 — 20.65 x 5000 is not a box."""
    c = pack_size_conflict(pack_size=20.65, qty=30, box_net=592.95,
                           unit_pack_size=5000, material_type="RM")
    assert c is not None
    assert c["per_box_actual"] == 19.765


def test_missing_inputs_are_not_conflicts():
    """Weight-only transfers carry no pack size and no boxes."""
    assert pack_size_conflict(0, 20, 200.22, 1) is None
    assert pack_size_conflict(16, 0, 200.22, 1) is None
    assert pack_size_conflict(16, 20, 0, 1) is None
    assert pack_size_conflict(None, None, None, None) is None


def test_no_unit_pack_size_falls_back_to_pack_size_as_kg():
    """With no unit pack recorded, the only sane reading is pack_size = kg/box."""
    assert pack_size_conflict(pack_size=10, qty=5, box_net=50.0) is None
    assert pack_size_conflict(pack_size=16, qty=5, box_net=50.0) is not None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
    print("\npack_size x unit_pack_size = kg per box. pack_size x qty is never a weight.")
