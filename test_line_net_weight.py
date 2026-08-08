"""Deriving a line's net weight must follow the material's own rule.

THE RULES (owner, 2026-08-07)
    PM / packaging   kg per box = pack_size            (unit_pack_size is a
                                                        piece count, not a weight)
    FG / RM          kg per box = pack_size x unit_pack_size

THE BUG THIS PINS
    create_request, create_transfer and update_transfer each carried:

        if provided > 0:            net = provided
        elif material == "FG":      net = unit_pack_size * pack_size * qty
        else:                       net = pack_size * qty          <-- RM lands here

    RM is not PM. It needs unit_pack_size, and the `else` silently dropped it.
    The branch fires whenever the form omits net_weight — which the transfer
    form always does — so it governs every manually-keyed line. 261 live RM
    lines carry a figure this produced, e.g. pack_size 30 x unit_pack_size 34
    stored as 30.00 kg.

    A caveat worth keeping: what the RIGHT number is also depends on issue B3,
    the unit_pack_size grams-vs-kilograms inconsistency. This function fixes
    which formula is applied; it cannot fix a unit that was keyed wrong.

Dependency-free:  python test_line_net_weight.py
"""
from services.ims_service.interunit_tools import line_net_weight


def test_a_provided_weight_always_wins():
    """The operator's own figure is never second-guessed."""
    assert line_net_weight("RM", 16, 0.5, 20, provided=123.456) == 123.456
    assert line_net_weight("PM", 8.8, 5000, 28, provided=1.0) == 1.0


def test_pm_uses_pack_size_times_qty():
    """28 cartons at 8.8 kg. unit_pack_size 5000 is a piece count — ignored."""
    assert line_net_weight("PM", 8.8, 5000, 28) == 246.4
    assert line_net_weight("PM", 12, 1, 10) == 120.0


def test_pm_by_category_when_material_type_is_blank():
    assert line_net_weight("", 8.8, 5000, 28, item_category="PACKAGING") == 246.4


def test_fg_uses_pack_size_times_unit_pack():
    """100 packs x 100 g = 10 kg a box."""
    assert line_net_weight("FG", 100, 0.1, 5) == 50.0


def test_rm_uses_pack_size_times_unit_pack():
    """The regression: RM must NOT fall through to pack_size x qty.
    16 packs x 500 g = 8 kg a box, 50 boxes = 400 kg — not 800."""
    assert line_net_weight("RM", 16, 0.5, 50) == 400.0


def test_the_live_case_that_exposed_it():
    """pack_size 30 x unit_pack_size 34, qty 1 — stored as 30.00 kg."""
    assert line_net_weight("RM", 30, 34, 1) == 1020.0
    # what the old code produced, kept here so the difference is explicit
    assert 30 * 1 == 30.0


def test_rm_without_a_unit_pack_falls_back_to_pack_size():
    """No unit pack recorded: pack_size is the only reading left."""
    assert line_net_weight("RM", 10, 0, 5) == 50.0
    assert line_net_weight("RM", 10, None, 5) == 50.0


def test_zero_and_junk_inputs_give_zero():
    assert line_net_weight("RM", 0, 0.5, 20) == 0.0
    assert line_net_weight("RM", 16, 0.5, 0) == 0.0
    assert line_net_weight(None, None, None, None) == 0.0
    assert line_net_weight("RM", "abc", "x", "y") == 0.0


def test_result_is_rounded_to_three_places():
    assert line_net_weight("RM", 3, 0.333, 1) == 0.999


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
    print("\nPM: pack_size x qty.  FG/RM: pack_size x unit_pack_size x qty.")
