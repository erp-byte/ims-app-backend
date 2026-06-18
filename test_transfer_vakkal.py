"""
Vakkal on interunit transfer lines.

No database required:  python test_transfer_vakkal.py

Covers:
  - TransferLineCreate accepts `vakkal` (and the camel/alias line shape still works)
  - _map_transfer_line surfaces `vakkal` from a row
  - _map_transfer_line defaults `vakkal` to "" when the row has no such column
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.ims_service.interunit_models import TransferLineCreate
from services.ims_service.interunit_tools import _map_transfer_line


class Row:
    """Minimal stand-in for a SQLAlchemy Row (attribute access)."""
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _full_row(**overrides):
    base = dict(
        id=1, header_id=10, rm_pm_fg_type="RM", item_category="CAT",
        sub_category="SUB", item_desc_raw="CASHEW", qty=5, uom="KG",
        pack_size=10, unit_pack_size=1, net_weight=50, total_weight=52,
        batch_number="B1", lot_number="L1", vakkal="VK-100",
        created_at=None, updated_at=None,
    )
    base.update(overrides)
    return Row(**base)


def test_create_model_accepts_vakkal():
    line = TransferLineCreate(
        rm_pm_fg_type="RM", item_category="CAT", sub_category="SUB",
        item_desc_raw="CASHEW", qty="5", vakkal="VK-100",
    )
    assert line.vakkal == "VK-100", line.vakkal
    # vakkal is optional — omitting it must not break the model
    line2 = TransferLineCreate(
        material_type="RM", item_category="C", sub_category="S",
        item_description="X",
    )
    assert line2.vakkal is None, line2.vakkal
    print("test_create_model_accepts_vakkal: PASS")


def test_map_surfaces_vakkal():
    mapped = _map_transfer_line(_full_row())
    assert mapped["vakkal"] == "VK-100", mapped["vakkal"]
    print("test_map_surfaces_vakkal: PASS")


def test_map_defaults_vakkal_when_missing():
    # A row produced by an INSERT/SELECT that did not include the column.
    row = _full_row()
    del row.__dict__["vakkal"]
    mapped = _map_transfer_line(row)
    assert mapped["vakkal"] == "", repr(mapped["vakkal"])
    print("test_map_defaults_vakkal_when_missing: PASS")


if __name__ == "__main__":
    test_create_model_accepts_vakkal()
    test_map_surfaces_vakkal()
    test_map_defaults_vakkal_when_missing()
    print("\nAll transfer-vakkal tests passed.")
