"""Dependency-free: factory_unit is canonicalized on create.
Run: python test_rtv_canonical_warehouse.py
"""
from services.ims_service import rtv_tools


def test_canonical_factory_unit_maps_alias():
    assert rtv_tools._canonical_factory_unit("new savla") == "Savla D-514"
    assert rtv_tools._canonical_factory_unit("D-39") == "Savla D-39"
    assert rtv_tools._canonical_factory_unit("Savla D-39") == "Savla D-39"


def test_canonical_factory_unit_passes_through_unknown():
    assert rtv_tools._canonical_factory_unit("Savla") == "Savla"
    assert rtv_tools._canonical_factory_unit("W202") == "W202"
    assert rtv_tools._canonical_factory_unit("") == ""
    assert rtv_tools._canonical_factory_unit(None) is None


if __name__ == "__main__":
    test_canonical_factory_unit_maps_alias()
    test_canonical_factory_unit_passes_through_unknown()
    print("ALL PASS")
