"""Dependency-free: the customer-return transaction id prefix is CR- (not RTV-).
Run: python test_rtv_cr_prefix.py
"""
from services.ims_service.rtv_tools import _generate_rtv_id


def test_id_prefix_is_cr():
    rid = _generate_rtv_id()
    assert rid.startswith("CR-"), f"expected CR- prefix, got {rid!r}"
    assert not rid.startswith("RTV-"), f"still RTV-: {rid!r}"
    # format: CR-YYYYMMDDHHMMSS
    assert len(rid) == len("CR-") + 14, f"unexpected length: {rid!r}"
    print("test_id_prefix_is_cr: PASS")


if __name__ == "__main__":
    test_id_prefix_is_cr()
    print("ALL PASS")
