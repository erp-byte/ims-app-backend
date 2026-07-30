"""remark + reason_code are NOT NULL in interunit_transfers_header.

If they can arrive as None, POST/PUT /interunit/transfers dies on the INSERT with an
unhandled IntegrityError — a 500 the browser reports as a CORS failure. Run:
python test_transfer_header_defaults.py
"""
from services.ims_service.interunit_models import TransferHeaderCreate

BASE = {
    "stock_trf_date": "30-07-2026",
    "from_warehouse": "A185",
    "to_warehouse": "W202",
    "vehicle_no": "MH-01-AB-1234",
}


def test_omitted_fields_default_to_blank():
    h = TransferHeaderCreate(**BASE)
    assert h.remark == "" and h.reason_code == "", (h.remark, h.reason_code)


def test_explicit_nulls_become_blank():
    h = TransferHeaderCreate(**BASE, remark=None, reason_code=None)
    assert h.remark == "" and h.reason_code == "", (h.remark, h.reason_code)


def test_real_values_pass_through():
    h = TransferHeaderCreate(**BASE, remark="urgent", reason_code="Production Need")
    assert (h.remark, h.reason_code) == ("urgent", "Production Need")


if __name__ == "__main__":
    test_omitted_fields_default_to_blank()
    test_explicit_nulls_become_blank()
    test_real_values_pass_through()
    print("OK")
