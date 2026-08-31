"""Dependency-free: the RTV header must carry `location` and `poc_contact`
end to end -- create, update, read and the change-diff that drives the mail.
Run: python test_rtv_location_poc_contact.py
"""
from services.ims_service import rtv_tools
from services.ims_service.rtv_models import (
    RTVCreate, RTVHeaderCreate, RTVHeaderUpdate, RTVApprovalHeaderFields,
    RTVLineCreate,
)


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)

    def __getattr__(self, name):
        # Any column the mapper reads that we didn't explicitly set is None.
        return None


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row

    def fetchall(self):
        return [self._row] if self._row is not None else []

    def scalar(self):
        return 0


class CaptureDB:
    """Records every (sql, params) pair; echoes INSERT/UPDATE params back as a row."""

    def __init__(self):
        self.captured = []

    def execute(self, clause, params=None):
        sql = str(clause)
        params = params or {}
        self.captured.append((sql, params))
        if "rtv_header" in sql and ("INTO" in sql or "UPDATE" in sql):
            echoed = {k: v for k, v in params.items() if k != "hid"}
            echoed.setdefault("rtv_id", "CR-1")
            echoed["id"] = 1
            return _Result(_Row(**echoed))
        if "rtv_header" in sql:
            return _Result(_Row(id=1, rtv_id="CR-1"))
        if "rtv_lines" in sql and "INTO" in sql:
            return _Result(_Row(id=2, created_at=None, updated_at=None, **params))
        return _Result(None)

    def commit(self):
        pass


def _header_sql(db, verb):
    for sql, p in db.captured:
        if "rtv_header" in sql and verb in sql:
            return sql, p
    raise AssertionError(f"no {verb} against rtv_header captured")


def test_models_accept_both_fields():
    for model in (RTVHeaderCreate, RTVHeaderUpdate, RTVApprovalHeaderFields):
        fields = model.model_fields
        for f in ("location", "poc_contact"):
            assert f in fields, f"{f} missing from {model.__name__}"
    # Both must be optional so existing payloads keep validating.
    RTVHeaderCreate(factory_unit="W202", customer="c")
    print("test_models_accept_both_fields: PASS")


def test_create_persists_location_and_poc_contact():
    db = CaptureDB()
    data = RTVCreate(
        company="CFPL",
        header=RTVHeaderCreate(
            factory_unit="Savla D-39", customer="c",
            location="Bhiwandi Gate 3", poc_contact="9876543210",
        ),
        lines=[RTVLineCreate(
            material_type="rm", item_category="DATES", sub_category="KHALAS",
            item_description="al barakah khalas dates", uom="10",
        )],
    )
    rtv_tools.create_rtv(data, "tester", db)
    sql, p = _header_sql(db, "INSERT")
    for col in ("location", "poc_contact"):
        assert col in sql, f"{col} column missing from rtv_header INSERT SQL"
        assert f":{col}" in sql, f":{col} bind param missing from INSERT VALUES"
        assert sql.index("RETURNING") < sql.rindex(col), f"{col} missing from RETURNING"
    assert p["location"] == "Bhiwandi Gate 3"
    assert p["poc_contact"] == "9876543210"
    print("test_create_persists_location_and_poc_contact: PASS")


def test_update_sets_both_fields():
    db = CaptureDB()
    payload = RTVHeaderUpdate(location="Taloja Yard", poc_contact="+91 98765 43210")
    rtv_tools.update_rtv("CFPL", 1, payload, db)
    sql, p = _header_sql(db, "UPDATE")
    assert "location = :location" in sql
    assert "poc_contact = :poc_contact" in sql
    assert p["location"] == "Taloja Yard"
    assert p["poc_contact"] == "+91 98765 43210"
    print("test_update_sets_both_fields: PASS")


def test_update_with_only_new_fields_is_not_a_400():
    """If the fields reach RTVHeaderUpdate but not field_map, this raises HTTP 400."""
    db = CaptureDB()
    rtv_tools.update_rtv("CFPL", 1, RTVHeaderUpdate(location="Bhiwandi"), db)
    print("test_update_with_only_new_fields_is_not_a_400: PASS")


def test_read_paths_select_both_columns():
    for fn_name, verb in (("get_rtv", "SELECT"),):
        db = CaptureDB()
        try:
            getattr(rtv_tools, fn_name)("CFPL", 1, db)
        except Exception:
            pass
        sql, _ = _header_sql(db, verb)
        for col in ("location", "poc_contact"):
            assert col in sql, f"{col} missing from {fn_name} {verb}"
    print("test_read_paths_select_both_columns: PASS")


def test_map_header_row_surfaces_both():
    row = _Row(id=1, rtv_id="CR-1", location="Bhiwandi", poc_contact="9876543210")
    mapped = rtv_tools._map_header_row(row)
    assert mapped["location"] == "Bhiwandi"
    assert mapped["poc_contact"] == "9876543210"
    # A row from a DB that predates the migration must not raise.
    legacy = rtv_tools._map_header_row(_Row(id=1, rtv_id="CR-0"))
    assert legacy["location"] is None and legacy["poc_contact"] is None
    print("test_map_header_row_surfaces_both: PASS")


def test_diff_labels_present():
    assert rtv_tools._HEADER_DIFF_LABELS["location"] == "Location"
    assert rtv_tools._HEADER_DIFF_LABELS["poc_contact"] == "POC Contact"
    print("test_diff_labels_present: PASS")


def test_phone_number_edit_is_not_swallowed_by_numeric_norm():
    """_norm coerces float-parseable strings to 6 significant digits, so two
    different phone numbers normalise identically. The free-text fields must
    bypass that branch or a corrected POC number silently vanishes from the
    'What changed' summary while still persisting to the DB."""
    # The hazard is real for any field that still goes through _norm.
    assert rtv_tools._norm("9876543210") == rtv_tools._norm("9876543211")

    # ...and must NOT apply to the two free-text header fields.
    for field in ("location", "poc_contact"):
        assert rtv_tools._norm_field(field, "9876543210") != \
            rtv_tools._norm_field(field, "9876543211"), \
            f"{field} still routed through the numeric branch"
    assert rtv_tools._norm_field("poc_contact", "  9876543210  ") == \
        rtv_tools._norm_field("poc_contact", "9876543210")
    assert rtv_tools._norm_field("poc_contact", None) == ""

    # Numeric fields must keep the loose comparison: "85" and "85.00" are equal.
    assert rtv_tools._norm_field("qty", "85") == rtv_tools._norm_field("qty", "85.00")
    print("test_phone_number_edit_is_not_swallowed_by_numeric_norm: PASS")


if __name__ == "__main__":
    test_models_accept_both_fields()
    test_create_persists_location_and_poc_contact()
    test_update_sets_both_fields()
    test_update_with_only_new_fields_is_not_a_400()
    test_read_paths_select_both_columns()
    test_map_header_row_surfaces_both()
    test_diff_labels_present()
    test_phone_number_edit_is_not_swallowed_by_numeric_norm()
    print("ALL PASS")
