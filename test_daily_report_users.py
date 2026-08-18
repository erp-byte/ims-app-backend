"""Identity resolution for the idle-user block — the part that can libel someone.

Every other bug in this block is cosmetic. Getting identity wrong is not: merge
two people and one of them is reported as idle on a day they worked; fail to
merge one person's two spellings and they are reported as idle while sitting in
the active list under another name. Both print a name next to "took no action".

Pure functions, no database: `python test_daily_report_users.py`.
"""
from datetime import date

from services.ims_service.daily_report_users import (
    _apply_merges,
    _compatible,
    _functional,
    _merge_partials,
    _shared_emails,
    _union,
    base_fold,
    clean_raw,
    display,
    fold,
    is_person,
    rank,
    squash,
    titlecase,
    usable,
)


def test_fold():
    # case, honorifics and separators all collapse
    assert fold("MONIKA") == fold("monika") == fold("Monika") == "MONIKA"
    assert base_fold("Shabana. Ansari") == "SHABANA ANSARI"
    assert base_fold("soham.damgude@candorfoods.in") == "SOHAM DAMGUDE"
    assert base_fold("stores-a185@candorfoods.in") == "STORES A185"
    assert base_fold("Vaibhav Sir") == "VAIBHAV"
    assert base_fold("") == "" and base_fold(None) == ""

    # aliases resolve typos and short forms onto the fullest spelling
    assert fold("Maduri") == fold("Madiri") == "MADHURI"
    assert fold("Samikshq") == "SAMIKSHA"
    assert fold("bhrithik") == fold("Hrithik") == fold("Hrithik B") == "B HRITHIK"
    assert fold("yash@candorfoods.in") == "YASH GAWDI"

    # a shared store login is real activity but not a person to chase
    assert not is_person("STORES A185")
    assert not is_person("ADMIN") and not is_person("TEST") and not is_person("")
    assert is_person("VAISHALI DHURI")


def test_display_prefers_the_fuller_spelling():
    # the resolved key beats the login that got us there
    assert display("RAJU PAIKRAO", {"RAJU PAIKRAO": "rajupaikrao"}) == "Raju Paikrao"
    # ...but a real spelling with the same shape is kept
    assert display("SHABANA SAYYED", {"SHABANA SAYYED": "Shabana sayyed"}) == "Shabana Sayyed"
    assert display("MONIKA", {}) == "Monika"
    # initials stay upper, separators open out
    assert titlecase("R M PATIL") == "R M Patil"
    assert clean_raw("B.hrithik") == "B hrithik"
    assert display("B HRITHIK", {"B HRITHIK": "B.hrithik"}) == "B Hrithik"
    assert rank("RAJU PAIKRAO") > rank("rajupaikrao")
    assert squash("SOHAM DAMGUDE") == "SOHAMDAMGUDE"


def test_partial_merge_refuses_ambiguity():
    # one Namrata on the roster -> the bare first name is safe to merge
    assert _merge_partials({"NAMRATA", "NAMRATA NACHARE"}, {}) == \
        {"NAMRATA": "NAMRATA NACHARE"}
    # ...even when only the directory knows the full name
    assert _merge_partials({"PRIYANSHU"}, {"PRIYANSHU": {"PRIYANSHU SHRIVASTAV"}}) == \
        {"PRIYANSHU": "PRIYANSHU SHRIVASTAV"}
    # four Shubhams work here: never guess which one was idle
    assert _merge_partials(
        {"SHUBHAM", "SHUBHAM SETH"},
        {"SHUBHAM": {"SHUBHAM SETH", "SHUBHAM SHIVEKAR", "SHUBHAM MHATRE"}}) == {}
    # ambiguity inside the window alone is also refused
    assert _merge_partials({"SHUBHAM", "SHUBHAM SETH", "SHUBHAM MHATRE"}, {}) == {}


def test_apply_merges_folds_one_person_into_one_row():
    d1, d2 = date(2026, 8, 13), date(2026, 8, 14)
    days = {"SOHAMDAMGUDE": {d1}, "SOHAM DAMGUDE": {d2}, "RAJUPAIKRAO": {d1},
            "NAMRATA": {d1}, "NAMRATA NACHARE": {d2}}
    mods = {"SOHAMDAMGUDE": {"Stock take"}, "SOHAM DAMGUDE": {"Job cards"}}
    systems = {"SOHAMDAMGUDE": {"IMS"}, "SOHAM DAMGUDE": {"ERP"}}
    seen = {"SOHAMDAMGUDE": "sohamdamgude", "RAJUPAIKRAO": "rajupaikrao"}

    _apply_merges(days, mods, systems, seen,
                  {"NAMRATA": {"NAMRATA NACHARE"}},
                  {"RAJUPAIKRAO": "RAJU PAIKRAO"})

    # spacing variants are one man, and his two days survive the merge
    assert "SOHAMDAMGUDE" not in days
    assert days["SOHAM DAMGUDE"] == {d1, d2}
    assert mods["SOHAM DAMGUDE"] == {"Stock take", "Job cards"}
    assert systems["SOHAM DAMGUDE"] == {"IMS", "ERP"}
    # a login with no spaced spelling anywhere still resolves via the directory
    assert "RAJUPAIKRAO" not in days and days["RAJU PAIKRAO"] == {d1}
    assert display("RAJU PAIKRAO", seen) == "Raju Paikrao"
    # first-name-only rows join the full name
    assert "NAMRATA" not in days and days["NAMRATA NACHARE"] == {d1, d2}


def test_usable_rejects_the_literal_null():
    # nine stocktake_users rows hold the WORD 'null' in the name column. Folded
    # into an identity key it becomes 'NULL', which all nine then share, and
    # Vaibhav Kumkar, Samal Kumar, Raju Paikrao and Harsh Arora arrive as one
    # person whose activity is the union of theirs.
    assert usable("null") == "" and usable("NULL") == "" and usable(None) == ""
    assert usable("-") == "" and usable("n/a") == ""
    assert usable("  Raju   Paikrao ") == "Raju Paikrao"


def test_compatible_separates_two_people_from_two_spellings():
    # one man written twice
    assert _compatible("DIGAMBER", "DIGAMBER SAWANT")
    assert _compatible("SATYENDRA GARG", "SATYENDRA KUMAR GARG")
    assert _compatible("SURAJ", "SURAJ SALUNKHE")
    # two people sharing a mailbox
    assert not _compatible("PURVA NALAWADE", "AAKANKSHA PADWAL")
    assert not _compatible("STORES A185", "SWAPNIL RAIKAR")


def test_shared_emails_only_fires_on_genuinely_shared_boxes():
    rows = [
        # npd1@ really is two people -> not an identity
        {"name": "Purva Nalawade", "email": "npd1@candorfoods.in"},
        {"name": "Aakanksha Padwal", "email": "npd1@candorfoods.in"},
        # satyendra@ is one man spelled two ways -> still an identity
        {"name": "Satyendra Garg", "email": "satyendra@candorfoods.in"},
        {"name": "Satyendra Kumar Garg", "email": "satyendra@candorfoods.in"},
    ]
    shared = _shared_emails(rows)
    assert "NPD1" in shared
    assert "SATYENDRA" not in shared


def test_union_folds_accounts_sharing_any_key():
    # Harsh holds ERP 'harsh@' and Stock Take 'harsharora'; they share HARSHARORA
    groups = _union([{"HARSHARORA", "HARSH"}, {"HARSHARORA"},
                     {"VAIBHAVKUMKAR"}, {"RAJUPAIKRAO"}])
    sizes = sorted(len(g) for g in groups)
    assert sizes == [1, 1, 2]
    merged = next(g for g in groups if len(g) == 2)
    assert set(merged) == {0, 1}


def test_functional_accounts_are_not_people_to_chase():
    assert _functional({"STORESA185"}) and _functional({"QUALITYA185"})
    assert _functional({"ADMIN"}) and _functional({"PRINTING"})
    assert not _functional({"RAJUPAIKRAO"}) and not _functional({"HARSHARORA"})


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
    print("all identity tests passed")
