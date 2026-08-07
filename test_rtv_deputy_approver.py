"""Customer-return approver list — who holds the buttons, and whose click is honoured.

The two must never drift: a deputy sent an Approve button whose click 403s is worse
than no deputy at all. Both the mail and the gate in rtv_tools.apply_rtv_email_action
read rtv_approver_emails, and this pins its behaviour.

Pure functions, no database, no SMTP: `python test_rtv_deputy_approver.py`.
"""
from shared.email_notifier import (
    RTV_CC_CONSTANT,
    RTV_DEPUTY_APPROVERS,
    _rtv_approver_note,
    _build_rtv_cc,
    rtv_approver_emails,
)

BH = "rakesh@candorfoods.in"
SATYENDRA = "satyendra@candorfoods.in"
PATIL = "rmpatil@candorfoods.in"


def test_mandatory_cc():
    # R M Patil is CC'd on every customer-return mail, not only the ones he approves.
    assert PATIL in RTV_CC_CONSTANT
    cc = _build_rtv_cc("Rakesh Ratra", "samal.kumar@candorfoods.in", factory_unit="W202")
    assert PATIL in cc
    assert BH not in cc, "the BH is a TO recipient, never CC"
    assert len(cc) == len(set(a.lower() for a in cc)), "CC deduped"


def test_approver_list():
    assert rtv_approver_emails("Rakesh Ratra") == [BH, SATYENDRA, PATIL]

    # A BH who is himself a deputy is ONE approver — otherwise he is mailed twice
    # and a second click of his own button looks like a second decision.
    assert rtv_approver_emails("R M Patil") == [PATIL, SATYENDRA]

    # An unmapped/blank BH used to mean nobody could action the return from the
    # mail at all. The deputies now cover it.
    assert rtv_approver_emails("Nobody At All") == RTV_DEPUTY_APPROVERS
    assert rtv_approver_emails(None) == RTV_DEPUTY_APPROVERS

    # Case/whitespace on the stored business_head must still resolve.
    assert rtv_approver_emails("  rakesh ratra  ") == [BH, SATYENDRA, PATIL]


def test_gate_matches_the_buttons():
    """Every address the mail hands a button to must pass the click gate."""
    for head in ("Rakesh Ratra", "R M Patil", "Nobody At All", None):
        allowed = {a.lower() for a in rtv_approver_emails(head)}
        for addr in rtv_approver_emails(head):
            assert addr.lower() in allowed
        # ...and nobody else does.
        assert "stores-a185@candorfoods.in" not in allowed
        assert "pooja.parkar@candorfoods.in" not in allowed


def test_banner_names_the_others():
    approvers = rtv_approver_emails("Rakesh Ratra")
    primary = _rtv_approver_note(approvers, 0, BH)
    deputy = _rtv_approver_note(approvers, 1, BH)
    assert "assigned Business Head" in primary and "R M Patil" in primary
    assert "deputy approver" in deputy and "Rakesh Ratra" in deputy
    for note in (primary, deputy):
        assert "whoever decides first" in note

    # A lone approver gets the plain instruction, not "— can also action it".
    assert "also action" not in _rtv_approver_note([BH], 0, BH)


if __name__ == "__main__":
    test_mandatory_cc()
    test_approver_list()
    test_gate_matches_the_buttons()
    test_banner_names_the_others()
    print("RTV deputy approver: OK")
