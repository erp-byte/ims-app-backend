"""Dependency-free test: RTV mails thread on one Message-ID per CR.

Run: python test_rtv_mail_threading.py
"""
import shared.email_notifier as en

captured = []

def _fake_send(**kwargs):
    captured.append(kwargs)

en._send_email_background = _fake_send  # monkeypatch the background sender

DETAIL = {
    "rtv_id": "CR-20260609120000",
    "business_head": "unknown-head",  # unmapped → no buttons, still sends
    "created_by": "creator@candorfoods.in",
    "status": "Pending",
    "lines": [],
    "boxes": [],
}
KEY = "RTV-CR-20260609120000@candorfoods.in"

def reset():
    captured.clear()

# Root mail sets message_id
reset(); en.notify_rtv_created(DETAIL)
assert captured, "notify_rtv_created sent nothing"
assert captured[0].get("message_id") == KEY, captured[0].get("message_id")
assert not captured[0].get("in_reply_to")

# Follow-ups set in_reply_to to the same key
reset(); en.notify_rtv_approved(DETAIL, "head@candorfoods.in")
assert captured[0].get("in_reply_to") == KEY, ("approved", captured[0].get("in_reply_to"))

reset(); en.notify_rtv_weight_discrepancy(DETAIL, {"lines": [], "total_diff": 0})
assert captured[0].get("in_reply_to") == KEY, ("discrepancy", captured[0].get("in_reply_to"))

reset(); en.notify_rtv_deleted("CR-20260609120000", 1, "x@y.com",
                              business_head="unknown-head", created_by="creator@candorfoods.in")
assert captured[0].get("in_reply_to") == KEY, ("deleted", captured[0].get("in_reply_to"))

reset(); en.notify_rtv_status_changed(DETAIL, "Approved", "head@candorfoods.in")
assert captured[0].get("in_reply_to") == KEY, ("status_changed", captured[0].get("in_reply_to"))

reset(); en.notify_rtv_rejected(DETAIL, "head@candorfoods.in")
assert captured[0].get("in_reply_to") == KEY, ("rejected", captured[0].get("in_reply_to"))

reset(); en.notify_rtv_held(DETAIL, "head@candorfoods.in")
assert captured[0].get("in_reply_to") == KEY, ("held", captured[0].get("in_reply_to"))

print("OK: RTV mail threading")
