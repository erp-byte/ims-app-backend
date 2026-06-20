"""
One-shot test script: creates a real test RTV in the DB and emails its
"RTV Created" notification to a recipient of your choice so you can reply
"Approved" and exercise the IMAP listener end-to-end.

Usage:
    .venv\\Scripts\\python.exe send_mock_rtv_email.py
    .venv\\Scripts\\python.exe send_mock_rtv_email.py --to someone@example.com --company CFPL
"""

from __future__ import annotations

import argparse
import sys
import time
from decimal import Decimal

from services.ims_service.rtv_models import (
    RTVCreate,
    RTVHeaderCreate,
    RTVLineCreate,
)
from services.ims_service.rtv_tools import create_rtv
from shared.database import SessionLocal
from shared.email_notifier import (
    _rtv_email_html,
    _send_email_background,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Send a mock RTV creation email.")
    parser.add_argument("--to", default="ai.1@candorfoods.in", help="Recipient (To) address.")
    parser.add_argument("--company", default="CFPL", choices=["CFPL", "CDPL"])
    parser.add_argument("--business-head", default="Prashant Pal",
                        help="Business head name (must match BUSINESS_HEAD_EMAILS key).")
    parser.add_argument("--created-by", default="ai.1@candorfoods.in")
    parser.add_argument("--customer", default="TEST CUSTOMER — please ignore")
    parser.add_argument("--factory-unit", default="UNIT-01")
    parser.add_argument("--no-db", action="store_true",
                        help="Skip DB insert — useful if RTV tables don't exist locally.")
    args = parser.parse_args()

    if args.no_db:
        from datetime import datetime
        rtv_id_str = f"RTV-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        rtv_detail = {
            "id": 0,
            "rtv_id": rtv_id_str,
            "rtv_date": datetime.now(),
            "factory_unit": args.factory_unit,
            "customer": args.customer,
            "invoice_number": "TEST-INV-001",
            "challan_no": "TEST-CHN-001",
            "dn_no": None,
            "conversion": "1",
            "sales_poc": "Test POC",
            "business_head": args.business_head,
            "remark": "Mock RTV for IMAP listener test",
            "status": "Pending",
            "created_by": args.created_by,
            "lines": [{
                "material_type": "RM",
                "item_category": "TEST",
                "sub_category": "TEST",
                "item_description": "Test Item",
                "uom": "KG",
                "qty": "10",
                "rate": "100",
                "value": "1000",
                "net_weight": "10",
                "carton_weight": "0.5",
            }],
            "boxes": [],
        }
        print(f"[no-db] Skipped DB insert; using ephemeral RTV id {rtv_id_str}")
    else:
        payload = RTVCreate(
            company=args.company,
            header=RTVHeaderCreate(
                factory_unit=args.factory_unit,
                customer=args.customer,
                invoice_number="TEST-INV-001",
                challan_no="TEST-CHN-001",
                conversion="1",
                sales_poc="Test POC",
                business_head=args.business_head,
                remark="Mock RTV for IMAP listener test",
            ),
            lines=[
                RTVLineCreate(
                    material_type="RM",
                    item_category="TEST",
                    sub_category="TEST",
                    item_description="Test Item",
                    uom="KG",
                    qty="10",
                    rate="100",
                    value="1000",
                    net_weight="10",
                    carton_weight="0.5",
                )
            ],
        )
        db = SessionLocal()
        try:
            rtv_detail = create_rtv(payload, args.created_by, db)
            db.commit()
        except Exception as exc:
            db.rollback()
            print(f"DB insert failed: {exc}", file=sys.stderr)
            return 1
        finally:
            db.close()
        rtv_detail["_company"] = args.company
        print(f"Created RTV {rtv_detail['rtv_id']} (id={rtv_detail['id']}) in {args.company}")

    # Build the action-required email (with buttons) and route it to `args.to`
    # for testing. The token's head_email field is set to args.to so the audit
    # log shows that recipient as the actor when they click.
    from services.ims_service.rtv_approval_token import action_url

    rtv_id_str = rtv_detail["rtv_id"]
    rtv_db_id = int(rtv_detail.get("id") or 0)
    head_email = args.to

    base_html, base_plain = _rtv_email_html(
        action="Action Required (TEST)",
        header=rtv_detail,
        lines=rtv_detail.get("lines", []),
        boxes=rtv_detail.get("boxes", []),
        extra_info=(
            "TEST EMAIL — click one of the buttons below to exercise the "
            "magic-link approval flow (Approve / Reject / Hold)."
        ),
    )

    if rtv_db_id:
        approve = action_url(rtv_id_str, rtv_db_id, args.company, head_email, "approve")
        reject = action_url(rtv_id_str, rtv_db_id, args.company, head_email, "reject")
        hold = action_url(rtv_id_str, rtv_db_id, args.company, head_email, "hold")
        btn_style = (
            "display:inline-block;padding:12px 28px;margin:0 6px;border-radius:6px;"
            "color:#fff;text-decoration:none;font-weight:bold;font-size:14px;"
            "font-family:Arial,sans-serif;"
        )
        buttons_html = f"""
        <table style="margin:18px 0;width:100%;border-collapse:collapse;">
          <tr><td style="text-align:center;padding:18px;background:#f8f9fa;border-radius:8px;">
            <p style="margin:0 0 12px;font-size:13px;color:#555;">
              Action required by <strong>{head_email}</strong>
            </p>
            <a href="{approve}" style="{btn_style}background:#16a34a;">&#10003; Approve</a>
            <a href="{reject}" style="{btn_style}background:#dc2626;">&#10007; Reject</a>
            <a href="{hold}" style="{btn_style}background:#f59e0b;">&#9208; Hold</a>
          </td></tr>
        </table>"""
        html = base_html.replace(
            '<tr><td style="padding:20px 24px;">',
            f'<tr><td style="padding:20px 24px;">{buttons_html}',
            1,
        )
        plain = (
            f"{base_plain}\n\n--- ACTION LINKS ---\n"
            f"Approve: {approve}\nReject:  {reject}\nHold:    {hold}\n"
        )
    else:
        html, plain = base_html, base_plain

    _send_email_background(
        subject=f"ACTION REQUIRED — RTV {rtv_id_str} [TEST]",
        html_body=html,
        plain_body=plain,
        to=args.to,
    )

    print(f"Action email dispatched to {args.to}")
    print("Waiting 5s for background SMTP thread to flush...")
    time.sleep(5)
    print(f"Done. Click Approve/Reject/Hold in the email to test the endpoint.")
    print(f"(Server must be running and reachable at APP_BASE_URL for clicks to work.)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
