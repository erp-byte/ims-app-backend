import smtplib
import threading
from datetime import datetime
from email.message import EmailMessage
from html import escape
from urllib.parse import quote

from shared.canonicalize import canonical_warehouse
from shared.config_loader import settings
from shared.logger import get_logger
from shared.timezone import now_ist, fmt_ist

logger = get_logger("email.notifier")


def _build_rtv_action_url(rtv_id: str, bh_email: str, action: str) -> str:
    base = settings.BACKEND_URL.rstrip("/")
    return (
        f"{base}/rtv/action?rtv_id={quote(rtv_id)}"
        f"&bh_email={quote(bh_email)}&action={action}"
    )

RTV_NOTIFY_TO = "pooja.parkar@candorfoods.in"
JOB_WORK_TO = "billing@candorfoods.in"
JOB_WORK_CC = ["b.hrithik@candorfoods.in", "vaibhav.kumkar@candorfoods.in"]
WEEKLY_DIGEST_TO = ["b.hrithik@candorfoods.in", "vaibhav.kumkar@candorfoods.in"]
INWARD_DELETE_TO = "b.hrithik@candorfoods.in"

# Business Head -> email map for RTV notifications.
# Keys are matched case-insensitively against the business_head value stored on the RTV.
BUSINESS_HEAD_EMAILS = {
    "Prashant Pal": "prashant.pal@candorfoods.in",
    "Ajay Bajaj": "ajay@candorfoods.in",
    "Rakesh Ratra": "rakesh@candorfoods.in",
    "Yash Gawdi": "yash@candorfoods.in",
    "Satyendra Garg": "satyendra@candorfoods.in",
    "R M Patil": "rmpatil@candorfoods.in",
}

# Sales POC -> email map for RTV notifications. When a Sales POC is selected on an
# RTV, their email is added to CC (alongside the constant CC list below). Matched
# case-insensitively against the sales_poc value stored on the RTV; unmapped /
# legacy free-text values simply add no CC.
SALES_POC_EMAILS = {
    "Shubham Shivekar": "shubham@candorfoods.in",
    "Shubham Seth": "shubham.seth@candorfoods.in",
    "Mayuresh Mahadik": "mayuresh@candorfoods.in",
    "Suraj Salunkhe": "suraj@candorfoods.in",
    "B Hrithik": "b.hrithik@candorfoods.in",
    "Sachin More": "sachin.more@candorfoods.in",
    "Dashrath Birajdar": "dashrath@candorfoods.in",
    "Ashwin Baghul": "ashwin@candorfoods.in",
    "Rakesh Ratra": "rakesh@candorfoods.in",
    "Ajay Bajaj": "ajay@candorfoods.in",
    "Yash Gawdi": "yash@candorfoods.in",
    "R M Patil": "rmpatil@candorfoods.in",
    "Satyendra Garg": "satyendra@candorfoods.in",
    "Prashant Pal": "prashant.pal@candorfoods.in",
    "Suresh Luthra": "suresh@candorfoods.in",
    "Swadhin Joshi": "swadhin.joshi@candorfoods.in",
}

# Constant CCs added to every RTV notification.
RTV_CC_CONSTANT = [
    "sunil.jasoria@candorfoods.in",
    "b.hrithik@candorfoods.in",
    "billing@candorfoods.in",
    "satyendra@candorfoods.in",
    "sachin.more@candorfoods.in",
    "dipesh.sharma@ofbusiness.in",
    "yash@candorfoods.in",   # Yash Gawdi — mandatory CC on every customer-return mail
]

# Warehouse (factory_unit) -> additional CC recipient, layered on top of the
# constant CC. W202 and all cold storages route to Vaibhav; A185/A68 to their
# stores owners. Unlisted warehouses add no extra CC. Matching is tolerant of
# alias/hyphen/space variants (e.g. "A-68" == "A68", "Savla D-39" -> cold).
def _warehouse_cc_email(factory_unit: str | None) -> str | None:
    if not factory_unit:
        return None
    canon = canonical_warehouse(factory_unit, None) or str(factory_unit)
    low = canon.strip().lower()
    if any(k in low for k in ("savla", "rishi", "supreme", "eskimo")):
        return "vaibhav.kumkar@candorfoods.in"          # cold storages
    code = low.replace("-", "").replace(" ", "")
    if code == "w202":
        return "vaibhav.kumkar@candorfoods.in"
    if code == "a185":
        return "stores-a185@candorfoods.in"             # Sumit Baikar
    if code == "a68":
        return "pankaj.ranga@candorfoods.in"
    return None


def _lookup_business_head_email(business_head: str | None) -> str | None:
    if not business_head:
        return None
    key = business_head.strip().lower()
    for name, email in BUSINESS_HEAD_EMAILS.items():
        if name.lower() == key:
            return email
    return None


def _lookup_sales_poc_email(sales_poc: str | None) -> str | None:
    if not sales_poc:
        return None
    key = sales_poc.strip().lower()
    for name, email in SALES_POC_EMAILS.items():
        if name.lower() == key:
            return email
    return None


def _name_for_email(email: str | None) -> str | None:
    """Reverse lookup email -> known display name from the name maps."""
    if not email:
        return None
    key = email.strip().lower()
    for name, addr in {**SALES_POC_EMAILS, **BUSINESS_HEAD_EMAILS}.items():
        if addr.lower() == key:
            return name
    return None


def _derive_name_from_email(email: str) -> str:
    """Best-effort name from an email local-part: b.hrithik -> 'B Hrithik'."""
    local = email.split("@", 1)[0]
    for ch in "._-":
        local = local.replace(ch, " ")
    parts = [p for p in local.split(" ") if p]
    return " ".join(p.capitalize() for p in parts) or email


def _format_actor(email: str | None) -> str:
    """Render an actor for RTV mail as 'Name (email)'; '-' when empty."""
    if not email or not email.strip():
        return "-"
    email = email.strip()
    return f"{_name_for_email(email) or _derive_name_from_email(email)} ({email})"


def _build_rtv_cc(
    business_head: str | None,
    *actors: str | None,
    sales_poc: str | None = None,
    sales_poc_email: str | None = None,
    factory_unit: str | None = None,
) -> list[str]:
    """Build CC list: selected Sales POC + constant CC + entry-maker(s) +
    the warehouse-specific recipient for ``factory_unit``.

    The business head goes in TO (not CC), so ``business_head`` is used only to
    EXCLUDE that address from CC (e.g. when the approver/rejecter is the BH).
    ``sales_poc`` is resolved against the SALES_POC_EMAILS map (dropdown picks);
    ``sales_poc_email`` is a manually entered address (the "Other" option) and is
    added as-is. ``factory_unit`` adds the warehouse owner (see _warehouse_cc_email).
    Duplicates and the TO address (pooja) are removed; empties skipped.
    """
    cc: list[str] = []
    poc_email = _lookup_sales_poc_email(sales_poc)
    if poc_email:
        cc.append(poc_email)
    if sales_poc_email and sales_poc_email.strip():
        cc.append(sales_poc_email.strip())
    cc.extend(RTV_CC_CONSTANT)
    for actor in actors:
        if actor:
            cc.append(actor)
    wh_cc = _warehouse_cc_email(factory_unit)
    if wh_cc:
        cc.append(wh_cc)

    head_email = (_lookup_business_head_email(business_head) or "").strip().lower()
    seen: set[str] = set()
    deduped: list[str] = []
    for addr in cc:
        normalized = addr.strip().lower()
        if (not normalized or normalized in seen
                or normalized == RTV_NOTIFY_TO.lower()
                or (head_email and normalized == head_email)):
            continue
        seen.add(normalized)
        deduped.append(addr.strip())
    return deduped


def _rtv_thread_key(rtv_id: str | None) -> str | None:
    """Stable Message-ID per CR so all its mails land in one Gmail thread."""
    return f"RTV-{rtv_id}@candorfoods.in" if rtv_id else None


def _rtv_subject(rtv_id: str | None, *, reply: bool = False) -> str:
    """One subject per return so every mail threads into a single Gmail
    conversation. Gmail groups by (normalized subject + References); a changing
    subject splits the thread, so the action (Created / Approved / Rejected /
    ...) is shown in the banner, NOT the subject. Replies carry the "Re:" prefix
    Gmail strips before matching."""
    base = f"Customer Returns Created: {rtv_id or ''}"
    return f"Re: {base}" if reply else base


def _send_email_background(
    subject: str,
    html_body: str,
    plain_body: str,
    to: str | list[str] = RTV_NOTIFY_TO,
    cc: list[str] | None = None,
    message_id: str | None = None,
    in_reply_to: str | None = None,
) -> None:
    """Send email in a background thread so API response is not delayed."""
    def _send():
        try:
            to_list = [to] if isinstance(to, str) else list(to)

            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = settings.SMTP_EMAIL
            msg["To"] = ", ".join(to_list)
            if cc:
                msg["Cc"] = ", ".join(cc)
            msg.set_content(plain_body)
            msg.add_alternative(html_body, subtype="html")

            if message_id:
                msg["Message-ID"] = f"<{message_id}>"
            if in_reply_to:
                msg["In-Reply-To"] = f"<{in_reply_to}>"
                msg["References"] = f"<{in_reply_to}>"

            recipients = to_list + (cc or [])

            with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
                server.starttls()
                server.login(settings.SMTP_EMAIL, settings.SMTP_APP_PASSWORD)
                server.send_message(msg, to_addrs=recipients)

            logger.info(f"Notification sent to {recipients}: {subject}")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")

    threading.Thread(target=_send, daemon=True).start()


def _build_lines_html(lines: list[dict]) -> str:
    """Build HTML table rows for RTV line items."""
    if not lines:
        return "<tr><td colspan='8' style='text-align:center;padding:8px;'>No line items</td></tr>"

    rows = ""
    for line in lines:
        rows += f"""<tr>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{line.get('material_type', '')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{line.get('item_category', '')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{line.get('sub_category', '')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{line.get('item_description', '')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{line.get('uom', '')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{line.get('qty', '0')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{line.get('rate', '0')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{line.get('value', '0')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{line.get('net_weight', '0')}</td>
        </tr>"""
    return rows


def _build_boxes_html(boxes: list[dict]) -> str:
    """Build HTML table rows for RTV boxes."""
    if not boxes:
        return ""

    rows = ""
    for box in boxes:
        rows += f"""<tr>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{box.get('box_id', '') or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{box.get('article_description', '')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:center;">{box.get('box_number', '')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{box.get('net_weight', '0')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{box.get('gross_weight', '0')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{box.get('lot_number', '') or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:center;">{box.get('count', '') or '-'}</td>
        </tr>"""
    return rows


def _rtv_email_html(
    action: str,
    header: dict,
    lines: list[dict],
    boxes: list[dict],
    extra_info: str = "",
    action_buttons: list[tuple[str, str, str]] | None = None,
) -> tuple[str, str]:
    """Return (html_body, plain_body) for an RTV notification email.

    action_buttons: optional list of (label, url, hex_color) tuples rendered as
    a call-to-action row above the header table.
    """

    header_fields = [
        ("Return ID", header.get("rtv_id", "")),
        ("Return Date", fmt_ist(header.get("rtv_date")) or "-"),
        ("Factory Unit", header.get("factory_unit", "")),
        ("Customer", header.get("customer", "")),
        ("Invoice Number", header.get("invoice_number", "") or "-"),
        ("Challan No", header.get("challan_no", "") or "-"),
        ("DN No", header.get("dn_no", "") or "-"),
        ("Sales POC", header.get("sales_poc", "") or "-"),
        ("Business Head", header.get("business_head", "") or "-"),
        ("Remark", header.get("remark", "") or "-"),
        ("Status", header.get("status", "")),
        ("Created By", _format_actor(header.get("created_by"))),
    ]

    header_rows = ""
    for label, value in header_fields:
        header_rows += f"""<tr>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;width:160px;">{label}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{value}</td>
        </tr>"""

    lines_html = _build_lines_html(lines)
    boxes_html = _build_boxes_html(boxes)

    boxes_section = ""
    if boxes_html:
        boxes_section = f"""
        <h3 style="color:#29417A;margin:24px 0 8px;">Boxes</h3>
        <table style="border-collapse:collapse;width:100%;font-size:13px;">
          <thead>
            <tr style="background:#29417A;color:#fff;">
              <th style="padding:8px 10px;text-align:left;">Box ID</th>
              <th style="padding:8px 10px;text-align:left;">Article</th>
              <th style="padding:8px 10px;text-align:center;">Box #</th>
              <th style="padding:8px 10px;text-align:right;">Net Wt</th>
              <th style="padding:8px 10px;text-align:right;">Gross Wt</th>
              <th style="padding:8px 10px;text-align:left;">Lot No</th>
              <th style="padding:8px 10px;text-align:center;">Count</th>
            </tr>
          </thead>
          <tbody>{boxes_html}</tbody>
        </table>"""

    extra_section = ""
    if extra_info:
        # Prominent, action-coloured banner so "Approved/Rejected/Held by: <name>"
        # stands out for every recipient (was a dim grey line before).
        _extra_palette = {
            "Approved": ("#1e7e44", "#eafaf1", "#27ae60"),  # (text, bg, border)
            "Rejected": ("#a82315", "#fdecea", "#c0392b"),
            "On Hold":  ("#9a6212", "#fff4e5", "#e67e22"),
            "Deleted":  ("#a82315", "#fdecea", "#c0392b"),
        }
        _txt, _bg, _border = _extra_palette.get(action, ("#1f4e79", "#eef4fb", "#29417A"))
        extra_section = (
            f'<div style="margin:18px 0;padding:14px 18px;background:{_bg};'
            f'border-left:5px solid {_border};border-radius:6px;">'
            f'<span style="font-size:18px;font-weight:bold;color:{_txt};">{extra_info}</span></div>'
        )

    buttons_section = ""
    if action_buttons:
        button_cells = "".join(
            f'<a href="{url}" style="display:inline-block;padding:10px 24px;margin:0 6px;'
            f'background:{color};color:#fff;text-decoration:none;font-weight:bold;'
            f'border-radius:6px;font-size:14px;">{label}</a>'
            for label, url, color in action_buttons
        )
        buttons_section = (
            f'<div style="text-align:center;margin:8px 0 22px;">{button_cells}</div>'
        )

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;margin:0;padding:0;background:#f4f4f4;">
  <table width="100%" cellpadding="0" cellspacing="0" style="max-width:800px;margin:20px auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
    <tr><td style="background:#29417A;color:#fff;padding:20px 24px;">
      <h2 style="margin:0;">Customer Returns {action}</h2>
      <p style="margin:4px 0 0;opacity:0.85;font-size:14px;">{header.get('rtv_id', '')} &mdash; {now_ist().strftime('%d %b %Y, %I:%M %p')}</p>
    </td></tr>
    <tr><td style="padding:20px 24px;">

      {extra_section}
      {buttons_section}

      <h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>

      <h3 style="color:#29417A;margin:24px 0 8px;">Line Items</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead>
          <tr style="background:#29417A;color:#fff;">
            <th style="padding:8px 10px;text-align:left;">Material</th>
            <th style="padding:8px 10px;text-align:left;">Category</th>
            <th style="padding:8px 10px;text-align:left;">Sub Category</th>
            <th style="padding:8px 10px;text-align:left;">Description</th>
            <th style="padding:8px 10px;text-align:left;">UOM</th>
            <th style="padding:8px 10px;text-align:right;">Qty</th>
            <th style="padding:8px 10px;text-align:right;">Rate</th>
            <th style="padding:8px 10px;text-align:right;">Value</th>
            <th style="padding:8px 10px;text-align:right;">Net Wt</th>
          </tr>
        </thead>
        <tbody>{lines_html}</tbody>
      </table>

      {boxes_section}

    </td></tr>
    <tr><td style="background:#f8f9fa;padding:12px 24px;text-align:center;font-size:12px;color:#888;">
      Candor Foods &mdash; IMS Customer Returns Notification
    </td></tr>
  </table>
</body></html>"""

    # Plain text fallback
    plain_lines = [f"Customer Returns {action}: {header.get('rtv_id', '')}"]
    plain_lines.append("")
    if extra_info:
        plain_lines.append(extra_info)
        plain_lines.append("")
    for label, value in header_fields:
        plain_lines.append(f"{label}: {value}")
    plain_lines.append("")
    plain_lines.append("Line Items:")
    for line in lines:
        plain_lines.append(
            f"  {line.get('item_description', '')} | {line.get('qty', '0')} {line.get('uom', '')} | "
            f"Rate: {line.get('rate', '0')} | Value: {line.get('value', '0')} | Net Wt: {line.get('net_weight', '0')}"
        )
    if boxes:
        plain_lines.append("")
        plain_lines.append("Boxes:")
        for box in boxes:
            plain_lines.append(
                f"  {box.get('article_description', '')} Box#{box.get('box_number', '')} | "
                f"Net: {box.get('net_weight', '0')} | Gross: {box.get('gross_weight', '0')} | "
                f"Lot: {box.get('lot_number', '-')} | Box ID: {box.get('box_id', '-')}"
            )

    if action_buttons:
        plain_lines.append("")
        plain_lines.append("Actions:")
        for label, url, _color in action_buttons:
            plain_lines.append(f"  {label}: {url}")

    return html, "\n".join(plain_lines)


def notify_rtv_created(rtv_detail: dict) -> None:
    """Send the 'Created' notification for a customer return as ONE conversation.

    A single thread, never two disjoint mails:
      * The assigned business head gets the email WITH the Approve / Reject / Hold
        buttons (thread root). Only the business head receives the buttons.
      * Everyone else (pooja + CC) gets the SAME email WITHOUT buttons, threaded
        under that root, so the later 'Approved' mail lands in the trail for all.
    If the return has no recognised business head, a single button-less broadcast
    goes to everyone.
    """
    rtv_id = rtv_detail.get("rtv_id", "")
    bh_email = _lookup_business_head_email(rtv_detail.get("business_head"))
    thread_id = _rtv_thread_key(rtv_id)
    subject = _rtv_subject(rtv_id)
    lines = rtv_detail.get("lines", [])
    boxes = rtv_detail.get("boxes", [])

    # CC = constant CC + selected/manual Sales POC + creator.
    cc_candidates: list[str] = list(RTV_CC_CONSTANT)
    poc_email = _lookup_sales_poc_email(rtv_detail.get("sales_poc"))
    if poc_email:
        cc_candidates.append(poc_email)
    manual_poc_email = rtv_detail.get("sales_poc_email")
    if manual_poc_email and manual_poc_email.strip():
        cc_candidates.append(manual_poc_email.strip())
    created_by = rtv_detail.get("created_by")
    if created_by:
        cc_candidates.append(created_by)
    wh_cc = _warehouse_cc_email(rtv_detail.get("factory_unit"))
    if wh_cc:
        cc_candidates.append(wh_cc)

    def _dedupe(addrs: list[str], exclude: set[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for addr in addrs:
            n = addr.strip().lower()
            if not n or n in seen or n in exclude:
                continue
            seen.add(n)
            out.append(addr.strip())
        return out

    if bh_email:
        # 1) Business head — WITH action buttons. This message is the thread root.
        action_buttons = [
            ("Approve", _build_rtv_action_url(rtv_id, bh_email, "approve"), "#27ae60"),
            ("Reject",  _build_rtv_action_url(rtv_id, bh_email, "reject"),  "#c0392b"),
            ("Hold",    _build_rtv_action_url(rtv_id, bh_email, "hold"),    "#e67e22"),
        ]
        html_bh, plain_bh = _rtv_email_html(
            action="Created", header=rtv_detail, lines=lines, boxes=boxes,
            extra_info="Please Approve / Reject / Hold this customer return using the buttons below.",
            action_buttons=action_buttons,
        )
        _send_email_background(
            subject=subject, html_body=html_bh, plain_body=plain_bh,
            to=[bh_email], message_id=thread_id,
        )

        # 2) Everyone else — NO buttons, threaded under the same conversation so
        #    the later status mail appears in their trail too.
        cc = _dedupe(cc_candidates, {bh_email.strip().lower(), RTV_NOTIFY_TO.strip().lower()})
        html_cc, plain_cc = _rtv_email_html(
            action="Created", header=rtv_detail, lines=lines, boxes=boxes,
            action_buttons=None,
        )
        _send_email_background(
            subject=subject, html_body=html_cc, plain_body=plain_cc,
            to=[RTV_NOTIFY_TO], cc=cc, in_reply_to=thread_id,
        )
    else:
        # No mapped business head — single button-less broadcast to everyone.
        cc = _dedupe(cc_candidates, {RTV_NOTIFY_TO.strip().lower()})
        html, plain = _rtv_email_html(
            action="Created", header=rtv_detail, lines=lines, boxes=boxes,
            action_buttons=None,
        )
        _send_email_background(
            subject=subject, html_body=html, plain_body=plain,
            to=[RTV_NOTIFY_TO], cc=cc, message_id=thread_id,
        )


def notify_rtv_weight_discrepancy(rtv_detail: dict, summary: dict) -> None:
    """Email a net-weight discrepancy SUMMARY at final box submit.

    ``summary`` is produced by compute_rtv_weight_discrepancy and has:
      rows: [{item_description, expected, actual, diff}], total_expected,
      total_actual, total_diff, has_discrepancy.

    The mail is a per-line totals summary (Item | Expected | Actual | Diff) plus
    an overall total row. It deliberately does NOT include a per-box table.
    Recipients follow the standard RTV pattern (BH + pooja in TO; constants +
    creator in CC).
    """
    rtv_id = rtv_detail.get("rtv_id", "")
    business_head = rtv_detail.get("business_head")
    created_by = rtv_detail.get("created_by")
    rows = summary.get("rows", [])

    def _fmt(n) -> str:
        try:
            return f"{float(n):.3f}"
        except (TypeError, ValueError):
            return "0.000"

    row_html = []
    for r in rows:
        diff = r.get("diff", 0) or 0
        diff_color = "#c0392b" if diff != 0 else "#27ae60"
        row_html.append(
            f"<tr>"
            f"<td style='padding:6px 10px;border:1px solid #e2e8f0;'>{escape(str(r.get('item_description', '')))}</td>"
            f"<td style='padding:6px 10px;border:1px solid #e2e8f0;text-align:right;'>{_fmt(r.get('expected'))}</td>"
            f"<td style='padding:6px 10px;border:1px solid #e2e8f0;text-align:right;'>{_fmt(r.get('actual'))}</td>"
            f"<td style='padding:6px 10px;border:1px solid #e2e8f0;text-align:right;color:{diff_color};font-weight:bold;'>{_fmt(diff)}</td>"
            f"</tr>"
        )

    total_diff = summary.get("total_diff", 0) or 0
    total_color = "#c0392b" if total_diff != 0 else "#27ae60"
    banner = (
        "Net-weight discrepancy detected between the expected (UOM × Total Qty) "
        "and the actual box-wise totals."
        if summary.get("has_discrepancy")
        else "Box entry submitted — expected and actual net weights match."
    )

    html = f"""<!DOCTYPE html>
<html><body style="font-family:Arial,Helvetica,sans-serif;color:#222;">
  <h2 style="font-size:18px;margin:0 0 6px;">Net-Weight Summary — {escape(rtv_id)}</h2>
  <p style="font-size:13px;color:#555;margin:0 0 14px;">{escape(banner)}</p>
  <table style="border-collapse:collapse;font-size:13px;min-width:480px;">
    <thead>
      <tr style="background:#29417A;color:#fff;">
        <th style="padding:8px 10px;border:1px solid #29417A;text-align:left;">Item</th>
        <th style="padding:8px 10px;border:1px solid #29417A;text-align:right;">Expected (kg)</th>
        <th style="padding:8px 10px;border:1px solid #29417A;text-align:right;">Actual (kg)</th>
        <th style="padding:8px 10px;border:1px solid #29417A;text-align:right;">Diff (kg)</th>
      </tr>
    </thead>
    <tbody>
      {''.join(row_html)}
      <tr style="background:#f4f4f4;font-weight:bold;">
        <td style="padding:8px 10px;border:1px solid #e2e8f0;">Total</td>
        <td style="padding:8px 10px;border:1px solid #e2e8f0;text-align:right;">{_fmt(summary.get('total_expected'))}</td>
        <td style="padding:8px 10px;border:1px solid #e2e8f0;text-align:right;">{_fmt(summary.get('total_actual'))}</td>
        <td style="padding:8px 10px;border:1px solid #e2e8f0;text-align:right;color:{total_color};">{_fmt(total_diff)}</td>
      </tr>
    </tbody>
  </table>
</body></html>"""

    plain_lines = [f"Net-Weight Summary — {rtv_id}", banner, ""]
    plain_lines.append("Item | Expected | Actual | Diff")
    for r in rows:
        plain_lines.append(
            f"{r.get('item_description', '')} | {_fmt(r.get('expected'))} | "
            f"{_fmt(r.get('actual'))} | {_fmt(r.get('diff'))}"
        )
    plain_lines.append(
        f"TOTAL | {_fmt(summary.get('total_expected'))} | "
        f"{_fmt(summary.get('total_actual'))} | {_fmt(total_diff)}"
    )
    plain = "\n".join(plain_lines)

    to_list: list[str] = []
    bh_email = _lookup_business_head_email(business_head)
    if bh_email:
        to_list.append(bh_email)
    to_list.append(RTV_NOTIFY_TO)

    _send_email_background(
        subject=_rtv_subject(rtv_id, reply=True),
        html_body=html,
        plain_body=plain,
        to=to_list,
        cc=_build_rtv_cc(
            business_head, created_by,
            sales_poc=rtv_detail.get("sales_poc"),
            sales_poc_email=rtv_detail.get("sales_poc_email"),
            factory_unit=rtv_detail.get("factory_unit"),
        ),
        in_reply_to=_rtv_thread_key(rtv_detail.get("rtv_id", "")),
    )


def notify_rtv_status_changed(rtv_detail: dict, new_status: str, actioned_by: str) -> None:
    """Send a confirmation mail after an Approve / Reject / Hold action.

    Uses the same recipient pattern as notify_rtv_created (BH + pooja in TO,
    constants + creator in CC) so everyone on the original Created mail sees
    the outcome.
    """
    rtv_id = rtv_detail.get("rtv_id", "")
    html, plain = _rtv_email_html(
        action=new_status,
        header=rtv_detail,
        lines=rtv_detail.get("lines", []),
        boxes=rtv_detail.get("boxes", []),
        extra_info=f"{new_status} by: {_format_actor(actioned_by)}",
    )

    bh_email = _lookup_business_head_email(rtv_detail.get("business_head"))
    to_list: list[str] = []
    if bh_email:
        to_list.append(bh_email)
    to_list.append(RTV_NOTIFY_TO)

    to_lower = {addr.strip().lower() for addr in to_list}
    cc_candidates: list[str] = list(RTV_CC_CONSTANT)
    poc_email = _lookup_sales_poc_email(rtv_detail.get("sales_poc"))
    if poc_email:
        cc_candidates.append(poc_email)
    manual_poc_email = rtv_detail.get("sales_poc_email")
    if manual_poc_email and manual_poc_email.strip():
        cc_candidates.append(manual_poc_email.strip())
    created_by = rtv_detail.get("created_by")
    if created_by:
        cc_candidates.append(created_by)
    wh_cc = _warehouse_cc_email(rtv_detail.get("factory_unit"))
    if wh_cc:
        cc_candidates.append(wh_cc)
    seen: set[str] = set()
    cc: list[str] = []
    for addr in cc_candidates:
        normalized = addr.strip().lower()
        if not normalized or normalized in seen or normalized in to_lower:
            continue
        seen.add(normalized)
        cc.append(addr.strip())

    _send_email_background(
        subject=_rtv_subject(rtv_id, reply=True),
        html_body=html,
        plain_body=plain,
        to=to_list,
        cc=cc,
        in_reply_to=_rtv_thread_key(rtv_detail.get("rtv_id", "")),
    )


def notify_rtv_rejected(rtv_detail: dict, rejected_by: str) -> None:
    """Send notification email when an RTV is rejected via the magic-link action."""
    html, plain = _rtv_email_html(
        action="Rejected",
        header=rtv_detail,
        lines=rtv_detail.get("lines", []),
        boxes=rtv_detail.get("boxes", []),
        extra_info=f"Rejected by: {_format_actor(rejected_by)}",
    )
    bh_email = _lookup_business_head_email(rtv_detail.get("business_head"))
    cc = _build_rtv_cc(
        rtv_detail.get("business_head"), rtv_detail.get("created_by"), rejected_by,
        sales_poc=rtv_detail.get("sales_poc"),
        sales_poc_email=rtv_detail.get("sales_poc_email"),
        factory_unit=rtv_detail.get("factory_unit"),
    )
    _send_email_background(
        subject=_rtv_subject(rtv_detail.get('rtv_id', ''), reply=True),
        html_body=html,
        plain_body=plain,
        to=[bh_email, RTV_NOTIFY_TO] if bh_email else [RTV_NOTIFY_TO],
        cc=cc,
        in_reply_to=_rtv_thread_key(rtv_detail.get("rtv_id", "")),
    )


def notify_rtv_held(rtv_detail: dict, held_by: str) -> None:
    """Notify everyone the RTV was placed on Hold; resend action email to business head."""
    html, plain = _rtv_email_html(
        action="On Hold",
        header=rtv_detail,
        lines=rtv_detail.get("lines", []),
        boxes=rtv_detail.get("boxes", []),
        extra_info=f"Placed on hold by: {_format_actor(held_by)}. The business head can still approve or reject later.",
    )
    bh_email = _lookup_business_head_email(rtv_detail.get("business_head"))
    cc = _build_rtv_cc(
        rtv_detail.get("business_head"), rtv_detail.get("created_by"), held_by,
        sales_poc=rtv_detail.get("sales_poc"),
        sales_poc_email=rtv_detail.get("sales_poc_email"),
        factory_unit=rtv_detail.get("factory_unit"),
    )
    _send_email_background(
        subject=_rtv_subject(rtv_detail.get('rtv_id', ''), reply=True),
        html_body=html,
        plain_body=plain,
        to=[bh_email, RTV_NOTIFY_TO] if bh_email else [RTV_NOTIFY_TO],
        cc=cc,
        in_reply_to=_rtv_thread_key(rtv_detail.get("rtv_id", "")),
    )


def notify_rtv_approved(rtv_detail: dict, approved_by: str) -> None:
    """Send notification email when an RTV is approved."""
    html, plain = _rtv_email_html(
        action="Approved",
        header=rtv_detail,
        lines=rtv_detail.get("lines", []),
        boxes=rtv_detail.get("boxes", []),
        extra_info=f"Approved by: {_format_actor(approved_by)}",
    )
    bh_email = _lookup_business_head_email(rtv_detail.get("business_head"))
    cc = _build_rtv_cc(
        rtv_detail.get("business_head"),
        rtv_detail.get("created_by"),
        approved_by,
        sales_poc=rtv_detail.get("sales_poc"),
        sales_poc_email=rtv_detail.get("sales_poc_email"),
        factory_unit=rtv_detail.get("factory_unit"),
    )
    _send_email_background(
        subject=_rtv_subject(rtv_detail.get('rtv_id', ''), reply=True),
        html_body=html,
        plain_body=plain,
        to=[bh_email, RTV_NOTIFY_TO] if bh_email else [RTV_NOTIFY_TO],
        cc=cc,
        in_reply_to=_rtv_thread_key(rtv_detail.get("rtv_id", "")),
    )


def notify_rtv_deleted(
    rtv_id: str,
    company: str,
    deleted_by: str | None = None,
    *,
    business_head: str | None = None,
    created_by: str | None = None,
    lines_count: int | None = None,
    boxes_count: int | None = None,
    factory_unit: str | None = None,
) -> None:
    """Send notification email when an RTV is deleted."""
    header = {"rtv_id": rtv_id, "status": "Deleted", "business_head": business_head,
              "created_by": created_by, "factory_unit": factory_unit}
    removed = []
    if lines_count is not None:
        removed.append(f"{lines_count} line item{'s' if lines_count != 1 else ''}")
    if boxes_count is not None:
        removed.append(f"{boxes_count} box{'es' if boxes_count != 1 else ''}")
    removed_txt = f" Removed: {', '.join(removed)}." if removed else " along with all its lines and boxes."
    extra = f"Customer Returns {rtv_id} in {company} has been permanently deleted.{removed_txt}"
    if deleted_by:
        extra += f" Deleted by: {_format_actor(deleted_by)}."
    html, plain = _rtv_email_html(
        action="Deleted",
        header=header,
        lines=[],
        boxes=[],
        extra_info=extra,
    )
    bh_email = _lookup_business_head_email(business_head)
    cc = _build_rtv_cc(business_head, created_by, deleted_by, factory_unit=factory_unit)
    _send_email_background(
        subject=_rtv_subject(rtv_id, reply=True),
        html_body=html,
        plain_body=plain,
        to=[bh_email, RTV_NOTIFY_TO] if bh_email else [RTV_NOTIFY_TO],
        cc=cc,
        in_reply_to=_rtv_thread_key(rtv_id),
    )


def notify_rtv_header_updated(rtv_detail: dict) -> None:
    """Send notification email when RTV header is updated."""
    html, plain = _rtv_email_html(
        action="Header Updated",
        header=rtv_detail,
        lines=[],
        boxes=[],
    )
    bh_email = _lookup_business_head_email(rtv_detail.get("business_head"))
    cc = _build_rtv_cc(
        rtv_detail.get("business_head"), rtv_detail.get("created_by"),
        sales_poc=rtv_detail.get("sales_poc"),
        sales_poc_email=rtv_detail.get("sales_poc_email"),
        factory_unit=rtv_detail.get("factory_unit"),
    )
    _send_email_background(
        subject=_rtv_subject(rtv_detail.get('rtv_id', ''), reply=True),
        html_body=html,
        plain_body=plain,
        to=[bh_email, RTV_NOTIFY_TO] if bh_email else [RTV_NOTIFY_TO],
        cc=cc,
        in_reply_to=_rtv_thread_key(rtv_detail.get("rtv_id", "")),
    )


# ── Consolidated "Updated" mail (one save -> one mail) ──────────────

_UPD_HEADER_FIELDS = [
    ("rtv_id", "Return ID"), ("rtv_date", "Return Date"),
    ("factory_unit", "Factory Unit"), ("customer", "Customer"),
    ("invoice_number", "Invoice Number"), ("challan_no", "Challan No"),
    ("dn_no", "DN No"), ("sales_poc", "Sales POC"),
    ("business_head", "Business Head"), ("remark", "Remark"),
    ("status", "Status"), ("created_by", "Created By"),
]


def _esc(v) -> str:
    return escape(str(v if v is not None else ""))


def _fmt_kg(v) -> str:
    try:
        return f"{float(v):g}"
    except (TypeError, ValueError):
        return str(v if v is not None else "")


def _rtv_updated_html(detail: dict, summary: dict) -> tuple[str, str]:
    """Render the consolidated 'Updated' mail: what-changed + highlighted header/line
    rows (old -> new) + box summary + short/short-weight. No full boxes table."""
    rtv_id = detail.get("rtv_id", "")
    hchanges = summary.get("header_changes", []) or []
    lchanges = summary.get("line_changes", {}) or {}
    bchanges = summary.get("box_changes", {}) or {}
    box_summary = summary.get("box_summary", {}) or {}
    short = summary.get("short", []) or []
    AMBER = "background:#fff7ed;"

    def _delta(old, new):
        return f"<s style='color:#999;'>{_esc(old) or '&mdash;'}</s> &rarr; <strong>{_esc(new) or '&mdash;'}</strong>"

    # ── What-changed block ──
    bullets = []
    for c in hchanges:
        bullets.append(f"<li>{_esc(c['label'])}: {_delta(c['old'], c['new'])}</li>")
    for item in lchanges.get("added", []):
        bullets.append(f"<li>Line added: <strong>{_esc(item)}</strong></li>")
    for item in lchanges.get("removed", []):
        bullets.append(f"<li>Line removed: <strong>{_esc(item)}</strong></li>")
    for c in lchanges.get("changed", []):
        bullets.append(f"<li>{_esc(c['item'])} &mdash; {_esc(c['label'])}: {_delta(c['old'], c['new'])}</li>")
    box_bits = []
    if bchanges.get("added"):
        box_bits.append(f"{bchanges['added']} added")
    if bchanges.get("deleted"):
        box_bits.append(f"{bchanges['deleted']} removed")
    if bchanges.get("updated"):
        box_bits.append(f"{bchanges['updated']} saved")
    if box_bits:
        bullets.append(f"<li>Boxes: {_esc(', '.join(box_bits))}</li>")
    if not bullets:
        bullets.append("<li>Box weights / data re-saved (no header or line field changes).</li>")
    what_changed = (
        "<div style='margin:0 0 18px;padding:12px 14px;border-left:4px solid #29417A;background:#f0f4fa;'>"
        "<div style='font-weight:bold;color:#29417A;margin-bottom:6px;'>What changed</div>"
        f"<ul style='margin:0;padding-left:18px;font-size:13px;line-height:1.6;'>{''.join(bullets)}</ul></div>"
    )

    # ── Header table (highlight changed) ──
    changed_fields = {c["field"]: c for c in hchanges}
    header_rows = ""
    for key, label in _UPD_HEADER_FIELDS:
        if key in changed_fields:
            c = changed_fields[key]
            val, row_style = _delta(c["old"], c["new"]), AMBER
        else:
            raw = detail.get(key)
            if key == "created_by":
                raw = _format_actor(raw)
            elif key == "rtv_date":
                raw = fmt_ist(raw)
            val, row_style = (_esc(raw) or "-"), ""
        header_rows += (
            f"<tr style='{row_style}'>"
            f"<td style='padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;width:160px;'>{_esc(label)}</td>"
            f"<td style='padding:6px 10px;border:1px solid #e0e0e0;'>{val}</td></tr>"
        )

    # ── Line items table (highlight changed/added) ──
    changed_items = {c["item"] for c in lchanges.get("changed", [])} | set(lchanges.get("added", []))
    line_rows = ""
    for l in detail.get("lines", []) or []:
        hl = AMBER if l.get("item_description") in changed_items else ""
        cells = "".join(
            f"<td style='padding:6px 10px;border:1px solid #e0e0e0;{align}'>{_esc(l.get(k))}</td>"
            for k, align in [
                ("material_type", ""), ("item_category", ""), ("sub_category", ""),
                ("item_description", ""), ("uom", ""),
                ("qty", "text-align:right;"), ("rate", "text-align:right;"),
                ("value", "text-align:right;"), ("net_weight", "text-align:right;"),
            ]
        )
        line_rows += f"<tr style='{hl}'>{cells}</tr>"

    # ── Box summary (per article; no per-box table) ──
    box_section = ""
    if box_summary:
        rows, tot_boxes, tot_net, tot_gross = "", 0.0, 0.0, 0.0
        for art, s in box_summary.items():
            tot_boxes += s["boxes"]
            tot_net += s["total_net"]
            tot_gross += s.get("total_gross", 0.0)
            rows += (
                f"<tr><td style='padding:6px 10px;border:1px solid #e0e0e0;'>{_esc(art)}</td>"
                f"<td style='padding:6px 10px;border:1px solid #e0e0e0;text-align:right;'>{_fmt_kg(s['boxes'])}</td>"
                f"<td style='padding:6px 10px;border:1px solid #e0e0e0;text-align:right;'>{_fmt_kg(s['total_net'])}</td>"
                f"<td style='padding:6px 10px;border:1px solid #e0e0e0;text-align:right;'>{_fmt_kg(s.get('total_gross', 0.0))}</td></tr>"
            )
        rows += (
            f"<tr style='background:#e8edf5;font-weight:bold;'>"
            f"<td style='padding:6px 10px;border:1px solid #e0e0e0;'>Total</td>"
            f"<td style='padding:6px 10px;border:1px solid #e0e0e0;text-align:right;'>{_fmt_kg(tot_boxes)}</td>"
            f"<td style='padding:6px 10px;border:1px solid #e0e0e0;text-align:right;'>{_fmt_kg(tot_net)}</td>"
            f"<td style='padding:6px 10px;border:1px solid #e0e0e0;text-align:right;'>{_fmt_kg(tot_gross)}</td></tr>"
        )
        box_section = (
            "<h3 style='color:#29417A;margin:24px 0 8px;'>Box Summary</h3>"
            "<table style='border-collapse:collapse;width:100%;font-size:13px;'>"
            "<thead><tr style='background:#29417A;color:#fff;'>"
            "<th style='padding:8px 10px;text-align:left;'>Article</th>"
            "<th style='padding:8px 10px;text-align:right;'>Boxes</th>"
            "<th style='padding:8px 10px;text-align:right;'>Total Net Wt</th>"
            "<th style='padding:8px 10px;text-align:right;'>Total Gross Wt</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )

    # ── Short-weight breakdown: "<full> full, <short> short -- received vs expected" ──
    short_section = ""
    if short:
        r = "".join(
            f"<tr><td style='padding:6px 10px;border:1px solid #fed7aa;'>{_esc(a['article'])}</td>"
            f"<td style='padding:6px 10px;border:1px solid #fed7aa;text-align:center;'>{_fmt_kg(a['full'])} full</td>"
            f"<td style='padding:6px 10px;border:1px solid #fed7aa;text-align:center;color:#c0392b;font-weight:bold;'>{_fmt_kg(a['short'])} short</td>"
            f"<td style='padding:6px 10px;border:1px solid #fed7aa;text-align:right;'>{_fmt_kg(a['received'])}</td>"
            f"<td style='padding:6px 10px;border:1px solid #fed7aa;text-align:right;'>{_fmt_kg(a['expected'])}</td>"
            f"<td style='padding:6px 10px;border:1px solid #fed7aa;text-align:right;color:#c0392b;font-weight:bold;'>-{_fmt_kg(a['shortfall'])}</td></tr>"
            for a in short
        )
        short_section = (
            "<h3 style='color:#c0392b;margin:24px 0 8px;'>&#9888; Short-Weight</h3>"
            "<table style='border-collapse:collapse;width:100%;font-size:13px;'>"
            "<thead><tr style='background:#e67e22;color:#fff;'>"
            "<th style='padding:8px 10px;text-align:left;'>Article</th>"
            "<th style='padding:8px 10px;text-align:center;'>Full</th>"
            "<th style='padding:8px 10px;text-align:center;'>Short</th>"
            "<th style='padding:8px 10px;text-align:right;'>Received (Net)</th>"
            "<th style='padding:8px 10px;text-align:right;'>Expected</th>"
            "<th style='padding:8px 10px;text-align:right;'>Short by</th></tr></thead>"
            f"<tbody>{r}</tbody></table>"
        )

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;margin:0;padding:0;background:#f4f4f4;">
  <table width="100%" cellpadding="0" cellspacing="0" style="max-width:800px;margin:20px auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
    <tr><td style="background:#29417A;color:#fff;padding:20px 24px;">
      <h2 style="margin:0;">Customer Returns Updated</h2>
      <p style="margin:4px 0 0;opacity:0.85;font-size:14px;">{_esc(rtv_id)} &mdash; {now_ist().strftime('%d %b %Y, %I:%M %p')}</p>
    </td></tr>
    <tr><td style="padding:20px 24px;">
      {what_changed}
      <h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;"><tbody>{header_rows}</tbody></table>
      <h3 style="color:#29417A;margin:24px 0 8px;">Line Items</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">
          <th style="padding:8px 10px;text-align:left;">Material</th>
          <th style="padding:8px 10px;text-align:left;">Category</th>
          <th style="padding:8px 10px;text-align:left;">Sub Category</th>
          <th style="padding:8px 10px;text-align:left;">Description</th>
          <th style="padding:8px 10px;text-align:left;">UOM</th>
          <th style="padding:8px 10px;text-align:right;">Qty</th>
          <th style="padding:8px 10px;text-align:right;">Rate</th>
          <th style="padding:8px 10px;text-align:right;">Value</th>
          <th style="padding:8px 10px;text-align:right;">Net Wt</th>
        </tr></thead>
        <tbody>{line_rows}</tbody>
      </table>
      {box_section}
      {short_section}
    </td></tr>
    <tr><td style="background:#f8f9fa;padding:12px 24px;text-align:center;font-size:12px;color:#888;">
      Candor Foods &mdash; IMS Customer Returns Notification
    </td></tr>
  </table>
</body></html>"""

    pl = [f"Customer Returns Updated: {rtv_id}", "", "What changed:"]
    for c in hchanges:
        pl.append(f"  - {c['label']}: {c['old']} -> {c['new']}")
    for item in lchanges.get("added", []):
        pl.append(f"  - Line added: {item}")
    for item in lchanges.get("removed", []):
        pl.append(f"  - Line removed: {item}")
    for c in lchanges.get("changed", []):
        pl.append(f"  - {c['item']} {c['label']}: {c['old']} -> {c['new']}")
    if box_bits:
        pl.append(f"  - Boxes: {', '.join(box_bits)}")
    if box_summary:
        pl.append("")
        pl.append("Box summary:")
        for art, s in box_summary.items():
            pl.append(f"  {art}: {_fmt_kg(s['boxes'])} boxes, net {_fmt_kg(s['total_net'])} kg, gross {_fmt_kg(s.get('total_gross', 0.0))} kg")
    if short:
        pl.append("")
        pl.append("Short-weight:")
        for a in short:
            pl.append(f"  {a['article']}: {_fmt_kg(a['full'])} full, {_fmt_kg(a['short'])} short -- received {_fmt_kg(a['received'])} kg vs expected {_fmt_kg(a['expected'])} kg (short {_fmt_kg(a['shortfall'])})")
    return html, "\n".join(pl)


def notify_rtv_updated(rtv_detail: dict, summary: dict) -> None:
    """Send ONE consolidated 'Updated' mail (replaces the separate header/lines mails)."""
    html, plain = _rtv_updated_html(rtv_detail, summary)
    bh_email = _lookup_business_head_email(rtv_detail.get("business_head"))
    cc = _build_rtv_cc(
        rtv_detail.get("business_head"), rtv_detail.get("created_by"),
        sales_poc=rtv_detail.get("sales_poc"),
        sales_poc_email=rtv_detail.get("sales_poc_email"),
        factory_unit=rtv_detail.get("factory_unit"),
    )
    _send_email_background(
        subject=_rtv_subject(rtv_detail.get("rtv_id", ""), reply=True),
        html_body=html,
        plain_body=plain,
        to=[bh_email, RTV_NOTIFY_TO] if bh_email else [RTV_NOTIFY_TO],
        cc=cc,
        in_reply_to=_rtv_thread_key(rtv_detail.get("rtv_id", "")),
    )


# ════════════════════════════════════════════════════════════
#  Job Work Notifications (billing@candorfoods.in)
# ════════════════════════════════════════════════════════════


def _jw_table(rows: list[tuple[str, str]]) -> str:
    out = ""
    for label, value in rows:
        out += (
            f'<tr>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;'
            f'background:#f8f9fa;width:180px;">{label}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{value if value not in (None, "") else "-"}</td>'
            f'</tr>'
        )
    return out


def _jw_lines_html(lines: list[dict], columns: list[tuple[str, str]]) -> str:
    if not lines:
        return f'<tr><td colspan="{len(columns)}" style="text-align:center;padding:8px;">No line items</td></tr>'
    rows = ""
    for line in lines:
        rows += "<tr>"
        for _, key in columns:
            rows += f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{line.get(key, "") or "-"}</td>'
        rows += "</tr>"
    return rows


def _jw_wrap(title: str, subtitle: str, body_html: str) -> str:
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;margin:0;padding:0;background:#f4f4f4;">
  <table width="100%" cellpadding="0" cellspacing="0" style="max-width:860px;margin:20px auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
    <tr><td style="background:#29417A;color:#fff;padding:20px 24px;">
      <h2 style="margin:0;">{title}</h2>
      <p style="margin:4px 0 0;opacity:0.85;font-size:14px;">{subtitle} &mdash; {now_ist().strftime('%d %b %Y, %I:%M %p')}</p>
    </td></tr>
    <tr><td style="padding:20px 24px;">{body_html}</td></tr>
    <tr><td style="background:#f8f9fa;padding:12px 24px;text-align:center;font-size:12px;color:#888;">
      Candor Foods &mdash; IMS Job Work Notification
    </td></tr>
  </table>
</body></html>"""


def notify_job_work_material_out_created(payload: dict, header_id: int, created_by: str) -> None:
    """Send notification email when a Job Work Material Out is created."""
    header = payload.get("header", {}) or {}
    dispatch_to = payload.get("dispatch_to", {}) or {}
    line_items = payload.get("line_items", []) or []

    challan_no = header.get("challan_no") or payload.get("challan_no", "")
    job_work_date = header.get("job_work_date") or payload.get("dated", "")

    city = dispatch_to.get("city", "") or ""
    state = dispatch_to.get("state", "") or ""
    city_state = f"{city}, {state}".strip(", ") if (city or state) else ""

    # Source → Destination summary (prominent banner above header table).
    # When dispatching from Cold storage, the from_warehouse is "Cold storage"; if any
    # line carries a more-specific cold_unit, use it as the source suffix.
    source = header.get("from_warehouse", "") or "-"
    cold_units = sorted({(it.get("cold_unit") or "").strip() for it in line_items if it.get("cold_unit")})
    if source.lower() == "cold storage" and cold_units:
        source = f"Cold storage ({', '.join(cold_units)})"
    destination = header.get("to_party") or dispatch_to.get("name", "") or "-"
    route_html = (
        f'<div style="margin:0 0 14px;padding:10px 14px;border-left:4px solid #29417A;'
        f'background:#f0f4fa;font-size:14px;">'
        f'<strong style="color:#29417A;">Source → Destination:</strong> '
        f'<span style="font-weight:600;">{source}</span> '
        f'<span style="color:#29417A;font-weight:bold;">&rarr;</span> '
        f'<span style="font-weight:600;">{destination}</span>'
        f'</div>'
    )

    # Only include header fields that have actual values
    all_header_fields = [
        ("Record ID", str(header_id)),
        ("Challan No", challan_no),
        ("Job Work Date", str(job_work_date)),
        ("From Warehouse", header.get("from_warehouse", "")),
        ("To Party", header.get("to_party") or dispatch_to.get("name", "")),
        ("Party Address", header.get("party_address") or dispatch_to.get("address", "")),
        ("Party City / State", city_state),
        ("Contact Person", header.get("contact_person", "")),
        ("Contact Number", header.get("contact_number", "")),
        ("Purpose / Sub Category", header.get("purpose_of_work") or dispatch_to.get("sub_category", "")),
        ("Expected Return Date", header.get("expected_return_date", "")),
        ("Vehicle No", header.get("vehicle_no") or payload.get("motor_vehicle_no", "")),
        ("Driver Name", header.get("driver_name", "")),
        ("E-Way Bill No", payload.get("e_way_bill_no", "")),
        ("Dispatched Through", payload.get("dispatched_through", "")),
        ("Remarks", header.get("remarks") or payload.get("remarks", "")),
        ("Created By", created_by or "-"),
    ]
    filled_header_fields = [
        (label, val) for label, val in all_header_fields
        if val not in (None, "", "-", "None", ", ")
    ]
    header_rows = _jw_table(filled_header_fields)

    # Build per-item summary: group by (item_description, lot_number)
    # Use net_weight from the line directly so the email always reports NET weight,
    # regardless of what the frontend put into quantity.kgs (which can be gross).
    summary: dict[tuple, dict] = {}
    for it in line_items:
        qty = it.get("quantity", {}) or {}
        desc = it.get("item_description") or it.get("description", "") or "-"
        lot = it.get("lot_number") or "-"
        category = it.get("item_category") or "-"
        key = (desc, lot, category)
        try:
            kgs = float(it.get("net_weight") or 0)
        except (TypeError, ValueError):
            kgs = 0.0
        boxes = qty.get("boxes", 0) if isinstance(qty, dict) else 0
        if key not in summary:
            summary[key] = {"item_description": desc, "lot_number": lot, "category": category, "total_kgs": 0, "total_boxes": 0}
        summary[key]["total_kgs"] += kgs
        summary[key]["total_boxes"] += boxes

    summary_rows = ""
    grand_kgs = 0.0
    grand_boxes = 0
    for i, entry in enumerate(summary.values(), 1):
        grand_kgs += entry["total_kgs"]
        grand_boxes += entry["total_boxes"]
        summary_rows += (
            f'<tr style="background:{"#fff" if i % 2 else "#f8f9fa"};">'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{i}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{entry["item_description"]}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{entry["category"]}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{entry["total_kgs"]:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{entry["total_boxes"]}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{entry["lot_number"]}</td>'
            f'</tr>'
        )
    if not summary_rows:
        summary_rows = '<tr><td colspan="6" style="text-align:center;padding:8px;">No line items</td></tr>'
    else:
        summary_rows += (
            f'<tr style="background:#e8edf5;font-weight:bold;">'
            f'<td colspan="3" style="padding:6px 10px;border:1px solid #e0e0e0;">Total</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{grand_kgs:g} Kgs</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{grand_boxes} Boxes</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;"></td>'
            f'</tr>'
        )

    summary_headers = "".join(
        f'<th style="padding:8px 10px;text-align:left;">{h}</th>'
        for h in ["Sl", "Item Description", "Category", "Net Wt (Kgs)", "Qty (Boxes)", "Lot No"]
    )

    body = f"""
      {route_html}
      <h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>

      <h3 style="color:#29417A;margin:24px 0 8px;">Material Summary</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">{summary_headers}</tr></thead>
        <tbody>{summary_rows}</tbody>
      </table>
    """

    html = _jw_wrap(
        title="Job Work — Material Out Created",
        subtitle=f"Challan {challan_no or header_id}",
        body_html=body,
    )

    plain_lines = [
        f"Job Work Material Out Created — Challan: {challan_no} (ID {header_id})",
        f"Date: {job_work_date}",
        f"Source → Destination: {source}  →  {destination}",
        f"Purpose: {header.get('purpose_of_work') or dispatch_to.get('sub_category', '')}",
        f"Vehicle: {header.get('vehicle_no') or payload.get('motor_vehicle_no', '')}",
        f"Created By: {created_by or '-'}",
        "",
        "Material Summary:",
    ]
    for entry in summary.values():
        plain_lines.append(
            f"  {entry['item_description']} | Lot: {entry['lot_number']} | "
            f"{entry['total_kgs']:g} Kgs / {entry['total_boxes']} Boxes"
        )
    plain_lines.append(f"  TOTAL: {grand_kgs:g} Kgs / {grand_boxes} Boxes")

    cc = list(JOB_WORK_CC)
    if created_by and created_by not in cc and created_by != JOB_WORK_TO:
        cc.append(created_by)
    _send_email_background(
        subject=f"Job Work Challan: {challan_no or header_id}",
        html_body=html,
        plain_body="\n".join(plain_lines),
        to=JOB_WORK_TO,
        cc=cc,
        message_id=f"JWO-{challan_no or header_id}@candorfoods.in",
    )


def notify_job_work_material_in_created(
    payload: dict,
    ir_number: str,
    inward_receipt_id: int,
    created_by: str,
    challan_summary: list | None = None,
    all_ir_lines: list | None = None,
) -> None:
    """Send notification email when a Job Work Material In (Inward Receipt) is created.

    challan_summary: list of dicts {item_description, sent_kgs, sent_boxes} from jb_materialout_lines
    all_ir_lines: list of dicts {ir_number, receipt_date, receipt_type, item_description,
                  finished_goods_kgs, waste_kgs, rejection_kgs, min_loss_pct, max_loss_pct}
                  for all IRs of this challan including the current one
    """
    items = payload.get("items", []) or []
    receipt_type = payload.get("receipt_type", "partial")
    original_challan_no = payload.get("original_challan_no", "") or str(inward_receipt_id)
    is_partial = receipt_type.lower() == "partial"

    # ── Section A: IR Header ─────────────────────────────────────────────
    header_rows = _jw_table([
        ("IR Number", ir_number),
        ("Against Challan", original_challan_no),
        ("Receipt Date", str(payload.get("received_date", ""))),
        ("Receipt Type", receipt_type.title()),
        ("Inward Warehouse", payload.get("inward_warehouse", "")),
        ("Vehicle No", payload.get("vehicle_no", "")),
        ("Driver Name", payload.get("driver_name", "")),
        ("Remarks", payload.get("remarks", "")),
        ("Created By", created_by or "-"),
    ])

    # ── Section B: Current IR Lines with inline loss status ──────────────
    def _loss_badge(loss_pct: float, max_pct: float, partial: bool) -> str:
        if loss_pct <= 0:
            return '<span style="color:#27ae60;font-weight:bold;">OK</span>'
        if loss_pct > max_pct:
            if partial:
                return (
                    f'<span style="color:#e67e22;font-weight:bold;">'
                    f'{loss_pct:.1f}% &mdash; Partial, more expected</span>'
                )
            return (
                f'<span style="color:#c0392b;font-weight:bold;">'
                f'Excess {loss_pct:.1f}% (max {max_pct:.1f}%)</span>'
            )
        return f'<span style="color:#27ae60;">{loss_pct:.1f}% OK</span>'

    cur_line_rows = ""
    for i, it in enumerate(items, 1):
        sent = float(it.get("sent_kgs") or 0)
        fg = float(it.get("finished_goods_kgs") or 0)
        waste = float(it.get("waste_kgs") or 0)
        rej = float(it.get("rejection_kgs") or 0)
        accounted = fg + waste + rej
        loss_pct = ((sent - accounted) / sent * 100) if sent > 0 else 0
        max_pct = float(it.get("max_loss_pct") or 10)
        badge = _loss_badge(loss_pct, max_pct, is_partial)
        bg = "#fff" if i % 2 else "#f8f9fa"
        cur_line_rows += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{i}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{it.get("description", "") or "-"}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{sent:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{fg:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{waste:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{rej:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{badge}</td>'
            f'</tr>'
        )
    if not cur_line_rows:
        cur_line_rows = '<tr><td colspan="7" style="text-align:center;padding:8px;">No line items</td></tr>'
    cur_line_headers = "".join(
        f'<th style="padding:8px 10px;text-align:left;">{h}</th>'
        for h in ["Sl", "Description", "Sent Kgs", "FG Kgs", "Waste Kgs", "Rejection Kgs", "Loss Status"]
    )

    # ── Section C: Challan IR History ────────────────────────────────────
    ir_history: dict[str, dict] = {}
    for ln in (all_ir_lines or []):
        key = ln.get("ir_number", "?")
        if key not in ir_history:
            ir_history[key] = {
                "ir_number": key,
                "receipt_date": str(ln.get("receipt_date", "")),
                "receipt_type": ln.get("receipt_type", ""),
                "fg": 0.0, "waste": 0.0, "rejection": 0.0,
            }
        ir_history[key]["fg"] += float(ln.get("finished_goods_kgs") or 0)
        ir_history[key]["waste"] += float(ln.get("waste_kgs") or 0)
        ir_history[key]["rejection"] += float(ln.get("rejection_kgs") or 0)

    history_rows = ""
    total_fg = total_waste = total_rej = 0.0
    for ir_key, ir in ir_history.items():
        accounted = ir["fg"] + ir["waste"] + ir["rejection"]
        total_fg += ir["fg"]
        total_waste += ir["waste"]
        total_rej += ir["rejection"]
        is_current = ir_key == ir_number
        row_style = "background:#dbeafe;font-weight:bold;" if is_current else "background:#fff;"
        history_rows += (
            f'<tr style="{row_style}">'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">'
            f'{ir_key}{"&nbsp;&#9664; current" if is_current else ""}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{ir["receipt_date"]}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{ir["receipt_type"].title()}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{ir["fg"]:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{ir["waste"]:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{ir["rejection"]:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;font-weight:bold;">{accounted:g}</td>'
            f'</tr>'
        )
    total_accounted_history = total_fg + total_waste + total_rej
    if history_rows:
        history_rows += (
            f'<tr style="background:#e8edf5;font-weight:bold;">'
            f'<td colspan="3" style="padding:6px 10px;border:1px solid #e0e0e0;">Total</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{total_fg:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{total_waste:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{total_rej:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{total_accounted_history:g}</td>'
            f'</tr>'
        )
    else:
        history_rows = '<tr><td colspan="7" style="text-align:center;padding:8px;">No history available</td></tr>'
    history_headers = "".join(
        f'<th style="padding:8px 10px;text-align:left;">{h}</th>'
        for h in ["IR Number", "Date", "Type", "FG Kgs", "Waste Kgs", "Rejection Kgs", "Accounted Kgs"]
    )

    # ── Section D: Pendency per item ─────────────────────────────────────
    received_by_item: dict[str, float] = {}
    for ln in (all_ir_lines or []):
        desc = ln.get("item_description", "") or "-"
        received_by_item[desc] = received_by_item.get(desc, 0.0) + (
            float(ln.get("finished_goods_kgs") or 0)
            + float(ln.get("waste_kgs") or 0)
            + float(ln.get("rejection_kgs") or 0)
        )

    pendency_rows = ""
    total_sent_all = total_pending_all = 0.0
    for item in (challan_summary or []):
        desc = item.get("item_description", "") or "-"
        sent = float(item.get("sent_kgs") or 0)
        accounted = received_by_item.get(desc, 0.0)
        pending = max(sent - accounted, 0.0)
        total_sent_all += sent
        total_pending_all += pending
        if pending > 0:
            row_style = "font-weight:bold;background:#fff9e6;"
            pending_cell = f'<span style="color:#c0392b;">{pending:g} Kgs</span>'
        else:
            row_style = "color:#888;background:#f8f9fa;"
            pending_cell = '<span style="color:#27ae60;">Fully accounted</span>'
        pendency_rows += (
            f'<tr style="{row_style}">'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{desc}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{sent:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{accounted:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{pending_cell}</td>'
            f'</tr>'
        )

    if not pendency_rows:
        pendency_rows = '<tr><td colspan="4" style="text-align:center;padding:8px;">No dispatch data available</td></tr>'
    else:
        total_pending_label = (
            f'<span style="color:#c0392b;">{total_pending_all:g} Kgs pending</span>'
            if total_pending_all > 0
            else '<span style="color:#27ae60;">All accounted</span>'
        )
        pendency_rows += (
            f'<tr style="background:#e8edf5;font-weight:bold;">'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">Total</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{total_sent_all:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{total_accounted_history:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{total_pending_label}</td>'
            f'</tr>'
        )

    pendency_note = ""
    if total_pending_all > 0 and is_partial:
        pendency_note = (
            f'<p style="margin:8px 0 0;color:#e67e22;font-size:13px;">'
            f'&#9432; Further Material In receipt(s) expected &mdash; '
            f'{total_pending_all:g} Kgs still pending.</p>'
        )
    pendency_headers = "".join(
        f'<th style="padding:8px 10px;text-align:left;">{h}</th>'
        for h in ["Item Description", "Sent Kgs", "Total Accounted", "Pending"]
    )

    # ── Assemble HTML ─────────────────────────────────────────────────────
    body = f"""
      <h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>

      <h3 style="color:#29417A;margin:24px 0 8px;">Current Receipt Lines</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">{cur_line_headers}</tr></thead>
        <tbody>{cur_line_rows}</tbody>
      </table>

      <h3 style="color:#29417A;margin:24px 0 8px;">Challan IR History</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">{history_headers}</tr></thead>
        <tbody>{history_rows}</tbody>
      </table>

      <h3 style="color:#29417A;margin:24px 0 8px;">Pendency</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">{pendency_headers}</tr></thead>
        <tbody>{pendency_rows}</tbody>
      </table>
      {pendency_note}
    """

    html = _jw_wrap(
        title=f"Job Work — Material In ({'Final' if not is_partial else 'Partial'})",
        subtitle=f"IR {ir_number} | Challan {original_challan_no}",
        body_html=body,
    )

    # ── Plain text fallback ───────────────────────────────────────────────
    plain_lines = [
        f"Job Work Material In {'Final' if not is_partial else 'Partial'} — IR: {ir_number}",
        f"Against Challan: {original_challan_no}",
        f"Receipt Type: {receipt_type}",
        f"Receipt Date: {payload.get('received_date', '')}",
        f"Inward Warehouse: {payload.get('inward_warehouse', '')}",
        f"Created By: {created_by or '-'}",
        "",
        "Current Receipt Lines:",
    ]
    for it in items:
        sent = float(it.get("sent_kgs") or 0)
        fg = float(it.get("finished_goods_kgs") or 0)
        waste = float(it.get("waste_kgs") or 0)
        rej = float(it.get("rejection_kgs") or 0)
        loss_pct = ((sent - fg - waste - rej) / sent * 100) if sent > 0 else 0
        plain_lines.append(
            f"  {it.get('description', '')} | Sent: {sent:g} | FG: {fg:g} | "
            f"Waste: {waste:g} | Rejection: {rej:g} | Loss: {loss_pct:.1f}%"
            + (" [PARTIAL]" if is_partial else "")
        )
    if total_pending_all > 0:
        plain_lines += ["", f"Pending: {total_pending_all:g} Kgs still outstanding."]

    cc = list(JOB_WORK_CC)
    if created_by and created_by not in cc and created_by != JOB_WORK_TO:
        cc.append(created_by)
    _send_email_background(
        subject=f"Job Work Challan: {original_challan_no}",
        html_body=html,
        plain_body="\n".join(plain_lines),
        to=JOB_WORK_TO,
        cc=cc,
        in_reply_to=f"JWO-{original_challan_no}@candorfoods.in",
    )


def notify_job_work_material_out_updated(payload: dict, record_id: int, updated_by: str) -> None:
    """Send notification email when a Job Work Material Out is updated."""
    header = payload.get("header", {}) or {}
    dispatch_to = payload.get("dispatch_to", {}) or {}
    line_items = payload.get("line_items", []) or []

    challan_no = header.get("challan_no") or payload.get("challan_no", "")
    job_work_date = header.get("job_work_date") or payload.get("dated", "")

    # Source → Destination summary (prominent banner above header table).
    # When dispatching from Cold storage, the from_warehouse is "Cold storage"; if any
    # line carries a more-specific cold_unit, use it as the source suffix.
    source = header.get("from_warehouse", "") or "-"
    cold_units = sorted({(it.get("cold_unit") or "").strip() for it in line_items if it.get("cold_unit")})
    if source.lower() == "cold storage" and cold_units:
        source = f"Cold storage ({', '.join(cold_units)})"
    destination = header.get("to_party") or dispatch_to.get("name", "") or "-"
    route_html = (
        f'<div style="margin:0 0 14px;padding:10px 14px;border-left:4px solid #29417A;'
        f'background:#f0f4fa;font-size:14px;">'
        f'<strong style="color:#29417A;">Source → Destination:</strong> '
        f'<span style="font-weight:600;">{source}</span> '
        f'<span style="color:#29417A;font-weight:bold;">&rarr;</span> '
        f'<span style="font-weight:600;">{destination}</span>'
        f'</div>'
    )

    all_header_fields = [
        ("Record ID", str(record_id)),
        ("Challan No", challan_no),
        ("Date", str(job_work_date)),
        ("Warehouse", header.get("from_warehouse", "")),
        ("Party", header.get("to_party") or dispatch_to.get("name", "")),
        ("Purpose", header.get("purpose_of_work") or dispatch_to.get("sub_category", "")),
        ("Vehicle No", header.get("vehicle_no") or payload.get("motor_vehicle_no", "")),
        ("Remarks", header.get("remarks") or payload.get("remarks", "")),
        ("Updated By", updated_by or "-"),
    ]
    filled_header_fields = [
        (label, val) for label, val in all_header_fields
        if val not in (None, "", "-", "None")
    ]
    header_rows = _jw_table(filled_header_fields)

    # Use net_weight directly so the email always reports NET weight, not gross.
    summary: dict[tuple, dict] = {}
    for it in line_items:
        qty = it.get("quantity", {}) or {}
        desc = it.get("item_description") or it.get("description", "") or "-"
        lot = it.get("lot_number") or "-"
        category = it.get("item_category") or "-"
        key = (desc, lot, category)
        try:
            kgs = float(it.get("net_weight") or 0)
        except (TypeError, ValueError):
            kgs = 0.0
        boxes = qty.get("boxes", 0) if isinstance(qty, dict) else 0
        if key not in summary:
            summary[key] = {"item_description": desc, "lot_number": lot, "category": category, "total_kgs": 0, "total_boxes": 0}
        summary[key]["total_kgs"] += kgs
        summary[key]["total_boxes"] += boxes

    summary_rows = ""
    grand_kgs = 0.0
    grand_boxes = 0
    for i, entry in enumerate(summary.values(), 1):
        grand_kgs += entry["total_kgs"]
        grand_boxes += entry["total_boxes"]
        summary_rows += (
            f'<tr style="background:{"#fff" if i % 2 else "#f8f9fa"};">'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{i}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{entry["item_description"]}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{entry["category"]}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{entry["total_kgs"]:g}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{entry["total_boxes"]}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{entry["lot_number"]}</td>'
            f'</tr>'
        )
    if not summary_rows:
        summary_rows = '<tr><td colspan="6" style="text-align:center;padding:8px;">No line items</td></tr>'
    else:
        summary_rows += (
            f'<tr style="background:#e8edf5;font-weight:bold;">'
            f'<td colspan="3" style="padding:6px 10px;border:1px solid #e0e0e0;">Total</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{grand_kgs:g} Kgs</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{grand_boxes} Boxes</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;"></td>'
            f'</tr>'
        )

    summary_headers = "".join(
        f'<th style="padding:8px 10px;text-align:left;">{h}</th>'
        for h in ["Sl", "Item Description", "Category", "Net Wt (Kgs)", "Qty (Boxes)", "Lot No"]
    )

    body = f"""
      {route_html}
      <h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>

      <h3 style="color:#29417A;margin:24px 0 8px;">Material Summary</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">{summary_headers}</tr></thead>
        <tbody>{summary_rows}</tbody>
      </table>
    """

    html = _jw_wrap(
        title="Job Work — Material Out Updated",
        subtitle=f"Challan {challan_no or record_id}",
        body_html=body,
    )

    plain_lines = [
        f"Job Work Material Out Updated — Challan: {challan_no} (ID {record_id})",
        f"Updated By: {updated_by or '-'}",
        f"Source → Destination: {source}  →  {destination}",
        "",
        "Material Summary:",
    ]
    for entry in summary.values():
        plain_lines.append(
            f"  {entry['item_description']} | Lot: {entry['lot_number']} | "
            f"{entry['total_kgs']:g} Kgs / {entry['total_boxes']} Boxes"
        )
    if summary:
        plain_lines.append(f"  TOTAL: {grand_kgs:g} Kgs / {grand_boxes} Boxes")

    cc = list(JOB_WORK_CC)
    if updated_by and updated_by not in cc and updated_by != JOB_WORK_TO:
        cc.append(updated_by)
    _send_email_background(
        subject=f"Job Work Challan: {challan_no or record_id}",
        html_body=html,
        plain_body="\n".join(plain_lines),
        to=JOB_WORK_TO,
        cc=cc,
        in_reply_to=f"JWO-{challan_no or record_id}@candorfoods.in",
    )


def notify_job_work_material_out_deleted(challan_no: str, record_id: int, deleted_by: str) -> None:
    """Send notification email when a Job Work Material Out is permanently deleted."""
    header_rows = _jw_table([
        ("Record ID", str(record_id)),
        ("Challan No", challan_no),
        ("Deleted By", deleted_by or "-"),
    ])

    body = f"""
      <p style="color:#c0392b;font-weight:bold;font-size:15px;">
        Material Out record <strong>{challan_no or record_id}</strong> has been permanently deleted.
      </p>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>
    """

    html = _jw_wrap(
        title="Job Work — Material Out Deleted",
        subtitle=f"Challan {challan_no or record_id}",
        body_html=body,
    )

    plain = (
        f"Job Work Material Out Deleted — Challan: {challan_no} (ID {record_id})\n"
        f"This record has been permanently deleted.\n"
        f"Deleted By: {deleted_by or '-'}"
    )

    cc = list(JOB_WORK_CC)
    if deleted_by and deleted_by not in cc and deleted_by != JOB_WORK_TO:
        cc.append(deleted_by)
    _send_email_background(
        subject=f"Job Work Challan: {challan_no or record_id}",
        html_body=html,
        plain_body=plain,
        to=JOB_WORK_TO,
        cc=cc,
        in_reply_to=f"JWO-{challan_no or record_id}@candorfoods.in",
    )


def notify_job_work_material_in_deleted(ir_number: str, ir_id: int, challan_no: str, deleted_by: str) -> None:
    """Send notification email when a Job Work Material In (Inward Receipt) is permanently deleted."""
    header_rows = _jw_table([
        ("IR Number", ir_number),
        ("Inward Receipt ID", str(ir_id)),
        ("Against Challan", challan_no),
        ("Deleted By", deleted_by or "-"),
    ])

    body = f"""
      <p style="color:#c0392b;font-weight:bold;font-size:15px;">
        Inward Receipt <strong>{ir_number or ir_id}</strong> has been permanently deleted.
      </p>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>
    """

    html = _jw_wrap(
        title="Job Work — Material In Deleted",
        subtitle=f"IR {ir_number or ir_id}",
        body_html=body,
    )

    plain = (
        f"Job Work Material In Deleted — IR: {ir_number} (ID {ir_id})\n"
        f"Against Challan: {challan_no}\n"
        f"This record has been permanently deleted.\n"
        f"Deleted By: {deleted_by or '-'}"
    )

    cc = list(JOB_WORK_CC)
    if deleted_by and deleted_by not in cc and deleted_by != JOB_WORK_TO:
        cc.append(deleted_by)
    _send_email_background(
        subject=f"Job Work Challan: {challan_no or ir_id}",
        html_body=html,
        plain_body=plain,
        to=JOB_WORK_TO,
        cc=cc,
        in_reply_to=f"JWO-{challan_no or ir_id}@candorfoods.in",
    )


def notify_job_work_status_changed(
    challan_no: str, header_id: int, vendor: str,
    old_status: str, new_status: str, changed_by: str,
) -> None:
    """Send notification email when a Job Work challan status changes."""
    header_rows = _jw_table([
        ("Challan No", challan_no),
        ("Record ID", str(header_id)),
        ("Vendor / Party", vendor or "-"),
        ("Previous Status", old_status or "-"),
        ("New Status", new_status or "-"),
        ("Changed By", changed_by or "-"),
    ])

    body = f"""
      <h3 style="color:#29417A;margin:0 0 8px;">Status Transition</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>
    """

    html = _jw_wrap(
        title="Job Work — Status Changed",
        subtitle=f"Challan {challan_no or header_id}",
        body_html=body,
    )

    plain = (
        f"Job Work Status Changed — Challan: {challan_no} (ID {header_id})\n"
        f"Vendor: {vendor or '-'}\n"
        f"Status: {old_status} -> {new_status}\n"
        f"Changed By: {changed_by or '-'}"
    )

    cc = list(JOB_WORK_CC)
    if changed_by and changed_by not in cc and changed_by != JOB_WORK_TO:
        cc.append(changed_by)
    _send_email_background(
        subject=f"Job Work Challan: {challan_no or header_id}",
        html_body=html,
        plain_body=plain,
        to=JOB_WORK_TO,
        cc=cc,
        in_reply_to=f"JWO-{challan_no or header_id}@candorfoods.in",
    )


def notify_job_work_excess_loss(
    challan_no: str, header_id: int, vendor: str,
    item: str, loss_pct: float, ir_number: str, created_by: str,
) -> None:
    """Send ALERT notification when excess loss is detected on a Job Work inward receipt."""
    header_rows = _jw_table([
        ("Challan No", challan_no),
        ("Record ID", str(header_id)),
        ("Vendor / Party", vendor or "-"),
        ("Item", item or "-"),
        ("Loss %", f"{loss_pct:.2f}%"),
        ("IR Number", ir_number or "-"),
        ("Created By", created_by or "-"),
    ])

    body = f"""
      <p style="color:#c0392b;font-weight:bold;font-size:16px;">
        &#9888; ALERT: Excess loss of <strong>{loss_pct:.2f}%</strong> detected on item
        <strong>{item or '-'}</strong>.
      </p>
      <h3 style="color:#29417A;margin:16px 0 8px;">Details</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>
    """

    html = _jw_wrap(
        title="Job Work — ALERT: Excess Loss",
        subtitle=f"Challan {challan_no or header_id}",
        body_html=body,
    )

    plain = (
        f"ALERT: Excess Loss on Job Work — Challan: {challan_no} (ID {header_id})\n"
        f"Vendor: {vendor or '-'}\n"
        f"Item: {item or '-'}\n"
        f"Loss: {loss_pct:.2f}%\n"
        f"IR Number: {ir_number or '-'}\n"
        f"Created By: {created_by or '-'}"
    )

    cc = list(JOB_WORK_CC)
    if created_by and created_by not in cc and created_by != JOB_WORK_TO:
        cc.append(created_by)
    _send_email_background(
        subject=f"Job Work Challan: {challan_no or header_id}",
        html_body=html,
        plain_body=plain,
        to=JOB_WORK_TO,
        cc=cc,
        in_reply_to=f"JWO-{challan_no or header_id}@candorfoods.in",
    )


def notify_rtv_lines_updated(rtv_detail: dict) -> None:
    """Send notification email when RTV lines are replaced."""
    html, plain = _rtv_email_html(
        action="Lines Updated",
        header=rtv_detail,
        lines=rtv_detail.get("lines", []),
        boxes=rtv_detail.get("boxes", []),
    )
    bh_email = _lookup_business_head_email(rtv_detail.get("business_head"))
    cc = _build_rtv_cc(
        rtv_detail.get("business_head"), rtv_detail.get("created_by"),
        sales_poc=rtv_detail.get("sales_poc"),
        sales_poc_email=rtv_detail.get("sales_poc_email"),
        factory_unit=rtv_detail.get("factory_unit"),
    )
    _send_email_background(
        subject=_rtv_subject(rtv_detail.get('rtv_id', ''), reply=True),
        html_body=html,
        plain_body=plain,
        to=[bh_email, RTV_NOTIFY_TO] if bh_email else [RTV_NOTIFY_TO],
        cc=cc,
        in_reply_to=_rtv_thread_key(rtv_detail.get('rtv_id', '')),
    )


def send_job_work_weekly_digest() -> None:
    """Weekly digest email — open JWOs, overdue, excess loss, status summary. Sent every Monday 9 AM IST."""
    from shared.database import SessionLocal
    from sqlalchemy import text as sa_text

    db = SessionLocal()
    try:
        open_jwos = db.execute(sa_text("""
            SELECT id, challan_no, to_party, sub_category, job_work_date, status,
                   EXTRACT(DAY FROM NOW() - job_work_date::timestamp) as days_open
            FROM jb_materialout_header
            WHERE status IN ('sent', 'partially_received')
            ORDER BY job_work_date ASC
        """)).fetchall()

        status_counts = db.execute(sa_text("""
            SELECT status, COUNT(*) as cnt
            FROM jb_materialout_header
            GROUP BY status ORDER BY cnt DESC
        """)).fetchall()

        excess_loss = db.execute(sa_text("""
            SELECT r.ir_number, r.receipt_date, h.challan_no, h.to_party, h.sub_category,
                   l.item_description, l.sent_kgs, l.finished_goods_kgs, l.waste_kgs, l.rejection_kgs
            FROM jb_work_inward_receipt r
            JOIN jb_materialout_header h ON h.id = r.header_id
            LEFT JOIN jb_work_inward_lines l ON l.inward_receipt_id = r.id
            WHERE r.created_at >= NOW() - INTERVAL '7 days'
              AND l.sent_kgs > 0
              AND ((l.sent_kgs - COALESCE(l.finished_goods_kgs, 0) - COALESCE(l.waste_kgs, 0) - COALESCE(l.rejection_kgs, 0)) / l.sent_kgs * 100) > 10
            ORDER BY r.created_at DESC LIMIT 20
        """)).fetchall()

        week_summary = db.execute(sa_text("""
            SELECT COALESCE(SUM(l.quantity_kgs), 0) as total_dispatched
            FROM jb_materialout_header h
            JOIN jb_materialout_lines l ON l.header_id = h.id
            WHERE h.created_at >= NOW() - INTERVAL '7 days'
        """)).fetchone()

        week_received = db.execute(sa_text("""
            SELECT COALESCE(SUM(l.finished_goods_kgs), 0) as total_fg,
                   COALESCE(SUM(l.waste_kgs), 0) as total_waste,
                   COALESCE(SUM(l.rejection_kgs), 0) as total_rejection
            FROM jb_work_inward_receipt r
            JOIN jb_work_inward_lines l ON l.inward_receipt_id = r.id
            WHERE r.created_at >= NOW() - INTERVAL '7 days'
        """)).fetchone()

        total_dispatched = float(week_summary[0]) if week_summary else 0
        total_fg = float(week_received[0]) if week_received else 0
        total_waste = float(week_received[1]) if week_received else 0
        total_rejection = float(week_received[2]) if week_received else 0
    except Exception as e:
        logger.error(f"Weekly digest data fetch failed: {e}")
        return
    finally:
        db.close()

    # Build open JWOs table
    open_rows = ""
    overdue_count = 0
    for j in open_jwos:
        days = int(j[6] or 0)
        is_overdue = days > 30
        if is_overdue:
            overdue_count += 1
        color = "color:#c0392b;font-weight:bold;" if is_overdue else ""
        open_rows += f"""<tr>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{j[1] or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{j[2] or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{j[3] or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{str(j[4] or '-')}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{j[5] or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;{color}">{days} days{' (OVERDUE)' if is_overdue else ''}</td>
        </tr>"""

    status_rows = ""
    for s in status_counts:
        status_rows += f"""<tr>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;">{s[0] or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">{s[1]}</td>
        </tr>"""

    excess_rows = ""
    for e in excess_loss:
        sent = float(e[6] or 0)
        fg = float(e[7] or 0)
        waste = float(e[8] or 0)
        rej = float(e[9] or 0)
        loss_pct = ((sent - fg - waste - rej) / sent * 100) if sent > 0 else 0
        excess_rows += f"""<tr>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{e[0] or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{e[2] or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{e[3] or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;">{e[5] or '-'}</td>
            <td style="padding:6px 10px;border:1px solid #e0e0e0;color:#c0392b;font-weight:bold;">{loss_pct:.1f}%</td>
        </tr>"""

    today = now_ist().strftime("%d %b %Y")

    excess_section = ""
    if excess_rows:
        excess_section = f"""
      <h3 style="color:#c0392b;margin:16px 0 8px;">Excess Loss Flags (Last 7 Days)</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#c0392b;color:#fff;">
          <th style="padding:8px 10px;text-align:left;">IR</th>
          <th style="padding:8px 10px;text-align:left;">Challan</th>
          <th style="padding:8px 10px;text-align:left;">Vendor</th>
          <th style="padding:8px 10px;text-align:left;">Item</th>
          <th style="padding:8px 10px;text-align:left;">Loss %</th>
        </tr></thead>
        <tbody>{excess_rows}</tbody>
      </table>"""

    body = f"""
      <h3 style="color:#29417A;margin:0 0 12px;">Weekly Summary — {today}</h3>
      <table style="border-collapse:collapse;font-size:13px;margin-bottom:16px;">
        <tr><td style="padding:4px 12px;font-weight:bold;">Dispatched (last 7 days)</td><td style="padding:4px 12px;">{total_dispatched:,.0f} Kgs</td></tr>
        <tr><td style="padding:4px 12px;font-weight:bold;">FG Received (last 7 days)</td><td style="padding:4px 12px;">{total_fg:,.0f} Kgs</td></tr>
        <tr><td style="padding:4px 12px;font-weight:bold;">Waste (last 7 days)</td><td style="padding:4px 12px;">{total_waste:,.0f} Kgs</td></tr>
        <tr><td style="padding:4px 12px;font-weight:bold;">Rejection (last 7 days)</td><td style="padding:4px 12px;">{total_rejection:,.0f} Kgs</td></tr>
      </table>
      <h3 style="color:#29417A;margin:16px 0 8px;">Status Distribution</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">
          <th style="padding:8px 10px;text-align:left;">Status</th>
          <th style="padding:8px 10px;text-align:right;">Count</th>
        </tr></thead>
        <tbody>{status_rows}</tbody>
      </table>
      <h3 style="color:#29417A;margin:16px 0 8px;">Open JWOs ({len(open_jwos)} total, {overdue_count} overdue)</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">
          <th style="padding:8px 10px;text-align:left;">Challan</th>
          <th style="padding:8px 10px;text-align:left;">Vendor</th>
          <th style="padding:8px 10px;text-align:left;">Item/Process</th>
          <th style="padding:8px 10px;text-align:left;">Date</th>
          <th style="padding:8px 10px;text-align:left;">Status</th>
          <th style="padding:8px 10px;text-align:left;">Days Open</th>
        </tr></thead>
        <tbody>{open_rows if open_rows else '<tr><td colspan="6" style="text-align:center;padding:8px;">No open JWOs</td></tr>'}</tbody>
      </table>
      {excess_section}
    """

    html = _jw_wrap(title="Weekly Jobwork Digest", subtitle=today, body_html=body)
    plain = f"Weekly Jobwork Digest — {today}\nOpen JWOs: {len(open_jwos)}, Overdue: {overdue_count}\nDispatched (7d): {total_dispatched:,.0f} Kgs, FG Received: {total_fg:,.0f} Kgs"

    _send_email_background(
        subject=f"Weekly Jobwork Digest — {today}",
        html_body=html,
        plain_body=plain,
        to=WEEKLY_DIGEST_TO,
    )
    logger.info(f"Weekly digest sent to {WEEKLY_DIGEST_TO}")


# ════════════════════════════════════════════════════════════
#  Inward Delete Notification
# ════════════════════════════════════════════════════════════


def notify_inward_deleted(
    transaction_no: str,
    company: str,
    entry_date: str | None = None,
    vendor: str | None = None,
    warehouse: str | None = None,
    articles_count: int = 0,
    boxes_count: int = 0,
    source: str = "inward",
    deleted_by: str | None = None,
    created_by: str | None = None,
    items: list | None = None,
) -> None:
    """Send notification to b.hrithik when an inward transaction is deleted."""
    deleted_at = now_ist().strftime("%d %b %Y, %I:%M %p")
    rows = [
        ("Transaction No", transaction_no),
        ("Company", company),
        ("Entry Date", entry_date or "-"),
        ("Vendor / Supplier", vendor or "-"),
        ("Warehouse", warehouse or "-"),
        ("Articles", str(articles_count)),
        ("Boxes", str(boxes_count)),
        ("Source", source),
        ("Deleted At", deleted_at),
    ]
    header_rows = ""
    for label, value in rows:
        header_rows += (
            f'<tr>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;'
            f'background:#f8f9fa;width:180px;">{label}</td>'
            f'<td style="padding:6px 10px;border:1px solid #e0e0e0;">{value}</td>'
            f'</tr>'
        )
    # Deleted By / Created By rows
    header_rows += (
        f'<tr style="background:#f8fafc;">'
        f'<td style="padding:6px 12px;color:#6b7280;font-size:13px;font-weight:bold;border:1px solid #e0e0e0;width:180px;">Deleted By</td>'
        f'<td style="padding:6px 12px;font-size:13px;font-weight:500;border:1px solid #e0e0e0;">{deleted_by or "—"}</td>'
        f'</tr>'
        f'<tr>'
        f'<td style="padding:6px 12px;color:#6b7280;font-size:13px;font-weight:bold;border:1px solid #e0e0e0;width:180px;">Created By</td>'
        f'<td style="padding:6px 12px;font-size:13px;font-weight:500;border:1px solid #e0e0e0;">{created_by or "—"}</td>'
        f'</tr>'
    )

    # Item details section
    items_rows = "".join([
        f"""<tr style="border-bottom:1px solid #f1f5f9;">
          <td style="padding:5px 12px;font-size:12px;">{i + 1}</td>
          <td style="padding:5px 12px;font-size:12px;">{it.get('item_description') or '—'}</td>
          <td style="padding:5px 12px;font-size:12px;">{it.get('lot_number') or it.get('lot_no') or '—'}</td>
          <td style="padding:5px 12px;font-size:12px;text-align:right;">{float(it.get('net_weight') or 0):.2f}</td>
          <td style="padding:5px 12px;font-size:12px;text-align:right;">{float(it.get('gross_weight') or 0):.2f}</td>
        </tr>"""
        for i, it in enumerate(items or [])
    ])
    items_section = f"""
    <h3 style="font-size:14px;font-weight:600;margin:20px 0 8px;">Item Details</h3>
    <table style="width:100%;border-collapse:collapse;">
      <thead><tr style="background:#f8fafc;">
        <th style="padding:6px 12px;text-align:left;font-size:12px;">#</th>
        <th style="padding:6px 12px;text-align:left;font-size:12px;">Item</th>
        <th style="padding:6px 12px;text-align:left;font-size:12px;">Lot</th>
        <th style="padding:6px 12px;text-align:right;font-size:12px;">Net Wt (Kg)</th>
        <th style="padding:6px 12px;text-align:right;font-size:12px;">Gross Wt (Kg)</th>
      </tr></thead>
      <tbody>{items_rows}</tbody>
    </table>""" if items else ""

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;margin:0;padding:0;background:#f4f4f4;">
  <table width="100%" cellpadding="0" cellspacing="0"
         style="max-width:640px;margin:20px auto;background:#fff;border-radius:8px;
                overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
    <tr><td style="background:#c0392b;color:#fff;padding:20px 24px;">
      <h2 style="margin:0;">Inward Deleted</h2>
      <p style="margin:4px 0 0;opacity:0.85;font-size:14px;">
        {transaction_no} &mdash; {company}
      </p>
    </td></tr>
    <tr><td style="padding:20px 24px;">
      <p style="color:#c0392b;font-weight:bold;margin:0 0 16px;">
        The following inward transaction has been permanently deleted along with all its articles, boxes, and cold stock entries.
      </p>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>
      {items_section}
    </td></tr>
    <tr><td style="background:#f8f9fa;padding:12px 24px;text-align:center;font-size:12px;color:#888;">
      Candor Foods &mdash; IMS Inward Notification
    </td></tr>
  </table>
</body></html>"""

    plain = (
        f"INWARD DELETED: {transaction_no} ({company})\n"
        f"Entry Date: {entry_date or '-'}\n"
        f"Vendor: {vendor or '-'}\n"
        f"Warehouse: {warehouse or '-'}\n"
        f"Articles: {articles_count} | Boxes: {boxes_count}\n"
        f"Source: {source}\n"
        f"Deleted At: {deleted_at}\n"
        f"Deleted By: {deleted_by or '-'}\n"
        f"Created By: {created_by or '-'}"
    )

    _send_email_background(
        subject=f"Inward Deleted: {transaction_no} [{company}]",
        html_body=html,
        plain_body=plain,
        to=INWARD_DELETE_TO,
    )
