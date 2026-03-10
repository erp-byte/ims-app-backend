import smtplib
import threading
from datetime import datetime
from email.message import EmailMessage

from shared.config_loader import settings
from shared.logger import get_logger

logger = get_logger("email.notifier")

RTV_NOTIFY_TO = "pooja.parkar@candorfoods.in"


def _send_email_background(subject: str, html_body: str, plain_body: str) -> None:
    """Send email in a background thread so API response is not delayed."""
    def _send():
        try:
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = settings.SMTP_EMAIL
            msg["To"] = RTV_NOTIFY_TO
            msg.set_content(plain_body)
            msg.add_alternative(html_body, subtype="html")

            with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
                server.starttls()
                server.login(settings.SMTP_EMAIL, settings.SMTP_APP_PASSWORD)
                server.send_message(msg)

            logger.info(f"RTV notification sent to {RTV_NOTIFY_TO}: {subject}")
        except Exception as e:
            logger.error(f"Failed to send RTV notification: {e}")

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


def _rtv_email_html(action: str, header: dict, lines: list[dict], boxes: list[dict], extra_info: str = "") -> tuple[str, str]:
    """Return (html_body, plain_body) for an RTV notification email."""

    header_fields = [
        ("RTV ID", header.get("rtv_id", "")),
        ("RTV Date", str(header.get("rtv_date", ""))),
        ("Factory Unit", header.get("factory_unit", "")),
        ("Customer", header.get("customer", "")),
        ("Invoice Number", header.get("invoice_number", "") or "-"),
        ("Challan No", header.get("challan_no", "") or "-"),
        ("DN No", header.get("dn_no", "") or "-"),
        ("Conversion", header.get("conversion", "0")),
        ("Sales POC", header.get("sales_poc", "") or "-"),
        ("Remark", header.get("remark", "") or "-"),
        ("Status", header.get("status", "")),
        ("Created By", header.get("created_by", "") or "-"),
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
        extra_section = f'<p style="color:#555;margin:16px 0;">{extra_info}</p>'

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;margin:0;padding:0;background:#f4f4f4;">
  <table width="100%" cellpadding="0" cellspacing="0" style="max-width:800px;margin:20px auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
    <tr><td style="background:#29417A;color:#fff;padding:20px 24px;">
      <h2 style="margin:0;">RTV {action}</h2>
      <p style="margin:4px 0 0;opacity:0.85;font-size:14px;">{header.get('rtv_id', '')} &mdash; {datetime.now().strftime('%d %b %Y, %I:%M %p')}</p>
    </td></tr>
    <tr><td style="padding:20px 24px;">

      {extra_section}

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
      Candor Foods &mdash; IMS RTV Notification
    </td></tr>
  </table>
</body></html>"""

    # Plain text fallback
    plain_lines = [f"RTV {action}: {header.get('rtv_id', '')}"]
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

    return html, "\n".join(plain_lines)


def notify_rtv_created(rtv_detail: dict) -> None:
    """Send notification email when an RTV is created."""
    html, plain = _rtv_email_html(
        action="Created",
        header=rtv_detail,
        lines=rtv_detail.get("lines", []),
        boxes=rtv_detail.get("boxes", []),
    )
    _send_email_background(
        subject=f"RTV Created: {rtv_detail.get('rtv_id', '')}",
        html_body=html,
        plain_body=plain,
    )


def notify_rtv_approved(rtv_detail: dict, approved_by: str) -> None:
    """Send notification email when an RTV is approved."""
    html, plain = _rtv_email_html(
        action="Approved",
        header=rtv_detail,
        lines=rtv_detail.get("lines", []),
        boxes=rtv_detail.get("boxes", []),
        extra_info=f"Approved by: {approved_by}",
    )
    _send_email_background(
        subject=f"RTV Approved: {rtv_detail.get('rtv_id', '')}",
        html_body=html,
        plain_body=plain,
    )


def notify_rtv_deleted(rtv_id: str, company: str) -> None:
    """Send notification email when an RTV is deleted."""
    header = {"rtv_id": rtv_id, "status": "Deleted"}
    html, plain = _rtv_email_html(
        action="Deleted",
        header=header,
        lines=[],
        boxes=[],
        extra_info=f"RTV {rtv_id} in {company} has been permanently deleted along with all its lines and boxes.",
    )
    _send_email_background(
        subject=f"RTV Deleted: {rtv_id}",
        html_body=html,
        plain_body=plain,
    )


def notify_rtv_header_updated(rtv_detail: dict) -> None:
    """Send notification email when RTV header is updated."""
    html, plain = _rtv_email_html(
        action="Header Updated",
        header=rtv_detail,
        lines=[],
        boxes=[],
    )
    _send_email_background(
        subject=f"RTV Updated: {rtv_detail.get('rtv_id', '')}",
        html_body=html,
        plain_body=plain,
    )


def notify_rtv_lines_updated(rtv_detail: dict) -> None:
    """Send notification email when RTV lines are replaced."""
    html, plain = _rtv_email_html(
        action="Lines Updated",
        header=rtv_detail,
        lines=rtv_detail.get("lines", []),
        boxes=rtv_detail.get("boxes", []),
    )
    _send_email_background(
        subject=f"RTV Lines Updated: {rtv_detail.get('rtv_id', '')}",
        html_body=html,
        plain_body=plain,
    )
