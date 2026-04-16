import smtplib
import threading
from datetime import datetime
from email.message import EmailMessage

from shared.config_loader import settings
from shared.logger import get_logger

logger = get_logger("email.notifier")

RTV_NOTIFY_TO = "pooja.parkar@candorfoods.in"
JOB_WORK_TO = "billing@candorfoods.in"
JOB_WORK_CC = ["b.hrithik@candorfoods.in", "vaibhav.kumkar@candorfoods.in"]
WEEKLY_DIGEST_TO = ["b.hrithik@candorfoods.in", "vaibhav.kumkar@candorfoods.in"]


def _send_email_background(
    subject: str,
    html_body: str,
    plain_body: str,
    to: str | list[str] = RTV_NOTIFY_TO,
    cc: list[str] | None = None,
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
      <p style="margin:4px 0 0;opacity:0.85;font-size:14px;">{subtitle} &mdash; {datetime.now().strftime('%d %b %Y, %I:%M %p')}</p>
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

    header_rows = _jw_table([
        ("Record ID", str(header_id)),
        ("Challan No", challan_no),
        ("Job Work Date", str(job_work_date)),
        ("From Warehouse", header.get("from_warehouse", "")),
        ("To Party", header.get("to_party") or dispatch_to.get("name", "")),
        ("Party Address", header.get("party_address") or dispatch_to.get("address", "")),
        ("Party City / State", f"{dispatch_to.get('city', '')}, {dispatch_to.get('state', '')}"),
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
    ])

    columns = [
        ("Sl", "sl_no"),
        ("Item Description", "item_description"),
        ("Material", "material_type"),
        ("Category", "item_category"),
        ("Sub Cat", "sub_category"),
        ("Qty (Kgs)", "_qty_kgs"),
        ("Qty (Boxes)", "_qty_boxes"),
        ("Rate", "rate_per_kg"),
        ("Amount", "amount"),
        ("Lot No", "lot_number"),
    ]
    normalized = []
    for it in line_items:
        qty = it.get("quantity", {}) or {}
        normalized.append({
            **it,
            "item_description": it.get("item_description") or it.get("description", ""),
            "_qty_kgs": qty.get("kgs", 0) if isinstance(qty, dict) else 0,
            "_qty_boxes": qty.get("boxes", 0) if isinstance(qty, dict) else 0,
        })

    line_rows = _jw_lines_html(normalized, columns)
    header_cells = "".join(
        f'<th style="padding:8px 10px;text-align:left;">{label}</th>' for label, _ in columns
    )

    body = f"""
      <h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>

      <h3 style="color:#29417A;margin:24px 0 8px;">Line Items</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">{header_cells}</tr></thead>
        <tbody>{line_rows}</tbody>
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
        f"From: {header.get('from_warehouse', '')}",
        f"To: {header.get('to_party') or dispatch_to.get('name', '')}",
        f"Purpose: {header.get('purpose_of_work') or dispatch_to.get('sub_category', '')}",
        f"Vehicle: {header.get('vehicle_no') or payload.get('motor_vehicle_no', '')}",
        f"Created By: {created_by or '-'}",
        "",
        "Line Items:",
    ]
    for it in normalized:
        plain_lines.append(
            f"  {it.get('item_description', '')} | "
            f"{it.get('_qty_kgs', 0)} kg / {it.get('_qty_boxes', 0)} box | "
            f"Rate: {it.get('rate_per_kg', 0)} | Amount: {it.get('amount', 0)}"
        )

    cc = list(JOB_WORK_CC)
    if created_by and created_by not in cc and created_by != JOB_WORK_TO:
        cc.append(created_by)
    _send_email_background(
        subject=f"Job Work Material Out Created: {challan_no or header_id}",
        html_body=html,
        plain_body="\n".join(plain_lines),
        to=JOB_WORK_TO,
        cc=cc,
    )


def notify_job_work_material_in_created(payload: dict, ir_number: str, inward_receipt_id: int, created_by: str) -> None:
    """Send notification email when a Job Work Material In (Inward Receipt) is created."""
    items = payload.get("items", []) or []
    boxes = payload.get("boxes", []) or []

    header_rows = _jw_table([
        ("IR Number", ir_number),
        ("Inward Receipt ID", str(inward_receipt_id)),
        ("Against Challan", payload.get("original_challan_no", "")),
        ("Receipt Date", str(payload.get("received_date", ""))),
        ("Receipt Type", payload.get("receipt_type", "")),
        ("Inward Warehouse", payload.get("inward_warehouse", "")),
        ("Vehicle No", payload.get("vehicle_no", "")),
        ("Driver Name", payload.get("driver_name", "")),
        ("Remarks", payload.get("remarks", "")),
        ("Created By", created_by or "-"),
    ])

    line_columns = [
        ("Sl", "sl_no"),
        ("Description", "description"),
        ("Sent Kgs", "sent_kgs"),
        ("Sent Boxes", "sent_boxes"),
        ("FG Kgs", "finished_goods_kgs"),
        ("FG Boxes", "finished_goods_boxes"),
        ("Waste Kgs", "waste_kgs"),
        ("Rejection Kgs", "rejection_kgs"),
        ("Process", "process_type"),
    ]
    line_rows = _jw_lines_html(items, line_columns)
    line_headers = "".join(
        f'<th style="padding:8px 10px;text-align:left;">{label}</th>' for label, _ in line_columns
    )

    boxes_section = ""
    if boxes:
        box_columns = [
            ("Box ID", "box_id"),
            ("Box #", "box_number"),
            ("Item", "item_description"),
            ("Net Wt", "net_weight"),
            ("Gross Wt", "gross_weight"),
            ("Lot No", "lot_no"),
            ("Location", "storage_location"),
        ]
        box_rows = _jw_lines_html(boxes, box_columns)
        box_headers = "".join(
            f'<th style="padding:8px 10px;text-align:left;">{label}</th>' for label, _ in box_columns
        )
        boxes_section = f"""
      <h3 style="color:#29417A;margin:24px 0 8px;">Boxes</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">{box_headers}</tr></thead>
        <tbody>{box_rows}</tbody>
      </table>"""

    body = f"""
      <h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>

      <h3 style="color:#29417A;margin:24px 0 8px;">Line Items</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">{line_headers}</tr></thead>
        <tbody>{line_rows}</tbody>
      </table>

      {boxes_section}
    """

    html = _jw_wrap(
        title="Job Work — Material In Created",
        subtitle=f"IR {ir_number}",
        body_html=body,
    )

    plain_lines = [
        f"Job Work Material In Created — IR: {ir_number} (ID {inward_receipt_id})",
        f"Against Challan: {payload.get('original_challan_no', '')}",
        f"Receipt Type: {payload.get('receipt_type', '')}",
        f"Receipt Date: {payload.get('received_date', '')}",
        f"Inward Warehouse: {payload.get('inward_warehouse', '')}",
        f"Created By: {created_by or '-'}",
        "",
        "Line Items:",
    ]
    for it in items:
        plain_lines.append(
            f"  {it.get('description', '')} | "
            f"Sent: {it.get('sent_kgs', 0)}kg/{it.get('sent_boxes', 0)}box | "
            f"FG: {it.get('finished_goods_kgs', 0)}kg | "
            f"Waste: {it.get('waste_kgs', 0)}kg | "
            f"Rejection: {it.get('rejection_kgs', 0)}kg"
        )
    if boxes:
        plain_lines.append("")
        plain_lines.append("Boxes:")
        for b in boxes:
            plain_lines.append(
                f"  {b.get('item_description', '')} Box#{b.get('box_number', '')} | "
                f"Net: {b.get('net_weight', 0)} | Gross: {b.get('gross_weight', 0)} | "
                f"Lot: {b.get('lot_no', '-')}"
            )

    cc = list(JOB_WORK_CC)
    if created_by and created_by not in cc and created_by != JOB_WORK_TO:
        cc.append(created_by)
    _send_email_background(
        subject=f"Job Work Material In Created: {ir_number}",
        html_body=html,
        plain_body="\n".join(plain_lines),
        to=JOB_WORK_TO,
        cc=cc,
    )


def notify_job_work_material_out_updated(payload: dict, record_id: int, updated_by: str) -> None:
    """Send notification email when a Job Work Material Out is updated."""
    header = payload.get("header", {}) or {}
    line_items = payload.get("line_items", []) or []

    challan_no = header.get("challan_no") or payload.get("challan_no", "")
    job_work_date = header.get("job_work_date") or payload.get("dated", "")

    header_rows = _jw_table([
        ("Record ID", str(record_id)),
        ("Challan No", challan_no),
        ("Date", str(job_work_date)),
        ("Warehouse", header.get("from_warehouse", "")),
        ("Party", header.get("to_party", "")),
        ("Purpose", header.get("purpose_of_work", "")),
        ("Vehicle No", header.get("vehicle_no") or payload.get("motor_vehicle_no", "")),
        ("Updated By", updated_by or "-"),
    ])

    columns = [
        ("Sl", "sl_no"),
        ("Item Description", "item_description"),
        ("Material", "material_type"),
        ("Qty (Kgs)", "_qty_kgs"),
        ("Qty (Boxes)", "_qty_boxes"),
        ("Rate", "rate_per_kg"),
        ("Amount", "amount"),
    ]
    normalized = []
    for it in line_items:
        qty = it.get("quantity", {}) or {}
        normalized.append({
            **it,
            "item_description": it.get("item_description") or it.get("description", ""),
            "_qty_kgs": qty.get("kgs", 0) if isinstance(qty, dict) else 0,
            "_qty_boxes": qty.get("boxes", 0) if isinstance(qty, dict) else 0,
        })

    line_rows = _jw_lines_html(normalized, columns)
    header_cells = "".join(
        f'<th style="padding:8px 10px;text-align:left;">{label}</th>' for label, _ in columns
    )

    body = f"""
      <h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <tbody>{header_rows}</tbody>
      </table>

      <h3 style="color:#29417A;margin:24px 0 8px;">Line Items</h3>
      <table style="border-collapse:collapse;width:100%;font-size:13px;">
        <thead><tr style="background:#29417A;color:#fff;">{header_cells}</tr></thead>
        <tbody>{line_rows}</tbody>
      </table>
    """

    html = _jw_wrap(
        title="Job Work — Material Out Updated",
        subtitle=f"Challan {challan_no or record_id}",
        body_html=body,
    )

    plain = (
        f"Job Work Material Out Updated — Challan: {challan_no} (ID {record_id})\n"
        f"Updated By: {updated_by or '-'}"
    )

    cc = list(JOB_WORK_CC)
    if updated_by and updated_by not in cc and updated_by != JOB_WORK_TO:
        cc.append(updated_by)
    _send_email_background(
        subject=f"Job Work Material Out Updated: {challan_no or record_id}",
        html_body=html,
        plain_body=plain,
        to=JOB_WORK_TO,
        cc=cc,
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
        subject=f"Job Work Material Out Deleted: {challan_no or record_id}",
        html_body=html,
        plain_body=plain,
        to=JOB_WORK_TO,
        cc=cc,
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
        subject=f"Job Work Material In Deleted: {ir_number or ir_id}",
        html_body=html,
        plain_body=plain,
        to=JOB_WORK_TO,
        cc=cc,
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
        subject=f"Job Work Status Changed: {challan_no or header_id} [{old_status} -> {new_status}]",
        html_body=html,
        plain_body=plain,
        to=JOB_WORK_TO,
        cc=cc,
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
        subject=f"ALERT: Excess Loss on Challan {challan_no or header_id} ({loss_pct:.2f}%)",
        html_body=html,
        plain_body=plain,
        to=JOB_WORK_TO,
        cc=cc,
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

    today = datetime.now().strftime("%d %b %Y")

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
