"""Demo: sends 3 threaded JWO emails simulating the full new email design.
Run from backend/: python scripts/demo_email_thread.py
"""
import os
import smtplib
import time
from email.message import EmailMessage
from datetime import datetime

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "erp@candorfoods.in")
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]  # from .env — never hardcode credentials

DEMO_TO = "b.hrithik@candorfoods.in"
DEMO_CC = ["ai1@candorfoods.in", "ai2@candorfoods.in"]
CHALLAN = "DEMO-FINAL-001"
ANCHOR_ID = f"JWO-{CHALLAN}@candorfoods.in"
NOW = datetime.now().strftime("%d %b %Y, %I:%M %p")

WRAP = """<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;margin:0;padding:0;background:#f4f4f4;">
  <table width="100%" cellpadding="0" cellspacing="0"
         style="max-width:860px;margin:20px auto;background:#fff;border-radius:8px;
                overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
    <tr><td style="background:#29417A;color:#fff;padding:20px 24px;">
      <h2 style="margin:0;">{title}</h2>
      <p style="margin:4px 0 0;opacity:0.85;font-size:14px;">{subtitle} &mdash; {ts}</p>
    </td></tr>
    <tr><td style="padding:20px 24px;">{body}</td></tr>
    <tr><td style="background:#f8f9fa;padding:12px 24px;text-align:center;
                   font-size:12px;color:#888;">
      Candor Foods &mdash; IMS Job Work Notification
    </td></tr>
  </table>
</body></html>"""


def send(subject, html, plain, message_id=None, in_reply_to=None):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_EMAIL
    msg["To"] = DEMO_TO
    msg["Cc"] = ", ".join(DEMO_CC)
    msg.set_content(plain)
    msg.add_alternative(html, subtype="html")
    if message_id:
        msg["Message-ID"] = f"<{message_id}>"
    if in_reply_to:
        msg["In-Reply-To"] = f"<{in_reply_to}>"
        msg["References"] = f"<{in_reply_to}>"
    recipients = [DEMO_TO] + DEMO_CC
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_EMAIL, SMTP_PASSWORD)
        server.send_message(msg, to_addrs=recipients)
    print(f"  Sent: {subject}")


# ── Email 1: Material Out Created ─────────────────────────────────────────
print("\n[1/3] Material Out Created (anchor email)...")
body1 = """
<div style="margin:0 0 14px;padding:10px 14px;border-left:4px solid #29417A;background:#f0f4fa;font-size:14px;">
  <strong style="color:#29417A;">Source &rarr; Destination:</strong>
  <span style="font-weight:600;">Cold Storage (D-39)</span>
  <span style="color:#29417A;font-weight:bold;"> &rarr; </span>
  <span style="font-weight:600;">Demo Processor Pvt Ltd</span>
</div>
<h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
<table style="border-collapse:collapse;width:100%;font-size:13px;">
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;width:180px;">Challan No</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">DEMO-FINAL-001</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Job Work Date</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">2026-05-20</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">To Party</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Demo Processor Pvt Ltd</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Purpose</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cracking</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Created By</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">b.hrithik@candorfoods.in</td></tr>
</table>
<h3 style="color:#29417A;margin:24px 0 8px;">Material Summary</h3>
<table style="border-collapse:collapse;width:100%;font-size:13px;">
  <thead><tr style="background:#29417A;color:#fff;">
    <th style="padding:8px 10px;">Sl</th><th style="padding:8px 10px;">Item Description</th>
    <th style="padding:8px 10px;">Category</th><th style="padding:8px 10px;text-align:right;">Net Wt (Kgs)</th>
    <th style="padding:8px 10px;text-align:right;">Qty (Boxes)</th><th style="padding:8px 10px;">Lot No</th>
  </tr></thead>
  <tbody>
    <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;">1</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cashew W320</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">Raw</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">300</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">6</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">LOT-24-A</td></tr>
    <tr style="background:#f8f9fa;"><td style="padding:6px 10px;border:1px solid #e0e0e0;">2</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cashew W240</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">Raw</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">200</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">4</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">LOT-24-B</td></tr>
    <tr style="background:#e8edf5;font-weight:bold;">
        <td colspan="3" style="padding:6px 10px;border:1px solid #e0e0e0;">Total</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">500 Kgs</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">10 Boxes</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;"></td></tr>
  </tbody>
</table>"""

send(
    subject=f"Job Work Challan: {CHALLAN}",
    html=WRAP.format(title="Job Work — Material Out Created", subtitle=f"Challan {CHALLAN}", ts=NOW, body=body1),
    plain=f"Job Work Material Out Created — Challan: {CHALLAN}\nFrom: Cold Storage (D-39) → Demo Processor Pvt Ltd\nTotal: 500 Kgs / 10 Boxes",
    message_id=ANCHOR_ID,
)

time.sleep(3)

# ── Email 2: Material In (Partial) ────────────────────────────────────────
print("[2/3] Material In Partial (reply with history + pendency)...")
body2 = """
<h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
<table style="border-collapse:collapse;width:100%;font-size:13px;">
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;width:180px;">IR Number</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">IR-DEMO-FINAL-001-01</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Against Challan</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">DEMO-FINAL-001</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Receipt Date</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">2026-05-22</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Receipt Type</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Partial</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Inward Warehouse</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cold Storage (D-39)</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Created By</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">b.hrithik@candorfoods.in</td></tr>
</table>

<h3 style="color:#29417A;margin:24px 0 8px;">Current Receipt Lines</h3>
<table style="border-collapse:collapse;width:100%;font-size:13px;">
  <thead><tr style="background:#29417A;color:#fff;">
    <th style="padding:8px 10px;">Sl</th><th style="padding:8px 10px;">Description</th>
    <th style="padding:8px 10px;text-align:right;">Sent Kgs</th><th style="padding:8px 10px;text-align:right;">FG Kgs</th>
    <th style="padding:8px 10px;text-align:right;">Waste Kgs</th><th style="padding:8px 10px;text-align:right;">Rejection Kgs</th>
    <th style="padding:8px 10px;">Loss Status</th>
  </tr></thead>
  <tbody>
    <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;">1</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cashew W320</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">300</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">256</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">18</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">0</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">
          <span style="color:#e67e22;font-weight:bold;">8.7% &mdash; Partial, more expected</span></td></tr>
  </tbody>
</table>

<h3 style="color:#29417A;margin:24px 0 8px;">Challan IR History</h3>
<table style="border-collapse:collapse;width:100%;font-size:13px;">
  <thead><tr style="background:#29417A;color:#fff;">
    <th style="padding:8px 10px;">IR Number</th><th style="padding:8px 10px;">Date</th>
    <th style="padding:8px 10px;">Type</th><th style="padding:8px 10px;text-align:right;">FG Kgs</th>
    <th style="padding:8px 10px;text-align:right;">Waste Kgs</th><th style="padding:8px 10px;text-align:right;">Rejection Kgs</th>
    <th style="padding:8px 10px;text-align:right;">Accounted Kgs</th>
  </tr></thead>
  <tbody>
    <tr style="background:#dbeafe;font-weight:bold;">
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">IR-DEMO-FINAL-001-01&nbsp;&#9664; current</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">2026-05-22</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Partial</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">256</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">18</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">0</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;font-weight:bold;">274</td>
    </tr>
    <tr style="background:#e8edf5;font-weight:bold;">
      <td colspan="3" style="padding:6px 10px;border:1px solid #e0e0e0;">Total</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">256</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">18</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">0</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">274</td>
    </tr>
  </tbody>
</table>

<h3 style="color:#29417A;margin:24px 0 8px;">Pendency</h3>
<table style="border-collapse:collapse;width:100%;font-size:13px;">
  <thead><tr style="background:#29417A;color:#fff;">
    <th style="padding:8px 10px;">Item Description</th><th style="padding:8px 10px;text-align:right;">Sent Kgs</th>
    <th style="padding:8px 10px;text-align:right;">Total Accounted</th><th style="padding:8px 10px;">Pending</th>
  </tr></thead>
  <tbody>
    <tr style="font-weight:bold;background:#fff9e6;">
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cashew W320</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">300</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">274</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;"><span style="color:#c0392b;">26 Kgs</span></td>
    </tr>
    <tr style="color:#888;background:#f8f9fa;">
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cashew W240</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">200</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">0</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;"><span style="color:#c0392b;">200 Kgs</span></td>
    </tr>
    <tr style="background:#e8edf5;font-weight:bold;">
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Total</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">500</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">274</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;"><span style="color:#c0392b;">226 Kgs pending</span></td>
    </tr>
  </tbody>
</table>
<p style="margin:8px 0 0;color:#e67e22;font-size:13px;">
  &#9432; Further Material In receipt(s) expected &mdash; 226 Kgs still pending.
</p>"""

send(
    subject=f"Job Work Challan: {CHALLAN}",
    html=WRAP.format(title="Job Work — Material In (Partial)", subtitle=f"IR IR-DEMO-FINAL-001-01 | Challan {CHALLAN}", ts=NOW, body=body2),
    plain=f"Job Work Material In Partial — IR: IR-DEMO-FINAL-001-01\nAgainst: {CHALLAN}\nW320: FG 256kg, Waste 18kg | Loss 8.7% [PARTIAL]\nPending: 226 Kgs still outstanding.",
    in_reply_to=ANCHOR_ID,
)

time.sleep(3)

# ── Email 3: Material In (Final) ──────────────────────────────────────────
print("[3/3] Material In Final (reply with full closure + excess loss alert)...")
body3 = """
<h3 style="color:#29417A;margin:0 0 8px;">Header Details</h3>
<table style="border-collapse:collapse;width:100%;font-size:13px;">
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;width:180px;">IR Number</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">IR-DEMO-FINAL-001-02</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Against Challan</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">DEMO-FINAL-001</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Receipt Type</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Final</td></tr>
  <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;font-weight:bold;background:#f8f9fa;">Created By</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">b.hrithik@candorfoods.in</td></tr>
</table>

<h3 style="color:#29417A;margin:24px 0 8px;">Current Receipt Lines</h3>
<table style="border-collapse:collapse;width:100%;font-size:13px;">
  <thead><tr style="background:#29417A;color:#fff;">
    <th style="padding:8px 10px;">Sl</th><th style="padding:8px 10px;">Description</th>
    <th style="padding:8px 10px;text-align:right;">Sent Kgs</th><th style="padding:8px 10px;text-align:right;">FG Kgs</th>
    <th style="padding:8px 10px;text-align:right;">Waste Kgs</th><th style="padding:8px 10px;text-align:right;">Rejection Kgs</th>
    <th style="padding:8px 10px;">Loss Status</th>
  </tr></thead>
  <tbody>
    <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;">1</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cashew W320 (remaining)</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">26</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">22</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">2</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">0</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;"><span style="color:#27ae60;">7.7% OK</span></td></tr>
    <tr style="background:#f8f9fa;"><td style="padding:6px 10px;border:1px solid #e0e0e0;">2</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cashew W240</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">200</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">162</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">12</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">0</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;"><span style="color:#27ae60;">13% OK</span></td></tr>
  </tbody>
</table>

<h3 style="color:#29417A;margin:24px 0 8px;">Challan IR History</h3>
<table style="border-collapse:collapse;width:100%;font-size:13px;">
  <thead><tr style="background:#29417A;color:#fff;">
    <th style="padding:8px 10px;">IR Number</th><th style="padding:8px 10px;">Date</th>
    <th style="padding:8px 10px;">Type</th><th style="padding:8px 10px;text-align:right;">FG Kgs</th>
    <th style="padding:8px 10px;text-align:right;">Waste Kgs</th><th style="padding:8px 10px;text-align:right;">Rejection Kgs</th>
    <th style="padding:8px 10px;text-align:right;">Accounted Kgs</th>
  </tr></thead>
  <tbody>
    <tr><td style="padding:6px 10px;border:1px solid #e0e0e0;">IR-DEMO-FINAL-001-01</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">2026-05-22</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;">Partial</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">256</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">18</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">0</td>
        <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;font-weight:bold;">274</td></tr>
    <tr style="background:#dbeafe;font-weight:bold;">
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">IR-DEMO-FINAL-001-02&nbsp;&#9664; current</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">2026-05-24</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Final</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">184</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">14</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">0</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;font-weight:bold;">198</td>
    </tr>
    <tr style="background:#e8edf5;font-weight:bold;">
      <td colspan="3" style="padding:6px 10px;border:1px solid #e0e0e0;">Total</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">440</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">32</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">0</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">472</td>
    </tr>
  </tbody>
</table>

<h3 style="color:#29417A;margin:24px 0 8px;">Pendency</h3>
<table style="border-collapse:collapse;width:100%;font-size:13px;">
  <thead><tr style="background:#29417A;color:#fff;">
    <th style="padding:8px 10px;">Item Description</th><th style="padding:8px 10px;text-align:right;">Sent Kgs</th>
    <th style="padding:8px 10px;text-align:right;">Total Accounted</th><th style="padding:8px 10px;">Pending</th>
  </tr></thead>
  <tbody>
    <tr style="color:#888;background:#f8f9fa;">
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cashew W320</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">300</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">298</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;"><span style="color:#27ae60;">Fully accounted</span></td>
    </tr>
    <tr style="color:#888;background:#f8f9fa;">
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Cashew W240</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">200</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">174</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;"><span style="color:#c0392b;">26 Kgs</span></td>
    </tr>
    <tr style="background:#e8edf5;font-weight:bold;">
      <td style="padding:6px 10px;border:1px solid #e0e0e0;">Total</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">500</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;text-align:right;">472</td>
      <td style="padding:6px 10px;border:1px solid #e0e0e0;"><span style="color:#c0392b;">28 Kgs pending</span></td>
    </tr>
  </tbody>
</table>

<div style="margin:24px 0 0;padding:14px;background:#fff3cd;border-left:4px solid #c0392b;border-radius:4px;">
  <p style="margin:0;color:#c0392b;font-weight:bold;font-size:15px;">
    &#9888; ALERT: Excess Loss on Final Closure
  </p>
  <p style="margin:8px 0 0;font-size:13px;color:#555;">
    Total sent: <strong>500 Kgs</strong> &mdash;
    Total accounted (FG + Waste + Rejection): <strong>472 Kgs</strong> &mdash;
    Unaccounted: <strong>28 Kgs</strong> &mdash;
    Loss: <strong style="color:#c0392b;">5.6%</strong> (max allowed: 3%)
  </p>
</div>"""

send(
    subject=f"Job Work Challan: {CHALLAN}",
    html=WRAP.format(title="Job Work — Material In (Final)", subtitle=f"IR IR-DEMO-FINAL-001-02 | Challan {CHALLAN}", ts=NOW, body=body3),
    plain=f"Job Work Material In Final — IR: IR-DEMO-FINAL-001-02\nAgainst: {CHALLAN}\nTotal accounted: 472/500 Kgs | Loss: 5.6% (EXCESS on final closure)",
    in_reply_to=ANCHOR_ID,
)

print(f"\nDone. Check b.hrithik@candorfoods.in — all 3 emails in one thread.")
print(f"CC: {', '.join(DEMO_CC)}")
