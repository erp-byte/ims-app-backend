# -*- coding: utf-8 -*-
"""Generate a self-contained HTML report (root-cause diagram + schema/endpoint/frontend
reconciliation). Overflow-safe: fixed table layout, word-wrap, page-break-avoid.
Then convert to PDF with headless Chrome."""
import json, html

S = json.load(open("_schema_dump.json", encoding="utf-8"))
COLS, CONS, CNT = S["columns"], S["constraints"], S["counts"]

def esc(x): return html.escape(str(x if x is not None else ""))

# ----- table order + friendly roles -----
TABLE_ROLE = {
    "interunit_transfer_requests": "Transfer request (pre-dispatch ask)",
    "interunit_transfers_header":  "Transfer-OUT header (the dispatch / challan)",
    "interunit_transfers_lines":   "ORDER lines — authoritative qty per (lot, item)",
    "interunit_transfer_boxes":    "Box rows attached to the dispatch (frontend-supplied)",
    "pending_transfer_stock":      "IN-TRANSIT ledger — one row per parked physical box",
    "interunit_transfer_in_header":"GRN / Transfer-IN header (receipt)",
    "interunit_transfer_in_boxes": "Received box rows (per GRN)",
    "cfpl_cold_stocks":            "Cold storage master (cdpl_cold_stocks is identical)",
}
ORDER = ["interunit_transfer_requests","interunit_transfers_header","interunit_transfers_lines",
         "interunit_transfer_boxes","pending_transfer_stock","interunit_transfer_in_header",
         "interunit_transfer_in_boxes","cfpl_cold_stocks"]

def cons_summary(t):
    out = []
    for c in CONS.get(t, []):
        if c["type"] == "PRIMARY KEY": out.append(f"PK: {c['col']}")
        elif c["type"] == "UNIQUE": out.append(f"UNIQUE: {c['col']}")
        elif c["type"] == "FOREIGN KEY": out.append(f"FK: {c['col']} → {c['ref']}")
    return out or ["(no PK/FK/UNIQUE in information_schema)"]

def schema_block(t):
    cols = COLS.get(t, [])
    rows = "".join(
        f"<tr><td class='mono'>{esc(c['col'])}</td><td class='mono'>{esc(c['type'])}</td>"
        f"<td class='ctr'>{'YES' if c['null']=='YES' else '<b>NO</b>'}</td></tr>"
        for c in cols)
    cons = "".join(f"<li>{esc(s)}</li>" for s in cons_summary(t))
    return f"""
    <div class="tbl">
      <div class="tbl-h"><span class="tn">{esc(t)}</span>
        <span class="role">{esc(TABLE_ROLE.get(t,''))}</span>
        <span class="cnt">{esc(CNT.get(t))} rows &middot; {len(cols)} cols</span></div>
      <table class="grid"><colgroup><col style="width:42%"><col style="width:43%"><col style="width:15%"></colgroup>
        <thead><tr><th>Column</th><th>Type</th><th>Null</th></tr></thead><tbody>{rows}</tbody></table>
      <div class="cons"><b>Constraints:</b><ul>{cons}</ul></div>
    </div>"""

# ----- endpoint payloads (curated from interunit_models.py + router) -----
ENDPOINTS = [
 ("POST","/interunit/transfers","create_transfer",
  "TransferCreate { header: TransferHeaderCreate{stock_trf_date, from_warehouse, to_warehouse, vehicle_no, driver_name?, approved_by?, remark?, reason_code?}; lines: TransferLineCreate[]{material_type,item_category,sub_category,item_description,quantity,uom,pack_size,unit_pack_size?,net_weight?,total_weight?,batch_number?,lot_number?}; boxes?: BoxCreate[]{box_number,box_id?,article,lot_number?,transaction_no?,net_weight,gross_weight}; request_id? }",
  "TransferHeaderResponse", "interunitApiService.createTransfer / transferform / directtransferform"),
 ("GET","/interunit/transfers/{id}","get_transfer","path: id",
  "TransferWithLines { header..., lines: TransferLineResponse[], boxes: BoxResponse[], grn_records[], from_cold_unit }",
  "PendingTransfersModal hover (fetchLines), transfer/view, transfer/page"),
 ("PUT","/interunit/transfers/{id}","update_transfer","TransferCreate (same as create)",
  "TransferHeaderResponse", "interunitApiService.updateTransfer"),
 ("DELETE","/interunit/transfers/{id}","delete_transfer","query: user_email, user_role",
  "TransferDeleteResponse{success,message,transfer_id,challan_no}", "PendingTransfersModal cancel"),
 ("GET","/interunit/pending-stock","list_pending_transfers","query: from_site?,to_site?,company?,from_date?,to_date?,search?",
  "{ records: PendingTransferRecord[]{transfer_out_id,transfer_out_challan_no,dispatched_at,from_site,to_site,from_company,to_company,from_storage_type,to_storage_type,total_boxes,total_cartons,total_kg,dispatched_by,status,header_status,unallocated_boxes,updated_ts}; filter_options }",
  "PendingTransfersModal (table), transfer/page summary"),
 ("GET","/interunit/pending-stock/by-lot","pending_by_lot","query: lot_no?,item_description?,from_site?,from_company?",
  "{ pending_cartons, pending_kg, box_count, transfers: [{challan_no,cartons,weight_kg,box_count,updated_ts,...}] }",
  "directtransferform 'Pending Transfers' hover / '+N in transit'"),
 ("GET","/interunit/pending-stock/in-transit-by-lot","in_transit_by_lot","query: company?",
  "{ <lot_no>: {cartons, kg, box_count} }  (batched map)",
  "cold-storage/dashboard '+N in transit' overlay (NEW)"),
 ("POST","/interunit/pending-stock/backfill","backfill_pending_from_existing_transfers","query: user_email, user_role, dry_run",
  "{ transfers_scanned, transfers_with_existing_pending, boxes_topped_up_by_lot, boxes_unallocatable, reconciled[] }",
  "PendingTransfersModal 'Sync existing' (auto on open)"),
 ("POST","/interunit/transfer-in","create_transfer_in","TransferInCreate{transfer_out_id, boxes[], cold_storage_items?}",
  "TransferInDetail", "interunitApiService.createTransferIn"),
 ("POST","/interunit/transfer-in/pending","create_pending_transfer_in","PendingTransferInCreate{transfer_out_id,...}",
  "GRN header (status=Pending)", "interunitApiService.createPendingTransferIn"),
 ("POST","/interunit/transfer-in/{id}/acknowledge[-batch]","acknowledge_pending_box[es_batch]","PendingBoxAcknowledge{box_id,transaction_no,...}",
  "ack result (GRN stays Pending)", "transfer-in scan UI"),
 ("POST","/interunit/transfer-in/{id}/finalize","finalize_transfer_in","FinalizeTransferIn{box_condition,condition_remarks,cold_storage_items?}",
  "GRN → Received; pick_from_pending; transfer-OUT → Received", "transfer-in finalize button (⚠ often skipped)"),
]
def ep_rows():
    r = ""
    for m,p,fn,req,resp,fe in ENDPOINTS:
        r += (f"<tr><td class='mono'><b>{esc(m)}</b><br>{esc(p)}</td>"
              f"<td class='mono sm'>{esc(fn)}</td>"
              f"<td class='sm'>{esc(req)}</td><td class='sm'>{esc(resp)}</td>"
              f"<td class='sm'>{esc(fe)}</td></tr>")
    return r

# ----- frontend call map -----
FE = [
 ("PendingTransfersModal.tsx","GET /pending-stock, GET /transfers/{id}, POST /pending-stock/backfill, DELETE /transfers/{id}"),
 ("directtransferform/page.tsx","GET /pending-stock/by-lot, GET /categorial-search, GET /box-lookup*, POST /transfers"),
 ("cold-storage/dashboard/page.tsx","GET /pending-stock/in-transit-by-lot"),
 ("transfer/page.tsx","GET /pending-stock, GET /transfers/{id}, GET/DELETE /transfer-in/{id}"),
 ("transferform/page.tsx","GET /categorial-search, GET /box-lookup*, POST /transfers"),
 ("lib/interunitApiService.ts","CRUD /transfers, /requests, /transfer-in (+pending/acknowledge/finalize/reconciliation)"),
]
def fe_rows():
    return "".join(f"<tr><td class='mono sm'>{esc(f)}</td><td class='sm'>{esc(c)}</td></tr>" for f,c in FE)

# ----- SVG root-cause diagram (carefully spaced, no overlap) -----
def box(x,y,w,h,title,lines,cls="b"):
    t=f'<rect class="{cls}" x="{x}" y="{y}" width="{w}" height="{h}" rx="7"/>'
    t+=f'<text class="bt" x="{x+w/2}" y="{y+17}">{esc(title)}</text>'
    for i,ln in enumerate(lines):
        t+=f'<text class="bl" x="{x+w/2}" y="{y+33+i*13}">{esc(ln)}</text>'
    return t
def arrow(x1,y1,x2,y2):
    return f'<line class="ar" x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" marker-end="url(#a)"/>'

SVG = f"""<svg viewBox="0 0 960 700" xmlns="http://www.w3.org/2000/svg" class="diagram">
<defs><marker id="a" markerWidth="9" markerHeight="9" refX="7" refY="3" orient="auto">
<path d="M0,0 L7,3 L0,6 Z" fill="#444"/></marker></defs>
{box(330,16,300,60,"interunit_transfers_lines  (ORDER = truth)",["qty = cartons (cold) OR pack-count (wh PM)","cold->warehouse: ordered = shipped, no scan"],"ok")}
{arrow(480,76,480,112)}
{box(330,112,300,60,"dispatch -> park_in_pending()",["cold: frontend fabricates box_ids","matched to cold_stocks by box_id"],"warn")}
{arrow(480,172,480,208)}
{box(330,208,300,50,"pending_transfer_stock  (in-transit ledger)",["one row per parked physical box"],"b")}
{arrow(430,258,175,300)}{arrow(480,258,480,300)}{arrow(530,258,785,300)}
{box(20,300,300,108,"(1) WRONG-LOT parked",["box_id collides across lots:","ordered 124679  ->  parked 183033","[RESOLVED] corrected;","tid 468 applied to prod"],"ok")}
{box(330,300,300,108,"(2) FALSE shortage",["counted SUM(qty)=units, not boxes:","75 boxes vs qty-sum 1963","[RESOLVED] reconcile now uses","box-count; 426 / 713 cleared"],"ok")}
{box(645,300,300,108,"(3) REAL gap",["under-park; lot later consumed:","tid 403 = 1 of 50 parked, lot now 0","[FLAGGED] 49 short","(never fabricated)"],"bad")}
{box(20,438,300,50,"Hover item list",["GET /transfers/{{id}}  ->  reads ORDER"],"ok")}
{box(645,438,300,50,"Pending modal row",["GET /pending-stock  ->  reads PENDING"],"b")}
{arrow(170,488,420,530)}{arrow(795,488,540,530)}
{box(300,530,360,52,"Display MISMATCH (reported symptom)",["hover != modal; boxes/cartons/kg/lot differ"],"warn")}
{box(20,606,930,68,"SECONDARY (GRN) - STILL OPEN",["Transfer-IN acknowledged (status 'Pending') but finalize_transfer_in NOT called -> transfer-OUT","never set 'Received', pending rows never picked -> transfer stuck in modal as 'Partial (GRN raised)'."],"bad")}
</svg>"""

HTML = f"""<!doctype html><html><head><meta charset="utf-8"><style>
@page {{ size: A4; margin: 13mm 11mm; }}
* {{ box-sizing: border-box; }}
body {{ font-family: Arial, Helvetica, sans-serif; color:#1c2530; font-size:9.5pt; line-height:1.4; }}
h1 {{ font-size:18pt; margin:0 0 2px; color:#0f172a; }}
h2 {{ font-size:13pt; margin:18px 0 6px; color:#0f172a; border-bottom:2px solid #2563eb; padding-bottom:3px; page-break-after:avoid; }}
.sub {{ color:#64748b; font-size:8.5pt; margin-bottom:4px; }}
.mono {{ font-family:"Consolas","Courier New",monospace; }}
.sm {{ font-size:8pt; }}
.ctr {{ text-align:center; }}
section {{ page-break-inside:auto; }}
table.grid, table.ep {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
table.grid th, table.grid td, table.ep th, table.ep td {{
  border:1px solid #cbd5e1; padding:3px 5px; text-align:left;
  word-break:break-word; overflow-wrap:anywhere; vertical-align:top; }}
table.grid th, table.ep th {{ background:#eff6ff; font-size:8.5pt; }}
table.ep td {{ font-size:8pt; }}
table.ep col.m {{ width:16%; }} table.ep col.f {{ width:15%; }} table.ep col.r {{ width:24%; }} table.ep col.s {{ width:24%; }} table.ep col.c {{ width:21%; }}
tr {{ page-break-inside:avoid; }}
.tbl {{ border:1px solid #94a3b8; border-radius:6px; margin:8px 0; padding:6px 8px; page-break-inside:auto; }}
.tbl-h {{ display:flex; flex-wrap:wrap; gap:8px; align-items:baseline; margin-bottom:4px; page-break-after:avoid; }}
table.grid thead {{ display:table-header-group; }}
table.grid tr, table.ep tr {{ page-break-inside:avoid; }}
.tn {{ font-family:"Consolas",monospace; font-weight:bold; font-size:10pt; color:#1d4ed8; }}
.role {{ font-size:8.5pt; color:#475569; flex:1; }}
.cnt {{ font-size:8pt; color:#64748b; }}
.cons {{ font-size:8pt; color:#334155; margin-top:3px; }}
.cons ul {{ margin:2px 0 0 16px; padding:0; }}
.diagram {{ width:100%; height:auto; border:1px solid #cbd5e1; border-radius:6px; background:#fbfdff; }}
svg .b {{ fill:#eef2ff; stroke:#6366f1; stroke-width:1.2; }}
svg .ok {{ fill:#ecfdf5; stroke:#10b981; stroke-width:1.3; }}
svg .warn {{ fill:#fffbeb; stroke:#f59e0b; stroke-width:1.3; }}
svg .bad {{ fill:#fef2f2; stroke:#ef4444; stroke-width:1.4; }}
svg .bt {{ font:bold 10px Arial; text-anchor:middle; fill:#0f172a; }}
svg .bl {{ font:9px Arial; text-anchor:middle; fill:#334155; }}
svg .ar {{ stroke:#444; stroke-width:1.3; }}
.legend span {{ display:inline-block; font-size:8pt; margin-right:12px; }}
.dot {{ display:inline-block; width:9px; height:9px; border-radius:2px; margin-right:3px; vertical-align:middle; }}
.note {{ background:#f8fafc; border-left:3px solid #2563eb; padding:5px 9px; font-size:8.8pt; margin:6px 0; }}
.fix li {{ margin-bottom:2px; font-size:9pt; }}
</style></head><body>

<h1>Inter-Unit Transfer &mdash; Pending Stock: Root-Cause &amp; System Reconciliation</h1>
<div class="sub">IMS (ims-app-backend / frontend-) &middot; prod DB warehouse_db &middot; <b>v2 (post-fix), 2026-06-01</b> &middot; generated from live schema + source. Module prefix: <span class="mono">/interunit</span></div>

<section>
<h2>1 &middot; Root-Cause Visualization</h2>
{SVG}
<div class="legend" style="margin-top:5px">
<span><span class="dot" style="background:#10b981"></span>authoritative / correct</span>
<span><span class="dot" style="background:#f59e0b"></span>artifact / gap</span>
<span><span class="dot" style="background:#ef4444"></span>corruption / mismatch</span>
</div>
<div class="note"><b>In one line:</b> One order lives in three tables that were never reconciled, and the in-transit ledger is built from <b>fabricated frontend box_ids</b> (cold&rarr;warehouse has no real scanning). That produced three mismatch modes &mdash; <b>(1)</b> wrong-lot parking from box_id collisions, <b>(2)</b> false shortages from counting pack-<i>units</i> instead of boxes, and <b>(3)</b> genuine under-dispatch gaps &mdash; so the hover (reads the order) and the modal (reads pending) disagreed. Modes 1 &amp; 2 are now <b>resolved</b>; mode 3 is correctly <b>flagged</b>. A separate open item: GRNs acknowledged but not finalized keep transfers stuck as &ldquo;Partial.&rdquo;</div>
<table class="ep" style="margin-top:6px"><colgroup><col style="width:34%"><col style="width:18%"><col style="width:48%"></colgroup>
<thead><tr><th>Item</th><th>Status</th><th>Detail</th></tr></thead><tbody>
<tr><td>Duplicate rows in modal</td><td><b>FIXED (code)</b></td><td>group by dispatch only; show 'mixed' for cross-company</td></tr>
<tr><td>Reconcile keying (item-case)</td><td><b>FIXED (code)</b></td><td>match by lot_no, not item_description</td></tr>
<tr><td>(1) Wrong-lot parking</td><td><b>CLEARED (prod)</b></td><td>tid 468: 50&times; wrong lot 183033 &rarr; 50&times; ordered lot 124679 (applied 2026-06-01)</td></tr>
<tr><td>(2) False shortage</td><td><b>FIXED (code)</b></td><td>expected = COUNT(transfer_boxes), not SUM(qty); 426 (1888) &amp; 713 (14) cleared</td></tr>
<tr><td>(3) Real gap</td><td><b>FLAGGED</b></td><td>tid 403: 1 of 50 parked, lot 125883 since consumed (handled by tid 778). Stale dispatch to cancel</td></tr>
<tr><td>GRN acknowledged-not-finalized</td><td><b>OPEN</b></td><td>6 transfers stuck 'Partial'; needs auto-finalize on complete acknowledgement</td></tr>
</tbody></table>
</section>

<section>
<h2>2 &middot; Backend Tables &amp; Constraints (live)</h2>
{''.join(schema_block(t) for t in ORDER)}
</section>

<section>
<h2>3 &middot; Endpoint Payload Schemas &amp; Frontend Callers</h2>
<table class="ep"><colgroup><col class="m"><col class="f"><col class="r"><col class="s"><col class="c"></colgroup>
<thead><tr><th>Method / Path</th><th>Handler</th><th>Request</th><th>Response</th><th>Frontend caller</th></tr></thead>
<tbody>{ep_rows()}</tbody></table>
</section>

<section>
<h2>4 &middot; Frontend &rarr; Endpoint Map</h2>
<table class="ep"><colgroup><col style="width:32%"><col style="width:68%"></colgroup>
<thead><tr><th>Frontend file</th><th>Endpoints called</th></tr></thead><tbody>{fe_rows()}</tbody></table>
</section>

<section>
<h2>5 &middot; Fixes Applied</h2>
<ul class="fix">
<li><b>Dedupe (code):</b> list_pending_transfers groups by dispatch only (1 row/challan; 'mixed' when cross-company).</li>
<li><b>Reconcile keyed by lot_no</b> (not case-mismatched item_description); receiving-aware (skips once a GRN exists).</li>
<li><b>Corrective reconcile, cold (code):</b> restore wrong-lot/excess rows to source, pull the ORDERED lot FIFO, flag genuine shortage on interunit_transfers_header.unallocated_boxes. No silent substitution.</li>
<li><b>Box-count measure (code):</b> warehouse "expected" = COUNT(interunit_transfer_boxes), not SUM(qty) &mdash; eliminates false shortages where qty is a pack count (e.g. tid 426/713).</li>
<li><b>Applied to prod:</b> tid 468 wrong-lot corruption corrected (50 boxes re-lotted to the order). Verified: zero wrong-lot/excess corruption remains.</li>
<li><b>New columns:</b> unallocated_boxes, updated_ts on interunit_transfers_header. New endpoint GET /pending-stock/in-transit-by-lot.</li>
<li><b>Frontend:</b> shortfall badge + 'Edited' badge (modal), '+N in transit' overlay (dashboard), 'Edited' chip (directtransferform).</li>
<li><b>Tooling:</b> apply_reconcile.py (confirm-gated, per-tid or --all). 11/11 dependency-free tests pass.</li>
<li><b>OPEN:</b> (a) GRN auto-finalize for acknowledged-but-not-finalized transfers; (b) cancel stale dispatch tid 403; (c) deploy &mdash; all code is uncommitted (resolve the in-repo merge first).</li>
</ul>
</section>
</body></html>"""

open("Pending_Transfer_Report.html","w",encoding="utf-8").write(HTML)
print("wrote Pending_Transfer_Report.html  (", len(HTML), "bytes )")
