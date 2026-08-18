# Transfer Module Audit — legacy_frontend + legacy_backend

Generated 2026-08-17 by a 25-agent parallel audit (11 area sweeps + adversarial verification).

- 272 raw findings, 240 verifier verdicts, 21 refuted and removed
- **251 deduplicated findings** below

Severity counts: medium 101, high 94, critical 33, low 23

Category counts: correctness 67, payload-contract 50, aggregation 49, fetching 40, pagination 31, ux 9, perf 5

---


# CRITICAL (33)

## SUM(CAST(net_weight AS NUMERIC)) over a VARCHAR column that legitimately stores '' — 500s the whole JWO list and reports dashboard

**legacy_backend/services/ims_service/job_work_server.py:855** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** jb_materialout_lines.net_weight is VARCHAR(20) (declared line 108) and both insert paths write it as `str(item.get("net_weight", ""))` (line 654 for POST /out, line 785 for PUT /out/{id}), so a line whose payload omits net_weight is stored as the empty string (and `net_weight: null` is stored as the literal text 'None'). CAST('' AS NUMERIC) raises InvalidTextRepresentation in Postgres. The same unguarded cast appears at lines 1786, 2608, 2653, 2690, 2709, 2729, 2756 and 2777. The module itself knows this — the email-summary query at line 1582 guards with `net_weight IS NOT NULL AND net_weight != ''` before casting; none of the other 9 sites do.

**Failure scenario.** Operator uploads a challan via POST /job-work/extract-excel; the parser returns line_items without any `net_weight` key (see the dict built at lines 2283-2294), so /job-work/out stores net_weight=''. From then on GET /job-work/list?page=1 raises `invalid input syntax for type numeric: ""` -> HTTP 500. Not just that record: the correlated subquery runs for every header on the page, so ONE bad line takes down the entire Job Work list screen, GET /job-work/reports/dashboard, and GET /job-work/material-in/{id}.

**Fix.** Replace every `CAST(x.net_weight AS NUMERIC)` with a safe conversion, e.g. `COALESCE(NULLIF(regexp_replace(x.net_weight,'[^0-9.\\-]','','g'),'')::numeric, 0)` or `CASE WHEN x.net_weight ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN x.net_weight::numeric ELSE 0 END`; better, migrate the column to NUMERIC(12,3) and stop writing str('').


## Date-range filters compare a DD-MM-YYYY text column against ISO strings — silently returns the wrong rows

**legacy_backend/services/ims_service/jobwork_dashboard_server.py:67** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** job_work_date is VARCHAR (job_work_server.py:60) holding DD-MM-YYYY (proved by the SUBSTRING(x,4,7)->MM-YYYY assumption at job_work_server.py:2754 and the LENGTH(...)>=10 guard at line 2761), so these become lexicographic string comparisons, not date comparisons. Note the inconsistency inside this very file: line 138/197 cast with ::date, these clauses do not. The identical bug exists in job_work_server.py:2557-2561 (/reports/dashboard from_date/to_date) and job_work_server.py:835 (`h.job_work_date = :date`, documented as YYYY-MM-DD).

**Failure scenario.** User picks 01-Jan-2026 to 17-Aug-2026 in the dashboard date picker; the FE sends date_from=2026-01-01. A JWO dated '15-03-2026' fails the test because '1' < '2' lexicographically, so the dashboard reports 0 JWOs / 0 kg for a period that actually contains hundreds. In the same request a JWO dated '31-12-2025' PASSES ('3' > '2'), so out-of-range records are pulled in. Both KPIs and the Excel export are wrong, with no error shown.

**Fix.** Store the column as DATE (or normalize to ISO on write with _parse_date_to_iso) and compare with an explicit safe cast; until then convert the filter values to the stored format instead of comparing raw text.


## Jobwork dashboard summary casts a free-text date column to ::date on every row — empty/DD-MM-YYYY values 500 the endpoint

**legacy_backend/services/ims_service/jobwork_dashboard_server.py:197** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** jb_materialout_header.job_work_date is VARCHAR(20) (job_work_server.py:60) and is inserted verbatim as `header.get("job_work_date") or payload.get("dated", "")` (job_work_server.py:593) — no format validation, empty string allowed. `''::date` raises `invalid input syntax for type date: ""`. Worse, job_work_server.py:2754 (`SUBSTRING(h.job_work_date, 4, 7) as month_year`) only produces a MM-YYYY key if the stored format is DD-MM-YYYY, and `'15-03-2026'::date` raises `date/time field value out of range` under the default DateStyle. The same cast is used at line 138 (group_by=month) and line 377 (group-details month filter).

**Failure scenario.** A single JWO saved from a form where the date field was left blank (job_work_date='') makes GET /jobwork/dashboard/summary?company=cdpl return 500 for every user, because overdue_jwos evaluates the cast for every row that survives the WHERE clause. There is no per-row tolerance and no fallback — the KPI cards and the entire grouped table go blank.

**Fix.** Normalize job_work_date to ISO at write time (the file already has _parse_date_to_iso at job_work_server.py:32 but only applies it to receipt_date at line 1364) and use a safe cast in SQL until the data is clean: `CASE WHEN h.job_work_date ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN h.job_work_date::date END`.


## reconcile_transfer_to_order double-deducts cold stock for transfers received through the cold path

**legacy_backend/services/ims_service/pending_stock_tools.py:1692** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** The comment at lines 1687-1691 states the purpose exactly: 'Topping up here would re-deduct already-received stock (double count)'. But the guard queries only interunit_transfer_in_header/boxes, which cold_transfer_in_tools.py:353-358/561-562 deletes on a cold receive. With received == 0 the function falls through to the cold branch, sees parked < ordered (because the received boxes' pending rows were consumed), and calls _park_cold_row (line 1788) for the shortfall — which ends with `DELETE FROM {source_table} WHERE id = :rid` at line 1589. This runs on every backfill iteration (line 3046) and from apply_reconcile.py.

**Failure scenario.** Transfer orders 100 boxes of lot 185900 from cfpl cold. 60 are received through the cold path -> 40 pending rows remain. reconcile_transfer_to_order(tid) sees received=0, ordered=100, have=40, need=60; _find_available_cold_by_lot pulls 60 *unrelated, still-in-store* rows of lot 185900 (FIFO by inward_dt) and DELETEs them from cfpl_cold_stocks while inserting them as 'In Transit' on a months-old transfer. 60 boxes of good stock disappear from the cold sheet and the transfer shows 100 in transit though 60 were already delivered.

**Fix.** Make the 'has any receipt started' check source-agnostic (union interunit_transfer_in_boxes with the cold receipt ledger keyed on transfer_out_id) before any by-lot top-up, and never pull rows that a disposition record already marks as dispatched/received.


## Backfill re-parks already-received boxes and DELETEs them out of cold_stocks, because the receipt check is blind to cold receives

**legacy_backend/services/ims_service/pending_stock_tools.py:2844** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** Both dedupe gates — this candidate filter and _already_received() at line 258-291, which joins interunit_transfer_in_boxes -> interunit_transfer_in_header — look only at the interunit transfer-in tables. Cold receives delete exactly those rows: cold_transfer_in_tools.py:353-358 and 561-562 run `DELETE FROM interunit_transfer_in_boxes` + `DELETE FROM interunit_transfer_in_header`, and transfer_dashboard_server.py:93-96 documents that cold receipts live in cold_transfer_in_headers instead. The out-header is only flipped to 'Received' when pending_remaining == 0 (cold_transfer_in_tools.py:755-758), so a partially-received cold transfer stays 'Dispatch' and remains a backfill candidate. Line 3029-3034 then DELETEs the source row for any cold table, and _find_in_cold_stocks (line 64) searches BOTH companies' *_cold_stocks by (box_id, transaction_no) — which after a cold receive resolves to the row the receipt just created at the destination.

**Failure scenario.** TRANS20260528 moves 300 boxes cold->cold; 120 are received through the cold path (their pending rows are consumed, the interunit staging header+boxes are purged, out-header stays 'Dispatch'). An admin hits POST /interunit/pending-stock/backfill?user_email=...&dry_run=false. For each of the 120 received boxes: no pending row exists (dup check passes), _already_received returns False (the interunit rows were purged), so a new 'In Transit' pending row is INSERTed and `DELETE FROM cdpl_cold_stocks WHERE id = :rid` removes the box from the destination cold sheet. 120 boxes of physically-present stock vanish from inventory and reappear as in-transit — the exact 1,960-row incident described in _already_received's own docstring, just via the cold path the fix never covered.

**Fix.** Extend the candidate NOT EXISTS and _already_received to also check cold_transfer_in_headers / its box table (and cold_stock_disposition), i.e. treat a box as received if EITHER receipt ledger knows about it; also refuse to delete a source row whose transaction ties it to a completed receive.


## Cold OUT submits one `lines` entry PER BOX, inflating the challan Qty to boxes + (boxes - groups) — this is the Qty 198 / 100 boxes bug

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:2684** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** `scannedBoxes` holds ONE ROW PER PHYSICAL BOX (created in the `for (let i = 0; i < qty; i++)` loop at line 1893, each with `quantityUnits: '1'` at line 1930). `lines` maps that array 1:1, so a 100-box dispatch POSTs 100 document lines of qty 1 instead of one aggregated line per (item, lot). The backend (cold_transfer_out_tools.py:289-321) inserts all 100 as real `interunit_transfers_lines` rows, but `line_id_by_key[(item_desc_raw, lot_number)]` is overwritten on each iteration, so only the LAST line of each (item, lot) group stays in the map. Every box then FKs to that one line (cold_transfer_out_tools.py:340-344), and `_apply_box_totals` (cold_transfer_out_tools.py:157-175) rewrites ONLY that line's qty to the true box count — the other N-1 lines are explicitly left untouched (see comment at cold_transfer_out_tools.py:154). The list's Qty column is `SUM(qty)` over those lines (interunit_tools.py:1481).

**Failure scenario.** Cold Storage -> A185 dispatch of 100 boxes drawn from 2 piles (60 boxes of lot 125860, 40 boxes of lot 125861). FE POSTs 100 line objects (qty 1 each) + 100 box objects. DB ends up with 100 line rows: the 60th A-line is rewritten to qty=60, the 100th B-line to qty=40, and the remaining 98 rows keep qty=1. Transfer list renders `Qty: 198` (interunit_tools.py:1474/1481) next to `Boxes: 100`, and `pending_items = max(0, 198-100) = 98` (interunit_tools.py:1511) makes a fully dispatched challan look 98 boxes short forever. One pile gives 199, three piles give 197 — the formula is boxes + (boxes - groups).

**Fix.** Aggregate before building the payload: group `scannedBoxes` by (itemDescription, lotNumber, materialType) and emit ONE line per group with `qty = group.length`, `net_weight = sum(net)`, `total_weight = sum(gross)`. The per-box detail already travels in the `boxes` array; `lines` is the document view and must be one row per (item, lot).


## lot_number 'N/A' is nulled in `lines` but sent verbatim in `boxes`, so every line is treated as uncovered and pending stock is parked twice

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:2696** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** The same source value (`box.lotNumber`) is sanitised in the `lines` array but not in the `boxes` array. Blank-lot piles are explicitly supported (see the comment at line 1791 about transfer-in / disposition-recovered piles having no lot_no), and line 1923 stamps `lotNumber: article.lot_number || 'N/A'` for exactly those rows. The backend then keys coverage on (item.upper(), lot) for boxes and (item.upper(), lot) for lines (cold_transfer_out_tools.py:387-396): boxes yield ('X','N/A') while lines yield ('X',''), so `_take` is always 0 and EVERY line lands in `_uncovered`. `park_lines_in_pending` (pending_stock_tools.py:1479-1504) then inserts one phantom `LINE-<id>-<n>` pending row per unit on top of the real parked boxes. The same key mismatch also makes `line_id_by_key.get(line_key)` miss at cold_transfer_out_tools.py:344, so every box falls back to `fallback_line_id` (the FIRST line, not the intended one).

**Failure scenario.** Dispatch 100 boxes from a transfer-in pile that legitimately carries no lot_no. FE sends 100 lines with `lot_number: null` and 100 boxes with `lot_no: 'N/A'`. Backend parks 100 real box rows AND 100 phantom LINE- rows into `pending_transfer_stock`, all status 'In Transit'. `pending_cartons` for that lot now reads 200 for a 100-box dispatch, so the picker's own '+N in transit' badge (this file, line 422) and the Pending Transfers tooltip (line 448) show double the truth, and any receive-side reconciliation that counts pending rows is off by 100.

**Fix.** Use one sanitiser for both arrays. Build the box payload with `lot_number: cleanNull(box.lotNumber)` (i.e. `null` for 'N/A') so the lines key and the boxes key are byte-identical, or stop writing the 'N/A' sentinel into `lotNumber` at line 1923 and keep empty string throughout.


## Dashboard "Total Boxes" KPI and every group/item box total multiply the per-transfer box count by the number of line rows (JOIN fan-out double count)

**legacy_frontend/app/[company]/transfer/dashboard/page.tsx:260** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** `/transfer-dashboard/all-data` returns ONE RECORD PER LINE (`INNER JOIN interunit_transfers_lines l ON h.id = l.header_id`, transfer_dashboard_server.py:58) and then stamps a PER-TRANSFER box count onto every one of those rows (`rec["box_count"] = box_counts.get(rec["transfer_id"], 0)`, transfer_dashboard_server.py:89-91, where box_counts is `SELECT header_id, COUNT(*) ... GROUP BY header_id`). The FE then SUMs `box_count` across line rows. A transfer with N lines contributes N × its real box count. This is the textbook `COUNT(DISTINCT transfer_id)` paired with `SUM(fanned-out column)` bug: `total_transfers` is correctly de-duplicated with a Set on line 257 while `total_boxes` on line 260 is not.

**Failure scenario.** Challan TRANS20260817 moves 100 physical boxes across 2 item lines (Prawn 8/12, Prawn 16/20). all-data returns 2 rows, each with box_count=100. Dashboard "Total Boxes" KPI shows 200 for that single transfer; a 3-line transfer of 100 boxes shows 300. Across a month of ~450 transfers averaging 2.5 lines each the Total Boxes KPI reads ~2.5× the boxes that physically moved, and the Copy-to-WhatsApp summary (line 337) ships those inflated numbers to management.

**Fix.** De-duplicate by transfer before summing: `const totalBoxes = [...new Map(filtered.map(r => [r.transfer_id, r.box_count || 0]))].reduce((s, [, b]) => s + b, 0)` — i.e. one box_count per distinct transfer_id, exactly as `transfers`/`pending`/`notReceived` already do with `new Set(...)`.


## Records tab date filter sends ISO YYYY-MM-DD to a column that stores DD-MM-YYYY → always zero results

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:1068** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** recordsFilterDate comes from <input type="date">, i.e. "2026-05-13". The backend does an exact string equality on a VARCHAR(20) column (job_work_server.py:835 `h.job_work_date = :date`, DDL line 60 `job_work_date VARCHAR(20)`), and material-out writes that column as DD-MM-YYYY (material-out/page.tsx:392 currentDate = `${DD}-${MM}-${YYYY}`, POST handler line 593). "2026-05-13" can never equal "13-05-2026". The Summary tab in this same file explicitly converts to DD-MM-YYYY (lines 1226-1233), proving the stored format — the Records tab does not.

**Failure scenario.** Operator picks 13 May 2026 in the Records date filter. loadRecords(1) requests /job-work/list?page=1&per_page=15&date=2026-05-13 → total=0 → the page renders the "No job work records yet" empty state with a "New Material Out" CTA, even though 12 challans exist for that date. Users conclude the records were deleted.

**Fix.** Convert before sending, exactly as the Summary tab does: `const [y,m,d] = recordsFilterDate.split('-'); params.append('date', `${d}-${m}-${y}`)` — or better, normalize job_work_date to a DATE column and send ISO everywhere.


## "By Item" drill-down adds each JWO's FULL dispatched weight to every item it contains (fan-out double-count)

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:1435** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** renderTree loops over every key returned by level1Fn(r) and adds the whole record's aggregates (recDisp(r) = total_net_weight of the ENTIRE JWO, fg_received_kgs, waste+rejection, and count++) once per key. For the "By Item" view level1Fn is itemKeys(), which returns one key per item_description in the JWO (backend /job-work/list returns item_descriptions as a string_agg of DISTINCT descriptions). So a JWO containing N items contributes its full weight N times. Worse, item descriptions containing a comma (e.g. "DATES, PITTED 5KG") are split into two bogus item buckets, multiplying again. The correct per-line data is already fetched from the server as rptData.by_item (GROUP BY l.item_description, SUM(net_weight)) and is computed into fItem at line 1285 but never rendered.

**Failure scenario.** JWO JB202605131331 dispatches 1,000 kg across 3 SKUs (item_descriptions = "KHALAS, SAFAWI, MEDJOOL"). Summary → By Item shows KHALAS 1,000 kg / SAFAWI 1,000 kg / MEDJOOL 1,000 kg = 3,000 kg dispatched, and "1 JWO" under each, while the KPI card above (server-computed) says 1,000 kg. Out%/Pend% per item are computed from the same inflated numbers, so a vendor with genuine 8% loss can be shown as within/over tolerance arbitrarily.

**Fix.** Render the server's rptByItem/fItem for the item view, or make renderTree accept per-key weights: split the record's dispatched/fg/waste by line before distributing (requires per-line data from the API), and never split item_descriptions on a bare comma — return line objects from the backend instead of a comma-joined string.


## Jobwork dashboard: dispatch_date kept as DD-MM-YYYY, so date-range filter, month grouping and turnaround are all wrong

**legacy_frontend/app/[company]/transfer/jobwork/dashboard/page.tsx:124** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** r.job_work_date is the raw VARCHAR "DD-MM-YYYY" (see job_work_server.py DDL line 60 and the material-out writer). It is stored verbatim into dispatch_date and then (a) compared lexicographically against dateFrom/dateTo which are ISO "YYYY-MM-DD" from <input type="date">, (b) sliced 0..7 as a "YYYY-MM" month key, and (c) fed to Date.parse for turnaround. All three assume ISO.

**Failure scenario.** Record with job_work_date "13-05-2026". (a) Set From = 2026-05-01 → "13-05-2026" < "2026-05-01" is true ('1'<'2') → every record is filtered out, the table shows "No records match your filters" for any From date. (b) Group by = Month → key is "13-05-2", monthLabel splits it to y="13", mo="05" and renders the group as "May 13"; every distinct day becomes its own "month" row. (c) Date.parse("13-05-2026") = NaN → turnaround_days is null for every JWO, so the TAT column is permanently "-" and avg_turnaround_days is always 0.

**Fix.** Normalize on ingest: reuse the toYmd() helper that job-work/page.tsx:1182 already implements (`/^\d{4}-\d{2}-\d{2}/ ? slice(0,10) : DD-MM-YYYY → YYYY-MM-DD`) when building dispatch_date, and parse turnaround from the normalized value.


## Boxes with no matching article line are never rendered, never counted, and can never be acknowledged

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:172** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** When boxes exist on a non-cold transfer, allLinesCoveredByBoxes is true, so the page renders ONLY article lines (there is no boxes section anywhere in the JSX — groupedBoxes/handleAcknowledgeArticleBoxes/handleAcknowledgeAllBoxes/handleUnacknowledgeBox at L272/684/1071/660 are never called from render). totalItems and totalMatched also ignore boxes. The design silently assumes boxes.length === lines.length; the weight-init code at L398 explicitly contemplates boxes.length > lines.length.

**Failure scenario.** Warehouse→warehouse transfer of 40 scanned cartons across 4 article lines. The receive screen shows 'Article Entries (4)' and 'Acknowledge All (4)'. Confirm Receipt writes 4 interunit_transfer_in_boxes rows; finalize_transfer_in's bridge invariant picks only 4 of the 40 pending_transfer_stock rows, so 36 boxes stay 'In Transit' forever and the transfer never leaves Partially Received. Nothing on screen tells the operator 36 boxes were dropped.

**Fix.** Derive the acknowledgeable unit set from boxes when boxes.length > lines.length (as the cold page does via linesFromBoxes), or restore a Scanned Boxes section and include matchedBoxes/totalBoxes in totalItems/totalMatched. At minimum, render a blocking warning when boxes.length !== lines.length.


## acknowledgeBatch per-box conflicts (409/422) are discarded — partial failures shown as full success

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:1005** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** acknowledge_pending_boxes_batch (interunit_tools.py L2695-2727) returns HTTP 200 with {success, count, boxes, conflicts[]} and swallows per-box 409 duplicate / 422 reconciliation-conflict errors into `conflicts`. Every call site discards the response and unconditionally marks all lines matched: regular page L708, 1005, 1058, 1095, 1650, 1828; cold page L857, 1120, 1173, 1210, 1778, 2060. AcknowledgeBatchResponse in interunitApiService.ts even types `conflicts` — the FE just never reads it.

**Failure scenario.** 40-box receipt; 5 boxes were already received on another GRN so STBR returns duplicate → the batch answers 200 with conflicts.length === 5 and count === 35. The UI shows 40/40 resolved, allMatched === true, Confirm Receipt enables, finalize runs, and 5 cartons are never recorded — with a green '40 items acknowledged successfully' toast.

**Fix.** Read the response: only set linesMatchMap[i] for box_ids present in `resp.boxes`, and surface `resp.conflicts` as per-row error badges plus a blocking toast. Do the same at all 12 call sites.


## Confirm Receipt re-sync writes a SECOND transfer_in_boxes row for every scanned line (duplicate boxes)

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:1781** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** handleAcknowledgeLine sends box_id = scannedLineData[i].box_id (the physically scanned id) but the Confirm-Receipt re-sync at L1772-1803 rebuilds the same line's payload WITHOUT the scanned id. The backend upsert key is (header_id, box_id) (interunit_tools.py acknowledge_pending_box, `ON CONFLICT (header_id, box_id)`), so the two different box_ids create two rows for one physical carton. Identical defect on the cold page (L878 vs L2011).

**Failure scenario.** Transfer with 10 boxes, txn TRANS2026…; carton is physically labelled 88881234-07 but IMS FIFO-picked 88881234-03. Operator scans it → matchedLineIndex path at L1237 stores scanRef {box_id:'88881234-07'} → acknowledge inserts row box_id='88881234-07'. Operator clicks Confirm Receipt → re-sync posts the same line as box_id='88881234-03' → a 2nd row is inserted. interunit_transfer_in_boxes now has 11 rows for 10 cartons; finalize's pick_from_pending claims a slot with the stale id, leaving one real pending row In Transit, and the receipt reports 11 boxes received.

**Fix.** Build the acknowledge payload once (a single buildAckItem(lineIndex) helper used by handleAcknowledgeLine, handleAcknowledgeAll*, handleBulkPrintQR and the Confirm-Receipt re-sync) so scannedLineData[i].box_id/transaction_no always take the same precedence, and drop the re-sync entirely if the per-item acknowledges already succeeded.


## transferform builds lines[] from the article form, not from the loaded request — every request item after the first is silently dropped

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:1669** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** loadRequestDetails stores every request line in loadedItems (line 606) but copies only firstItem into articles[0] (lines 586-598). The payload maps `articles`, so only that one line is transmitted, while the UI explicitly promises otherwise: line 2617 renders 'All {loadedItems.length} items will be included in the transfer.' Worse, boxes scanned for the missing articles are still sent; the backend matches boxes to lines by article name and falls back to the FIRST line when no match exists (interunit_tools.py:1257 `line_id_by_article.get(box_article_key, fallback_line_id)`), so those boxes get attached to the wrong article, and _boxes_authoritative (interunit_tools.py:980-983) skips them entirely because no line carries their name.

**Failure scenario.** Request REQ20260817 has 3 lines: ALMOND WHOLE 10 boxes, CASHEW W240 8 boxes, WALNUT 5 boxes. Operator opens transferform?requestId=…, scans all 23 boxes, submits. Payload contains lines[] with ONE entry (ALMOND WHOLE, qty 1) and 23 boxes. DB: one transfer line for ALMOND, with the 13 CASHEW/WALNUT boxes hung off that ALMOND line; the delivery challan and the Transfer-IN screen show cashews and walnuts as almonds, and the request is marked 'Transferred' (interunit_tools.py:1363-1371).

**Fix.** Build lines[] from loadedItems (falling back to `articles` when no requestId), or from scannedBoxes grouped by item_description as directtransferform does, so every requested/scanned article appears as its own line.


## transferform sends manually-added article entries in boxes[] as transaction_no='DIRECT' — backend parks NO stock for them

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:1684** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** handleAddArticleToList (line 896) stamps transactionNo:'DIRECT' on every manually-added entry, and the submit payload maps ALL scannedBoxes into boxes[] with no filter. The sibling form does filter these out (directtransferform/page.tsx:1978 `scannedBoxes.filter(box => box.transactionNo !== 'DIRECT')`). Backend consequences are two-fold and both silent: (a) park_in_pending explicitly skips DIRECT boxes (legacy_backend/services/ims_service/pending_stock_tools.py:1285 `if not box_id or not transaction_no or transaction_no == "DIRECT": continue`) so no pending_transfer_stock row is written and no source inventory is deducted; (b) because a DIRECT box row IS inserted into interunit_transfer_boxes with transfer_line_id set and box_id NOT LIKE 'ART-%', _uncovered_lines (interunit_tools.py:1081-1088) treats the line as already covered, so park_lines_in_pending (interunit_tools.py:1334-1344) is skipped too. Sending the DIRECT boxes also suppresses _synthesise_article_entry_boxes (interunit_tools.py:1221), which is the mechanism that would otherwise have created parkable ART-n rows.

**Failure scenario.** Operator opens transferform for request REQ..., fills Article Entry (ALMOND WHOLE, qty 20, 10 kg/box) and clicks 'Add to Articles List' (20 rows appear), then submits. POST body: lines[0].qty=20, boxes[0..19] each {transaction_no:'DIRECT', box_id:'<sku>'}. DB: header + 1 line + 20 box rows created, pending_transfer_stock rows = 0. Source warehouse stock is never decremented and the destination warehouse's Transfer-IN screen shows nothing to receive — 200 kg is dispatched on paper and invisible in inventory.

**Fix.** Filter DIRECT entries out of the boxes[] payload exactly as directtransferform:1978 does, letting the backend synthesise ART-n boxes and park the lines: `boxes: scannedBoxes.filter(b => b.transactionNo !== 'DIRECT').map(...)`.


## No double-submit protection on either form — a second click creates a whole second transfer with a re-minted challan

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:3036** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** handleSubmit (transferform:1539, directtransferform:1843) is async and sets no in-flight flag; neither submit button is disabled while the POST is running, and the POST is slow (it parks every box and runs reconcile_transfer_to_order). The backend does not reject a repeat: unique_challan_no (legacy_backend/services/ims_service/interunit_tools.py:500-521) deliberately RE-MINTS a fresh number when the requested challan_no is taken, so the duplicate submit succeeds as a brand-new transfer instead of failing. Other forms in this codebase already guard this (innercoldtransfer/page.tsx:897 `disabled={submitting || ...}`, job-work/material-out/page.tsx:1951), so the omission here is an oversight, not a convention.

**Failure scenario.** Operator scans 40 boxes and double-taps 'Submit Transfer' on a slow 4G tablet. Request #1 saves TRANS202608171432 and parks 40 boxes; request #2 arrives 400 ms later, unique_challan_no sees the number taken and allocates TRANS20260817143251, inserting a second header, 40 more box rows and attempting to park the same 40 physical boxes again. Result: two challans for one truck, and every box appears twice in the transfer ledger.

**Fix.** Add `const [submitting, setSubmitting] = useState(false)`, return early if submitting, set it true at the top of handleSubmit and false in a finally block, and put `disabled={submitting}` on both submit buttons.


## 'Total Count (PM)' sums mixed units: unit_pack_size falls back to pack_size, which is kilograms per box for PM lines

**legacy_frontend/components/transfer/DeliveryChallan.tsx:91** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** For PM/packaging the backend is explicit that `unit_pack_size` is a per-box PIECE COUNT while `pack_size` is KG PER BOX (services/ims_service/interunit_tools.py:849-851 and 891-893: 'PM/packaging kg per box = pack_size; unit_pack_size is a piece count'). `_map_transfer_line` returns `unit_pack_size` as `None` whenever the column is NULL. When it is null this code silently substitutes the kilogram figure and multiplies it by qty, adding kilograms into a field labelled 'Total Count' (pieces). The same fallback exists in `itemCountFor` (line 99), so both the per-row Count column and the header banner (line 217), the DC totals cell (line 364) and the Gate Pass 'Total Count' (line 557) are contaminated. Note ChallanHoverCard.groupLinesByItem:291 does NOT do this fallback, so the hover card and the printed DC disagree for the same transfer.

**Failure scenario.** The observed Gate Pass: 4 items, 'Total Count 50,368.44' = 25,000 + 25,000 + 360 + 8.44. The 8.44 term is a pack_size in kg (a box weight) for a PM/packaging line whose unit_pack_size is NULL; it is added to piece counts of 25,000 and 360. Security at the gate is handed a 'count' that is neither pieces nor kilograms and cannot be verified against anything.

**Fix.** Drop the `|| item.pack_size` fallback for countable items - if `unit_pack_size` is null/0 the piece count is unknown, so render '-' and exclude the row from the total. Optionally show a separate 'Total Kg' that already exists rather than folding kg into Count.


## Count defaults quantity to 1 when qty is 0/blank, fabricating a full pack-size count for a zero-quantity line

**legacy_frontend/components/transfer/DeliveryChallan.tsx:100** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** `consolidatedItems` normalises qty to a NUMBER (line 133: `qty: parseFloat(...) || 0`) but keeps the original string `quantity` from the spread. For a zero-quantity row the chain evaluates `0 || "0" || "1"` -> `"0"` -> parseFloat -> 0 -> `|| 1` -> **1**. So a line that moved nothing is counted as one full pack. The same expression is used in `totalPMCount` (line 92) and in the Gate Pass per-row count (line 504), so the fabricated value propagates into every printed total.

**Failure scenario.** A PM line for 'PP BAG 25000 PCS' is saved with qty 0 (cancelled pick, or qty never keyed - the transfer form defaults exist). unit_pack_size = 25000. The Gate Pass prints 'Count 25,000' for that row and adds 25,000 to 'Total Count', for an item where zero pieces are on the truck. Two such lines produce the observed 25,000 + 25,000 pair.

**Fix.** Use a single numeric qty resolved once (`const q = Number(item.qty ?? item.quantity ?? 0) || 0`) and return 0 when q is 0. Never default a quantity to 1 on a printed document.


## DC/Gate Pass 'Total Boxes' double-counts physical boxes: per-description box count is re-assigned to every consolidated row of that description

**legacy_frontend/components/transfer/DeliveryChallan.tsx:144** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** `boxCountByDesc` is a per-description tally of physical box rows, but `itemMap` groups by `description__category__pack_size`. Whenever one description yields more than one consolidated row (different category or a different pack_size string), each of those rows is assigned the SAME full physical box count for the description. The DC totals (line 347) and the Gate Pass 'Total Boxes' (line 540) then sum those duplicated values, so N physical boxes print as N x (number of consolidated rows for that description). This is guaranteed to happen for cold-source transfers: `_boxes_authoritative` in the backend (services/ims_service/interunit_tools.py:965) returns early for cold sites so per-box/per-lot duplicate lines are NEVER collapsed, and the direct transfer form emits one line per scanned box (app/[company]/transfer/directtransferform/page.tsx:1945-1964) each carrying its own `pack_size`.

**Failure scenario.** Cold dispatch of 100 physical boxes of 'CASHEW W240', picked from two lots whose lines carry different pack_size (e.g. 25.000 vs 24.500 kg/box). Backend returns 100 boxes and >=2 lines with the same item_description. DeliveryChallan builds 2 consolidated rows; boxCountByDesc['CASHEW W240'] = 100; both rows get box_count = 100. Printed DC/Gate Pass: 'Total Boxes 200' for 100 boxes on the truck. To land on exactly 198: one box row is stored with a blank/variant `article` (backend `_map_box_row` returns `row.article or ""`, and line 115 silently drops empty articles), so boxCountByDesc = 99, and 99 x 2 rows = 198 boxes printed for 100 physical boxes.

**Fix.** Key the box tally the same way the item rows are keyed (or better: allocate boxes to rows and never re-use a count). Build the tally from `bx.transfer_line_id` -> line (the backend already returns it) and sum per consolidated group, then assert that the sum of all `box_count` equals `boxes.length` before printing; if the article can't be matched, show '-' rather than a wrong number.


## transformFormDataToApi sends `package_size`, backend field is `unit_pack_size` — FG package size silently discarded on every transfer request

**legacy_frontend/lib/interunitApiService.ts:834** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** `ArticleDataCreate` (interunit_models.py:29-40) declares `unit_pack_size`, not `package_size`, and has no `model_config` so Pydantic v2's default `extra='ignore'` silently drops the unknown key. The FE `ArticleData` interface (interunitApiService.ts:183) declares `package_size?: string` and `validateRequestData` (line 893) hard-requires it for FG material — the client validates and transmits a field the server never reads, and never populates the field the server actually stores. `total_weight` (backend `ArticleDataCreate.total_weight`) is never sent at all. The mismatch is invisible: no 422, no error, HTTP 201.

**Failure scenario.** Operator creates an interunit request for material_type='FG', item 'ALMOND KERNEL', packageSize='25'. FE validation passes (package_size present). POST /interunit/requests?created_by=... returns 201. The persisted `interunit_transfer_request_lines.unit_pack_size` is NULL. GET /interunit/requests then returns `lines[0].unit_pack_size = null` and no `package_size` key at all, so the edit/approval screen renders the FG pack size as blank and the downstream transfer is built with pack size 0.

**Fix.** Rename the emitted key to `unit_pack_size` in transformFormDataToApi (and in the `ArticleData` / `RequestLine` interfaces, which also declare `package_size` at lines 183 and 217). Also emit `total_weight`. Longer term, set `model_config = ConfigDict(extra='forbid')` on ArticleDataCreate so this class of drift 422s instead of silently dropping.


## buildSummary sums a header-level box_count across fanned-out line rows — Total Boxes multiplied by the number of lines

**legacy_frontend/lib/transfer/buildSummary.ts:69** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** The records fed to buildSummary come from GET /transfer-dashboard/all-data, which is `interunit_transfers_header h INNER JOIN interunit_transfers_lines l` — ONE ROW PER LINE (transfer_dashboard_server.py:57-58). The backend then attaches a HEADER-LEVEL box total onto every one of those line rows: `box_counts = {header_id: COUNT(*) FROM interunit_transfer_boxes GROUP BY header_id}` and `rec["box_count"] = box_counts.get(rec["transfer_id"], 0)` (transfer_dashboard_server.py:84-91). Summing that replicated per-header value across the fanned-out line rows multiplies the box count by the number of lines. This is the exact COUNT(DISTINCT id) + SUM(y)-over-a-fanned-out-join pattern: tx_count uses `new Set(map(transfer_id)).size` (:70) so transfer counts are de-duplicated, while total_boxes is not. The dashboard KPI does the same thing at app/[company]/transfer/dashboard/page.tsx:260 (`filtered.reduce((s, r) => s + (r.box_count || 0), 0)`).

**Failure scenario.** TRANS202608171318 (Cold Storage -> A185) has 100 physical boxes in interunit_transfer_boxes and 2 distinct line rows. /transfer-dashboard/all-data returns 2 records, each with box_count = 100. buildSummary's group for from_warehouse='Cold Storage' reports total_boxes = 200 for a 100-box transfer. Worse, because the cold transfer-out writer emits ONE LINE PER BOX (coldtransferform/page.tsx:2684 + cold_transfer_out_tools.py:289-317), a cold challan with 100 boxes and 100 line rows yields 100 x 100 = 10,000 boxes in the 'Total Boxes' KPI and in every group/sub-group/item row of the summary tree.

**Fix.** box_count is a per-TRANSFER attribute, not a per-line one. De-duplicate by transfer_id before summing: `const sumBoxes = (rs) => { const seen = new Map(); for (const r of rs) if (!seen.has(r.transfer_id)) seen.set(r.transfer_id, r.box_count || 0); return [...seen.values()].reduce((a,b)=>a+b,0) }`. Apply the same de-dup at dashboard/page.tsx:260. (Alternatively have the backend divide box_count across lines or expose a separate per-line box count.)


## finalize_cold_transfer_in re-inserts every box with no idempotency guard — a second submit doubles cold_stocks and the box count

**services/ims_service/cold_transfer_in_tools.py:654** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** _process_box_loop does a bare INSERT into cold_transfer_inboxes AND into <company>_cold_stocks for every box in the payload, with no ON CONFLICT, no delete-then-insert, and no check for boxes already recorded on this header. finalize_cold_transfer_in is explicitly designed to be re-callable ('Idempotent on resume' at line 221; the hdr-exists branch at line 245 UPDATEs the header and falls straight through to _process_box_loop without clearing prior boxes). The only accidental protection is cold_stocks' UNIQUE(transaction_no, box_id) — but the module's own comment at lines 590-593 states 'the TX-In page regenerates real {epoch}-{n} box_ids before submit', so a re-submit carries FRESH box_id/transaction_no values and the unique key does not fire. Worse, this is self-triggering: because the pending lookup at line 625 keys on the regenerated box_id it finds nothing, no pending rows are deleted, _reconcile_statuses (line 750) still sees pending_remaining > 0 and returns 'Pending', so the UI keeps offering Finalize and the operator clicks it again.

**Failure scenario.** Cold receive of 100 boxes into Rishi. Operator clicks Finalize; the transaction commits but the response is lost to a proxy timeout, and the header still reads 'Pending' (because no pending row matched the regenerated ids). Operator clicks Finalize again; the page has re-run Generate QR so the 100 boxes now carry new {epoch}-{n} ids and a new TR- transaction_no. The second call inserts 100 more cold_transfer_inboxes rows and 100 more cdpl_cold_stocks rows. get_cold_transfer_in_by_id returns 200 boxes, list_cold_transfer_ins.total_boxes_scanned (line 3369-3370) shows 200, and CDPL cold stock is inflated by 100 phantom boxes / ~2x the kg for that lot.

**Fix.** Make the loop idempotent: add a UNIQUE (header_id, box_id) index on cold_transfer_inboxes and use ON CONFLICT DO UPDATE, or delete this header's existing inboxes+cold_stocks rows before re-inserting. Additionally short-circuit finalize when the header status is already 'Received', and reject a submit whose box set is already fully present on the header.


## Cold receive sweeps ALL LINE-% sentinels for the transfer after a single box, flipping a 1-of-100 receipt to 'Received'

**services/ims_service/cold_transfer_in_tools.py:734** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** The sweep is gated only on `inserted > 0` — one received box deletes EVERY LINE- sentinel for the whole dispatch. _reconcile_statuses (line 750-762) then counts ALL remaining In-Transit rows; for a warehouse->cold transfer every parked row is a LINE- sentinel (warehouse sources have no per-box stock, so park_lines_in_pending is the only parker — see interunit_tools.py:3126-3128), so the count drops to 0 and both the cold IN header and the transfer OUT header are stamped 'Received'. The interunit side is careful about exactly this: _claimed_pending_box_ids (interunit_tools.py:2996-3001) hands out 'at most as many sentinels as that article has unclaimed boxes, so a partial receipt claims a partial set and the shortfall stays on the bridge', and count_remaining_in_transit (pending_stock_tools.py:1955) excludes LINE- rows from the completion gate. The cold path has neither bound.

**Failure scenario.** Warehouse W202 dispatches 100 units of a PM article to Savla D-39; park_lines_in_pending writes 100 rows LINE-<lid>-1..100. The receiving operator scans and finalizes 1 box. _process_box_loop inserts 1 cold_transfer_inboxes row + 1 cfpl_cold_stocks row, inserted=1 > 0, so the DELETE removes all 100 LINE- rows. _reconcile_statuses sees pending_remaining=0 and sets interunit_transfers_header.status='Received' and cold_transfer_in_headers.status='Received'. 99 units silently vanish from the in-transit ledger with no shortage record, the transfer disappears from the Pending Transfers modal, and no one can receive the remaining 99.

**Fix.** Bound the sweep the way _claimed_pending_box_ids does: delete at most `inserted` sentinels, matched per article (DELETE ... WHERE id IN (SELECT id ... AND article = :art ORDER BY id LIMIT :n)). Alternatively exclude LINE-% from _reconcile_statuses' count and only sweep when the received count covers the ordered quantity.


## ROOT CAUSE: cold transfer-OUT writes one interunit_transfers_lines row per BOX, and only the last line per (item,lot) is ever corrected — SUM(qty) becomes 2*boxes − distinct(item,lot)

**services/ims_service/cold_transfer_out_tools.py:318** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** The cold form builds its payload as `lines = scannedBoxes.map(...)` (legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:2684) and `scannedBoxes` holds ONE entry per physical box with `quantityUnits: '1'` (same file, line 1893-1939, `for (let i = 0; i < qty; i++)`). So a 100-box cold dispatch arrives at create_cold_transfer_out with lines.length == 100 (each qty=1) and boxes.length == 100. The loop at line 289-321 INSERTs all 100 line rows verbatim. `line_id_by_key` is keyed on (item_desc_raw, lot_number), so after the loop it holds only the LAST row id for each key — K entries for K distinct (item, lot) pairs. Every box then resolves to that one line id (line 344), so `_apply_box_totals` (line 147/157) rewrites qty/net for exactly K lines and never touches the other B−K, which keep the form's qty=1 and their per-box net_weight. Unlike the warehouse path, nothing deletes the duplicates: `_boxes_authoritative` (interunit_tools.py:959) does `DELETE FROM interunit_transfers_lines WHERE id = ANY(:ids)` for duplicate article lines, but it returns early for cold sites (line 965) and is never called from the cold path. Net effect on interunit_transfers_lines for a cold header: SUM(qty) = B + (B − K) = 2B − K, and SUM(net_weight) ≈ 2× the real kg.

**Failure scenario.** TRANS202608171318 (Cold Storage → A185, 17-08-2026, MH43BX1881 / Sachin): operator picks 100 boxes across 2 (item, lot) piles, say 60 + 40. Frontend POSTs /interunit/cold-transfer-out/create with 100 lines (qty 1 each) + 100 boxes. Backend inserts 100 line rows; `_apply_box_totals` sets the last line of pile A to qty=60 and the last line of pile B to qty=40; the remaining 98 lines keep qty=1. list_transfers then reports items_count = COUNT(DISTINCT item_desc_raw) = 2 and total_qty = SUM(qty) = 60+40+98 = 198. The list renders "2 Items" + "Qty: 198" (legacy_frontend/app/[company]/cold-transfer/page.tsx:778,781) for a dispatch of exactly 100 boxes. The formula 2B−K reproduces the observed 198 exactly (2×100−2). pending_transfer_stock is unaffected (park_in_pending receives the 100 real boxes), which is why the physical deduction is right while every lines-derived number is inflated. A68 rows are correct because the warehouse path runs `_boxes_authoritative`, which collapses to one line per article — hence boxes == qty (41=41, 26=26) there.

**Fix.** Fold the incoming per-box lines to one row per (item_desc_raw, lot_number) BEFORE inserting, using exactly the same key `line_id_by_key` uses, so box→line resolution is unchanged and `_apply_box_totals` then owns every box-backed line. Add to cold_transfer_out_tools.py:

def _fold_lines(raw: List[ColdOutLineInput]) -> List[ColdOutLineInput]:
    """One line per (item, lot). The cold form sends one line PER BOX
    (scannedBoxes.map), so 100 boxes arrive as 100 lines of qty 1; only the last
    line per key is corrected by _apply_box_totals, leaving SUM(qty)=2B-K."""
    folded: Dict[tuple, ColdOutLineInput] = {}
    for l in raw:
        key = ((l.item_desc_raw or "").strip(), (l.lot_number or "").strip())
        cur = folded.get(key)
        if cur is None:
            folded[key] = l.model_copy(deep=True)
            continue
        cur.qty          = float(cur.qty or 0)          + float(l.qty or 0)
        cur.net_weight   = float(cur.net_weight or 0)   + float(l.net_weight or 0)
        cur.total_weight = float(cur.total_weight or 0) + float(l.total_weight or 0)
        cur.pack_size      = max(float(cur.pack_size or 0),      float(l.pack_size or 0))
        cur.unit_pack_size = max(float(cur.unit_pack_size or 0), float(l.unit_pack_size or 0))
        cur.uom = cur.uom or l.uom
    return list(folded.values())

then change line 270 to `derived_lines: List[ColdOutLineInput] = _fold_lines(payload.lines)` (and the identical line 500 in edit_cold_transfer_out). After the fold, `_apply_box_totals` sets qty = real box count on every box-backed line, so SUM(qty) == B == COUNT(interunit_transfer_boxes) and SUM(net_weight) == the boxes' true kg. Manual/DIRECT lines with no boxes keep their (now summed) typed qty, which is what the `_uncovered`/park_lines_in_pending block at line 388-420 already assumes. Also fix the existing rows with the repair SQL in `notes`.


## Same per-box line explosion in the cold EDIT path — re-saving the transfer reproduces the inflated qty instead of healing it

**services/ims_service/cold_transfer_out_tools.py:547** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** edit_cold_transfer_out repeats the create logic verbatim (derived_lines = list(payload.lines) at line 500, insert loop 518-550, last-write-wins key map at 547, `_apply_box_totals` at 596). The edit form re-hydrates `scannedBoxes` from `transfer.boxes` one entry per box (coldtransferform/page.tsx:1283-1312) and its `manualLines` filter (1325-1329) excludes every line whose (article|lot) is covered by a box — so all 100 duplicate lines are dropped from the UI and re-sent as 100 fresh per-box lines. The DELETE at 464-465 does remove the old lines first, so an edit does NOT compound the error (candidate (b) refuted) — but it also means the operator cannot correct the number by opening and re-saving the challan: it lands on 2B−K again.

**Failure scenario.** Support opens TRANS202608171318 in the cold transfer form to 'fix the qty', changes nothing, and saves. restore_to_source + DELETE lines/boxes run, then 100 boxes are re-sent, 100 line rows are re-inserted, `_apply_box_totals` corrects 2 of them, and the list still shows Qty: 198. The only visible change is `updated_ts`, so the record now also carries an 'Edited' stamp for a no-op.

**Fix.** Apply the same `_fold_lines(payload.lines)` at line 500 (see the previous finding). Both call sites must change together — the create path alone would leave every edited cold challan wrong.


## Whole /interunit router is unauthenticated; deletes/backfills are 'authorized' by a caller-supplied user_email/user_role query string

**services/ims_service/interunit_server.py:91** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** `dependencies.verify_token` exists and is used throughout services/ims_service/server.py, but no endpoint in interunit_server.py (46 routes) declares it, and main.py:383 mounts the router with no router-level dependency. Authorization for the destructive routes is derived entirely from attacker-controlled query params: `DELETE /transfers/{id}?user_email=&user_role=admin` (line 306-313), `POST /pending-stock/backfill?user_role=admin` (line 160-171), `DELETE /transfer-in/{id}?user_email=yash@candorfoods.in` (line 768-775), `DELETE /cold-transfer-in/{id}?user_email=...` (line 399-405). Every write route (POST /transfers, PUT /transfers/{id}, POST /transfer-in, acknowledge, finalize, cold-transfer-out/create+edit) has no identity requirement whatsoever.

**Failure scenario.** `curl -X DELETE 'https://<host>/interunit/transfers/1615?user_email=x@x.com&user_role=admin'` returns 200 and deletes transfer 1615 for any caller — the `user_role in ADMIN_ROLES` branch fires on a string the caller typed. Likewise `POST /interunit/pending-stock/backfill?user_email=x&user_role=developer` runs a stock-mutating reconciliation migration against production.

**Fix.** Attach `dependencies=[Depends(verify_token)]` to `APIRouter(prefix="/interunit")` and derive email/role from the decoded JWT payload inside `_check_delete_permission` instead of from `Query(...)`. Delete the `user_email`/`user_role` query params so they cannot be spoofed.


## DELETE /interunit/cold-transfer-out/{header_id} has NO authorization check at all — anyone can destroy a dispatch and its receipt

**services/ims_service/interunit_server.py:432** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** Every other delete in this router demands a `user_email` query param and runs `_check_delete_permission` (line 91) or an email allowlist. This one takes no identity parameter at all, and `delete_cold_transfer_out(db, header_id)` (cold_transfer_out_tools.py:649-689) performs no permission check either — it goes straight to `unpick_to_pending`, then `DELETE FROM interunit_transfer_in_boxes / interunit_transfer_in_header / interunit_transfer_boxes / interunit_transfers_lines / interunit_transfers_header`. The router is also mounted with no `Depends(verify_token)` (main.py:383), so there is no authentication either.

**Failure scenario.** `curl -X DELETE https://<host>/interunit/cold-transfer-out/1615` from any unauthenticated client deletes cold transfer-out 1615, deletes the already-completed GRN (transfer_in_header + boxes) for it, and re-parks/restores its stock. No email is recorded, no audit trail, no 403. The operator sees the challan simply vanish from Transfer Out Records.

**Fix.** Add `user_email: str = Query(...)` and `user_role: str = Query("")` and call `_check_delete_permission(user_email, user_role)` before delegating — matching `delete_transfer_endpoint` (line 305-313); better, move authorization behind `Depends(verify_token)` so the identity comes from the JWT and not the query string.


## _fetch_boxes LEFT JOIN on box_id alone fans out transfer boxes — this is the '100 boxes shows as 198' bug

**services/ims_service/interunit_tools.py:645** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** The join key is box_id ONLY. It is not scoped to this transfer (pts.transfer_out_id = itb.header_id) and not scoped by transaction_no. pending_transfer_stock's uniqueness is (box_id, transaction_no) — see the ON CONFLICT (box_id, transaction_no) arbiter used by every insert path (pending_stock_tools.py:1354, 1504, 1568, 2169, 2995) and the constraint named in the park_lines_in_pending docstring (pending_stock_tools.py:1439, 'uq_in_transit_box (box_id, transaction_no)'). box_id is therefore NOT unique across In-Transit rows. Every duplicate box_id in pending_transfer_stock multiplies its matching interunit_transfer_boxes row, so get_transfer() (interunit_tools.py:1600) returns the same physical box 2+ times. This is exactly the data condition COLD_TRANSFER_DUPBOX_INCIDENT.md documents as still present in production (Mode A: '{base}-1' minted into two transaction_no groups; the doc's deferred fix list explicitly leaves box_id non-unique). get_transfer even acknowledges the collision at interunit_tools.py:1605-1607 ('the per-box JSONB which can carry noise from prior transfers that shared the same box_id') but only fixes lot-origin attribution, not the row fan-out. Note the direct contradiction with list_transfers, which uses COUNT(DISTINCT box_id) (line 1487) — so the same transfer under-counts on the list page and over-counts on the detail page.

**Failure scenario.** Cold dispatch TRANS-A ships 100 boxes of Fresho Kimia Dates lot 125859 under transaction TR-...751 (box_ids 90671000-1 .. 90671000-100). Ninety-eight of those box_id values also exist as In-Transit rows for a different still-open dispatch TRANS-B under transaction TR-...513 (the documented cross-transaction base reuse). GET /interunit/transfers/<A> runs _fetch_boxes: the 98 colliding boxes each match 2 pending rows and are emitted twice, the other 2 once. result['boxes'] has 100 + 98 = 198 entries; the server's own GET_TRANSFER_DEBUG log (line 1687) prints boxes_count=198. The transfer detail screen, the DC print and any box-count badge driven off this array show 198 boxes for a 100-box truck, and the duplicated rows carry a source_unit copied from the unrelated transfer.

**Fix.** Scope the join to this transfer and the full box identity, and make it non-fanning: LEFT JOIN LATERAL (SELECT cold_storage_data FROM pending_transfer_stock p WHERE p.transfer_out_id = itb.header_id AND p.box_id = itb.box_id AND COALESCE(p.transaction_no,'') = COALESCE(itb.transaction_no,'') AND p.status = 'In Transit' ORDER BY p.id LIMIT 1) pts ON TRUE. The LIMIT 1 guarantees one row per box even if the ledger ever holds duplicates.


## total_qty is SUM(lines.qty) — a PACK/unit sum — but is rendered as the transfer's box quantity next to items_count (COUNT DISTINCT article)

**services/ims_service/interunit_tools.py:1481** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** Two aggregates with incompatible denominators are taken from the same GROUP BY: items_count de-duplicates on the article label, total_qty does NOT de-duplicate anything — it sums `qty` over the raw line multiset. `interunit_transfers_lines.qty` is not a box count. pending_stock_tools.py:1738-1742 states it outright: "WAREHOUSE source: each order LINE is one box and `qty` is the PACK count (units per box), so SUM(qty) is NOT the box count (that caused false shortages, e.g. 75 boxes parked vs SUM(qty)=1963 → bogus 1888 short)". reconcile_transfer_to_order was fixed to stop using SUM(qty); list_transfers still uses it and labels it "Qty". The live schema dump confirms the shape: 452 headers / 35,457 line rows / 14,362 box rows — ~78 line rows per header against ~32 boxes, i.e. lines are per-box and/or duplicated, never one-per-article. Because items_count is DISTINCT and total_qty is not, the card can only ever read "few items, huge qty".

**Failure scenario.** Cold Storage → A185, 100 physical boxes, 2 articles. interunit_transfers_lines holds the per-(article,lot)/per-box rows whose `qty` values (packs per box, or the operator's typed carton figure on a line that received no boxes) add up to 198. SQL returns items_count = COUNT(DISTINCT item_desc_raw) = 2, total_qty = SUM(qty) = 198. The list card (legacy_frontend/app/[company]/transfer/page.tsx:806-809 → `{t.items_count} Items` / `Qty: {t.total_qty}`) prints "2 Items · Qty: 198" for a 100-box truck. The only column that holds the real 100 is boxes_count, which that card does not render.

**Fix.** Stop deriving the headline quantity from lines. Either report the physical count from the boxes subquery (`bc.boxes_count`) as Qty, or make the two figures dimensionally consistent by aggregating the line subquery per distinct article first, e.g. `SELECT header_id, COUNT(*) AS items_count, SUM(article_qty) AS total_qty FROM (SELECT header_id, item_desc_raw, SUM(qty) AS article_qty FROM interunit_transfers_lines GROUP BY header_id, item_desc_raw) a GROUP BY header_id`, and label it "packs" not "Qty". Add a separate explicit `total_boxes` field.


## pending_items subtracts a DISTINCT box-id count from a pack/unit sum — the exact 'bogus short' math reconcile_transfer_to_order was rewritten to abandon

**services/ims_service/interunit_tools.py:1511** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** total_qty is SUM(interunit_transfers_lines.qty) (packs/units, per-box lines) and boxes_count is COUNT(DISTINCT COALESCE(box_id, id::text)) over interunit_transfer_boxes (physical boxes). The two operands are different units of measure over different tables with different row multiplicities, so the difference has no meaning. pending_stock_tools.py:1738-1742 records that this precise subtraction produced "75 boxes parked vs SUM(qty)=1963 → bogus 1888 short" and replaced it with COUNT(*) over interunit_transfer_boxes; list_transfers never got the same fix. max(0, ...) hides only the negative half of the error.

**Failure scenario.** The same Cold Storage → A185 transfer: total_qty 198, boxes_count 100 → pending_items = 98. A fully dispatched, fully parked 100-box transfer is reported as 98 units outstanding forever, and stays that way after the GRN is completed because neither operand moves on receipt.

**Fix.** Compute the outstanding figure from the in-transit ledger instead: COUNT(*) of pending_transfer_stock rows with status='In Transit' for the header (or boxes_count minus received boxes from interunit_transfer_in_boxes). If a lines-vs-boxes gap is genuinely wanted, compare box counts to box counts, never qty to boxes.


## reconcile_transfer_to_order treats the inflated SUM(qty) as the ordered box count for COLD sources and will pull real cold_stocks rows to 'top up' a phantom shortfall

**services/ims_service/pending_stock_tools.py:1712** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** The warehouse branch (line 1743-1760) deliberately ignores SUM(qty) and uses COUNT(interunit_transfer_boxes) because 'qty is the PACK count, so SUM(qty) is NOT the box count'. The COLD branch does the opposite: it takes SUM(qty) per lot as the ordered box count and, when parked < ordered, actively pulls rows out of <company>_cold_stocks into pending_transfer_stock (`_find_available_cold_by_lot` → `_park_cold_row`, lines 1780-1793). With the per-box line explosion, ordered-per-lot = 2*boxes_in_lot − 1, so reconcile computes a shortfall of boxes_in_lot − 1 for every cold lot and deducts that many extra boxes from cold inventory. This is not a display bug — it moves stock.

**Failure scenario.** Someone runs `POST /interunit/pending-stock/backfill` (interunit_server.py) or `apply_reconcile.py <tid> --confirm`; backfill_pending_from_existing_transfers (pending_stock_tools.py:2834) iterates every in-transit dispatch and calls reconcile_transfer_to_order at line 3046. For TRANS202608171318 (lot A: 60 boxes → 60 lines, one corrected to 60 and 59 left at 1 → ordered=119; parked=60) reconcile pulls 59 more boxes of lot A plus 39 of lot B out of cold_stocks with box_ids `RC-<tid>-<srcid>`, marks them In Transit against a truck that already left, and stamps unallocated_boxes on the header. 98 real boxes vanish from cold available stock. The guard that would have saved it — `if received: skip` at line 1699 — only fires after a GRN has started.

**Fix.** Fix the data (see root-cause finding) AND harden this branch the same way the warehouse branch already is: when the header has box rows, the ordered quantity per lot must come from those boxes, not from SUM(qty). Replace the ordered_rows query for box-backed headers with

  SELECT COALESCE(lot_number,'') AS lot_no, MIN(article) AS item_description, COUNT(*) AS ordered
  FROM interunit_transfer_boxes WHERE header_id = :tid GROUP BY COALESCE(lot_number,'')

and keep the interunit_transfers_lines query only for headers with zero box rows. Until that lands, do not run the backfill/apply_reconcile scripts against cold-source transfers.


## PUT /auth/users/{user_id}/companies and PUT /auth/permissions/... let ANY authenticated user grant themselves admin — vertical privilege escalation

**services/ims_service/server.py:197** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** `verify_token` only proves the JWT is valid; it never checks that the caller is an admin, and `user_id` is taken from the path with no comparison against `user['user_id']`. Same defect at line 170 (`PUT /permissions/{company_code}/{user_id}`) and line 160 (`GET /permissions/...`, IDOR read). Additionally `GET /users` (line 40), `POST /users` (line 45), `PUT /users/{user_id}` (line 63) and `DELETE /users/{email}` (line 94) declare NO `Depends(verify_token)` at all, while every neighbouring route in the same file does — so the user directory can be listed, and accounts created/edited/deleted, with no token.

**Failure scenario.** A warehouse operator logs in normally, reads their own id from `GET /auth/me`, then calls `PUT /auth/users/<own-id>/companies` with `{"companies":[{"company_code":"CFPL","role":"admin"}]}` → 200. They are now admin in CFPL, which also satisfies `_check_delete_permission`'s `user_role in ADMIN_ROLES` branch, unlocking every delete in the transfer module. Separately, `curl https://<host>/auth/users` with no Authorization header dumps the full user list.

**Fix.** Add an admin-role dependency (not just `verify_token`) to all four permission/role-assignment routes and assert `user['user_id'] == user_id` for self-scoped reads; add `Depends(verify_token)` plus an admin check to `GET/POST/PUT/DELETE /auth/users*`.



# HIGH (94)

## loss_statuses filter is accepted by three endpoints and silently ignored

**legacy_backend/services/ims_service/jobwork_dashboard_server.py:155** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** /summary (line 155), /group-details (line 353) and /export-excel (line 549) all declare loss_statuses, but _build_where_clauses (lines 49-102) does not take the parameter and no other code path references it — the value is dropped on the floor. The frontend sends it on all three calls: legacy_frontend/lib/jobworkApiService.ts lines 83, 113 and 138 all do `params.set('loss_statuses', filters.loss_statuses.join(','))`.

**Failure scenario.** User ticks 'Excess Loss' in the filter panel to review problem JWOs. The request goes out as ...&loss_statuses=Excess%20Loss, the backend ignores it, and the table comes back with every JWO including Normal and Pending ones. The filter chip stays lit, so the operator believes they are looking at a filtered set of loss cases.

**Fix.** Compute loss_status in a subquery/CTE and add a HAVING/WHERE on it (or filter the materialized rows in Python before returning), so the three endpoints honour the parameter — or remove it from the signature and the FE so the UI cannot offer a filter that does nothing.


## excess_loss_flags is computed on a different denominator than the loss % shown in the table

**legacy_backend/services/ims_service/jobwork_dashboard_server.py:214** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** has_excess (feeding the excess_loss_flags KPI at line 199) divides by SUM(il.sent_kgs) — the sent quantity typed on the inward receipt lines — while avg_loss_pct (lines 186-194) and the per-JWO loss_status in /group-details (lines 401-422) divide by dispatched.total_net_kgs from jb_materialout_lines. The two bases are different numbers whenever a receipt is partial or the IR's sent_kgs was keyed differently from the dispatch, and only one IR line with sent_kgs=0 for the whole JWO makes has_excess unconditionally false.

**Failure scenario.** JWO dispatched net 1,000 kg. A partial IR records sent_kgs=500, FG 460, waste 0. has_excess = (500-460)/500 = 8% -> false, so the 'Excess Loss Flags' KPI counts 0. The same JWO in /group-details computes (1000-460)/1000 = 54% -> loss_status 'Excess Loss' and is rendered red. Two panels on the same screen disagree about the same order.

**Fix.** Use one denominator everywhere — dispatched.total_net_kgs — for has_excess, avg_loss_pct and loss_status, and fall back to sent_kgs only when the dispatch has no net weight.


## Groups labelled 'Unknown' can never be expanded — group-details filters on the literal string

**legacy_backend/services/ims_service/jobwork_dashboard_server.py:241** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** /summary substitutes the display string 'Unknown' for a NULL/empty group key, but /group-details takes the label back verbatim and applies it as an equality predicate (lines 366-381: `where_clause += " AND h.to_party = :grp_label"` with params['grp_label'] = group_label). 'Unknown' is truthy, so the filter IS applied and matches no row — the NULL rows it was meant to represent are unreachable. Same for item / process_type / month groupings.

**Failure scenario.** 12 JWOs were imported without a to_party. The summary table shows a row 'Unknown — 12 JWOs — 4,300 kg dispatched'. Clicking to expand fires GET /jobwork/dashboard/group-details?group_by=vendor&group_label=Unknown, which runs `h.to_party = 'Unknown'` and returns []. The UI renders 'no records' under a row that claims 12, and those 4,300 kg are invisible to drill-down and to the Excel export.

**Fix.** Send a sentinel the backend understands (e.g. group_key null / __NULL__) or translate it back: `if group_label == 'Unknown': where_clause += " AND (h.to_party IS NULL OR h.to_party = '')"`.


## KPI avg_loss_pct is an unweighted mean of group percentages with non-positive groups dropped

**legacy_backend/services/ims_service/jobwork_dashboard_server.py:268** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** Each summary row's avg_loss_pct is already a ratio of that group's totals; averaging those ratios across groups weights a 20 kg vendor the same as a 10,000 kg vendor. The `> 0` guard additionally excludes every group with zero or negative loss (over-receipt), so the denominator only counts loss-making groups — the average is biased upward twice. The KPI is also exported to Excel unchanged (line 582).

**Failure scenario.** group_by=vendor. Vendor A: 10,000 kg dispatched, 200 kg unaccounted (2.0%). Vendor B: 20 kg dispatched, 12 kg unaccounted (60%). Vendor C: 5,000 kg with a small over-receipt (-0.5%). True overall loss = (200+12-25)/15020 = 1.24%. The KPI card shows (2.0+60)/2 = 31.0% — a 25x overstatement that will trigger a false loss investigation.

**Fix.** Compute the KPI from the totals, not from the per-group ratios: `(sum(total_dispatched) - sum(fg) - sum(waste) - sum(rejection)) / sum(total_dispatched) * 100`, including groups with zero/negative loss.


## /all-data attaches header-level box_count to every line row; the client sums it, multiplying total boxes by lines-per-transfer

**legacy_backend/services/ims_service/transfer_dashboard_server.py:90** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** records is the header x lines fan-out (INNER JOIN at line 58), so a transfer with N line rows yields N records, each stamped with the SAME per-header box_count. The identical duplication is applied to issue_count / issue_weight / issue_details at lines 144-150. The consumer aggregates by summing across records: legacy_frontend/lib/transfer/buildSummary.ts:69 `const sumBoxes = (rs) => rs.reduce((s, r) => s + (r.box_count || 0), 0)`, used for total_boxes on every group and item node (lines 112, 126) and as a sort key. Note the FE correctly de-dupes transfer counts with `new Set(...transfer_id).size` (line 70) — boxes and issue weights were never given the same treatment because the backend flattened them.

**Failure scenario.** Transfer 4821 (12 physical boxes) has 3 line items. /all-data returns 3 records each with box_count=12. The Transfer Summary tree reports 36 boxes for that transfer, and the 'Total Boxes' KPI over a month with ~2.5 lines per transfer is inflated ~2.5x. Same for issue_weight, which is summed into per-group totals.

**Fix.** Either return box_count/issue_* only on the first record of each transfer (0 elsewhere), or return a separate per-transfer map/array in the payload so the client aggregates header metrics by transfer_id rather than by row.


## received_status is decided by whichever transfer-in row the DB happens to return last

**legacy_backend/services/ims_service/transfer_dashboard_server.py:97** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: plausible

**Problem.** Neither query has an ORDER BY or an aggregate, yet both assign into the same key — last row wins. A transfer_out_id with multiple transfer-in headers (partial receipts are a first-class flow in this module) resolves to an arbitrary one, and Postgres row order is not stable across vacuum/plan changes. The cold loop then unconditionally overwrites the interunit status, so a *draft* cold receipt beats a finalized interunit one, which is the opposite of the stated intent in the comment at lines 93-96 ('let the cold status win — otherwise a completed cold receive would read as Not Received'). r.status is also passed through unguarded, so a NULL status becomes JSON null for a field the UI treats as a string.

**Failure scenario.** Transfer 5210 is received in two GRNs: header A status 'Received', header B status 'Partial'. Depending on physical row order the dashboard shows either 'Received' or 'Partial' for the same transfer, and the value can flip after an unrelated UPDATE reorders the heap. If a cold receive was started and abandoned (status 'Draft') on an already-received transfer, the dashboard reports 'Draft'.

**Fix.** Aggregate deterministically per transfer_out_id — e.g. rank the statuses (Received > Partial > Draft) with DISTINCT ON / MAX over a CASE, union both sources in one SQL statement, and COALESCE the result to 'Not Received'.


## /all-data returns the entire line-level dataset with no pagination and reports `total` as a row count, not a transfer count

**legacy_backend/services/ims_service/transfer_dashboard_server.py:152** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: plausible

**Problem.** The endpoint has no page/per_page/limit at all: it materializes every interunit_transfers_header x interunit_transfers_lines row ever created (line 62 `.fetchall()`), then issues three more unbounded full-table queries — box counts for ALL headers (line 84), ALL interunit_transfer_in_header rows plus ALL cold_transfer_in_headers rows (lines 98-107), and every issue box in interunit_transfer_in_boxes (line 112) — and builds four Python dicts over them. Response size and DB memory grow linearly and forever. Separately, `total` is len(records) = number of LINE rows, while the field sits next to `records` in a payload the UI presents as 'transfer records'.

**Failure scenario.** With 30k transfers averaging 3 lines, one dashboard open sends ~90k JSON objects (each ~20 fields plus a nested issue_details array) in a single response; the FastAPI worker holds all four dicts plus the serialized body in memory, and the browser must parse it before any paint. Any UI element bound to `total` reports 90,000 'transfers' when there are 30,000.

**Fix.** Add page/per_page (or a date-window default) with a real COUNT over the same WHERE clause, return `total` as COUNT(DISTINCT h.id) alongside a separate row count, and push the box/issue lookups into the paged query as correlated subqueries so they only run for the returned page.


## Cold page: lineBoxDataMap is keyed by transfer-line ids but looked up with pending-stock ids

**legacy_frontend/app/[company]/cold-transfer/coldtransfer-in/page.tsx:302** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: plausible

**Problem.** For cold→cold receipts `lines` is linesFromBoxes, whose id is pending_transfer_stock.id (backend get_pending_boxes_by_transfer_out returns pts.id, interunit_tools.py L3152), while lineBoxDataMap is built from transferData.lines keyed by interunit_transfers_lines.id. Two unrelated id sequences are used as one key space: a lookup either misses (usually) or hits a colliding row from a different article. lineIsScanned drives the Qty/Net/Total columns (L2816-2818), which box_id and transaction_no get printed (L1411-1417), and whether handleGenerateQRs skips the row (L1539). The regular page has the same defect (linesFromBoxes id = interunit_transfer_boxes.id at L142 vs map keyed by line id at L261).

**Failure scenario.** Cold→cold receipt whose pending rows have ids 4101-4110 and whose transfer-out has a line with id 4103 mapped to a PISTA box. Row 3 (pending id 4103) is treated as 'scanned', so its Net Wt column renders the PISTA box's 18.00 kg instead of its own 24.00 kg, Print QR encodes the PISTA box_id/transaction_no onto this carton's sticker, and Generate QR ID's skips the row so it never gets its own id.

**Fix.** Key the map by a source-tagged key (e.g. `${_source}:${id}`) or, for box-derived rows, read the box fields straight off line._box_origin instead of going through lineBoxDataMap.


## Cold-destination alias mismatch makes some transfers un-receivable on BOTH pages

**legacy_frontend/app/[company]/cold-transfer/coldtransfer-in/page.tsx:357** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: plausible

**Problem.** The regular page classifies the destination through normalizeWarehouseName + isColdWarehouse (which maps aliases 'Supreme Cold', 'Rishi Cold Storage', 'savla bond', 'D-39', 'Eskimo Cold' → cold canonical codes, warehouses.ts L76-117/154/190), while the cold page compares the raw string against the literal array COLD_STORAGE_WAREHOUSES = ['Cold Storage','Rishi','Savla D-39','Savla D-514','Supreme','Eskimo'] (L160). Any alias spelling is 'cold' to one page and 'not cold' to the other.

**Failure scenario.** interunit_transfers_header.to_site = 'Supreme Cold' (the value WAREHOUSE_DISPLAY_NAMES itself produces for Supreme). Regular page: normalize → 'Supreme' → isColdWarehouse true → refuses, 'use the Cold Transfer-In page'. Cold page: 'supreme cold' is not in the raw array → refuses, 'use the regular Transfer-In page'. The transfer cannot be received anywhere; both pages call setTransferData(null) so there is not even a form to override.

**Fix.** Use isColdWarehouse(normalizeWarehouseName(x)) in both gates (and for isColdStorageFrom), and delete the local COLD_STORAGE_WAREHOUSES arrays in favour of the shared helper so the two pages can never disagree.


## Cold finalize hardcodes no_of_cartons: 1 while sending the whole line's weight

**legacy_frontend/app/[company]/cold-transfer/coldtransfer-in/page.tsx:2070** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** For warehouse→cold transfers isColdStorageFrom is false, so linesFromBoxes is null and `lines` are the transfer-OUT article lines (L217-220) — one displayed row can represent qty=100 cartons. Each such row is finalized as a single cold box with no_of_cartons: 1 but weight_kg = the line's full net weight. ColdTransferInBoxInput accepts no_of_cartons (cold_transfer_in_tools.py L51), and the unused buildColdStoragePayload at L1941 uses item.totalQty — showing the intended value. Same hardcode on the create path at L2163.

**Failure scenario.** Warehouse W202 → Savla D-39, one line: CASHEW W240, qty 100 cartons, net 2000 kg. park_lines_in_pending created 100 In-Transit rows (pending_stock_tools.py L1510). The cold page shows 1 row, 'Total Boxes 1'. Finalize writes one cold_transfer_inboxes / cfpl_cold_stocks row with no_of_cartons = 1 and weight_kg = 2000 — cold-stock carton counts are understated by 99 for this receipt while the weight is right, so any carton-based report or pile pick is wrong.

**Fix.** Send no_of_cartons = the row's real carton count (line.qty for line-derived rows, 1 for box-derived rows), and split line-derived rows into per-carton entries when the pending ledger holds one row per unit.


## Cold page never sends box_condition / condition_remarks on the finalize path

**legacy_frontend/app/[company]/cold-transfer/coldtransfer-in/page.tsx:2075** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** headerColdDetails (L1956-1970) carries only the cold-storage fields; box_condition and conditionRemarks — collected by the Condition Assessment card at L3266-3290 — are omitted. The backend model ColdTransferInFinalize (cold_transfer_in_tools.py L88-90) has no such fields either, so even sending them would be dropped. The fallback create path (L2175-2176) DOES send them, so the two paths disagree; the pending path is the normal one.

**Failure scenario.** Operator receives a cold transfer, selects Box Condition = 'Damaged' and types 'two cartons crushed in transit', then clicks Confirm Receipt with a pending header present. cold_transfer_in_headers.box_condition stays at the default 'Good' and condition_remarks stays NULL. The cold view page (coldtransfer-in/[transferInId]/page.tsx L210-214) shows a green 'Good' badge, and the damage record is lost.

**Fix.** Add box_condition/condition_remarks to ColdTransferInFinalize (backend) and include them in the finalize payload; until then, disable/hide the Condition Assessment card on the pending path so operators are not misled.


## Cold stock search has no AbortController — an older, slower response overwrites a newer one and the wrong pile gets selected

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:597** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** The 400 ms debounce (line 619-624) only clears a pending TIMER; it does nothing about a request already in flight. `setResults` is unconditional, so whichever response resolves LAST wins regardless of which query it belongs to. There is no AbortController, no request-id/sequence guard, and no `cancelled` flag (unlike the pending-stock effect at line 658 which at least has one). The `catch {}` at line 612 also discards every error — a 500, a network drop or an auth failure all render as the benign 'No results found.' message at line 744, so a broken backend is indistinguishable from empty stock.

**Failure scenario.** Operator types lot '125' (broad query, ~50 piles, slow) then finishes typing '125860' (narrow, 1 pile, fast). The 125860 response arrives first and paints 1 row; the 125 response arrives second and repaints 50 rows while the input still reads '125860'. The operator clicks Select on the top row believing it is lot 125860, and `handleSelect` (line 633) hands `handleSelectColdStorageStock` a different pile's `pile_key`, `box_id` and `cs_company`. pick-boxes then dispatches boxes from the wrong lot — the box_ids are internally consistent, so no guard in the form or the backend catches it.

**Fix.** Keep an incrementing `requestIdRef` (or an AbortController per call) in `doSearch`; on resolution, apply `setResults` only when the response's id still matches the latest. Surface caught errors in a visible error state instead of collapsing them into the empty-results branch.


## Cold stock picker sends no `limit` and has no pagination — backend silently caps at 50 rows PER COMPANY and reports the truncated count as the total

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:606** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** No `limit` is sent, so `/cold-storage/stocks/search` uses its default `limit: int = Query(50, ge=1, le=500)` (cold_storage_server.py:412). Worse, the `LIMIT :limit` (cold_storage_server.py:492) is applied SEPARATELY inside the per-table loop over cfpl_cold_stocks then cdpl_cold_stocks (cold_storage_server.py:501-508), so the cap is 50 piles per company, silently. The endpoint exposes no `page`/`offset` parameter at all and the component renders no pager — there is literally no way to reach row 51. The footer at line 805 prints `Showing {results.length} result{...}` and the API's own `total` is `len(results)` (cold_storage_server.py:538), i.e. the truncated page size, so neither the user nor the code can detect that rows were dropped. The near-identical inner form asks for `limit: "200"` (innercoldtransfer:74), so the two routes return different result sets for the same query.

**Failure scenario.** Operator searches the description field for 'ALMOND' at a site holding 300 cfpl piles. The picker lists the first 50 cfpl piles (ordered `inward_dt ASC NULLS LAST, MIN(id) ASC`) plus up to 50 cdpl piles and the footer confidently states 'Showing 100 results'. The pile the operator actually needs — anything inwarded after the 50th oldest — is invisible and cannot be selected or transferred. The operator concludes the stock does not exist.

**Fix.** Send an explicit `limit` (the endpoint accepts up to 500 via `le=500`) and surface truncation: when `results.length` equals the requested limit, render a 'showing first N of possibly more — narrow your search' warning. Long term, add real `page`/`per_page` to the endpoint and apply LIMIT to the UNION of both tables rather than per-table, and return a genuine COUNT(*) as `total`.


## Edit mode turns unmatched lines into single 'DIRECT' rows carrying multi-unit quantities, so one UI row counts as 1 box but N in the submitted qty

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:1352** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: plausible

**Problem.** Every other row in `scannedBoxes` represents exactly one physical box with `quantityUnits: '1'` (line 1930). Edit-mode `directEntries` break that invariant by putting the full line quantity on a single pseudo-row. On resubmit the `lines` builder at line 2689 reads that multi-unit `quantityUnits` verbatim, while the `boxes` builder at line 2711 filters DIRECT rows out entirely — so this one row contributes N to the qty and 0 to the box count. Every display that counts rows is also wrong: 'Scanned Boxes ({scannedBoxes.length})' at line 3484, 'Total Boxes' at line 3751, and the `handleRemoveBox` decrement at line 2003 which subtracts 1 no matter what `quantityUnits` says. The `manualLines` matcher at line 1325 keys on `transfer_line_id` and `article|lot_number`, so any line whose lot differs only by the 'N/A'/null drift of finding #2 is mis-classified as manual.

**Failure scenario.** Reopen a cold transfer whose 100 boxes came from a blank-lot pile. Boxes carry lot 'N/A'; the stored line carries lot NULL, so `boxedArticleLot.has('ITEM|')` is false and the qty-100 line becomes ONE DIRECT row. The UI shows 101 rows / 'Total Boxes 101'. On Update the FE sends 100 per-box lines (qty 1) + 1 DIRECT line (qty 100) and 100 boxes. `_apply_box_totals` rewrites the box-linked line to 100, leaving 99 stray qty-1 lines plus the qty-100 DIRECT line: `total_qty` = 100 + 99 + 100 = 299 for a 100-box transfer, and pending_items reads 199.

**Fix.** Expand a DIRECT line into `line.quantity` separate rows of `quantityUnits: '1'` at load time (mirroring the create path at line 1893), or tag the row with an explicit `units` field and make every count/aggregate (`scannedBoxes.length`, Total Boxes, remove decrement, `lines` builder) sum `units` instead of counting rows.


## `cs_max_boxes` is set from CARTONS (`SUM(no_of_cartons)`) but used as a BOX-ROW limit for pick-boxes, making the advertised quantity unfillable

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:1541** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: plausible

**Problem.** `record.net_qty_on_cartons` is `SUM(no_of_cartons)` aggregated over every physical row in the pile (cold_storage_server.py:478), i.e. a CARTON total. It is stored as `cs_max_boxes` and rendered as 'Available: N boxes' (line 3309), used as the `<input max>` (line 3295) and as the ceiling in the qty handler (line 3299). But `qty` is then passed to `/cold-storage/stocks/pick-boxes`, which returns individual ROWS with `LIMIT :qty` (cold_storage_server.py:592-593) — one row per box. When any row carries `no_of_cartons > 1` (a real case: pending_stock_tools.py:1302 and :2086 both read `int(getattr(row, "no_of_cartons", 1) or 1)` from the source row), cartons exceed rows and the two numbers diverge. Note also line 1551 stuffs this carton count into `packaging_type`, which is the pack-size field consumed by `calculateNetWeight` (line 1581).

**Failure scenario.** A pile of 99 cold_stocks rows whose no_of_cartons sum to 198. The picker's 'Qty of Cartons' column and the article panel both say 198, and 'Available: 198 boxes' invites the operator to type 198. `pickBoxes({qty: 198})` returns only the 99 existing rows, so the guard at line 1857 fires 'Insufficient Boxes Available. Requested 198 boxes ... but only 99 unique boxes exist' and the add is refused outright. The operator cannot dispatch the pile at all — and has no way to learn that 99 is the real ceiling, because the UI keeps advertising 198.

**Fix.** Have the search return both figures (`net_qty_on_cartons` and a `row_count`/`box_count`) and drive `cs_max_boxes` + the 'Available' label from the box-row count, keeping the carton total as a separate read-only display. Stop writing the carton count into `packaging_type` (line 1551).


## Duplicate-pile guard reads stale `scannedBoxes` before an await, so a double-click adds the same FIFO box_ids twice and the whole submit is rejected

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:1824** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** The guard reads the `scannedBoxes` closure value and the state append happens only after an `await` (line 1839). The 'Add to Articles List' button (line 3344-3351) has no `disabled` / in-flight ref (the `isProcessingRef` at line 956 is only used by the disabled QR handler). Two clicks inside the pickBoxes round-trip both observe the pre-add `scannedBoxes`, both pass `alreadyAdded === false`, and both call pick-boxes with the same `pile_key` — which returns the SAME rows every time because it is a stateless `ORDER BY id ASC LIMIT :qty` (cold_storage_server.py:592-593) with no memory of the draft (acknowledged in the comment at line 1811-1814). The per-add uniqueness check at line 1869 only inspects the current batch, never the accumulated list.

**Failure scenario.** Operator double-clicks 'Add to Articles List' with qty 100. Two pick-boxes calls return the identical 100 box_ids; 200 rows land in `scannedBoxes` and 'Total Boxes' (line 3751) reads 200. On submit the backend's dedupe (cold_transfer_out_tools.py:329-338) raises `400 Duplicate box_id '<id>' for transaction '<txn>'` and the entire transfer is rejected, with no indication of which of the 200 identical-looking rows to delete.

**Fix.** Disable the button while an add is in flight (a per-article `adding` state or ref), and re-check `alreadyAdded` inside the functional `setScannedBoxes(prev => ...)` update so the guard sees the latest state; abort the append if `prev` already contains any of the picked box_ids.


## Picked boxes with a NULL/empty transaction_no are written to the challan but silently skipped by park_in_pending — dispatched stock stays available

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:1903** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: plausible

**Problem.** The form validates `box_id` uniqueness thoroughly (lines 1869-1887) but never validates `transaction_no`. `/cold-storage/stocks/pick-boxes` returns `transaction_no` straight from the row with no COALESCE (cold_storage_server.py:603) and the search itself uses `MIN(transaction_no)` (cold_storage_server.py:487), both nullable — the repo even ships `fix_null_boxid_piles.py` for the sibling NULL-id problem. An empty transaction_no is serialised as `""` at line 2731, the backend still INSERTs the box into `interunit_transfer_boxes` (cold_transfer_out_tools.py:347-368), but `park_in_pending` short-circuits on `if not box_id or not transaction_no ...: continue` (pending_stock_tools.py:1285-1287) — no cold_stocks deduction, no In-Transit row. The response field that would expose this, `boxes_parked` (ColdTransferOutCreateResponse), is never read: line 2838 only reads `response?.challan_no`.

**Failure scenario.** A pile recovered via disposition has 40 rows with NULL transaction_no. Operator picks all 40. The challan prints 40 boxes and the success toast appears. Backend parks 0 of them: the 40 rows remain in cfpl_cold_stocks, so the very next search still shows them as available and a second operator dispatches the same 40 boxes on another challan. Physical stock 40, system stock 80.

**Fix.** Reject the add when any picked box has a falsy `transaction_no` (extend the guard at line 1869 to cover it, with the same 'report this pile to support' message), and after submit compare `response.boxes_parked` against `boxes.length`, surfacing a destructive toast when they differ.


## innercoldtransfer drops the backend line `id`, so removing a SAVED entry does nothing while the UI reports 'Transfer Updated'

**legacy_frontend/app/[company]/cold-transfer/innercoldtransfer/page.tsx:318** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** `GET /cold-storage/inner-transfer/{challan_no}` returns each line's audit `id` (cold_storage_server.py:1178), and the backend exposes `DELETE /cold-storage/inner-transfer/{challan_no}/line/{audit_id}` to reverse one saved line — flipping its cold_stocks rows back to the original lot and location (cold_storage_server.py:714-774). The form throws that id away in favour of a synthetic `edit-${idx}-${Date.now()}`, so it can never call the reverse endpoint. `handleRemoveEntry` (line 462) removes the row from local state only, and `linesToSubmit` (line 479) deliberately excludes `isExisting` rows — so the removal is never communicated to the server. The submit then reports success unconditionally (line 536).

**Failure scenario.** Operator opens ICT202608171420 for editing, sees a wrong saved row (lot 125860 -> 125999 on 40 boxes), clicks the red X, and clicks 'Update Transfer'. The row disappears, the toast says 'Transfer Updated ... 0 record(s) updated', and the operator leaves believing the mistake is fixed. Nothing changed: the 40 boxes are still labelled 125999 in cfpl_cold_stocks and the `inner_cold_transfer` audit row is still there. The lot the operator wanted freed never reappears in Search.

**Fix.** Preserve `line.id` on the TransferEntry (e.g. `auditId: line.id`), and have `handleRemoveEntry` call `DELETE /cold-storage/inner-transfer/{challan}/line/{auditId}` for `isExisting` rows, only removing from local state after the call succeeds. Disable the X on saved rows if the reverse call cannot be wired up.


## innercoldtransfer sends `record.id` as `stock_record_id` although the search unions cfpl+cdpl (whose ids collide) and the backend resolves the table by id alone

**legacy_frontend/app/[company]/cold-transfer/innercoldtransfer/page.tsx:407** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: plausible

**Problem.** `params.company` is sent on the search (line 74) but the endpoint declares it `Optional[str] = Query(None, description="Ignored — always searches cfpl_cold_stocks first, then cdpl_cold_stocks")` (cold_storage_server.py:404) and unconditionally iterates BOTH tables (cold_storage_server.py:501). So the result list mixes companies, each row carrying a `company` tag that this form never reads or displays. The form stores the row's `record.id` — which is `MIN(id)` over the pile group (cold_storage_server.py:469) — and the submit handler posts it with the URL's company. The backend then ignores `payload.company` entirely and resolves the table with `_resolve_record_table(record_id)` (cold_storage_server.py:702-711), which returns the FIRST of cfpl_cold_stocks / cdpl_cold_stocks that contains that id. The sibling cold-transfer form documents this exact hazard at coldtransferform:769-771 ('the search unions cfpl + cdpl, whose id sequences overlap') and defends against it with `pile_key` + `record.company`; this form does neither.

**Failure scenario.** A cdpl pile whose MIN(id) is 41822 is selected from a cdpl row in the results. The form posts `stock_record_id: 41822`. `_resolve_record_table` finds id 41822 in cfpl_cold_stocks first and returns that table, so the lot renumber (`UPDATE cfpl_cold_stocks SET lot_no = :new_lot_no`, cold_storage_server.py:882-889) is applied to an unrelated CFPL item's boxes. Two companies' inventories are corrupted at once: the intended CDPL boxes keep the old lot, and CFPL boxes silently acquire a lot number that belongs to another company.

**Fix.** Send `pile_key` (already returned by the search and typed on `ColdStorageStockRecord`) plus the row's own `record.company` instead of the raw id, and have the endpoint resolve the table from that company rather than by id lookup. As an interim fix, display `record.company` in the results table so the operator can at least see the mix.


## innercoldtransfer never resets the article after 'Add to Transfer List' and has no duplicate guard, so a second click silently double-books the same pile

**legacy_frontend/app/[company]/cold-transfer/innercoldtransfer/page.tsx:433** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** Unlike the cold OUT form, which resets the article to a blank `resetArticle` after adding (coldtransferform:1949-1981) and blocks re-adding the same pile (coldtransferform:1824-1836), this handler leaves every field populated and performs no duplicate check. The button stays enabled with valid data, so a second click appends an identical entry. There is also no cumulative check against `available_boxes` — that ceiling is enforced only per-keystroke on the input (line 704) and never against the sum of entries already on the list. `id: Date.now().toString()` additionally collides as a React key for two adds inside the same millisecond.

**Failure scenario.** Operator selects a 100-carton pile, enters 100 boxes and new lot L2, clicks 'Add to Transfer List', and clicks again (or is unsure the first click registered). The list shows two 100-box rows for the same pile, total 200 against 100 available, with no warning. On submit the backend processes line 1 and relabels all 100 rows to L2, then line 2 re-queries `WHERE lot_no = old_lot` (cold_storage_server.py:830-844), finds 0 cartons and appends 'Line 2: Transfer qty (100) exceeds available (0.0)'. The UI shows 'Partial Success' (line 532) with the entries still on screen, and the operator has no way to tell which half applied.

**Fix.** Reset the article to blank after a successful add (mirror coldtransferform:1949-1981), reject an entry whose `stock_record_id`/pile already appears in `transferEntries`, and validate `sum(existing entries for this pile) + new qty <= available_boxes` before appending. Use a counter or crypto.randomUUID for the entry id.


## Cold Transfer-IN hover fetches the LEGACY transfer-in table using a cold_transfer_in_headers id — shows another GRN's boxes or nothing

**legacy_frontend/app/[company]/cold-transfer/page.tsx:1134** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** The rows come from `InterunitApiService.getColdTransferIns()` → GET /interunit/cold-transfer-in/list, whose `id` is `cold_transfer_in_headers.id` (interunit_tools.py:3350-3390). The hover then calls GET /interunit/transfer-in/{id}, which is `get_transfer_in()` → `SELECT ... FROM interunit_transfer_in_header h WHERE h.id = :tid` (interunit_tools.py:3494-3506). Two different tables with two independent id sequences. Everything else on the row correctly uses the cold endpoints (delete → `deleteColdTransferIn` line 429, view → `/cold-transfer/coldtransfer-in/${ti.id}` line 1233) — only the hover was left on the legacy path.

**Failure scenario.** Cold GRN with cold_transfer_in_headers.id = 12 is hovered. GET /interunit/transfer-in/12 returns the UNRELATED legacy warehouse GRN whose interunit_transfer_in_header.id is 12 → the hover card shows that other receipt's articles, lots, weights, received_by, condition and status under this cold GRN's challan number. If no legacy row 12 exists the endpoint 404s and the card falls back to the single stub line `Transfer: <transfer_out_no>` (line 1136), so cold receipts never show their real items.

**Fix.** Call `/interunit/cold-transfer-in/${ti.id}` (`getColdTransferInById`, already in InterunitApiService) and map its `cold_transfer_inboxes` rows (`item_description`, `lot_no`, `box_id`, `unit`) instead of the legacy `article`/`lot_number` box shape.


## 'Total Qty' is computed over unfiltered lines by the caller, so it doesn't match the Qty column on the challan it labels

**legacy_frontend/app/[company]/transfer/dc/[transferId]/page.tsx:73** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** DeliveryChallan deliberately drops phantom lines with a blank/'N/A' description (lines 70-76, comment: 'don't inflate the box / item / count totals'), and every other total on the document is computed from the filtered `consolidatedItems`. But 'Total Qty' comes from this prop, which sums EVERY line the API returned, including the ones the component just filtered out. `transferData.total_qty_required` is also a field the backend never returns (`get_transfer` / `_map_transfer_header` produce no such key), so the reduce always runs. Backend `_map_transfer_line` returns `item_description: row.item_desc_raw or ""`, so blank descriptions do reach the frontend.

**Failure scenario.** Transfer with 3 real lines (qty 10 + 6 + 4 = 20) plus one phantom line with a blank description and qty 6. The DC prints rows summing to 20 in the Qty column, TOTAL (3 items), but the totals cell and the Gate Pass read 'Total Qty: 26'.

**Fix.** Let DeliveryChallan compute Total Qty from `consolidatedItems` (it already computes boxes and net weight that way) and delete the prop, or apply the same phantom-line filter in the caller before reducing.


## Gate Pass always prints COMPLETE - boxesPending is hardcoded to 0

**legacy_frontend/app/[company]/transfer/dc/[transferId]/page.tsx:75** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** DeliveryChallan renders the consignment status badge purely from this prop (line 546-549: `boxesPending > 0 ? 'PARTIAL' : 'COMPLETE'`). The DC page passes a literal 0, so the status cell is a constant. The information needed is available on the payload the page already fetched - the header `status` can be 'Partial', and `grn_records[].received_boxes` plus the box count give the real shortfall.

**Failure scenario.** Transfer 1615 dispatched 40 of 100 ordered boxes (header status 'Partial', unallocated_boxes 60). The printed Gate Pass shows a green 'COMPLETE'; security passes the truck as a full consignment and the shortfall is only discovered at the receiving GRN.

**Fix.** Derive pending from the payload (`status === 'Partial'`, or ordered qty minus boxes.length) and pass it through; also render the shortfall count, not just a colour.


## directtransferform emits one line per box including DIRECT entries while boxes[] excludes them — backend deletes the manual lines for any article that was also scanned

**legacy_frontend/app/[company]/transfer/directtransferform/page.tsx:1945** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** lines[] carries one row per scanned/added box (so the same article appears N times), while boxes[] carries only the QR-scanned subset. On the server, _boxes_authoritative groups the payload boxes by article, then collects EVERY line whose item_desc_raw matches that article, keeps the first, sets its qty to the QR-box count and net/total to the summed QR-box weights, and DELETES the rest (legacy_backend/services/ims_service/interunit_tools.py:980-1001). Manually-added lines for an article that was also scanned are therefore destroyed, and because the surviving line is 'covered' by boxes (_uncovered_lines, interunit_tools.py:1081-1088), park_lines_in_pending never runs for them either.

**Failure scenario.** Direct transfer of ALMOND WHOLE: operator scans 3 boxes (18 kg each) and, for 2 loose boxes without labels, uses 'Add to Articles List' (10 kg each). Payload: 5 lines + 3 boxes. Server keeps line #1 with qty=3, net=54.000 and deletes the other 4 lines. The transfer records 3 boxes / 54 kg instead of 5 boxes / 74 kg, and the 20 kg of manual stock is never parked in pending_transfer_stock.

**Fix.** Group lines by article before sending (one line per item_description with qty = box count and summed weights), or keep the DIRECT boxes in boxes[] with blank transaction_no so the backend treats them as article-entry units.


## Cold-storage stock search is capped at the backend's default limit=50 with no pagination and no truncation signal

**legacy_frontend/app/[company]/transfer/job-work/material-out/page.tsx:154** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** GET /cold-storage/stocks/search declares `limit: int = Query(50, ge=1, le=500)` (cold_storage_server.py:412). The caller sends no limit, so at most 50 piles come back. The service's typed response includes `total`, but the component destructures only `results` and the footer prints results.length as if it were the complete set. There is no Load-more/pagination control.

**Failure scenario.** Operator searches by group name "DATES" from the Cold storage warehouse; 380 piles match. The table lists 50 rows and states "Showing 50 results". The lot the operator needs (a later inward date) is not in the list, and nothing indicates more exist — so they either dispatch the wrong pile or conclude the stock is missing.

**Fix.** Pass limit explicitly (e.g. 500, the server max) and render data.total: `Showing {results.length} of {data.total}` plus a warning/paging control when results.length < total.


## Inward Receipt list: search / type / sort run only over the current 500-row server page

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:686** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** miRecords holds one server page (per_page=500) and the search box, type filter and sort operate on that slice only, while the Prev/Next footer pages the server independently (total_pages = ceil(total/500) from the backend). Typing in the search box does not re-query and does not reset miRecordsPage, so the "{miFilteredRecords.length} shown" counter and the sort order describe only the current 500 rows. Sorting by "Oldest first" or "FG high→low" reorders a page, not the dataset.

**Failure scenario.** With 640 inward receipts, receipt IR-2026-0007 is row 570 (page 2). On page 1 the user types "IR-2026-0007" → "0 shown" and an empty table, with no hint that another page exists. Choosing "FG (high→low)" on page 1 shows the largest FG of the newest 500 receipts, not of the 640.

**Fix.** Send the search term / receipt_type / sort to /job-work/material-in/list as query params and reset miRecordsPage to 1 whenever any of them changes; or fetch all pages before filtering client-side.


## Summary drill-down and the Jobwork dashboard silently truncate at the backend's 1000-record cap and never refetch

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:1265** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** /job-work/list caps per_page at 1000 (job_work_server.py:818 `per_page: int = Query(15, ge=1, le=1000)`) and orders by created_at DESC. Once there are more than 1000 job-work headers, every tree view (Drill-down, By Group, By Vendor, By Process, By Item) and the Monthly-Trend hover detail are computed over only the newest 1000 records, while the KPI cards on the same screen come from /job-work/reports/dashboard which aggregates the whole table. `data.total`/`total_pages` are discarded, so nothing signals the truncation. The `rptAllRecords.length > 0` guard also means the set is fetched once per mount: after submitting an inward receipt (handleSubmitMaterialIn calls loadRecords(1) and loadMiRecords(1) but not a tree refresh) the Summary tab keeps showing pre-receipt FG numbers. The identical pattern exists in the standalone dashboard at jobwork/dashboard/page.tsx:85 (`/job-work/list?per_page=1000`), where the header even prints "{filtered.length} of {jwoRows.length} JWOs" — a count capped at 1000.

**Failure scenario.** With 1,450 headers, the KPI card shows "Total JWOs 1,450 / Dispatched 812,000 kg" while the drill-down beneath it sums to ~1,000 JWOs and ~560,000 kg; months older than the newest 1000 records show a bar (server data) with an empty hover (client data) reporting FG 0 kg and 0 vendors.

**Fix.** Page through /job-work/list (loop until records.length >= total) or add a dedicated non-paginated aggregate endpoint for the tree; surface data.total vs loaded count in the UI; and reset rptAllRecords after any material-in/material-out mutation.


## Jobwork dashboard: unknown/blank process types are silently relabelled "Cracking"

**legacy_frontend/app/[company]/transfer/jobwork/dashboard/page.tsx:93** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** Any sub_category that is not an exact case-insensitive match of the six hard-coded names is coerced to "Cracking" rather than to an "Unknown"/passthrough bucket. The values actually written to h.sub_category come from material-out's subCatOptions = ["De seeding", "Dicing", "Cracking", "Stuffing", "Vacuum Packaging", "Slicing"] (material-out/page.tsx:488) plus free-text "Other" values — so "De seeding" (with a space), "Vacuum Packaging", and every blank sub_category all become "Cracking".

**Failure scenario.** 200 deseeding JWOs (sub_category "De seeding") and 20 real cracking JWOs. Group by Process shows Cracking = 220 JWOs with the deseeding tonnage folded in, Deseeding = 0; the Deseeding chip is never selectable; per-process avg loss % for cracking is computed over deseeding data and drives the "Excess Loss" flags.

**Fix.** Return the raw trimmed value (or "Unknown") when there is no match, and build the ProcessType chip list from the data instead of a hard-coded tuple; align the vocabulary with material-out's subCatOptions.


## Jobwork dashboard: available-option useMemos omit jwoRows, so every filter chip is permanently disabled

**legacy_frontend/app/[company]/transfer/jobwork/dashboard/page.tsx:205** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** availableVendors / availableItems / availableProcess / availableStatuses / availableLoss all read jwoRows but do not list it as a dependency. On mount jwoRows is [] so each memo evaluates to []. When the fetch resolves and setJwoRows re-renders, the dependency arrays are unchanged, so React returns the cached empty arrays. Every Chip therefore receives available={false} and, being inactive, renders disabled.

**Failure scenario.** Open /cdpl/transfer/jobwork/dashboard. Rows load and the table populates, but every Vendor / Item / Process / Status / Loss chip is greyed out and unclickable — the entire filter panel is dead until some other filter changes state (impossible, since all chips are disabled). Only the free-text search and date inputs work.

**Fix.** Add jwoRows to each dependency array (the same fix for all five memos).


## Jobwork dashboard: JWO receipt expansion is a fake 200 ms stub that always yields an empty list

**legacy_frontend/app/[company]/transfer/jobwork/dashboard/page.tsx:340** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** toggleJWO never calls an API. It sleeps 200 ms and stores an empty array, so the render at line 740 always falls through to "No inward receipts recorded". A working endpoint exists (JobworkApiService.getJWOReceipts → GET /jobwork/dashboard/jwo-receipts/{header_id}, backend jobwork_dashboard_server.py:473) and jwo.id is exactly the header_id it expects — it is simply never invoked.

**Failure scenario.** A JWO with 3 inward receipts and jwo_status "Fully Received" is expanded: the spinner runs for 200 ms then prints "No inward receipts recorded", contradicting the FG Received column on the same row.

**Fix.** Call JobworkApiService.getJWOReceipts(company, id) and store the result. Note the response contract differs from the InwardReceipt type: the backend returns receipt_type as "partial"/"final" (lowercase) while line 725 compares against "Final", and it returns no `remarks` field although line 732 renders ir.remarks — normalize both when wiring it up.


## Any active filter silently truncates the dataset to the newest 500 records and hides the pager, so search/warehouse filters miss older records entirely

**legacy_frontend/app/[company]/transfer/page.tsx:84** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** In filter mode the FE requests exactly ONE page of 500 rows (server caps per_page at 1000, interunit_server.py:271) and forces total_pages to 1, so the pagination bar is hidden (`tp > 1` is false, line 414). Everything past row 500 in `created_ts DESC` order is unreachable while ANY filter is on — and the filter is on by default for most users: `getUserDefaultWarehouses(user.name)` sets `warehouseFilter` to a specific code or `my_warehouses` on mount (lines 42-52), so `transferOutFilterActive`/`requestsFilterActive` are permanently true for them. There is no "results truncated" indicator anywhere.

**Failure scenario.** `interunit_transfers_header` holds 452 rows today (_schema_dump.json:995) — already 90% of the cap. Once it passes 500, a warehouse-manager whose default is A185 opens Transfer Out, the page fetches the newest 500 headers, filters them to A185 client-side, and every A185 challan older than the 500th newest transfer is invisible with no pager to reach it. Searching a challan number from three months ago returns "No matching records" even though the record exists.

**Fix.** Send the filter to the server (`from_site`/`to_site` params already exist on GET /interunit/transfers and `challan_no` is supported) and keep server pagination with the server's `total`/`total_pages`; or, if bulk-fetch must stay, page through until `all.length >= total` the way cold-transfer/page.tsx:165-192 `loadColdOut` does, and paginate the filtered array client-side.


## Three concurrent unguarded loadTransfers/loadRequests calls fire on mount with different per_page values — no AbortController, no sequence guard, slowest response wins

**legacy_frontend/app/[company]/transfer/page.tsx:114** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** On first paint the [activeTab] effect and the [filterActive, warehouseFilter] effect both fire loadTransfers(1) (activeTab defaults to "transferout", :37), and loadRequests(1) fires twice (:157 and :167). Then the auth effect (:42-52) sets warehouseFilter to the user's default, flipping transferOutFilterActive true and firing a THIRD loadTransfers — this one with per_page=500 instead of 15. None of these are cancelled or sequence-tagged, and every one unconditionally calls setTransfers/setTransfersTotalPages/setTransfersTotal. Whichever HTTP response lands last defines the state.

**Failure scenario.** User 'Sumit Baikar' (default A185, :136 of warehouses.ts) opens the page. Request A: per_page=15, warehouse=all. Request B: identical duplicate. Request C: per_page=500 (the filtered fetch). C is a 500-row query and typically returns slowest, but under load A or B can land last — then `transfers` holds 15 unfiltered rows while warehouseFilter is "A185", `transfersTotalPages` is set to the server value (e.g. 228) even though the UI is in filter mode, and the A185 view shows 1-2 rows with a pager that no longer matches. The result differs run to run, which is why it reads as a flaky bug.

**Fix.** Give each loader a monotonically increasing request id (or an AbortController stored in a ref) and ignore/abort stale responses: `const seq = ++reqRef.current; const response = await ...; if (seq !== reqRef.current) return;`. Also collapse the duplicate mount effects — drop the `[]` effect at :157 and the `transfers.length === 0` effect at :159 once activeTab is in the filter effect's deps.


## Filter mode hard-caps the fetch at 500 records and forces total_pages=1 — every match beyond the newest 500 is silently dropped with the pagination bar hidden

**legacy_frontend/app/[company]/transfer/page.tsx:119** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: unverified

**Problem.** As soon as any filter is active (search text OR warehouse != all — :88), the client fetches only page 1 of 500 and pins total_pages to 1, which makes PaginationBar render nothing (:414). Everything older than the 500th transfer is unreachable while filtering, and there is no indicator that the result set was truncated. The header still prints the unfiltered server total (:705 `{transfersTotal} record{...}`), so the UI simultaneously claims thousands of records and offers no way to reach them. Identical pattern for requests (:99) and transfer-ins (:142), and in cold-transfer/page.tsx:201-206 for cold GRNs.

**Failure scenario.** interunit_transfers_header holds 4,000 rows; 900 of them are A68. User with A68 as their default warehouse (warehouseFilter auto-set to "A68" at :47) opens the page: only the 500 newest transfers are fetched, of which maybe 90 are A68. The list shows 90 rows, the header says "4000 records", and there is no Next button. The other ~810 A68 transfers are invisible — searching a challan number from three months ago returns "No matching records" even though the record exists.

**Fix.** Push the filters to the server (the endpoint already accepts from_site/to_site/status/challan_no — interunit_tools.py:1416-1444) and keep real server pagination, or at minimum keep paginating in filter mode: page through until `all.length >= response.total` (as cold-transfer's loadColdOut attempts at :165-192) and set the header count to the post-filter row count instead of `response.total`.


## Transfer-out refetch effect omits `activeTab` from its deps and early-returns — filter changes made on another tab never refetch, leaving the list filtering a stale 15-row page

**legacy_frontend/app/[company]/transfer/page.tsx:172** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** When warehouseFilter changes while the user is on a different tab, the effect body returns early and no fetch is queued. Switching back to Transfer Out does not repair it, because the [activeTab] effect at :159 only fetches when `transfers.length === 0`. The list then client-filters (`filteredTransfers`, :245) the stale 15-row unfiltered page-1 payload instead of the 500-row filtered payload the filter mode requires. Same defect at :178 (transferIns) and at cold-transfer/page.tsx:257 (details tab).

**Failure scenario.** Open /cfpl/transfer (Transfer Out loads 15 newest transfers, warehouse=all). Click the Transfer In tab. Set the warehouse dropdown to A185. Click back to Transfer Out: no refetch fires, so the page filters the 15 newest transfers by A185. If none of the 15 newest happen to be A185, the user sees the "No matching records" empty state while the stat card above still reads "Transfers Out: 3,412" and hundreds of A185 transfers exist.

**Fix.** Add `activeTab` to the dep array so the fetch fires on tab entry with the current filter, and drop the early return (or keep it but re-run on tab change): `}, [transferOutFilterActive, warehouseFilter, activeTab])`. Do the same at :178 and cold-transfer/page.tsx:257/267.


## Transfer Out list client-filters the already server-paginated 15-row page: rows silently disappear and "Showing 1-15 of N" lies

**legacy_frontend/app/[company]/transfer/page.tsx:245** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** When no filter is active the fetch is server-paginated at per_page=15 (line 120) but `isPureColdTransfer` still removes cold→cold rows client-side from that 15-row slice. `transfersTotal` / `transfersTotalPages` come from the server COUNT over the UNFILTERED table (interunit_tools.py:1454-1457, whose WHERE clause knows nothing about the cold→cold exclusion), so page size, page count and the "Showing X-Y of N" text are all computed against a different population than the rows on screen.

**Failure scenario.** 452 headers, of which 120 are cold→cold. Page 1 fetches 15 rows, 6 of them cold→cold → 9 rows render, footer says "Showing 1-15 of 452", pager says "1 / 31". Pages later in the list may render 15, 12 or 3 rows at random, and the last pages can render zero rows while Next is still enabled. A user counting records can never reconcile the header ("452 records", line 705) with what they see.

**Fix.** Push the exclusion server-side (add a `exclude_cold_to_cold` / `from_site`+`to_site` filter to GET /interunit/transfers so COUNT and rows share one WHERE), or drop server pagination for this tab and paginate the client-filtered array the way cold-transfer/page.tsx:369-376 does (`coldOutClientTotal` / `pagedColdOut`).


## Cold and cold-destination rows are filtered out of an already server-paginated 15-row page — short pages, empty pages, and counts that never match the rows

**legacy_frontend/app/[company]/transfer/page.tsx:245** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: unverified

**Problem.** The server returns 15 rows per page and counts ALL headers in `total` (interunit_tools.py:1454-1457 counts with the same WHERE, which has no cold exclusion). The client then removes the cold->cold rows and the cold-destination GRNs from that already-truncated slice. Page size becomes non-deterministic, `total`/`total_pages` no longer describe what is displayed, and a page whose 15 rows are all cold->cold renders the empty state while the pager says there are more pages.

**Failure scenario.** Cold->cold transfers are created in bulk on a single day. Page 4 of Transfer Out fetches rows 46-60, all of which are Cold Storage -> Savla D-39 pairs. `filteredTransfers` is empty, so the table is replaced by the "No outbound transfers" empty state, while PaginationBar (:935) still shows "Showing 46-60 of 3412 · 4 / 228" and Next remains enabled. Pages 3 and 5 show 12 and 9 rows respectively. Users conclude records were deleted.

**Fix.** Move the cold exclusion server-side (add an `exclude_cold` / `source_class` query param to the list endpoint so the COUNT and the LIMIT/OFFSET agree), or adopt the cold-transfer page's approach: bulk-fetch, filter, then paginate the filtered array client-side and report the filtered length (cold-transfer/page.tsx:370-376).


## "Qty: NNN" badge renders ordered line qty (total_qty) while the column is labelled "Items/Boxes"; the real box count (boxes_count) is returned by the API and ignored

**legacy_frontend/app/[company]/transfer/page.tsx:899** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** The FE does NO client-side summing here — it prints two scalars straight off the list payload. Backend (interunit_tools.py:1472-1484, 1508-1512) computes them from `interunit_transfers_lines` only:\n  items_count = COUNT(DISTINCT item_desc_raw)\n  total_qty   = COALESCE(SUM(qty), 0)\n  boxes_count = COUNT(DISTINCT COALESCE(box_id, id::text)) FROM interunit_transfer_boxes  ← never rendered\nSo "Qty:" is the ORDERED/typed line quantity, not the physical boxes, under a column header that promises "Items/Boxes". For a cold-source dispatch the two diverge by design: create_cold_transfer_out only rewrites a line's qty when boxes actually map to it (cold_transfer_out_tools.py:147-175 `_apply_box_totals` iterates only `totals_by_line(box_assignments)`), and box→line matching is an exact `(item_description.strip(), lot_no.strip())` key (cold_transfer_out_tools.py:340-344) with everything unmatched dumped onto `fallback_line_id` (the FIRST line). A line that receives no boxes keeps whatever the operator typed. `_apply_boxes_authoritative` in the warehouse path is explicitly disabled for cold sources too (interunit_tools.py:965 `if not payload_boxes or _is_cold_site(from_site): return lines`).

**Failure scenario.** The reported Cold Storage → A185 case: 2 lines (2 distinct item_desc_raw → "2 items"), 100 physical boxes. Boxes all key to line 1 (lot string on the boxes doesn't match the line's lot_number, e.g. box lot_no 'CF100326 ' vs line lot_number 'CF100326'), so line1.qty := 100 and line2 keeps its typed 98 → SUM(qty)=198. The list prints "2 Items / Qty: 198" while `boxes_count` in the very same JSON row is 100 and `pending_items` (line 1511) is 98. The operator has no way to see 100 anywhere in the list.

**Fix.** Render the box count in the "Items/Boxes" column — `<Badge>{t.boxes_count} Boxes</Badge>` — and either drop "Qty:" or relabel it "Ordered: {t.total_qty}" and flag the mismatch when `t.pending_items > 0` (the backend already computes it). Apply the same change to the mobile card at lines 806-810 and to cold-transfer/page.tsx:778-782 / 872-873.


## Transfer Out row shows unit-quantity under a column headed "Items/Boxes" while the boxes_count the API returns in the same row is never rendered

**legacy_frontend/app/[company]/transfer/page.tsx:899** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** GET /interunit/transfers returns THREE per-row aggregates: items_count = COUNT(DISTINCT item_desc_raw), total_qty = SUM(lines.qty) (units), and boxes_count = COUNT(DISTINCT COALESCE(box_id, id::text)) over interunit_transfer_boxes — interunit_tools.py:1472-1474, 1478-1490, 1508-1510. The UI renders items_count and total_qty and silently drops boxes_count, even though the column is labelled "Items/Boxes" and operators read it as a box count. total_qty is a UNITS sum whose per-line qty is written one-line-per-box from the scanned box's `quantity_units` (coldtransferform/page.tsx:2684, :2689, :2801) — and '1' for picker-added boxes (:1930) — so it equals the box count only by accident. grep across legacy_frontend confirms `boxes_count` is read nowhere in the transfer module.

**Failure scenario.** Challan TRANS202608171318, Cold Storage -> A185, 100 physical boxes across 2 articles. Backend row = {items_count: 2, boxes_count: 100, total_qty: 198}. The list renders "2 Items" + "Qty: 198" under the header "Items/Boxes"; the warehouse team reads 198 boxes dispatched and reconciles against 100 received. The correct 100 was in the payload the whole time as boxes_count and was thrown away. The DC for the same challan prints both numbers on one sheet — "Total Qty: 198" (DeliveryChallan.tsx:539) beside "Total Boxes: 100" (DeliveryChallan.tsx:540).

**Fix.** Render the field that matches the header: `<Badge>{t.boxes_count ?? 0} Box{t.boxes_count !== 1 ? 'es' : ''}</Badge>` and either drop the Qty badge or relabel it explicitly ("Units: {t.total_qty}"). Apply to all four sites: transfer/page.tsx:806/809 and :898/899, cold-transfer/page.tsx:777-782 and :872-873. Separately, fix the writer so cold lines carry qty = 1 per box (coldtransferform/page.tsx:2689) rather than the source box's quantity_units.


## ReferenceError: discrepanciesMap is not defined in the mobile Transfer-IN hover — the silent catch turns every mobile hover into "No item details available"

**legacy_frontend/app/[company]/transfer/page.tsx:1043** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** `discrepanciesMap` is never declared in the mobile `fetchLines` closure (it is a `const` inside the desktop closure at line 1167, a different function scope), so line 1043 throws `ReferenceError: discrepanciesMap is not defined` at runtime. TypeScript would reject this as "Cannot find name", but next.config.mjs sets `typescript.ignoreBuildErrors: true`, so it ships. ChallanHoverCard swallows it: `try { const result = await fetchLines() } catch { setFetched([]) }` (ChallanHoverCard.tsx:95-99), so the user gets no error — just an empty card. Identical bug at cold-transfer/page.tsx:1017 (declaration only at 1141).

**Failure scenario.** On a phone/tablet (the `md:hidden` card list is the only list rendered below 768px), a warehouse operator taps a GRN number to see what is inside it. `fetchLines` throws immediately after building the item list, the catch sets `fetched = []`, and the card renders "No item details available" — for EVERY Transfer-IN row, permanently. Desktop works, so the bug is invisible to anyone testing on a laptop.

**Fix.** Declare `discrepanciesMap` and populate it in the mobile closure exactly as the desktop one does (lines 1167, 1177-1196), or extract the shared box-grouping logic into one module-level helper used by both closures. Also stop swallowing the error in ChallanHoverCard.open — log it and surface a retry state.


## Transfer IN hover card on mobile references an undefined `discrepanciesMap` — throws ReferenceError, hover silently shows "No item details available" forever

**legacy_frontend/app/[company]/transfer/page.tsx:1043** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** `discrepanciesMap` is declared only inside the DESKTOP fetchLines closure at :1167 and is not in the component scope. The mobile Transfer IN hover therefore throws `ReferenceError: discrepanciesMap is not defined` at :1043, after the boxes have already been fetched and grouped. ChallanHoverCard.open() swallows it: `catch { setFetched([]) }` (ChallanHoverCard.tsx:99), and because it then sets `fetched` to `[]` (no longer null) the `fetched === null` guard at :92 blocks every retry for the life of the component. The identical bug exists at cold-transfer/page.tsx:1017.

**Failure scenario.** On a phone/tablet (md breakpoint), open Transfer In tab and tap GRN e.g. GRN2026081701. The card opens, shows the spinner, then permanently renders "No item details available" — no error toast, nothing in the UI. Re-hovering never retries. Desktop shows the full item list for the same GRN, so the bug is invisible to anyone testing on a laptop. Receivers on the warehouse floor (mobile) can never see scanned box lines or discrepancies.

**Fix.** Move the discrepanciesMap declaration + population loop into the mobile closure (copy lines 1141, 1151-1170 from the desktop version), or extract the whole box-grouping routine into one shared helper used by both breakpoints. Also change ChallanHoverCard.tsx:99 to `catch { setFetched(null); setError(true) }` so a transient failure can be retried and is visible.


## Gross weight recorded as 0: `boxWt.gross || boxWt.net` treats the string "0" as present

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:406** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** The backend serialises transfer-out box weights as STRINGS with a "0" default for NULL (interunit_tools.py _map_box_row L589-590: `str(row.net_weight) if row.net_weight is not None else "0"`). The non-empty string "0" is truthy, so `boxWt.gross || boxWt.net` never falls back to net — total_weight becomes "0". lineWeights then feeds every acknowledge payload (`w.total_weight ? Number(w.total_weight) : ...`, L738/971/1045/1791), sending gross_weight: 0. The same expression on the cold page (L486) instead yields the literal string "null" because pending boxes return null/float (interunit_tools.py get_pending_boxes_by_transfer_out L3159-3160).

**Failure scenario.** Transfer-out box with net_weight 24.000 and gross_weight NULL. Search the transfer: Total Wt column shows 0, and Confirm Receipt stores gross_weight = 0 on interunit_transfer_in_boxes for that carton — the receipt's Gross Weight total drops by the full carton weight, and the printed label reads 'Gross: 0kg'. On the cold page the same box renders the text 'null' in the Net/Total Wt cells and posts net_weight: null.

**Fix.** Coerce first and test numerically: `const g = Number(boxWt?.gross); const n = Number(boxWt?.net); total_weight = Number.isFinite(g) && g > 0 ? String(g) : (Number.isFinite(n) ? String(n) : "")`. Never rely on string truthiness for numeric API fields.


## Un-acknowledge sends a different box_id than acknowledge did — 404, row cannot be undone

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:798** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** handleAcknowledgeLine (L729) puts scannedLineData[i].box_id first; handleUnacknowledgeLine omits it entirely. The backend DELETE /transfer-in/{h}/acknowledge/{box_id} raises 404 'Box {id} not found in this transfer-in' when the id does not match (interunit_tools.py L2689-2690), and the FE only clears local state on success. Cold page identical at L947.

**Failure scenario.** Carton physically labelled 88881234-07 is scanned against DB box 88881234-03 → acknowledged as 88881234-07. Operator spots a wrong lot and clicks the green 'Acknowledged' badge to undo → DELETE …/acknowledge/88881234-03 → 404 → red toast 'API call failed: 404' and the row stays acknowledged. There is no way to un-acknowledge that carton from the UI, so the lot can never be corrected before finalize.

**Fix.** Compute the box id with a single shared helper (same precedence as the acknowledge payload, scannedLineData first) and use it for both acknowledge and un-acknowledge.


## Printed QR label transaction_no can differ from the stored transaction_no (label unscannable afterwards)

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:969** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** Three different precedence orders for the same field. Because the Confirm-Receipt re-sync (L1785) upserts the same box_id last, its boxRef-first ordering overwrites whatever the single-acknowledge path stored, while the QR encoded on the sticker (L1309 `JSON.stringify({tx: txNo, bi: bId})`) used the generated inwardTransactionNo. Same split on the cold page: L885 vs L1084/L1158/L2015 vs L1412.

**Failure scenario.** Operator clicks 'Generate QR ID's' (inwardTransactionNo = 'TR-20260817143012'), acknowledges rows one by one (DB rows carry TR-20260817143012), prints labels encoding tx=TR-20260817143012, then clicks Confirm Receipt. The re-sync rewrites transaction_no to the dispatch txn 'TRANS202608171200'. Scanning that carton later (handleAckQRScan matches on prefix + transaction_no, L1141-1148) never matches — the box is unfindable by its own label.

**Fix.** Pick one precedence and export it as a single helper used by the acknowledge, batch, re-sync and label-print paths — and use the same value that gets QR-encoded.


## Scan handler falls back to transfer_line_id, which the same file documents as untrustworthy

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:1177** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: plausible

**Problem.** The cold page's own comment (coldtransfer-in/page.tsx L266-268) states the outward side keys interunit_transfer_boxes.transfer_line_id by ARTICLE and falls back to lines[0] on no match — 'that's what put a PM box on a CASHEW row' — yet both scan handlers still use it (regular L1177, cold L1292). Worse, when `lines` are box-derived, l.id is a box/pending id compared against a transfer_lines id (different tables), so a numeric collision silently selects an unrelated row. The positional fallback (L1181) is equally unfounded whenever boxes.length !== lines.length.

**Failure scenario.** Mixed transfer, 10 boxes / 3 lines. Scanning box #7 finds matchedBox but no lineBoxDataMap binding; the transfer_line_id fallback resolves to line 0 (the outward side pointed every box of that article at one line). handleAcknowledgeLine(0) then posts line 0's box_id/lot/weights, the UI shows 'Matched — <article>' in green, and box #7 is recorded as line 0 — an acknowledgement for a carton nobody scanned, while box #7 stays In Transit.

**Fix.** Delete both fallbacks. If a scanned box cannot be bound to exactly one displayed row, show 'Not matched — cannot map this box to a row' rather than guessing.


## Scan-to-acknowledge reads stale linesMatchMap — rapid or repeated scans collapse onto one row

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:1227** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: plausible

**Problem.** The scanner fires onScanSuccess without awaiting (high-performance-qr-scanner.tsx L170/243) and only de-dupes the SAME decoded value for 2 s (SCAN_COOLDOWN_MS, L10/L61); the pages do not pass the `scannedValues` prop, so nothing else de-dupes. handleAckQRScan closes over the linesMatchMap of the render in which it was created, and the map is only updated after the network round-trip, so concurrent scans see identical state. Matching is by box-id PREFIX + transaction_no (L1141-1148), so any carton of the same series matches any unacknowledged row of that series. Same code on the cold page L1342.

**Failure scenario.** (a) Two different cartons of one series scanned ~200 ms apart: both compute matchedLineIndex = 3, both POST, linesMatchMap ends with only index 3 true → counter reads 1 received for 2 cartons. (b) One carton scanned twice, 3 s apart (past the cooldown): scan 1 acknowledges row 3, scan 2 acknowledges row 4 with the SAME box_id — the backend upsert on (header_id, box_id) updates the single row, but the UI now shows 2/2 resolved and enables Confirm Receipt while only one carton exists.

**Fix.** Serialise scans through a ref-backed queue and track claimed rows in a ref (not state) that is updated synchronously before the await; reject a scan whose box_id is already present in that ref.


## originalTotalWeightsRef is never reset between transfers — weights leak from a previously searched transfer

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:1517** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** The ref is populated once per page mount and is guarded by its own emptiness, so a second search on the same screen keeps transfer A's totals. loadTransferDetails (L284) never clears it. The stale values are then written into lineWeights by the carton-weight effect at L1544-1552 (`origTotal` branch), and lineWeights feeds every acknowledge payload. Identical bug on the cold page L1644-1653.

**Failure scenario.** Search TRANS-A (10 lines, total_weight 25.50 each) then, without reloading, search TRANS-B (12 lines, total_weight 30.00 each). Type a carton weight for an article, then clear it: rows 0-9 reset to 25.50 (transfer A's numbers) and rows 10-11 reset to "0". Confirm Receipt stores those weights against transfer B's cartons.

**Fix.** Clear originalTotalWeightsRef.current = {} at the start of loadTransferDetails (alongside setTransferData/setGeneratedBoxIds), or key the ref by transferData.id.


## Bulk Print QR silently rewrites gross weight to net when no empty-carton weight is entered

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:1601** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** The carton-weight inputs are optional (the validation loop at L1569-1578 only checks non-empty values, and the button is enabled whenever bulkRangeArticles.length > 0), but the gross weight is ALWAYS recomputed as net + carton. With no carton weight the tare is 0, so the dispatched gross is overwritten with net for every line in the range. Identical code on the cold page at L1729/L1750.

**Failure scenario.** Cold-source transfer, 60 boxes, dispatched net 24.00 / gross 25.50 per box. Operator sets From 1 / To 60, leaves the carton-weight boxes blank, and clicks 'Bulk Print QR (60 boxes)'. All 60 acknowledges post gross_weight = 24.00; 90 kg of packaging weight vanishes from the receipt and the labels print 'Gross: 24.00kg' against cartons that weigh 25.50.

**Fix.** Only override gross when a carton weight was actually supplied: `const gross = emptyCartonWt > 0 ? netWt + emptyCartonWt : (parseFloat(w.total_weight || line.total_weight || "0") || netWt)`; or require a carton weight per article before enabling the button.


## transferform does not resync boxIdCounterRef after a localStorage restore — box ids collide, and removing one box deletes two

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:412** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** useFormPersistence restores scannedBoxes straight into state (hooks/useFormPersistence.ts:30-34) while boxIdCounterRef stays at its initial 1 (line 382). Every subsequently scanned/added box therefore reuses ids 1,2,3… that restored boxes already hold. That id is the React key (line 2694/2811), the removal key, and the submitted box_number (line 1688). handleRemoveBox filters with loose inequality over the whole array (line 979 `prev.filter(box => box.id != boxId)`), so it removes EVERY row sharing that id. The sibling form contains the exact fix, so the intent is documented.

**Failure scenario.** Operator scans 12 boxes, the tablet reloads (draft restored with ids 1-12), then scans 3 more which are assigned ids 1,2,3. The list now has two rows with id 1. Clicking the X on either one removes both, dropping a real box from the shipment; the payload also contains two boxes with box_number 1, and React logs duplicate-key warnings that can mis-associate the inline weight inputs.

**Fix.** Use the same custom setter as directtransferform:429-438 to advance boxIdCounterRef past the maximum restored id.


## transferform never loads the request quantity into the article — payload qty is always 1 and every scan past the first triggers a false 'Extra Box Scanned' alarm

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:591** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** loadRequestDetails copies only the 4 classification fields from request.lines[0]; quantity_units keeps its initial value 1 (line 341). That single value then drives (a) the submitted line qty (line 1676), (b) the 'Request Qty'/'Remaining' summary tiles (lines 2913, 2923) and (c) the scan-progress toast logic at lines 1376-1398, which declares an 'Extra Box Scanned' warning as soon as scannedBoxes.length exceeds 1. The equivalent edit path in the sibling form does copy quantity/pack_size/net_weight (directtransferform:709-713), so the omission is specific to this file.

**Failure scenario.** Request asks for 30 boxes of ALMOND WHOLE. Operator opens the transfer form: 'Request Qty' shows 1 and 'Remaining' shows 0. From the 2nd scan onward every scan pops a red '⚠️ Extra Box Scanned! Request Qty 1 boxes, but N scanned' toast, training operators to ignore the warning. The POST sends qty:1; if the scanned box's article text does not match the line text exactly, _boxes_authoritative cannot correct it and the stored line reads qty 1 against 30 parked boxes.

**Fix.** In loadRequestDetails also copy `quantity_units: parseInt(firstItem.quantity)||0, pack_size: parseFloat(firstItem.pack_size)||0, unit_pack_size: parseFloat(firstItem.unit_pack_size)||0, net_weight: parseFloat(firstItem.net_weight)||0, uom: firstItem.uom, lot_number: firstItem.lot_number` into articles[0].


## Editing 'Case Pack' on a scanned box silently zeroes its net weight when packageSize is unset

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:1004** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** updateScannedBox recomputes netWeight = casePack × packageSize whenever Case Pack is edited, with no guard for a missing/zero packageSize. In transferform, boxes created by QR scan (newBox at line 1344-1370), by manual box fetch (line 1054-1080) and by the plain-text QR path (line 1458-1483) never set packageSize at all, so parseFloat(undefined)||0 = 0 and the authoritative scanned weight is overwritten with '0'. The same code exists at directtransferform:1340, where QR boxes only get packageSize for PM material (line 1683 `pmCount`), leaving every FG/RM scanned box exposed. Both the Case Pack and Net Wt cells are adjacent editable inputs in the same row (lines 2828-2863 / 3198-3245), so this is a routine edit.

**Failure scenario.** Operator scans a box that returns net_weight 25.400 kg, then corrects Case Pack from blank to 12 in the boxes table. netWeight becomes '0'. The summary tile 'Total Net Wt' drops by 25.4 kg and the payload sends net_weight:'0' for that box; the backend's _boxes_authoritative then sums 0 into the line net weight (interunit_tools.py:973-977), so the transfer ships a 0 kg box.

**Fix.** Only recompute when both factors are > 0: `const ups = parseFloat(box.packageSize) || 0; if (casePack > 0 && ups > 0) updated.netWeight = ...` — otherwise leave the scanned weight untouched.


## Old-format QR duplicate check compares raw QR fields against API-enriched stored fields — the same box can be scanned twice

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:1140** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** For TX*/CONS* QR codes the stored box takes sku_id and box_number from the /inward lookup (boxData.sku_id, boxData.box_number at lines 1312/1326 → newBox lines 1349/1351), while the duplicate test uses the values decoded from the QR itself. When the QR omits sku_id (skuId=null) or encodes box_number as a string, the strict comparison never matches a previously scanned copy of the same box. Identical code at directtransferform:1476-1478. Note handleQRScanSuccess is re-created each render and the scanner keeps it in a ref (high-performance-qr-scanner.tsx:46,54), so state freshness is not the issue — the comparison itself is wrong.

**Failure scenario.** QR payload {"cn":"TX20260801","bx":5} (no sku). First scan stores {transactionNo:'TX20260801', skuId:4471, boxNumberInArray:5}. Operator rescans the same carton: box.skuId(4471) === skuId(null) is false → no duplicate warning → a second row with the same 18 kg is added. Either the DC double-counts 18 kg, or — if the lookup supplied a box_id — the backend rejects the entire submit with HTTP 400 'Duplicate box_id ... in this transfer' (interunit_tools.py:1236-1245) after all 40 boxes have been scanned.

**Fix.** Compare against the same normalised identity that gets stored, e.g. `String(box.boxId) === String(qrBoxId || fetchedBoxId)` or `box.transactionNo === transactionNo && String(box.boxNumberInArray) === String(boxNumber)`, and re-run the duplicate check after the lookup enrichment.


## Failed box lookup is toasted but execution continues — a phantom 'N/A' box with 0 kg is added and submitted

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:1198** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** All three lookup branches (new QR format line 1198, bulk-entry line 1237, TX/CONS line 1331) catch and continue, so boxData stays as the bare QR object and the row is created with itemDescription 'N/A', netWeight '0', totalWeight '0' (lines 1344-1370). The same pattern is at directtransferform:1535, 1575, 1670. transferform then submits that row in boxes[] with `article: box.itemDescription || "Unknown Article"`; directtransferform drops the corresponding LINE (its filter `clean(box.itemDescription) !== ''` at line 1946 turns 'N/A' into '') but still sends the BOX (line 1978), and the backend attaches an unmatched article to the first line via fallback_line_id (interunit_tools.py:1257).

**Failure scenario.** Warehouse Wi-Fi blips during a scan. Operator sees a red 'Box Lookup Failed' toast and immediately after a green 'Box Scanned!' toast, so they move on. Row #17 shows 'N/A / 0 kg'. On submit, interunit_transfer_boxes gets a box row with article 'N/A' (or 'Unknown Article'), net 0, glued to the first line of the transfer — a phantom box on the DC that the receiving site can never scan in.

**Fix.** `return` (after resetting isProcessingRef) from each catch block so no box row is created when the lookup fails, or mark the row invalid and block submit until it is re-scanned.


## transferform omits net_weight/total_weight from lines[] — the operator's typed net weight is discarded and the backend re-derives it from pack size

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:1678** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** TransferLineCreate accepts net_weight/total_weight (interunit_models.py:197-198) and create_transfer uses the supplied value when present, otherwise recomputes it: `frontend_net_weight = float(line.net_weight) if line.net_weight else 0.0` → line_net_weight(...) (interunit_tools.py:1156-1166, 845-875). The form has a Net Weight (Kg) input the operator can override (line 2442-2453) and shows a 'Weight Comparison — Over/Under by X Kg' card built from it (lines 2946-2994), yet the value never leaves the browser. directtransferform does send both (`net_weight: String(box.netWeight)` at line 1959), proving the field is expected.

**Failure scenario.** FG line: Case Pack 10, Unit Pack Size 0.5, qty 4 → auto net 20.000 kg. Operator weighs the pallet and corrects Net Weight to 19.200. Submit: payload has no net_weight, so the backend stores line_net_weight(FG, 10, 0.5, 4) = 20.000. The DC, the in-transit weight and the receiving GRN all carry 20.000 kg against 19.200 kg of actual goods, and the 0.8 kg discrepancy is invisible.

**Fix.** Add `net_weight: String(article.net_weight ?? 0), total_weight: String(article.total_weight ?? 0)` to the lines[] mapping.


## transferform hard-codes lot_number and batch_number to null in lines[], throwing away the lot the operator typed

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:1680** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** The form collects a Lot Number per article (input at lines 2456-2467, bound to article.lot_number) and uses it for the cold-storage summary popup (line 1739 `Lot Number : ${art.lot_number || "-"}`), but the payload pins lot_number/batch_number to null while sending vakkal from the same object. The backend stores '' (interunit_tools.py:1195-1196) and lot identity is load-bearing downstream: reconcile_transfer_to_order fills box shortfalls BY LOT (interunit_tools.py:1373-1375) and _synthesise_article_entry_boxes propagates line.lot_number onto the generated boxes (interunit_tools.py:1063).

**Failure scenario.** Cold dispatch to 'Savla D-39': operator types Lot CF100326, sees it echoed in the summary popup that gets WhatsApped to the cold store, but the transfer line is saved with lot_number = ''. Every ART-n box is created lot-less, lot-based reconciliation matches nothing, and the receiving side cannot tell which lot arrived.

**Fix.** Send the real values: `batch_number: article.batch_number || null, lot_number: article.lot_number || null`.


## 'Total Count' is computed over raw lines while the Count column is computed over consolidated rows using only the first line's unit_pack_size

**legacy_frontend/components/transfer/DeliveryChallan.tsx:88** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** `totalPMCount` iterates `validItems` (one entry per backend line) while the printed Count cells iterate `consolidatedItems`, whose `unit_pack_size` is whatever the FIRST merged line carried (line 131 `itemMap.set(key, { ...item, ... })`) multiplied by the SUMMED qty. When two lines share description+category+pack_size but differ in `unit_pack_size`, the column and the total disagree. That this happens in production is proven by the backend: `_boxes_authoritative` takes `max([float(l.unit_pack_size or 0) for l in art_lines])` across duplicate lines of the same article (interunit_tools.py:985).

**Failure scenario.** Two PM lines for the same article, same pack_size: line A unit_pack_size 1000 x qty 2, line B unit_pack_size 500 x qty 2. totalPMCount = 2000 + 1000 = 3,000. The single consolidated row prints Count = 1000 x 4 = 4,000. The DC prints a Count column that sums to 4,000 next to a totals cell reading 3,000.

**Fix.** Compute the total by summing `itemCountFor(row)` over `consolidatedItems` (single source of truth), and make consolidation refuse to merge lines whose unit_pack_size differs (add it to the group key).


## Consolidation prints the first merged line's vakkal / lot / batch / UOM against the summed quantity of all merged lines

**legacy_frontend/components/transfer/DeliveryChallan.tsx:131** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** The group key is `description__category__pack_size`; vakkal, lot_number and batch_number are not part of it and are not reconciled on merge. The row then prints the first line's `vakkal` (line 298 / 513) beside a qty and a net weight aggregated across lines that belong to different vakkals and lots. Vakkal is mandatory per article for cold destinations (directtransferform/page.tsx:1906-1918) and is round-tripped by the backend (`_map_transfer_line` line 543), i.e. it is traceability data on the printed challan.

**Failure scenario.** Cold transfer of 'PISTA' with 60 boxes on vakkal VK-11/lot 130273 and 40 on vakkal VK-12/lot 130274, both with the same pack_size. They merge into one DC row: 'PISTA | VK-11 | 100 boxes | 2,480.500 kg'. The receiving warehouse books all 100 boxes to vakkal VK-11 and lot 130274 disappears from the paperwork.

**Fix.** Include vakkal and lot_number in the consolidation key (or render them as a comma list per row) so a printed row never asserts a single vakkal for goods from several.


## DeliveryChallan assigns the full per-article box count to every consolidated row sharing that description — printed Total Boxes doubles

**legacy_frontend/components/transfer/DeliveryChallan.tsx:144** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** box_count is keyed by DESCRIPTION ONLY, but consolidatedItems is keyed by description + category + pack_size (:124). Whenever the same article yields two consolidated rows (different item_category or pack_size across its lines), each row is stamped with the FULL box count for that description, and the totals rows at :347 and :540 add them together. The cold write path makes this common: it emits one line per box and `clean()` maps 'N/A' categories to "" while QR-sourced boxes carry a real category (coldtransferform/page.tsx:2405, 2686), so the same article routinely lands in two buckets. The `|| it.qty` fallback at :144 compounds it — when box.article does not string-match the line description, the row silently falls back to the units qty.

**Failure scenario.** Challan with 100 scanned boxes of "SHRIMP HLSO 21/25": 60 boxes came through the QR scanner (item_category "FROZEN"), 40 through the cold-lot picker (item_category ""). consolidatedItems produces 2 rows, both stamped box_count = 100. The printed DC shows two lines of 100 boxes each and "Total Boxes: 200" for a 100-box truck, while "Total Qty: 198" (:539) shows the unit sum — three different numbers for one shipment on one page. Security at the gate counts 100.

**Fix.** Key the box map by the SAME composite used for consolidation (description + category + pack_size), or distribute a description's boxes across its consolidated rows proportionally to qty. Drop the `|| it.qty` fallback when a boxes array is present — falling back to a units qty for a Boxes column is never correct.


## 'No. of Boxes' silently falls back to the line quantity when the box article doesn't match the item description

**legacy_frontend/components/transfer/DeliveryChallan.tsx:144** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** The lookup key is built from `item_description` first, while `boxCountByDesc` is built from `bx.article`. These are separate DB columns (`interunit_transfers_lines.item_desc_raw` vs `interunit_transfer_boxes.article`) populated by different code paths - the box `article` comes from the scan/pick payload (cold_transfer_out_tools.py:362 `"article": box.item_description`) while the line description comes from the keyed line. Any drift (case is handled, but trailing text, grade suffix, relabelling, or a blank article) makes `.get(d)` undefined and the code then prints the LINE QUANTITY in a column headed 'No. of Boxes'. It also silently mixes units: for PM lines qty can be pieces, not boxes. Note the display column uses the opposite precedence (`item_desc_raw || item_description`, line 295), so the DC can display one string and match on another.

**Failure scenario.** A PM line 'LAMINATE ROLL' with qty 25,000 (pieces) and boxes stored under article 'LAMINATE ROLL 250MM'. The lookup misses, so the DC prints 'No. of Boxes: 25,000' and the Gate Pass 'Total Boxes: 25,000' for a consignment of 4 physical rolls.

**Fix.** Match boxes to lines via `bx.transfer_line_id` (already returned by `_map_box_row`) instead of by description string; when no boxes exist for the transfer at all, label the column 'Qty' rather than silently substituting qty for a box count.


## Delivery Challan renders no DC page at all (no totals, no reason, no signature) when every line is filtered out

**legacy_frontend/components/transfer/DeliveryChallan.tsx:151** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** When `consolidatedItems` is empty (all lines had a blank or 'N/A' description - exactly the case the filter at line 70 exists for - or the API returned `lines: []`), `itemPages` is `[]`, so the entire DC table is skipped: header block, transfer number, from/to addresses, vehicle, totals row, Reason and Auth Sign never render. The component then prints only the Gate Pass, and `window.print()` still fires 500ms later (line 54).

**Failure scenario.** Operator opens /cfpl/transfer/dc/1732 for a transfer whose lines all carry a blank item_desc_raw. The printer emits a page containing only a cut-here line and a Gate Pass with 'Total Items: 0' - no delivery challan, no reason, no signature block - and the operator has no indication anything is wrong.

**Fix.** Always render at least one DC page (`itemPages.length ? itemPages : [[]]`) and show an explicit 'No printable line items' banner plus the header/footer blocks, or block printing and surface an error when `consolidatedItems.length === 0`.


## A failed pending-stock fetch renders as 'No pending transfers - All in-transit goods have been received'

**legacy_frontend/components/transfer/PendingTransfersModal.tsx:118** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** There is no error state. Any non-2xx (thrown at line 98) or network failure clears `records`, and the render path at line 459-464 then shows the empty-state copy 'No pending transfers' / 'All in-transit goods have been received' - an affirmative claim about inventory produced by a failed request. The summary bar simultaneously shows Transfers 0 / Total boxes 0 / Total weight 0 kg.

**Failure scenario.** API returns 500 while 14 transfers carrying 812 boxes are in transit. The operator sees a clean 'All in-transit goods have been received' screen and closes the modal, and nothing in the UI indicates the data never loaded.

**Fix.** Track an `error` state, keep the previous records on failure, and render a retry banner instead of the empty state when the last fetch failed.


## Pending-transfer search fires a request on every keystroke with no debounce and no AbortController

**legacy_frontend/components/transfer/PendingTransfersModal.tsx:129** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** `search` is a dependency of `loadData`, and `loadData` is a dependency of the effect, so each character typed in the search box creates a new callback and triggers a new `/interunit/pending-stock` request. Nothing cancels the in-flight request, so responses are applied in completion order, not request order. Compare app/[company]/transfer/request/page.tsx:208-212, which does debounce. Additionally, on open the component fires `loadData()` and `handleSyncExisting(true)` (which itself awaits `loadData()` in its finally), so opening the modal always issues at least two identical queries.

**Failure scenario.** Typing 'TRANS2026' issues 9 queries against a scan of pending_transfer_stock + interunit_transfers_header. The response for 'TRANS' returns after the response for 'TRANS2026' and overwrites it - the table shows dozens of unrelated challans while the search box reads 'TRANS2026'. The user cancels a transfer selected from that stale list (the Cancel action deletes the transfer and restores stock).

**Fix.** Debounce `search` into a separate state (300ms) before putting it in the fetch deps, and carry an AbortController per request, discarding responses whose controller was aborted.


## Pending-transfer hover silently drops every line-only item when the transfer has any scanned box

**legacy_frontend/components/transfer/PendingTransfersModal.tsx:497** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** This is the exact pattern that ChallanHoverCard.tsx:406-409 documents as a fixed bug ('Previously the hover showed ONLY boxed items whenever any box existed, so line-only items ... silently vanished'). The fix is `groupTransferItems(boxes, lines, fromColdUnit)`, which this call site never adopted. Two consequences: (1) any article entered by qty without a scan is missing from the hover item list AND from the 'Total Count' meta computed at line 517; (2) `groupBoxesByItem` is called WITHOUT its third `lines` argument, so the unit_pack_size / material_type backfill from the parent line (ChallanHoverCard.tsx:336-347) never runs - boxes whose `transfer_line_id` is NULL come back from the backend LEFT JOIN with `unit_pack_size: null` and contribute 0 to Count.

**Failure scenario.** Transfer TRANS202608141210 carries 3 box-scanned articles and one PM article entered as a line (qty 20, unit_pack_size 500). Hovering the challan lists 3 items and 'Total Count 12,000' instead of 4 items and 22,000 - the reviewer concludes the fourth article was never dispatched.

**Fix.** Call `groupTransferItems(data.boxes || [], data.lines || [], fromColdUnit)` here, matching the transfer dashboard.


## fetchPeriodTransferSummary sends from_date/to_date to /interunit/requests, which ignores them — period tile shows the all-time total

**legacy_frontend/lib/api.ts:418** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** `GET /interunit/requests` (interunit_server.py:217-226) has no `from_date`/`to_date` query parameters. FastAPI ignores unknown query params, so the date window is dropped and `list_requests` (interunit_tools.py:315-337) counts every non-Deleted request. `_fetchPeriodSummary` (api.ts:396) then returns `data.total` — the unfiltered COUNT(*). The route was fixed from the dead `/transfer/requests` but the params were never checked against the new endpoint, so the bug changed from 'always 0' to 'always the all-time number', which is far harder to notice.

**Failure scenario.** Dashboard 'This week's transfers' calls fetchPeriodTransferSummary('CDPL', '2026-08-11', '2026-08-17'). The DB holds 4 requests created this week and 3,180 all-time. The tile renders 3,180 for 'this week' (and the monthly tile renders the identical 3,180).

**Fix.** Either add `from_date`/`to_date` Query params to list_requests_endpoint and wire them into the WHERE clause on `r.created_ts`, or drop this helper's date arguments and stop presenting the result as a period figure.


## Three transfer dashboard helpers still call the non-existent /transfer/requests route and swallow the 404 as zero

**legacy_frontend/lib/api.ts:492** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** There is no `/transfer/requests` route anywhere in the backend (verified: main.py:379-398 router list; the only transfer routers are `/interunit` and `/transfer-dashboard`). All three helpers 404 on every call, and each one converts the 404 into a successful-looking empty result (`[]`, `{count:0,total:0}`, `0`) rather than an error state. The comment at api.ts:416-417 admits this, but only fetchPeriodTransferSummary was repointed — these three were left behind.

**Failure scenario.** Company dashboard mounts. fetchTodayTransferSummary → 404 → renders 'Today's Transfers: 0'. fetchAllTimeTransferTotal → 404 → renders 'Total Transfers: 0'. fetchRecentTransfers → 404 → 'Recent Transfers' list renders empty. All three show a plausible zero-activity dashboard on a day with 40 real transfers, and nothing appears in the error UI — only a console.warn.

**Fix.** Repoint all three to `/interunit/requests` (or `/interunit/transfers` for actual transfers) and remove the 404→empty coercions so a routing regression surfaces as an error state instead of a zero.


## lib/api/interunit.ts InterUnitListResponse {items,total,pages} does not match the backend's {records,total,page,per_page,total_pages}

**legacy_frontend/lib/api/interunit.ts:221** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: plausible

**Problem.** `InterUnitAPI.list()` (line 301-321) is typed to return `{items, pages}` while `GET /interunit/transfers` (interunit_server.py:268, response_model=TransferListResponse) returns `{records, total_pages}`. Both `items` and `pages` are `undefined` at runtime. The row type is equally wrong: `InterUnitListItemEnhanced` (line 8-22) declares `from_site`/`to_site`/`lines_count`/`qty_total` while `_map_transfer_header` (interunit_tools.py:549-569) emits `from_warehouse`/`to_warehouse` and `list_transfers` adds `items_count`/`boxes_count`/`total_qty` (interunit_tools.py:1508-1510). This client is currently unimported (verified: zero `from '@/lib/api/interunit'` matches repo-wide), so it is a landmine rather than a live outage — but it is the documented second client for the same backend and every field in it is wrong.

**Failure scenario.** Any component wired to `interUnitAPI.list(filters, 1, 20)` renders `res.items.map(...)` → TypeError: Cannot read properties of undefined (reading 'map'); if guarded with `res.items ?? []` it renders an empty table with `pages = undefined` so the pager shows 'Page 1 of NaN' while `total` correctly reports 340 rows.

**Fix.** Delete lib/api/interunit.ts (nothing imports it) or realign it to the backend contract: `records`/`total_pages` and `from_warehouse`/`to_warehouse`/`items_count`/`total_qty`. Keeping a second, wholly-divergent client for the same endpoints guarantees the next consumer picks the broken one.


## TransferRecord.box_count / issue_count / issue_weight are per-transfer values fanned out across every line row — summing them multiplies by line count

**legacy_frontend/lib/api/transferDashboardApi.ts:23** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** `/transfer-dashboard/all-data` emits one record per (header × line) pair (INNER JOIN at transfer_dashboard_server.py:58), then stamps header-scoped aggregates onto every one of those rows: `box_count` from a `GROUP BY header_id` count (line 89-91), and `issue_count`/`issue_weight`/`issue_details` from a per-transfer_out_id map (line 144-150). The FE type exposes them as plain per-record numbers with nothing marking them as header-scoped, and consumers sum them: `lib/transfer/buildSummary.ts:69` `rs.reduce((s,r)=>s+(r.box_count||0),0)` and `app/[company]/transfer/dashboard/page.tsx:260,265`.

**Failure scenario.** Transfer 4412 has 3 line items (ALMOND, CASHEW, PISTA) and 50 physical boxes, with 2 issue boxes weighing 40 kg. /all-data returns 3 records, each with box_count=50, issue_count=2, issue_weight=40. The dashboard 'Total Boxes' tile renders 150 instead of 50 and 'Issues' renders 6 instead of 2. dashboard/page.tsx:878 then computes perBoxNet = line_net / 50 using the whole transfer's box count for a single line, so the ALMOND row displays '10.00 Kg × 50 boxes' when that line actually has 18 boxes.

**Fix.** Split the payload: return a header-level array (transfer_id, box_count, issue_count, issue_weight, issue_details, received_status) plus a line-level array, and have the FE join them client-side. Failing that, add per-line box counts (GROUP BY header_id, line_id) and rename the header-scoped fields (e.g. `transfer_box_count`) so a `reduce` over records is obviously wrong.


## SecureApiClient retries POST/PUT/PATCH/DELETE three times — non-idempotent mutations duplicated on timeout or 5xx

**legacy_frontend/lib/auth/secureApiClient.ts:102** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: plausible

**Problem.** `retryRequest` delegates to `ErrorRecovery.retryOperation` (errorHandling.tsx:179-205), which retries on *any* thrown error with no method or status discrimination, `retries = 3` (line 23). `makeRequest` throws on a 30s client-side abort (line 55-57) and on every non-2xx via `ErrorRecovery.handleAPIError` (which always throws — errorHandling.tsx:237, 290-299). A POST that the server actually committed but that timed out client-side, or that returned a 500 after a partial commit, is re-sent twice more.

**Failure scenario.** `inwardFormApiClient.createInward(payload, 'CDPL')` POSTs /api/inward/create. The server takes 32s (large box batch) and commits transaction TXN-9001. At 30s the client AbortController fires → APIError('Request timeout') → retryOperation waits 1s and re-POSTs → commits TXN-9002 → times out again → waits 2s, re-POSTs → TXN-9003. The user sees one failure toast and three inward entries exist. Same pattern applies to every `secureApiClient.post/put/delete` call site in approvalApiService.ts and outwardApiService.ts (approveOutward, deleteOutwardBox, etc.).

**Fix.** Only route GET/HEAD through retryRequest. For POST/PUT/PATCH/DELETE call `makeRequest` directly, or gate retries on an explicit idempotency key echoed by the server.


## SecureApiClient.upload() sends FormData with a forced Content-Type: application/json — multipart boundary is never generated

**legacy_frontend/lib/auth/secureApiClient.ts:151** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: plausible

**Problem.** `makeRequest` routes through `createAuthenticatedFetch` (authGuard.tsx:421-455), which unconditionally sets `Content-Type: application/json`. `upload()` never passes a `headers` override, so the explicit JSON content type wins and the browser does NOT auto-generate the `multipart/form-data; boundary=...` header it normally would for a FormData body. The request arrives with a multipart payload labelled as JSON and FastAPI's `File(...)`/`UploadFile` parser rejects it.

**Failure scenario.** User picks an .xlsx in the inward import dialog → `inwardFormApiClient.importData(file, 'CDPL')` → POST /api/inward/import with body=FormData, Content-Type: application/json → FastAPI 422 'Expected UploadFile, received: <str>'. Then, per the previous finding, it is retried twice more before surfacing. Import is 100% broken.

**Fix.** In `upload()`, pass `headers: { 'Content-Type': undefined }`-equivalent handling: change createAuthenticatedFetch to skip the JSON Content-Type when `options.body instanceof FormData`.


## InterunitApiService.getRequests advertises from_date/to_date/sort_by/sort_order that /interunit/requests does not accept

**legacy_frontend/lib/interunitApiService.ts:292** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: plausible

**Problem.** This is the live client (33 call sites across app/[company]/transfer and cold-transfer). Its published parameter type promises date filtering and sortability that `list_requests_endpoint` does not implement — the params are appended to the query string (line 300-304) and dropped by FastAPI. `list_requests` hard-codes `ORDER BY r.created_ts DESC` (interunit_tools.py:351), so `sort_by:'request_date', sort_order:'asc'` produces created_ts DESC. Note the sibling `getTransfers` (line 407-417) uses the *same* names and they are genuinely supported there (interunit_server.py:275-279), which makes the requests-side no-op look like it works.

**Failure scenario.** A caller does `getRequests({page:1, per_page:15, from_date:'01-08-2026', to_date:'07-08-2026', sort_by:'request_no', sort_order:'asc'})`. Backend returns page 1 of ALL non-Deleted requests ordered by created_ts DESC, with `total`/`total_pages` reflecting the unfiltered set. The screen shows an unfiltered, unsorted list plus a pager sized to the whole table, and no error is raised — indistinguishable from 'the date range genuinely matched everything'.

**Fix.** Remove from_date/to_date/sort_by/sort_order from this signature until the backend supports them (add them to list_requests_endpoint and parameterise the ORDER BY with a whitelist, mirroring list_transfers at interunit_tools.py:1448-1451).


## buildSummary sums the same fanned-out box_count at every tree level (L1, L2 and item rows all over-count boxes)

**legacy_frontend/lib/transfer/buildSummary.ts:69** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** Same defect as the KPI but inside the grouping engine that renders the whole table. `txCount` (line 70) correctly uses `new Set(r.transfer_id)`, but `sumBoxes` adds the per-transfer box count once per line row. Worse, at the ITEM level (buildItems, line 112) each item row gets the FULL transfer box count rather than that item's boxes, so two item rows under one transfer each claim all 100 boxes and the group row claims 200.

**Failure scenario.** Group by From WH, view mode "Boxes". Cold Storage → A185 transfer of 100 boxes with 2 items renders: item row "Prawn 8/12 — 100 Boxes", item row "Prawn 16/20 — 100 Boxes", group row "Cold Storage — 200 Boxes", Grand Total 200 Boxes. Sorting by "Boxes" (sortNodes case "boxes", line 86) then ranks warehouses by line-count-weighted boxes, so a warehouse with many small multi-line transfers outranks one that actually shipped more boxes.

**Fix.** Compute boxes per distinct transfer inside `sumBoxes` (`new Map(rs.map(r => [r.transfer_id, r.box_count || 0]))`), and for item rows either return no box figure or have the backend supply a per-line box count (`COUNT(*) FROM interunit_transfer_boxes GROUP BY header_id, transfer_line_id`).


## The printed Delivery Challan totals for a cold dispatch double both Qty and Kg (the orphan lines keep per-box net_weight while the corrected line already holds the total)

**components/transfer/DeliveryChallan.tsx:128** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** The DC consolidates `transferData.lines` by (description, category, pack_size) and sums qty and net_weight, and the page-level total is `lines.reduce(... quantity)` (app/[company]/transfer/dc/[transferId]/page.tsx:73). For a cold transfer every box contributed its own line whose net_weight is the PER-BOX weight, while `_apply_box_totals` (cold_transfer_out_tools.py:157-175) wrote the FULL consignment weight onto one of them. Summing therefore yields ≈2× the real kg and 2B−K for qty. Only the 'No. of Boxes' column is right, because it is counted from the `boxes` array (line 112-116). The same double-count reaches the hover card: ChallanHoverCard.groupLinesByItem sums `l.quantity` and `l.net_weight` per (item, lot) at lines 293-294.

**Failure scenario.** Printing the DC for TRANS202608171318 (100 boxes, pile A 60 × 5 kg = 300 kg, pile B 40 × 5 kg = 200 kg): item A row shows No. of Boxes 60 but Qty 119 and Net 595 kg (300 from the corrected line + 59×5 from the orphans); the footer prints 'Total Qty: 198 / Total Boxes: 100 / Total Kg: ~990' against 500 kg actually on the truck. A driver/receiver reconciling against this document sees an internally contradictory challan.

**Fix.** No change needed here once the backend stops writing per-box duplicate lines (root-cause finding) — the consolidation is correct for one-line-per-(item,lot) data. If a belt-and-braces guard is wanted, ignore lines that carry no boxes when a `boxes` array is present, or drive the DC quantity column from `boxCountByDesc` the way the box column already is.


## delete_cold_transfer_in guesses the source table from the DESTINATION company — restores cross-company stock to the wrong ledger

**services/ims_service/cold_transfer_in_tools.py:467** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** from_storage_type is derived from the real from_site (line 466), but the COMPANY prefix comes from to_company — the destination. The re-parked pending row (line 533 'source_table': source_table_guess) therefore claims the box came from the destination company's table. pending_stock_tools already has a dedicated fix for precisely this mistake: _ledger_source_for (pending_stock_tools.py:1964-1973) exists because 'Unpick used to throw all of that away and re-derive source_table from the DESTINATION company, which is wrong for any cross-company transfer.' The cold delete path reintroduces the bug the interunit path already fixed. It also writes 'from_company': to_company (line 528) with the same defect.

**Failure scenario.** Savla D-39 (cfpl) ships 50 boxes to Rishi (cdpl). The receipt is deleted. Every re-parked pending row is written with source_table='cdpl_cold_stocks' and from_company='cdpl', though the boxes were deducted from cfpl_cold_stocks at dispatch. If the transfer is subsequently cancelled, restore_to_source inserts all 50 boxes into cdpl_cold_stocks — CFPL permanently loses 50 boxes and CDPL gains 50 it never held, across a company boundary that matters for valuation.

**Fix.** Resolve the true source via _ledger_source_for(db, box_id, transaction_no) (the un-reverted 'transfer_out_pending' disposition row park_in_pending wrote at dispatch) and fall back to _company_from_table on that, instead of deriving the prefix from to_company.


## Cold receive claims pending rows from other transfers — no transfer_out_id scope on the pending lookup

**services/ims_service/cold_transfer_in_tools.py:625** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** The transfer_out_id parameter is passed into _process_box_loop (used only by the LINE- sweep at line 737) but is deliberately absent from this WHERE. Since (box_id, transaction_no) is globally unique in pending_transfer_stock, the row found is either this transfer's row or ANOTHER live transfer's row — and line 719-723 unconditionally DELETEs it. There is no verification that the matched row belongs to the transfer being received.

**Failure scenario.** Transfer T1 (Savla D-39 -> Rishi) has box 90512000-7 / TR-...513 parked In Transit. An operator receiving unrelated transfer T2 mis-scans (or the form pre-fills from a stale cache) that same box_id+txn. _process_box_loop finds T1's pending row, writes a cdpl_cold_stocks row for it under T2's header, and DELETEs T1's In-Transit row. T1's ledger silently loses a box; _reconcile_statuses for T1 will later see pending_remaining=0 and stamp T1 'Received' even though that carton was never received against T1.

**Fix.** Add `AND transfer_out_id = :oid` to the WHERE and pass transfer_out_id; if no row matches, either reject the box or record it explicitly as an over-scan rather than consuming a stranger's ledger row.


## Cold OUT box→line mapping is case-sensitive while the coverage check is case-insensitive — all boxes collapse onto line 1 and document qty inflates

**services/ims_service/cold_transfer_out_tools.py:344** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** line_id_by_key is built at line 318-321 from ((line.item_desc_raw).strip(), (line.lot_number).strip()) — raw case. The box lookup at line 340-344 uses ((box.item_description).strip(), (box.lot_no).strip()) — also raw case. Any case or whitespace difference between the line's item text (SKU-master derived) and the cold box's item_description (cold_stocks derived) makes the lookup miss, and EVERY box silently falls back to `fallback_line_id` = the first line inserted (line 323). _apply_box_totals (line 370 / 147-192) then writes the box count and summed weight of ALL boxes onto that one line, while every other line keeps its typed qty untouched (the docstring at 152-154 states 'Lines carrying no boxes are untouched'). Note the same file uses `.upper()` on the very same fields 45 lines later for the coverage check (lines 389, 393) — the two key derivations are inconsistent, which is the proof this is an oversight, not intent. Identical code in edit at lines 547-570.

**Failure scenario.** Cold dispatch with 2 items: line A item_desc_raw='KIMIA DATES 500 GM' lot 125859 (60 boxes) and line B item_desc_raw='AJWA DATES 1 KG' lot 93289 (40 boxes); the scanned cold boxes carry item_description='Kimia Dates 500 Gm' / 'Ajwa Dates 1 Kg' from cold_stocks. Both lookups miss, so all 100 boxes get transfer_line_id = line A's id. _apply_box_totals sets line A qty=100 and net = sum of ALL 100 boxes (both articles' weight booked to Kimia Dates); line B keeps qty=40. list_transfers then reports total_qty = 140 against boxes_count = 100 and renders pending_items = 40 (interunit_tools.py:1511) for a transfer that shipped complete, and the challan attributes 40 boxes of Ajwa to Kimia.

**Fix.** Key both dicts identically and case-insensitively: build line_id_by_key on ((item_desc_raw or '').strip().upper(), (lot or '').strip()) and look up with the same normalization. Also make the fallback explicit — if a box matches no line, raise 400 rather than silently attaching it to the first line.


## Cold OUT double-parks manual lines on top of already-parked boxes when the line key doesn't match the box key

**services/ims_service/cold_transfer_out_tools.py:397** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** park_in_pending has already parked one In-Transit row per scanned box (line 373). The 'never-drop manual entries' block then parks qty MORE rows (park_lines_in_pending inserts one row per unit — pending_stock_tools.py:1479-1534) for any line whose (item.upper(), lot) key doesn't consume the boxes. The two keys are derived from different payload fields: _covered from box.item_description/box.lot_no (line 389), _uncovered from line.item_desc_raw/line.lot_number (line 393). A blank line lot_number, a differently-typed lot, or any item-text divergence yields _take = 0 and the full line qty is parked a second time. Compounding this, _apply_box_totals at line 370 has ALREADY rewritten the DB line qty to the box count, but _uncovered reads the STALE payload qty — the ledger is parked against a quantity the stored document no longer claims.

**Failure scenario.** Operator scans 100 boxes of 'Fresho Kimia Dates 500 Gm' lot 125859 and the form also submits one line with item_desc_raw='Fresho Kimia Dates 500 Gm' and lot_number='' (lot left blank on the typed line) qty=98. park_in_pending parks 100 real rows. _covered = {('FRESHO KIMIA DATES 500 GM','125859'): 100}; _uncovered key is ('FRESHO KIMIA DATES 500 GM','') → miss → _take=0 → park_lines_in_pending parks 98 LINE-<lid>-1..98 rows. pending_transfer_stock now holds 198 In-Transit rows for a 100-box dispatch. The Pending Transfers modal and list_pending_transfers' COUNT(*) total_boxes (pending_stock_tools.py:2485) report 198 boxes / ~2x kg, and _autofinalize_if_complete never completes because 98 unclaimable sentinels linger.

**Fix.** Derive both keys from one normalizer, and — since _apply_box_totals already made boxes authoritative — compute _uncovered from the POST-update line qty (re-read interunit_transfers_lines) rather than the payload. Better: skip park_lines_in_pending entirely for any line that has at least one box attached to it via transfer_line_id.


## response_model=TransferInDetail strips inward_transaction_no and inward_box_id — the generated QR label data is unreachable from the detail endpoint

**services/ims_service/interunit_models.py:467** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** `POST /transfer-in/{header_id}/generate-qrs` (interunit_server.py:516) writes `inward_transaction_no` on the header and `inward_box_id` on each box (interunit_tools.py:3460-3485). `_map_transfer_in_header` returns `inward_transaction_no` (interunit_tools.py:2126) and `_map_transfer_in_box` returns `inward_box_id` (interunit_tools.py:2150), but `TransferInHeaderResponse` (models 467-481) and `TransferInBoxResponse` (models 450-464) declare neither, so `GET /interunit/transfer-in/{id}` (response_model=TransferInDetail, server line 760) drops both.

**Failure scenario.** After generating QRs for GRN-4471, reopening the receipt detail page returns a header with no `inward_transaction_no` and boxes with no `inward_box_id`. Any re-print / label-reconciliation flow reading the detail endpoint sees the receipt as 'QRs not generated', while `generate_transfer_in_qrs` refuses to regenerate them (409 at interunit_tools.py:3445) — the labels become unrecoverable through this endpoint. It only works today because the FE reads them from the un-modelled `/transfer-in/pending/by-transfer-out/{id}` route (transferIn/page.tsx:421).

**Fix.** Add `inward_transaction_no: Optional[str] = None` to `TransferInHeaderResponse` and `inward_box_id: Optional[str] = None` to `TransferInBoxResponse`.


## lot_numbers computed by list_transfer_ins is stripped by the response_model and never reaches the frontend

**services/ims_service/interunit_models.py:489** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** list_transfer_ins builds `lot_numbers` with an expensive per-row UNION subquery over interunit_transfer_in_boxes + interunit_transfer_boxes (interunit_tools.py:3246-3252) and sets item['lot_numbers'] at line 3268. But the endpoint declares response_model=TransferInListResponse (interunit_server.py:327), whose record type TransferInListItem inherits TransferInHeaderResponse and declares neither `lot_numbers` nor `inward_transaction_no`. Pydantic v2 BaseModel ignores extra keys by default, so FastAPI discards both fields on serialization. The cold twin endpoint (interunit_server.py:350) declares NO response_model and therefore does return lot_numbers — the two list endpoints emit different shapes for the same UI column.

**Failure scenario.** Operator searches the Transfer-IN register for lot '125859'. The backend's search clause (line 3212-3218) matches correctly and returns the GRN rows, but every row's Lot column renders empty because `lot_numbers` was stripped, while the same column is populated on the Cold Transfer-IN page. The user concludes the lot search is broken. The UNION subquery still runs per row, so the cost is paid for data that is thrown away.

**Fix.** Add `lot_numbers: str = ""` and `inward_transaction_no: Optional[str] = None` to TransferInListItem (and `from_warehouse` is already present on TransferInHeaderResponse), or drop the response_model so both list endpoints stay shape-identical.


## response_model=TransferListResponse strips lot_numbers_text, breaking lot search on the Transfer Out table

**services/ims_service/interunit_server.py:268** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** `list_transfers` builds a per-header `STRING_AGG(DISTINCT lot_number,' ')` subquery (interunit_tools.py:1491-1497) and sets `item["lot_numbers_text"]` on every record (interunit_tools.py:1512). `TransferListItem` (interunit_models.py:310-315) declares only `items_count`, `boxes_count`, `total_qty`, `pending_items` on top of `TransferHeaderResponse` — no `lot_numbers_text` — so the field is filtered out of the JSON.

**Failure scenario.** legacy_frontend/app/[company]/transfer/page.tsx:253 runs `searchMatch(t, transferOutSearch, [..., "lot_numbers_text"])`, and cold-transfer/page.tsx:367 does the same. Typing lot '125859' into the Transfer Out search box matches zero rows even though the backend aggregated that exact lot for the transfer, because `t.lot_numbers_text` is `undefined` on every record. The whole STRING_AGG subquery is executed on every page load and thrown away.

**Fix.** Add `lot_numbers_text: str = ""` to `TransferListItem` (interunit_models.py:310).


## response_model=TransferWithLines silently strips grn_records, lot_origin_unit, source_unit, source_storage, unit_pack_size, rm_pm_fg_type and item_category from GET /interunit/transfers/{id}

**services/ims_service/interunit_server.py:288** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** `get_transfer` (interunit_tools.py:1579-1691) deliberately computes and attaches four families of extra keys: `result["grn_records"]` (line 1672), `x["lot_origin_unit"]` on every box and line (line 1653), and `_map_box_row` adds `source_storage`, `source_unit`, `unit_pack_size`, `rm_pm_fg_type`, `item_category` (interunit_tools.py:593-598). `TransferWithLines` = `TransferHeaderResponse` + `lines: List[TransferLineResponse]` + `boxes: List[BoxResponse]` (interunit_models.py:305-307); none of those models declare any of these fields (BoxResponse is interunit_models.py:289-302). FastAPI 0.115 + Pydantic 2.9 filter the response to declared fields only, so every one of them is dropped before it reaches the browser.

**Failure scenario.** Open the Pending Transfer Stock modal and hover a challan: PendingTransfersModal.tsx:534 reads `data.grn_records || []` → always `[]`, so the 'GRN' and 'Rcvd boxes' chips never render even for a fully-received transfer. ChallanHoverCard.tsx:376-378 reads `b.lot_origin_unit` / `b.source_unit` / `b.source_storage` → all undefined, so the per-lot 'From' cold-unit chip falls back to the header value or disappears. ChallanHoverCard.tsx:353-355 and 366 read `b.unit_pack_size` / `b.rm_pm_fg_type` / `b.item_category` → undefined, so `isCountableLine` is false and the 'Total Count' chip for PM/packaging transfers never shows.

**Fix.** Either add these fields to `BoxResponse` / `TransferLineResponse` / `TransferWithLines` in interunit_models.py, or drop `response_model=TransferWithLines` from the route (the cold detail route at line 391 already documents exactly this reasoning: 'No response_model — … a strict model would strip them').


## response_model=TransferInListResponse strips lot_numbers, breaking lot search on the Transfer In table

**services/ims_service/interunit_server.py:327** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** `list_transfer_ins` computes a UNION'd `STRING_AGG(DISTINCT lot,' ') AS lot_numbers` over IN-boxes and OUT-boxes (interunit_tools.py:3246-3252) and sets `item["lot_numbers"]` (interunit_tools.py:3268). `TransferInListItem` (interunit_models.py:489-491) adds only `total_boxes_scanned` to `TransferInHeaderResponse`; `lot_numbers` is not declared and is filtered out.

**Failure scenario.** transfer/page.tsx:263 filters with `searchMatch(t, transferInSearch, [..., "lot_numbers"])`. Searching a lot number in the Transfer In tab returns nothing, so a receiver cannot find the GRN for lot 93289 by lot. Note the sibling cold endpoint `/cold-transfer-in/list` (line 350) has no response_model, so the same field DOES survive there — the two tabs behave differently for identical UI code.

**Fix.** Add `lot_numbers: str = ""` to `TransferInListItem` (interunit_models.py:489).


## GET /interunit/transfers/{id} duplicates box rows: pending_transfer_stock is joined on box_id alone, and box_id is only unique per transaction_no

**services/ims_service/interunit_tools.py:644** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** `_fetch_boxes` (called by `get_transfer`, which backs `GET /interunit/transfers/{transfer_id}`) joins `pending_transfer_stock` on `box_id` only — not on `transaction_no` and not on `pts.transfer_out_id = itb.header_id`. `pending_transfer_stock` is unique on `(box_id, transaction_no)` (see `ON CONFLICT (box_id, transaction_no)` at pending_stock_tools.py:1354/1504/1568/2169/2995), and the codebase explicitly documents that 'box_id is unique only *within* a transaction_no, and some legacy txns carry the same box_id for two different lots/items' (pending_stock_tools.py:67-71); COLD_TRANSFER_DUPBOX_INCIDENT.md records live data with exactly this collision. So one dispatch box can match N in-transit pending rows and the LEFT JOIN fans out.

**Failure scenario.** Box `90671000-1` is in transit under transaction `TR-...751` for transfer 1615 and also under `TR-...513` for transfer 1702. `GET /interunit/transfers/1615` returns that box TWICE in `boxes[]` (with different `source_unit` values). ChallanHoverCard's `groupBoxesByItem` does `g.qty += 1` and `g.netWeight += b.net_weight` per box (ChallanHoverCard.tsx:369-370), so the hover card reports 2 cartons and double the kg for a single physical box; the DC/print view built from the same array over-states the shipment.

**Fix.** Scope the join to this transfer and this transaction: `ON pts.box_id = itb.box_id AND COALESCE(pts.transaction_no,'') = COALESCE(itb.transaction_no,'') AND pts.transfer_out_id = itb.header_id AND pts.status = 'In Transit'`.


## _boxes_authoritative only collapses duplicate lines for articles present in the box payload — duplicate lines for unscanned/manual articles survive and inflate total_qty

**services/ims_service/interunit_tools.py:980** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** `agg` is built exclusively from `payload_boxes` (lines 968-978), so the de-duplication loop iterates only over articles that were scanned. Any article the operator typed manually — the mixed scan+manual case the surrounding comments (lines 823-835, 1331-1333) say is routine — is never visited, so its duplicate line rows stay in the table. Line 965 also returns early for cold sources (`if not payload_boxes or _is_cold_site(from_site): return lines`), so cold transfers get no de-duplication at all; cold_transfer_out_tools.py:289-321 inserts payload lines verbatim with no grouping on item_desc_raw, and its line_id_by_key dict (line 318) silently overwrites the earlier id when two lines share (item_desc_raw, lot_number), leaving the first line permanently box-less so _apply_box_totals never overwrites its typed qty.

**Failure scenario.** Warehouse dispatch: operator scans 100 boxes of 'CASHEW W240' (collapsed to 1 line, qty 100) and manually keys 'ALMOND NP' twice at 49 each (two surviving line rows). items_count = COUNT(DISTINCT item_desc_raw) = 2, total_qty = 100 + 49 + 49 = 198, boxes_count = 100, pending_items = 98. Cold variant: two cold lines sent with the same article+lot key, boxes all bind to the second → first keeps its typed qty 98, second becomes 100 → identical 2 items / Qty 198 output.

**Fix.** De-duplicate lines by article for every article on the header, not only the scanned ones: build the article set from `lines` and merge (sum qty/weights) before/after the box aggregation, and give cold the same treatment keyed on (article, lot). Also enforce it at the schema level with a unique index on (header_id, item_desc_raw, COALESCE(lot_number,'')).


## Cold sub-warehouse chip filter returns zero rows — from_cold_unit is only written by two unreachable code paths

**services/ims_service/interunit_tools.py:1428** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** The only two writers of interunit_transfers_header.from_cold_unit are lines 1327 (create_transfer) and 1954 (update_transfer), and both sit inside `if _is_cold_site(data.header.from_warehouse):` blocks that can never execute — create_transfer raises HTTPException(400) for cold sources at lines 1100-1107 and update_transfer at lines 1702-1709, before those blocks. The endpoint that actually creates cold dispatches, cold_transfer_out_tools.create_cold_transfer_out / edit_cold_transfer_out, never writes from_cold_unit (grep across the repo returns no other writer). Second defect in the same predicate: `h.from_site ILIKE 'cold%'` excludes headers stored with a concrete cold from_site — ColdTransferOutCreate.from_warehouse is documented as "'Cold Storage' or specific cold unit" (cold_transfer_out_tools.py:83) and COLD_STORAGE_SITE_NAMES (pending_stock_tools.py:37-48) accepts 'savla d-39', 'rishi', 'supreme', 'eskimo'.

**Failure scenario.** Operator clicks the 'Savla D-39' chip → GET /interunit/transfers?from_site=Savla%20D-39. _normalize_cold_unit maps it to 'Savla D-39', the WHERE becomes `h.from_site ILIKE 'cold%' AND h.from_cold_unit ILIKE '%Savla D-39%'`; every cold transfer created since the 2026-06-06 cold split has from_cold_unit NULL, so both the COUNT(*) at line 1455 and the row query return 0. The UI shows 'No transfers' and total 0 for a unit that dispatched all week. A transfer stored as from_site='Rishi' is missed twice over.

**Fix.** Either backfill/populate from_cold_unit in cold_transfer_out_tools (it already reads cold_storage_data->>'unit' per box for park_in_pending), or resolve the chip at query time from the boxes' pending_transfer_stock JSONB the way _fetch_boxes:634-640 does, and widen the site predicate to `(h.from_site ILIKE 'cold%' OR LOWER(TRIM(h.from_site)) = ANY(:cold_aliases))`.


## list_transfers pairs COUNT(DISTINCT item_desc_raw) with an un-deduplicated SUM(qty) over the same table, so the badge hides exactly the rows the total counts

**services/ims_service/interunit_tools.py:1480** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** items_count de-duplicates by item_desc_raw while total_qty sums every physical row. When a header carries duplicate line rows for the same item (which the cold path always produces — see the root-cause finding), the two numbers describe different row sets: the badge stays believable ('2 items') while the quantity silently double-counts. There is no cross-check against boxes_count, even though the very next subquery already computes it, and pending_stock_tools.py:2537-2543 has already adopted the safer rule ('COUNT(interunit_transfer_boxes) when > 0, else SUM(qty)') for the pending modal. The derived `pending_items` at line 1511 inherits the error: max(0, 198 − 100) = 98 phantom pending items on a fully dispatched transfer.

**Failure scenario.** Any header with N duplicate lines for one article. TRANS202608171318: 100 line rows / 2 distinct item_desc_raw → the row renders '2 Items' and 'Qty: 198' while interunit_transfer_boxes holds 100 rows and pending_transfer_stock holds 100 'In Transit' rows. An operator reconciling the truck against the screen sees a 98-box surplus that does not exist.

**Fix.** Make the badge and the quantity read the same row set, and prefer the physical boxes when they exist:

  COALESCE(NULLIF(bc.boxes_count, 0), lc.total_qty, 0) AS total_qty

i.e. add `bc.boxes_count` to the COALESCE so a box-backed dispatch reports its box count (the definition PENDING_TRANSFER_FIX_CHANGES.md §2 already locked: "expected boxes = COUNT(interunit_transfer_boxes), not SUM(qty)"), and fall back to SUM(qty) only for line-only (Article Entry) transfers. Keep items_count as-is. This is defence in depth — the data fix in cold_transfer_out_tools.py is still required, because the DC, the hover card and reconcile all read the lines directly.


## boxes_count collapses every blank box_id to one row — COALESCE(box_id, id::text) never fires because box_id is written as '' not NULL

**services/ims_service/interunit_tools.py:1487** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: plausible

**Problem.** COALESCE only substitutes SQL NULL. Both writers coerce a missing box_id to an empty string, never NULL: create_transfer line 1277 `"box_id": box.box_id or ""` and update_transfer line 1901 `"box_id": box.box_id or ""`. So for boxes without an id the DISTINCT key is the literal '' for every row and N boxes count as 1. Separately, duplicate real box_ids inside one header are explicitly allowed: the duplicate guard at lines 1236 / 1860 only runs `if bid and tno and tno != "DIRECT"`, so DIRECT dispatches and boxes with a blank transaction_no may repeat the same box_id, and _schema_dump.json shows interunit_transfer_boxes has NO unique constraint on (header_id, box_id) — only PK(id) and two FKs.

**Failure scenario.** A 40-box DIRECT dispatch where the scanner payload omits box_id: 40 rows inserted with box_id='' → boxes_count = 1. The list card shows 1 box, and pending_items = total_qty − 1 (e.g. 80 − 1 = 79) instead of 0. Same collapse for 10 rescanned boxes sharing box_id 'CS-4471' with transaction_no 'DIRECT'.

**Fix.** Use `COUNT(*)` (each row IS a physical box — that is what reconcile_transfer_to_order:1744-1747 does) or, if de-duplication is required, key it on the real identity `COUNT(DISTINCT COALESCE(NULLIF(TRIM(box_id), ''), id::text))` and add a unique index on (header_id, box_id) where box_id <> ''.


## Pagination is unstable: ORDER BY h.{sort_by} has no unique tiebreaker under LIMIT/OFFSET

**services/ims_service/interunit_tools.py:1499** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: plausible

**Problem.** sort_by is restricted to {challan_no, stock_trf_date, from_site, to_site, status, created_ts} (line 1448) — five of those six are massively non-unique. stock_trf_date is a DATE column (per _schema_dump.json), and status/from_site/to_site have a handful of values across 452 headers. Postgres gives no ordering guarantee among equal sort keys, and the page-1 and page-2 queries are separate executions that can pick different plans (index scan vs. seq scan + top-N sort). With OFFSET, ties re-shuffling between the two executions duplicates some rows and drops others. Note the SQL-injection angle is genuinely closed — sort_by falls back to 'created_ts' when not in the allowlist and sort_order is reduced to a boolean — so only the stability defect is real here.

**Failure scenario.** 25 transfers all stamped stock_trf_date = 12-08-2026, per_page=10, sort_by=stock_trf_date, sort_order=desc. Page 1 (OFFSET 0) returns challans A..J. Page 2 (OFFSET 10) re-sorts the tie group and returns C, D, K..R — challans C and D appear on both pages while two others (say S and T) never appear on any page, even though `total` correctly says 25.

**Fix.** Append the primary key as a final tiebreaker: `ORDER BY h.{sort_by} {direction}, h.id DESC`. For the deep-scroll case prefer keyset pagination on (sort_key, id).


## lot_numbers_text is computed by the backend and silently stripped by the response_model, so the UI's lot search can never match

**services/ims_service/interunit_tools.py:1512** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** list_transfers builds lot_numbers_text via a dedicated STRING_AGG subquery (lines 1491-1497) and attaches it to every record, but the route declares `response_model=TransferListResponse` (interunit_server.py:268) and TransferListItem (interunit_models.py:310-315) declares only items_count, boxes_count, total_qty, pending_items on top of TransferHeaderResponse. Pydantic v2 ignores undeclared fields, so FastAPI drops lot_numbers_text from the serialized response. The frontend depends on it: legacy_frontend/app/[company]/transfer/page.tsx:253 and app/[company]/cold-transfer/page.tsx:367 both list "lot_numbers_text" in the searchMatch field array. The whole STRING_AGG subquery is also pure wasted work on every list request.

**Failure scenario.** Operator types lot 'CF100326' into the Transfer Out search box. The client-side searchMatch reads t.lot_numbers_text, which is undefined on every record because the server stripped it, so zero transfers match even though three of the loaded records carry boxes with that lot. The operator concludes the lot was never dispatched.

**Fix.** Add `lot_numbers_text: str = ""` to TransferListItem in interunit_models.py (and to the cold list item model), or drop the subquery if the field is dead.


## update_transfer deletes transfer-out boxes that received Transfer-IN rows still reference — FK violation / orphaned receipt

**services/ims_service/interunit_tools.py:1763** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** interunit_transfer_in_boxes.transfer_out_box_id is a FOREIGN KEY to interunit_transfer_boxes.id (confirmed in _schema_dump.json constraints; created by main.py:71-75 with no ON DELETE clause, i.e. NO ACTION). update_transfer has no status guard — line 1719 explicitly says "No status restriction — authorized users can edit transfers in any status" — and unlike delete_transfer (which clears interunit_transfer_in_boxes at line 2046 before deleting out-boxes at 2081) it deletes the out-boxes with the receipt rows still pointing at them. Compounding it, restore_to_source has already run at line 1723 and park_in_pending (line 1921) skips any box whose source row is gone (pending_stock_tools.py:1311-1313), so a re-park after a completed receipt silently parks nothing.

**Failure scenario.** TRANS20260812... is dispatched (60 boxes), received via /interunit/transfer-in with each scanned box carrying transfer_out_box_id (TransferInBoxCreate declares the field, interunit_models.py:337). A supervisor then edits the vehicle number via PUT /interunit/transfers/{id}. Line 1763 raises ForeignKeyViolation → unhandled → HTTP 500, and because restore_to_source already ran inside the same request the operator sees a CORS-masked 'Failed to fetch' with no idea whether the transfer survived. If the FK were ever relaxed, the alternative outcome is a GRN whose box references dangle.

**Fix.** Guard the edit: reject (or take a dedicated reverse-then-rewrite path) when interunit_transfer_in_header rows exist for the transfer, the way create_transfer_in already refuses a second GRN at lines 2210-2214. At minimum NULL out interunit_transfer_in_boxes.transfer_out_box_id for the header before the DELETE, and re-point it afterwards.


## CREATE UNIQUE INDEX inside a swallowed try/except poisons the transaction and 500s every subsequent acknowledge

**services/ims_service/interunit_tools.py:2542** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** This DDL runs on EVERY acknowledge call, inside the request's transaction. If the index does not yet exist and the table already contains duplicate (header_id, box_id) rows — which is possible because create_transfer_in inserts boxes with a plain INSERT and no conflict handling (line 2271) — the CREATE raises a unique violation. In PostgreSQL a failed statement aborts the whole transaction; `except Exception: pass` hides the cause but does NOT recover it, so the very next statement (the STBR slot count at line 2561, which is not inside any try) fails with InFailedSqlTransactionError and the request 500s. Every acknowledge on that database then fails identically with a misleading error, and the actual root cause is unlogged.

**Failure scenario.** A database where an older create_transfer_in wrote two rows with header_id=195, box_id='34732254-5'. On the next acknowledge request the CREATE UNIQUE INDEX raises 23505, the except swallows it, the transaction is aborted, and the SELECT COUNT(*) FROM pending_transfer_stock at line 2561 raises 'current transaction is aborted, commands ignored until end of transaction block'. get_db rolls back and FastAPI returns 500. The receiving operator cannot acknowledge any box on any GRN, with no diagnostic beyond a generic 500.

**Fix.** Move the index creation into _ensure_interunit_schema / a migration run once at startup, wrap it in its own SAVEPOINT (db.begin_nested()) if it must stay inline, and log the exception instead of `pass` so a pre-existing duplicate is visible.


## acknowledge UPSERT keyed on (header_id, box_id) collapses two physical boxes into one while reporting both as acknowledged

**services/ims_service/interunit_tools.py:2616** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** The conflict target ignores transaction_no, but a box's identity in this system is (box_id, transaction_no) — cold_stocks enforces UNIQUE(transaction_no, box_id) and pending_transfer_stock enforces UNIQUE(box_id, transaction_no). COLD_TRANSFER_DUPBOX_INCIDENT.md §'Deferred: true systemic fix' items 2 and 3 call out this exact index and this exact ON CONFLICT target as the unfixed collapse, and the code at lines 2542-2544 and 2616 still uses the two-column key. The only thing blocking it today is a FRONTEND guard (coldtransferform/page.tsx:1840) — and POST /interunit/transfer-in/{id}/acknowledge-batch (interunit_server.py:476) bypasses that guard entirely. The batch response then over-reports: `"count": len(results)` at line 2723 counts loop iterations, not rows written.

**Failure scenario.** A 100-box GRN where boxes 90671000-1/TR-...751 and 90671000-1/TR-...513 are two distinct physical cartons (the documented Mode A collision). acknowledge-batch UPSERTs both onto the same interunit_transfer_in_boxes row; the second overwrites the first's transaction_no and net_weight. The API returns count=100 and 100 box objects, but SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id=<h> = 99. list_transfer_ins shows total_boxes_scanned=99 for a receipt the operator saw as 100, and _autofinalize_if_complete (acked=99 vs 100 in transit) never finalizes, so the GRN sticks at 'Partial (GRN raised)' forever and one carton is lost from inventory.

**Fix.** Land incident-doc items 2+3 together: replace the index with UNIQUE (header_id, box_id, COALESCE(transaction_no,'')) — the doc verified 0 existing violations under the COALESCE key — and retarget the ON CONFLICT to that expression index. Also set `count` to the number of distinct rows actually written.


## acknowledge-batch has no per-box SAVEPOINT — one DB error aborts and rolls back the entire batch

**services/ims_service/interunit_tools.py:2711** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** The docstring promises 'Conflicts on individual boxes are surfaced per row instead of failing the entire batch', but only HTTPException is caught. Any database error (IntegrityError from the ON CONFLICT arbiter being absent, a NOT NULL violation, a deadlock) propagates out of the loop, get_db rolls back (shared/database.py:19), and every box acknowledged earlier in the batch is discarded. And because there is no db.begin_nested() per iteration — unlike _autofinalize_if_complete which correctly SAVEPOINT-isolates itself at line 3039 — even catching the broader exception would not help: once a statement fails, Postgres aborts the transaction and every remaining box in the loop fails with 'current transaction is aborted'.

**Failure scenario.** An operator submits a 200-box batch. Box #137 hits an IntegrityError (e.g. the uq_transfer_in_boxes_header_box index does not exist so ON CONFLICT (header_id, box_id) raises InvalidColumnReference). The exception escapes acknowledge_pending_boxes_batch, get_db rolls back, and the operator receives a 500 with all 136 successfully-scanned boxes discarded. They must rescan the entire truck.

**Fix.** Wrap each iteration in `with db.begin_nested():` and catch Exception (not just HTTPException), appending a per-box failure record. This matches the pattern already used at line 3039.


## list_transfer_ins / list_cold_transfer_ins sort on a non-unique column with no tiebreaker — rows repeat and vanish across pages

**services/ims_service/interunit_tools.py:3258** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: unverified

**Problem.** valid_sort (line 3223) accepts 'status' and 'receiving_warehouse', both massively non-unique — hundreds of GRNs share status='Received' and receiving_warehouse='W202'. PostgreSQL gives no stable ordering for ties, and each page is a separate query with its own plan, so a row on page 1 can reappear on page 2 while another is never returned. 'grn_date' and 'created_at' are both written as CURRENT_TIMESTAMP (lines 2233, 2485), which in Postgres is transaction start time, so co-created GRNs also tie. list_cold_transfer_ins:3381 has the identical defect.

**Failure scenario.** 45 GRNs, all status='Received', per_page=10, sort_by=status&sort_order=desc. Page 1 returns ids [12,7,33,...]; page 2 (OFFSET 10) is planned independently and may re-emit id 33 while omitting id 19. The operator paging through the Transfer-IN register sees GRN-33 twice and never sees GRN-19 — and a per-page export silently drops it from the reconciliation.

**Fix.** Append a unique tiebreaker to both queries: ORDER BY h.{sort_by} {direction}, h.id DESC. Apply the same to list_cold_transfer_ins (line 3381) and list_transfers (line 1499).


## generate_transfer_in_qrs calls time.time() but `time` is never imported — the endpoint always raises NameError (HTTP 500)

**services/ims_service/interunit_tools.py:3461** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** interunit_tools.py imports only json, datetime, typing, fastapi, sqlalchemy, shared.logger, shared.timezone and the local models/pending_stock_tools (lines 1-22); there is no `import time` at module level and none inside generate_transfer_in_qrs (3424-3488). The reference at 3461 is reached on every successful call — after the 404/409/400 guards pass and after the header/boxes have been read — so the function raises NameError: name 'time' is not defined and FastAPI returns 500. Nothing else in the module uses `time`, so this is a missing import, not a shadowed name.

**Failure scenario.** A receiver finishes acknowledging 27 boxes on a GRN and clicks 'Generate QRs'. The header exists, inward_transaction_no is NULL, boxes are present — every guard passes — then line 3461 throws. The user gets an opaque 500 (which, per the note in interunit_models.py:161-166, also loses its CORS headers and surfaces in the browser as 'Failed to fetch'), and because the commit at 3486 never runs, no inward_box_id is written. The action is unrecoverable through the UI.

**Fix.** Add `import time` to the module imports at the top of interunit_tools.py (or replace the line with `base = str(int(now_ist().timestamp() * 1000))[-8:]`, which matches the IST convention used one line above and needs no new import).


## /transfer-dashboard/all-data stamps per-transfer aggregates onto every fanned-out line row, multiplying box and issue totals by the line count

**services/ims_service/transfer_dashboard_server.py:90** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** `records` is the product of `interunit_transfers_header h INNER JOIN interunit_transfers_lines l` (line 57-58) — one row per LINE, with all header columns repeated. Lines 90-91 then write the transfer-level `box_count` onto each of those duplicated rows, and lines 144-150 do the same for `issue_count`, `issue_weight`, `issue_items`, `issue_details`. Any consumer that sums these columns multiplies by the number of line items. `"total": len(records)` (line 152) is likewise a line count, not a transfer count, despite the field name.

**Failure scenario.** Transfer 1615 has 3 line items and 40 boxes. `/all-data` returns 3 records each with `box_count: 40`. The dashboard KPI at legacy_frontend/app/[company]/transfer/dashboard/page.tsx:260 (`filtered.reduce((s,r)=> s + (r.box_count||0), 0)`) reports 120 boxes instead of 40. Same at page.tsx:265 for `issue_count` (a transfer with 2 issues and 3 lines reports 6). page.tsx:878 then computes `perBoxNet = line_net_weight / hdr.box_count`, dividing one line's weight by the whole transfer's box count.

**Fix.** Return the per-transfer aggregates in a separate keyed map (e.g. `{"records": [...lines], "transfers": {id: {box_count, issue_count, issue_weight, issue_details}}}`) instead of denormalising them onto every line row, and rename `total` to `total_lines` or emit `COUNT(DISTINCT h.id)`.



# MEDIUM (101)

## OFFSET pagination over ORDER BY created_at DESC with no unique tiebreaker — rows repeat and vanish across pages

**legacy_backend/services/ims_service/job_work_server.py:887** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** created_at is `TIMESTAMP DEFAULT NOW()` (line 86) and Postgres NOW() is the transaction timestamp, so every header created inside one transaction (bulk import / seed) shares an identical value; with no secondary key in the ORDER BY the LIMIT/OFFSET window is free to return rows in a different order per query. The sort is also newest-first, so any record inserted between the page-1 and page-2 requests shifts the whole window by one. The COUNT at line 840 is a separate statement, so `total`/`total_pages` can also disagree with what the row query returns. Identical pattern in /material-in/list at line 1733 (created_at DEFAULT NOW(), line 166).

**Failure scenario.** A 40-row bulk import gives 40 headers the same created_at. The user pages through the Job Work list: header #15 appears as the last row of page 1 and again as the first row of page 2 while header #16 is never shown at all. Even without ties, one new challan submitted while the user is on page 1 pushes the previously-last row onto page 2 (duplicate) and drops one row off the end.

**Fix.** Make the sort total: `ORDER BY h.created_at DESC, h.id DESC` (and the same on ir.created_at DESC, ir.id DESC), or switch to keyset pagination on (created_at, id).


## IR numbers are generated from COUNT(*)+1, so deleting a receipt makes the next one collide

**legacy_backend/services/ims_service/job_work_server.py:1346** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** The sequence is derived from the live row count, not from a monotonic sequence or MAX(existing suffix). DELETE /job-work/material-in/{ir_id} (line 1888) physically removes rows, so the count goes back down. There is also no uniqueness constraint on ir_number to catch it, and two concurrent receives on the same header read the same count.

**Failure scenario.** JWO JW/0042 has IR-JW/0042-01, -02, -03. The user deletes IR-JW/0042-02 (a mis-keyed receipt). The next receive computes COUNT(*)+1 = 3 and issues IR-JW/0042-03 — a duplicate of the existing one. The prior-IR history at line 1064-1088 and the emailed IR breakdown now show two different receipts under one number, and any downstream match by ir_number picks an arbitrary one.

**Fix.** Derive the suffix from MAX of the existing numeric suffix for that header (or a dedicated per-header sequence) and add a UNIQUE constraint on (header_id, ir_number).


## /material-in/list has no bounds on page or per_page — page=0 produces a negative OFFSET and a 500

**legacy_backend/services/ims_service/job_work_server.py:1707** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: plausible

**Problem.** Unlike /job-work/list (line 817-818, which correctly uses `Query(1, ge=1)` and `Query(15, ge=1, le=1000)`), this endpoint validates nothing. page=0 or a negative page yields a negative OFFSET, which Postgres rejects with `OFFSET must not be negative`; per_page has no upper bound so a caller can request the whole table in one shot.

**Failure scenario.** The FE resets a paginated table to index 0 (a common 0-based/1-based mix-up) and calls GET /job-work/material-in/list?page=0&per_page=15 -> offset=-15 -> psycopg2 DataError -> HTTP 500 instead of an empty or first page. GET ...?per_page=999999 returns every inward receipt with its aggregates in one response.

**Fix.** Mirror /job-work/list: `page: int = Query(1, ge=1)` and `per_page: int = Query(15, ge=1, le=1000)`, and clamp offset to >= 0.


## cum_loss_pct returns (waste + rejection) / dispatched instead of the actual loss

**legacy_backend/services/ims_service/job_work_server.py:1829** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** cum_accounted is fg + waste + rejection (line 1827), so (cum_accounted - cum_fg) is simply waste + rejection. The loss is the UNACCOUNTED portion, which the very next lines compute correctly as `cum_unaccounted = max(0, dispatched_kgs - cum_accounted)` (line 1828). Every other loss formula in the codebase uses (dispatched - accounted)/dispatched — see job_work_server.py:902-903 and jobwork_dashboard_server.py:401-409 — so the material-in detail screen contradicts the list and the dashboard for the same JWO.

**Failure scenario.** JWO dispatched 1,000 kg; cumulative FG 800, waste 50, rejection 0. Real loss = 150 kg = 15%. GET /job-work/material-in/{ir_id} returns cumulative.cum_unaccounted_kgs = 150 but cum_loss_pct = 5.0. The screen shows '150 kg unaccounted' next to '5% loss' — and 5% sits under the usual 10% excess threshold, so the receipt looks acceptable.

**Fix.** `cum_loss_pct = round(cum_unaccounted / dispatched_kgs * 100, 2) if dispatched_kgs > 0 else 0`.


## Deleting one partial receipt downgrades a fully-received JWO back to partially_received

**legacy_backend/services/ims_service/job_work_server.py:1927** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** The recalculation only counts remaining receipts; it never looks at their receipt_type. submit_material_in sets 'fully_received' when receipt_type == 'final' (line 1563-1566), and that information is still present on the surviving rows — it is simply ignored here.

**Failure scenario.** JWO JW/0042 is closed by IR-01 (receipt_type='final' -> status fully_received). A user later deletes an extra IR-02 that was entered by mistake. remaining = 1 -> status is forced to 'partially_received'. The completed JWO reappears in the 'Open/Pending JWOs' KPI (jobwork_dashboard_server.py:195) and, if older than 30 days, in overdue_jwos, permanently until someone re-receives it.

**Fix.** Recompute as: fully_received if any surviving receipt has receipt_type='final', else partially_received if any remain, else 'sent'.


## Excel/PDF extractor emits a line-item shape the /out endpoint does not read — quantities land as zero

**legacy_backend/services/ims_service/job_work_server.py:2283** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: plausible

**Problem.** /job-work/extract-excel (line 2365) and /job-work/extract-pdf (its prompt schema, lines 2443-2456) produce flat `quantity_kgs` / `quantity_boxes` / `rate_per_unit` keys and no `net_weight`. The consumer, POST /job-work/out, reads a nested dict — `qty = item.get("quantity", {})`, `kgs = qty.get("kgs", 0)`, `boxes = qty.get("boxes", 0)` (lines 621-623) — and `item.get("rate_per_kg", 0)` (line 650), `str(item.get("net_weight", ""))` (line 654). Three of the four numeric fields drift; only `description` and `remarks` line up.

**Failure scenario.** The extracted JSON is posted through to /job-work/out (the two endpoints are designed as a pair). `quantity` is absent -> kgs=0, boxes=0; `rate_per_kg` absent -> 0; `net_weight` absent -> ''. The challan is stored with 0 kg dispatched and 0 boxes, so every dispatched-weight KPI reads 0 for it, and the empty net_weight then breaks every CAST(net_weight AS NUMERIC) query (see the critical finding above).

**Fix.** Have /out accept both shapes (`item.get('quantity',{}).get('kgs') or item.get('quantity_kgs')`, same for boxes/rate) and default net_weight from quantity_kgs when absent, or make the extractors emit the nested `quantity` object and `rate_per_kg`/`net_weight`.


## Monthly trend sorts MM-YYYY strings and truncates with LIMIT 12 — wrong months, silently dropped data

**legacy_backend/services/ims_service/job_work_server.py:2762** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** month_year is the text 'MM-YYYY' (SUBSTRING from position 4 of a DD-MM-YYYY string, line 2754), so DESC ordering compares the MONTH first and the year second. LIMIT 12 then keeps the 12 lexicographically largest buckets, not the 12 most recent. Any row stored in ISO format instead yields a garbage key ('2026-03-15' -> '6-03-15'), which becomes its own bucket.

**Failure scenario.** Data spans Jan-2025 to Aug-2026 (20 buckets). Sorting DESC gives 12-2025, 12-2024, 11-2025, 11-2024, 10-2025 ... so the chart renders December and November of both years while 08-2026, 07-2026, 06-2026 ... (every month whose MM < '10') are cut off by LIMIT 12. The 'monthly trend' chart shows neither the last 12 months nor a contiguous series, and the missing months are invisible to the user.

**Fix.** Bucket on a real date — `TO_CHAR(to_date(h.job_work_date,'DD-MM-YYYY'),'YYYY-MM')` (guarded for bad values) — order by that key DESC, and reverse in Python for display.


## list_pending_transfers counts LINE- placeholder rows as boxes but excludes them from the shortfall math

**legacy_backend/services/ims_service/pending_stock_tools.py:2485** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** Within the same SELECT, total_boxes counts every pending row while the unallocated_boxes computation (line 2501) deliberately filters out the synthetic 'LINE-<line_id>-<n>' rows written by park_lines_in_pending (line 1510) for box-less transfers. count_remaining_in_transit (line 1955) also excludes them. So for an article-only transfer the two numbers on the same modal row are computed against different populations.

**Failure scenario.** A warehouse->warehouse transfer of 300 units with no scanned boxes is parked as 300 'LINE-…' rows. The Pending Transfer Status modal shows total_boxes = 300 (looks fully tracked) and simultaneously unallocated_boxes = 300 - 0 - 0 = 300, i.e. a red 'shortfall 300' badge on a transfer where nothing is missing.

**Fix.** Report total_boxes with the same FILTER (real boxes) and surface the LINE- unit count separately, or exclude LINE- rows from both sides consistently.


## Pending-stock read endpoints return the entire dataset with no pagination and per-row correlated subqueries

**legacy_backend/services/ims_service/pending_stock_tools.py:2569** &nbsp;|&nbsp; perf &nbsp;|&nbsp; verdict: confirmed

**Problem.** GET /interunit/pending-stock (interunit_server.py:107-126) exposes no page/per_page; the function returns every in-transit transfer plus a second full-population query for chip counts (line 2606). Each row of the tracked branch runs 3 correlated subqueries (lines 2498-2504) and each row of the orphan branch runs 7 more (lines 2537-2558) — the box-count subquery alone is repeated 3 times per row instead of being computed once. It then runs a second unbounded UNION query for the chips. pending_by_lot (line 2691/2730) and in_transit_by_lot (line 2803) are likewise unbounded.

**Failure scenario.** With ~1,200 open transfers the modal issues one query that executes roughly 5,000 correlated subqueries against interunit_transfer_boxes / interunit_transfers_lines / interunit_transfer_in_boxes, then a second full scan for chips, and serializes all 1,200 records to the browser on every open — seconds of latency and no way for the client to ask for less.

**Fix.** Add page/per_page with a matching COUNT, and lift the repeated `(SELECT COUNT(*) FROM interunit_transfer_boxes ...)` into a single LEFT JOIN LATERAL / CTE computed once per header.


## Cold page reports a failed pending-boxes fetch as 'nothing left in transit'

**legacy_frontend/app/[company]/cold-transfer/coldtransfer-in/page.tsx:386** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** Any error from getPendingBoxesByTransferOut (network blip, 500, auth expiry) is indistinguishable from an empty result: the code blanks boxes AND lines and raises the same noPendingForCold flag that renders the amber banner at L2314-2320 ('All its boxes have already been received'). The error is not logged or shown.

**Failure scenario.** The API pod restarts while the operator searches TRANS202608171200. The page loads the header, shows 'Nothing left in transit to receive for this transfer. All its boxes have already been received', and disables Confirm Receipt (L3334). The operator concludes the transfer is done and the 40 in-transit cartons on the dock are never received.

**Fix.** Distinguish the two: only set noPendingForCold when the request succeeded with boxes.length === 0; on a thrown error show a red 'Could not load in-transit boxes — retry' state with a retry button.


## Cold page fetches per-item lookups sequentially inside the search, with a hardcoded localhost base URL and no auth header

**legacy_frontend/app/[company]/cold-transfer/coldtransfer-in/page.tsx:538** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** Two awaited round-trips per unique item, run serially inside loadTransferDetails and BEFORE the pending-GRN restore at L566, so the whole receive screen is blocked. The second call bypasses InterunitApiService entirely: it re-derives the base URL (falling back to http://localhost:8000 in any environment where NEXT_PUBLIC_API_URL is unset) and sends no Authorization header, unlike fetchJSON/getAuthHeaders in interunitApiService.ts. `res.ok` is checked but a non-ok response is silently ignored, and the response shape `catData.items` is assumed without guarding.

**Failure scenario.** A cold receipt with 25 distinct articles issues up to 50 serial requests; at 150 ms each the operator waits ~8 s after pressing Search before the transfer appears and before acknowledgements are restored. If the deployment does not set NEXT_PUBLIC_API_URL, every categorial-search hits localhost:8000 from the browser and fails, so group_name/item_subgroup stay blank on every cold-stock row — silently, since the catch only console.warns.

**Fix.** Move the categorial lookup into InterunitApiService (shared base URL + auth), run the per-item lookups with Promise.all after the transfer is rendered rather than inside the blocking search, and guard the response shape.


## Cold-storage details are matched to boxes by exact article string, so a case/whitespace difference drops them

**legacy_frontend/app/[company]/cold-transfer/coldtransfer-in/page.tsx:1910** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: plausible

**Problem.** coldStorageItems is keyed by the transfer-LINE name (item_desc_raw, L495-507) while coldDataForBox is called with the acknowledged row's article (L2072/L2165), which for box-derived rows is interunit_transfer_boxes.article / pending_transfer_stock.article. Every other article join in this codebase normalizes case and whitespace first (regular page L235 `String(article).trim().toUpperCase()`), but these two lookups are exact ===.

**Failure scenario.** Line item_desc_raw = 'CASHEW W240 ' (trailing space) and box.article = 'CASHEW W240'. coldStorageItems['CASHEW W240'] is undefined, so coldDataForBox returns all-null: every cold_transfer_inboxes / cfpl_cold_stocks row for that receipt lands with NULL vakkal, item_mark, group_name, exporter, rate and value — the entire Cold Storage Details form the operator filled in is silently discarded, with a success toast.

**Fix.** Build and read coldStorageItems through a normalized key (trim + toUpperCase), and warn on Confirm if any acknowledged article has no cold-details entry.


## Cold page's Edit / Re-open receipt call the interunit endpoints for a receipt that lives in the cold tables

**legacy_frontend/app/[company]/cold-transfer/coldtransfer-in/page.tsx:2267** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** A completed cold receipt is stored in cold_transfer_in_headers/cold_transfer_inboxes, and finalize_cold_transfer_in DELETEs the interunit staging header and its boxes once status is Received (cold_transfer_in_tools.py L348-358). Both buttons only render when transferData.status === 'Received' (L2245/L2257), i.e. exactly when the interunit rows are gone. reopen_transfer_in queries interunit_transfer_in_header WHERE status='Received' and raises 404 (interunit_tools.py L3741-3749); editTransferIn/getTransferInByTransferOut read the same table. The cold module exposes deleteColdTransferIn/getColdTransferInById for this purpose.

**Failure scenario.** hrithik opens a received Savla D-39 receipt to fix a lot number, clicks 'Edit receipt' → the dialog fetches, finds no header and closes with 'No transfer-in found to edit'. Clicking 'Re-open receipt' returns 404 'No Received transfer-in found for this transfer.' The cold receipt cannot be corrected from the page that created it.

**Fix.** On the cold page, route Edit/Re-open through cold-specific endpoints (cold header + cold boxes + <company>_cold_stocks reversal), or hide both buttons there until those endpoints exist.


## Pending-stock lookup fires one un-abortable, un-authenticated request per result row and swallows every failure

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:661** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** Three problems in one effect (lines 643-686). (1) N+1 fan-out: one request per unique (lot, item, company) across the whole result set, re-issued on every `results` change — up to 100 concurrent requests per settled search, since the search itself can return 100 rows (finding #3). (2) The `cancelled` flag at line 658 only gates the final `setPendingMap`; the in-flight requests are never aborted, so a fast retype leaves two full fan-outs racing. (3) This raw `fetch` sends no headers at all, whereas every call through `ColdStorageApiService` attaches `Authorization: Bearer <accessToken>` (coldStorageApiService.ts:19-29) — and both the non-OK branch (line 670) and the catch (line 675) are silent, so a 401 is indistinguishable from 'nothing pending'.

**Failure scenario.** Operator searches 'CASHEW' and gets 100 piles. The browser fires ~100 parallel GETs to /interunit/pending-stock/by-lot; the operator refines the search two seconds later and 100 more fire while the first 100 are still open, saturating the browser's per-host connection pool and stalling the search request itself. If the endpoint is auth-protected, all 200 return 401, every one is swallowed, and `CartonCellWithPending` renders the bare total with no '+N in transit' badge (line 403-405) — hiding exactly the in-transit context the comment block at lines 309-320 says operators depend on.

**Fix.** Replace the per-row fan-out with a single batch endpoint (POST a list of lot/item/company keys), pass an AbortController signal so a new search cancels the previous batch, route the call through a helper that attaches the auth header, and set an explicit error state when the lookup fails instead of rendering it as zero pending.


## 'Request Qty' and 'Remaining' read articles[0] after the add handler has zeroed it, so the summary always claims the dispatch is complete

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:3756** &nbsp;|&nbsp; ux &nbsp;|&nbsp; verdict: confirmed

**Problem.** `handleAddArticleToList` resets the article to `quantity_units: 0` after a successful add (lines 1956 and 1981), so immediately after every add `articles[0].quantity_units` is 0 and the Request Qty / Remaining tiles read 0 regardless of how many boxes are on the list. When the operator starts a SECOND pile in article #1, Request Qty shows only that new pile's qty while `scannedBoxes.length` is the running total of ALL piles, so the subtraction compares unrelated numbers and `Math.max(0, ...)` clamps the mismatch to 0. The header banner at line 3486-3490 has the same defect and also ignores articles[1..n] entirely.

**Failure scenario.** Operator adds pile A (60 boxes) — the article resets, so the tiles read Total Boxes 60 / Request Qty 0 / Remaining 0. Operator then selects pile B and types 40 into the qty field before clicking Add: the tiles now read Total Boxes 60 / Request Qty 40 / Remaining max(0, 40-60) = 0, i.e. green 'complete' while pile B has not been added at all. The operator submits a 60-box challan believing 100 boxes are on it.

**Fix.** Derive the summary from the committed list rather than the in-progress article: show `scannedBoxes.length` as the total and, if a target is wanted, track a separate 'planned quantity' accumulated per added pile. Drop the `articles[0]`-only assumption from both the summary tiles and the header banner.


## innercoldtransfer search: no AbortController, silent catch, and stale results left on screen after a failed query

**legacy_frontend/app/[company]/cold-transfer/innercoldtransfer/page.tsx:68** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: plausible

**Problem.** Same race as the cold OUT picker: the 400 ms debounce (line 86) cancels only pending timers, not in-flight requests, and `setResults` is applied unconditionally, so the last response to resolve wins irrespective of which query produced it. `doSearch` also depends on `storageLocation`, so toggling a location chip mid-flight can interleave a location-filtered and an unfiltered response. The `catch { setResults([]) }` at line 81 swallows every error and leaves `showResults` at its previous value — so on a failure after a successful search the table keeps rendering the PREVIOUS query's rows, which are still fully clickable. `data.results` is dereferenced with no shape guard.

**Failure scenario.** Operator searches lot '125860' (1 row), then types 'ALM' in the description field. The description query 500s; the catch sets `results` to [] but `showResults` stays true from the previous search — the UI flips to 'No results found.' with no error. Worse, in the reverse order (broad query slow, narrow query fast), the stale broad result set repaints over the narrow one and `handleSelect` (line 94) captures the wrong `record.id` as `stock_record_id`, which combined with finding #9 relabels an unrelated pile's lot numbers.

**Fix.** Add a request-sequence guard or AbortController and only apply `setResults` for the latest request; on error, set an explicit error state and clear `showResults` so stale rows cannot be clicked.


## innercoldtransfer fetches storage locations from the DB and then discards them, rendering a hardcoded list that disagrees with the sibling form

**legacy_frontend/app/[company]/cold-transfer/innercoldtransfer/page.tsx:236** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** `storageLocations` is written but never read — `LocationChips` (line 20-43) always iterates the hardcoded `COLD_STORAGE_LOCATIONS` at line 18. The request is therefore pure overhead, its non-OK branch is a silent no-op (only `res.ok` is checked, with no else), its error is console-only, it carries no Authorization header (unlike `ColdStorageApiService`, coldStorageApiService.ts:19-29), and its dep array omits `company` even though the URL interpolates it. The hardcoded list also drifts from the sibling form: this file lists `["Savla Bond", "Savla D-39", "Savla D-514", "Rishi", "Supreme", "Eskimo"]` while coldtransferform:268 lists `["Cold Storage", "Rishi", "Savla D-39", "Savla D-514", "Supreme", "Eskimo"]`. The selected chip is passed straight through as `params.storage_location` (line 77), and the backend only special-cases the split 'Savla' + unit representation for 'savla d-39' / 'savla d-514' (cold_storage_server.py:443-452); everything else falls through to a plain `storage_location ILIKE :storage_location` (cold_storage_server.py:454).

**Failure scenario.** A new cold site is added to the DB and correctly returned by /cold-storage/storage-locations. It never appears as a chip, so its stock can only be found by clearing the location filter. Conversely, clicking 'Savla Bond' sends `storage_location=Savla Bond` which hits the generic ILIKE branch; if the DB stores that site as storage_location='Savla' with a unit suffix (the exact pattern the D-39/D-514 special case exists for), the search returns zero rows and the operator concludes the location is empty.

**Fix.** Render the chips from `storageLocations` with `COLD_STORAGE_LOCATIONS` only as a loading fallback, add `company` to the dep array, add an error state for the non-OK branch, and route the call through the auth-aware service.


## Cold Transfer-Out bulk loader pages with LIMIT/OFFSET over a non-unique ORDER BY created_ts — rows duplicate or vanish across the 1000-row boundary

**legacy_frontend/app/[company]/cold-transfer/page.tsx:172** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: unverified

**Problem.** The backend sorts by a single non-unique column with no tiebreaker: `ORDER BY h.{sort_by} {direction} LIMIT :limit OFFSET :offset` (interunit_tools.py:1499-1500). Postgres gives no stable order among rows with equal created_ts, so a tie straddling the 1000-row page boundary can return the same header on both pages or skip one entirely. The loop concatenates pages without de-duplication, and the rows are keyed by `t.id` in the table (:828), so duplicates produce duplicate React keys. It is also a serial N+1 fetch: nothing renders until every page has round-tripped.

**Failure scenario.** A bulk import stamps 30 transfers with the identical created_ts, and that block spans rows 995-1024. Page 1 (OFFSET 0 LIMIT 1000) and page 2 (OFFSET 1000 LIMIT 1000) can both include header id 8123 and neither include id 8127. The cold list then shows challan TRANS20260817xxxx twice (React duplicate-key warning) while another cold challan is missing entirely, and the header count `{coldOutClientTotal}` (:675) is off by one against the database.

**Fix.** Add a unique tiebreaker to the ORDER BY server-side (`ORDER BY h.{sort_by} {dir}, h.id DESC`) or switch to keyset pagination. Client-side, de-duplicate on id when concatenating: `const seen = new Set(all.map(r => r.id)); all.push(...recs.filter(r => !seen.has(r.id)))`.


## Cold Transfer-IN list is hard-capped at 500 rows while the stat card shows the true server total

**legacy_frontend/app/[company]/cold-transfer/page.tsx:201** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: plausible

**Problem.** Unlike `loadColdOut` (lines 165-192), which loops until `all.length >= total`, `loadTransferIns` fetches exactly one 500-row page and never pages further, then client-filters and client-paginates that slice (lines 381-393). `transferInsTotal` is the real server count and is displayed on the stat card, so the two disagree the moment `cold_transfer_in_headers` exceeds 500 rows — with no indication that the list is truncated.

**Failure scenario.** 700 cold GRNs exist. Stat card reads "Transfers In: 700"; the Transfer IN tab paginates only the newest 500 (client pages 1-50 at 10/page) and the 200 oldest cold receipts cannot be reached or searched — the search box only filters the fetched 500 (line 384).

**Fix.** Reuse the `loadColdOut` paging loop for cold transfer-ins (fetch per_page=1000 pages until `all.length >= total`), or pass the search/warehouse filter to the server — GET /interunit/cold-transfer-in/list already accepts a `search` param (interunit_server.py:357) that the FE never sends.


## Cold Transfer-In list is hard-capped at 500 rows while the stat card reports the full server total

**legacy_frontend/app/[company]/cold-transfer/page.tsx:201** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: unverified

**Problem.** Unlike loadColdOut (:165-192) which loops until `all.length >= total`, loadTransferIns fetches exactly one 500-row page and never continues. The client then filters (isColdRelated, :382) and paginates the survivors at 10/page (:390-393). Everything past the 500th cold GRN is unreachable, and `transferInsTotal` still shows the true server count on the "Transfers In" stat card (:642), so the card and the list contradict each other.

**Failure scenario.** cold_transfer_in_headers grows to 780 rows. The stat card reads "Transfers In: 780"; the list header (`{transferInsClientTotal} records`, :940) reads e.g. 500, and the client pager tops out at page 50. Searching a GRN from the 600th-oldest receipt returns "No matching records" although the row exists — and the search only ever scans the 500 fetched rows (searchMatch, :288-295).

**Fix.** Reuse loadColdOut's paging loop for cold transfer-ins (page through until all.length >= response.total with a safety cap), or push the cold filter server-side so real server pagination can be used. Either way, drive the stat card from the same post-filter count the list shows.


## Non-2xx responses are treated as no-ops in the inner-cold and in-transit loaders — stale or empty data with no error surfaced

**legacy_frontend/app/[company]/cold-transfer/page.tsx:223** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** A 4xx/5xx never reaches the catch block (fetch only rejects on network failure), so the `if (res.ok)` guard swallows it: loading is cleared in `finally` and the component renders whatever state it already had. The inner-cold tab then shows the "No inner cold transfers" empty state after a 500, indistinguishable from a genuinely empty table. loadInTransitCount (transfer/page.tsx:356-366) does the same and additionally sends no Authorization header, so an auth-protected /interunit/pending-stock silently pins the "In Transit" KPI at 0.

**Failure scenario.** The cold-storage service returns 503 for /cold-storage/inner-transfer/list. The user clicks the Inner Cold tab, sees a spinner, then "No inner cold transfers — Inner cold transfer records will appear here once created" with a New Transfer button. No toast, no console error visible to them. They create a duplicate inner-cold transfer because the existing ones appeared to be gone.

**Fix.** Add an else branch that throws so the existing catch/toast fires: `if (!res.ok) throw new Error(await res.text().catch(() => `HTTP ${res.status}`))`. Track a per-list error state and render an error panel instead of the empty state. Add the bearer header to loadInTransitCount to match every other call.


## The same challan's hover card computes items differently on /transfer than on /cold-transfer — line-only items appear on one page and vanish on the other

**legacy_frontend/app/[company]/cold-transfer/page.tsx:844** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** Two pages that list the SAME cold->normal transfer (it passes /transfer's isPureColdTransfer filter at :247 and /cold-transfer's isColdRelated filter at :361) build the hover from different functions. groupTransferItems (ChallanHoverCard.tsx:410-420) merges boxed items with any line that has no matching box; the cold page's branch discards those lines whenever a single box exists. The comment on groupTransferItems (:406-409) documents this as a previously fixed bug — the fix was never applied to the cold page.

**Failure scenario.** TRANS202608171318 has 100 scanned boxes of article A plus one qty-entered line for article B (no boxes). Hovering the challan on /cfpl/transfer shows 2 items (A: 100 boxes, B: its qty). Hovering the SAME challan on /cfpl/cold-transfer shows only A. An operator comparing the two screens concludes one of them lost stock.

**Fix.** Replace the four `(data.boxes||[]).length > 0 ? groupBoxesByItem(...) : groupLinesByItem(...)` branches in cold-transfer/page.tsx (:753, :844, :1475, :1551) with `groupTransferItems(data.boxes || [], data.lines || [], fromColdUnit)`, and import it alongside the other two helpers at :23.


## Dashboard downloads every transfer LINE row (~35k) with no date bound or pagination on every visit, and mirrors it into localStorage

**legacy_frontend/app/[company]/transfer/dashboard/page.tsx:181** &nbsp;|&nbsp; perf &nbsp;|&nbsp; verdict: confirmed

**Problem.** `/transfer-dashboard/all-data` has no LIMIT, no date filter and no company filter (transfer_dashboard_server.py:34-61) — it returns one row per line for the entire history (`interunit_transfers_lines` = 35,457 rows per _schema_dump.json:996), each carrying `issue_details` arrays. Every dashboard open transfers and parses that payload, then `JSON.stringify`s it into localStorage (5-10MB quota) where the failure is swallowed by an empty catch (transferDashboardApi.ts:78-80). All filtering, grouping and the 4-level tree are then computed over the full array in `useMemo`s on the main thread.

**Failure scenario.** Opening the Transfer Summary pulls a multi-megabyte JSON, blocks the main thread through normalization (`applyData` maps every record through `normalizeWarehouseName` + `canonicalizeCategory`, lines 161-172) and quietly fails the localStorage write once the payload exceeds quota, so the "instant paint from cache" path silently stops working and every visit pays full load again.

**Fix.** Add `from_date`/`to_date` (default: current month) and pagination or server-side aggregation to /transfer-dashboard/all-data, and cache only the aggregated summary rather than the raw line array.


## Excel export writes the per-transfer box count on every line row, so the "Boxes" column sums to N× reality

**legacy_frontend/app/[company]/transfer/dashboard/page.tsx:348** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** `filtered` is the fanned-out line-level array and `r.box_count` is the per-TRANSFER count stamped onto each line (transfer_dashboard_server.py:89-91). The exported sheet therefore repeats the same box count on every line of a transfer; any SUM or pivot on the Boxes column in Excel double/triple counts, unlike Qty/Net Weight which are genuinely per-line.

**Failure scenario.** Export a month of transfers and pivot Boxes by From WH: a 100-box, 3-line challan contributes 300. The exported total will not match the Boxes figure anyone counts on the floor, and will not even match the (separately wrong) on-screen KPI once filters differ.

**Fix.** Either blank the Boxes cell on all but the first line of each transfer, or export two sheets — a per-line sheet without Boxes and a per-transfer sheet keyed by transfer_id carrying box_count once.


## DC page passes the raw DB site code as the address key, so non-canonical warehouse names print with a blank address

**legacy_frontend/app/[company]/transfer/dc/[transferId]/page.tsx:66** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** `WAREHOUSE_ADDRESSES` is keyed strictly by canonical codes (lib/constants/warehouses.ts:233-235 over WAREHOUSES: 'W202','A185','A101','A68','F53','Savla D-39','Savla D-514','Rishi','Supreme','Eskimo'). The backend returns `from_site` verbatim, and the codebase maintains WAREHOUSE_ALIASES precisely because the DB holds values like 'old_savla', 'savla bond', 'd-39', 'rishi cold', 'Supreme Cold'. Every other consumer normalises (`normalizeWarehouseName` in PendingTransfersModal, `getDisplayWarehouseName` in the view page); this page does not. DeliveryChallan then renders `warehouseAddresses[fromWarehouse]?.address || ''` (line 192/199) - an empty address line on a printed challan.

**Failure scenario.** Transfer from site 'old_savla' to 'W202'. The printed DC reads 'FROM: Candor Foods / old_savla' with a blank address block, while the TO block is complete. A challan carrying goods on public roads goes out with no dispatch address.

**Fix.** Pass `normalizeWarehouseName(transferData.from_warehouse || transferData.from_site)` (and the same for `to`), and make DeliveryChallan fall back to a visible '[address not configured]' marker rather than an empty string.


## directtransferform edit mode: restored draft is silently overwritten by the async load, and cold item-mark lookup takes an arbitrary pile

**legacy_frontend/app/[company]/transfer/directtransferform/page.tsx:869** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** Two issues in the edit path. (1) The item-mark refetch queries only by item_description with limit 1 and takes results[0]; cold stock holds many piles per description differing by lot/vakkal/inward, so the mark applied to every box of that description is whichever row the backend happens to sort first — it is not keyed on the box's lot_number even though the lot is available on each box. (2) The draft-restore effect in useFormPersistence runs on mount and the transfer fetch resolves later, so any locally persisted edits are discarded without warning, while the 300 ms save effect then writes the server data back over the draft.

**Failure scenario.** (1) 'CASHEW W240' exists as lot CF100326 (mark 'CD-11') and lot CF100811 (mark 'CD-42'). Reopening a transfer of the CF100811 boxes stamps 'CD-11' on all of them, and that wrong mark is what the cold-storage summary popup (line 2087-2097) puts on the operator's clipboard for the cold store. (2) Operator edits box weights, the tab reloads, the restored draft flashes and is then wiped by the fetch — the corrections are lost with no message.

**Fix.** Key the item-mark lookup on item_description + lot_no (and pile_key when available) per box; and in edit mode either skip the localStorage restore or merge it after the fetch resolves.


## DC printed from the Records list loses E-Way Bill, Dispatched Through and Case Pack that the just-after-submit print shows

**legacy_frontend/app/[company]/transfer/job-work/dc/[challanId]/page.tsx:122** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** When the page falls back to GET /job-work/out/{challan_no} (the path used by the Records tab's "DC Print" button, page.tsx:2799), the response contains neither e_way_bill_no nor dispatched_through — the handler selects and returns only id, challan_no, job_work_date, from_warehouse, to_party, status, vehicle_no, sub_category, dispatch_to, driver_name, authorized_person, remarks, party_address, purpose_of_work, contact_person, contact_number, expected_return_date and items (job_work_server.py:1282-1325), even though both columns exist and are populated on insert (line 582). Its item objects likewise have no unit_pack_size (the column does not exist in jb_materialout_lines) so JobWorkDC's "Case Pack" column renders "-" for every line. The sessionStorage path (`jw-dc-${challanNo}`, written at material-out/page.tsx:1240) does carry all three, so the same challan prints differently depending on which route the user came from.

**Failure scenario.** Challan JB202605131331 is created with E-Way Bill 391004512345, Dispatched Through "Self", 25 kg case packs. The DC printed immediately after submit shows all of them. The same challan reprinted next day from Records → DC Print shows "E-Way Bill: N/A", no "Dispatched Through", and "-" in every Case Pack cell — a GST document with a missing e-way bill reference.

**Fix.** Add e_way_bill_no, dispatched_through (and case_pack/unit_pack_size) to the SELECT and response of GET /job-work/out/{challan_no}; map item.case_pack as a fallback for unit_pack_size in the DC page.


## Material Out "Added Items": the Qty column and the editable Boxes column are the same field, and editing it inflates dispatched box counts for cold-storage rows

**legacy_frontend/app/[company]/transfer/job-work/material-out/page.tsx:1899** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** There is only one quantity field on JobWorkEntry; it is rendered twice under two different headers ("Qty" and "Boxes") and summed twice in the footer, so the two totals are always identical and neither is an independent box count. Worse, cold-storage rows are created one entry per physical box with quantity fixed at "1" and a unique boxId (handleAddToList lines 1005-1063, whose comments record a prior 700-box inventory-loss incident), yet the Boxes cell stays editable. The edited value is sent as quantity.boxes (line 1181) and stored in jb_materialout_lines.quantity_boxes, while only one physical box was picked and deducted from cold stock.

**Failure scenario.** Operator adds 3 cold-storage boxes (3 rows, quantity "1" each, 3 distinct box_ids), then types 10 into the Boxes cell of one row to "fix" a count. The challan is saved with quantity_boxes = 10+1+1; /job-work/out/search consolidates it as total_boxes 12 (`max(qty_boxes,1)`, job_work_server.py:1008) so the Material In screen shows 12 boxes dispatched against 3 actual boxes, and the DC prints 12 boxes for 3 lot rows.

**Fix.** Render a real box count (1 for cold rows, quantity for bulk rows) in the Qty column and make the Boxes input read-only for entries that carry a boxId; alternatively track boxes and units as separate fields on JobWorkEntry.


## Silent catch blocks render "no data" states on network/server failures

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:735** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** loadMiRecords swallows every error (including 500s and network failures) and sets an empty list with no error state, so the UI shows the "No inward receipts yet — Search a JWO challan above to create one" empty state; miRecordsTotal is left at its previous value, so the header can read "312 receipts" above an empty table. The cold-storage search does the same ("No results found." on a failed request), as do the drill-down tree fetch and the receipt-history expansion.

**Failure scenario.** The IMS API is restarted while a user is on the Material In tab. loadMiRecords fails; the operator sees "No inward receipts yet" and concludes the receipts were deleted, then re-enters an inward receipt that already exists, creating a duplicate IR and double-counting FG against the JWO.

**Fix.** Track an error state per loader and render a retry/error banner instead of the empty state; at minimum, toast the error the way loadRecords does (line 1083).


## Records list: the challan search term is applied by Prev/Next/Refresh but not by typing, desyncing page number from result set

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:1089** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** recordsFilterChallan is read inside loadRecords but is deliberately excluded from the auto-reload effect (Enter-to-search). Any other call — Next/Prev (line 2871-2873), Refresh (line 2660), post-delete reload (line 1099) — picks up the un-submitted text and requests a filtered result set at the *current* page number, which is not reset.

**Failure scenario.** 40 records, user is on page 3. They type "JB2026" (matches 3 records) but do not press Enter, then click Next. loadRecords(4) fires with challan_no=JB2026 → backend returns total=3, total_pages=1, page=4 → records is [] and the tab renders the "No job work records yet" empty state while the footer disappears; the user has to clear the box and reload to recover.

**Fix.** Debounce recordsFilterChallan into the same effect as the other filters (always resetting to page 1), or snapshot the applied filter into separate state on Enter and use that snapshot inside loadRecords.


## Deleting the last record on the last page leaves the list on an out-of-range page showing "no records"

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:1099** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** After a delete the same page index is re-requested. With per_page=15, deleting the only row of the final page reduces total below the page's lower bound, and the backend (LIMIT/OFFSET, job_work_server.py:843) returns an empty rows array for that offset. The FE then shows the "No job work records yet" empty state (line 2716) while recordsTotalPages/recordsTotal still describe a non-empty dataset. handleDeleteMiRecord (line 749, `loadMiRecords(miRecordsPage)`) has the identical issue.

**Failure scenario.** 31 records → page 3 holds exactly 1 row. Delete it. loadRecords(3) requests OFFSET 30 of 30 → records=[] , total=30, total_pages=2. The screen shows the empty-state illustration "Create your first material out…" plus a footer reading 3 / 2, although 30 records exist.

**Fix.** After a successful delete call loadRecords(Math.min(recordsPage, Math.max(1, Math.ceil((recordsTotal - 1) / 15)))) — or clamp on response: if records.length === 0 && page > 1, reload page - 1.


## Cumulative FG/waste/rejection double-counted when one challan has two dispatch lines with the same item_description

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:1121** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** /job-work/out/search consolidates cold-storage box rows into groups keyed by item_description||item_mark||lot_number (job_work_server.py:996) but joins the cumulative received amounts by item_description alone (`GROUP BY il.item_description`, line 1047, then `cumulative.get(desc, ...)` at line 1112). When one challan produces two consolidated lines with the same description (two lots / two item marks, or one cold + one non-cold line), each line receives the FULL cumulative FG/waste/rejection, and the FE sums those per-line values into totals.prev_fg / prev_waste / prev_rejection and into isFullyAccounted / canSubmitFinal (lines 1129-1134).

**Failure scenario.** Challan with 300 boxes of "KHALAS DATES" lot A (mark X) and 200 boxes of "KHALAS DATES" lot B (mark Y) → 2 consolidated lines. One prior IR received 4,000 kg FG total. Both lines show prev_fg 4,000 → the Cumulative Summary panel reports "FG Received 8,000 kg" and "Remaining Balance" 8,000 kg too low; validateMaterialIn (line 921) then rejects any further entry with "Total received exceeds dispatched", blocking the second receipt entirely.

**Fix.** Key the cumulative lookup on the same tuple the lines are consolidated by (description + item_mark + lot_number, and sl_no for non-cold lines) on the backend, and defensively de-duplicate prev_* in the FE mapping.


## Summary date range is sent as DD-MM-YYYY and compared lexicographically by the server, returning the wrong records

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:1226** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** The server applies these as plain string comparisons on the VARCHAR column: `h.job_work_date >= :from_date` / `<= :to_date` (job_work_server.py:2556-2561). On DD-MM-YYYY values a >= comparison orders by day-of-month first, so the returned set is not a date range at all. Meanwhile the client-side tree on the same screen filters with toYmd()-normalized ISO comparisons (lines 1319-1320), which are correct — so the KPI cards and the drill-down below them are computed over different record sets for the same picker values.

**Failure scenario.** From = 2026-05-01, To = 2026-05-31 → from_date "01-05-2026", to_date "31-05-2026". The SQL matches "02-01-2025" (2 Jan 2025), "15-11-2024", etc. — every record whose day-of-month is between 01 and 31 regardless of month/year, i.e. essentially all rows — so "Dispatched" KPI reports the all-time total while the drill-down beneath correctly shows only May 2026.

**Fix.** Store/compare dates as a real DATE (or ISO text) column and send the ISO value unchanged; until then, have the backend parse with to_date(h.job_work_date,'DD-MM-YYYY') for the range predicate.


## Report fetches have no AbortController or request sequencing — a slow response overwrites a newer one

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:1249** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** The cleanup only cancels a pending 100 ms timer, not an in-flight fetch. loadReportWithParams unconditionally does setRptData(data) when it resolves, so responses can land out of order. The /job-work/reports/dashboard query is heavy (9 aggregate queries incl. LATERAL joins over all_sku), making overlapping slow requests likely, and rptLoading is also set false by whichever finishes last.

**Failure scenario.** User clicks vendor chip "UNAZO CORPORATION" (query takes 4 s), then immediately clicks "HAG CORPORATION" (takes 1 s). The HAG data renders, then 3 s later the UNAZO response overwrites it: every KPI, chart and status count shows UNAZO numbers while the UNAZO chip is not selected and HAG is highlighted.

**Fix.** Create an AbortController per invocation (abort in the effect cleanup) or guard with a monotonically increasing request id and drop responses that are not the latest.


## Enter in any Material In field implicitly submits a partial inward receipt

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:1527** &nbsp;|&nbsp; ux &nbsp;|&nbsp; verdict: plausible

**Problem.** The whole Material In screen is one <form> whose onSubmit is the POST handler with receiptType defaulting to "partial". Only the challan search input guards Enter (line 1546 preventDefault); the Inward Challan No field, the Vehicle/Driver/Remarks fields and all the FG/Waste/Rejection number inputs do not. A single Enter keypress in any of them fires the browser's implicit submission and POSTs /job-work/material-in immediately, with whatever has been typed so far, bypassing the explicit Submit Partial / Submit Final buttons (which are type="button").

**Failure scenario.** Operator types the inward challan number, enters FG 1,200 kg for line 1, and presses Enter intending to move to the next field. validateMaterialIn passes (challan present, one item > 0) so a partial IR is created for 1,200 kg against a 5,000 kg challan, the JWO flips to partially_received, boxes/QR entries are dropped, and the whole form resets — the remaining lines must be re-entered as a second receipt.

**Fix.** Add onKeyDown={(e) => { if (e.key === 'Enter') e.preventDefault() }} on the form (or change the element to a <div> since both submit paths are already explicit button onClick handlers).


## No AbortController or request-sequence guard on any list fetch; mount fires duplicate concurrent requests whose responses can land out of order

**legacy_frontend/app/[company]/transfer/page.tsx:157** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** On mount the `[]` effect and the `[requestsFilterActive, warehouseFilter]` effect both call `loadRequests(1)`, and the `[activeTab]` effect plus the `[transferOutFilterActive, warehouseFilter]` effect both call `loadTransfers(1)` — two identical in-flight GETs each. Then the auth effect (lines 42-52) sets `warehouseFilter` to the user's default, firing a third round. None of `loadRequests`/`loadTransfers`/`loadTransferIns` (lines 93-155) passes an AbortSignal or checks a request id before `setRequests`/`setTransfers`, so whichever response resolves LAST wins regardless of which was issued last.

**Failure scenario.** User types "A18" in the Transfer Out search (crossing empty→non-empty flips `transferOutFilterActive`, issuing a per_page=500 request), then immediately clears it (issuing a per_page=15 request). The 15-row response returns first and sets `transfersTotalPages=31`; the slow 500-row response lands afterwards and overwrites `transfers` with 500 rows while `transfersPage`/`transfersTotalPages` still describe the 15-row paging. The table then shows 500 rows under a "1 / 31" pager. The mount duplication also doubles load on every page open.

**Fix.** Give each loader an AbortController stored in a ref (abort the previous call before issuing a new one) or a monotonically increasing request-id checked before every setState; and drop the redundant mount effects (`[]` + filter effect) so exactly one fetch runs per state change.


## Local COLD_WAREHOUSES list is matched without normalization and omits known aliases and Eskimo — cold→cold transfers leak into the main Transfer Out list

**legacy_frontend/app/[company]/transfer/page.tsx:229** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** lib/constants/warehouses.ts defines an alias table with values that occur in the database — "savla bond", "old savla", "new savla", "d-39", "d39", "d-514", "rishi cold storage" (:78-116) — plus a fifth cold warehouse, Eskimo (:60-64), that this local list omits entirely. isColdSource/isColdDest compare the RAW field against the hard-coded list without calling normalizeWarehouseName, unlike isColdDestTransferIn three lines below (:242-243), which does normalize. Any cold warehouse written under an alias is classified as non-cold, so isPureColdTransfer (:236) fails to exclude it.

**Failure scenario.** A cold->cold transfer stored as from_warehouse "Savla Bond" -> to_warehouse "Eskimo" (both legitimate values per the alias table and WAREHOUSES) returns isColdSource=false, so isPureColdTransfer=false and the row renders in the main /transfer Transfer Out list. The same row also matches /cold-transfer's isColdRelated (which DOES normalize, cold-transfer/page.tsx:338-339), so it appears in both lists — the exact double-listing the :226-236 comment says must not happen, and it is double-counted by anyone tallying the two screens.

**Fix.** Delete the local list and use the shared helpers: `import { isColdWarehouse, normalizeWarehouseName } from "@/lib/constants/warehouses"` then `const isCold = (w:any) => isColdWarehouse(normalizeWarehouseName(w))` — this is exactly what cold-transfer/page.tsx:21 and :338-339 already do.


## Transfer IN delete is sent without the Authorization header every other call carries

**legacy_frontend/app/[company]/transfer/page.tsx:298** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** Every other call in this module attaches the bearer token — InterunitApiService.getAuthHeaders() (interunitApiService.ts:80-90) and each inline hover fetch (`...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {})`, e.g. :777, :866, :1023). This hand-rolled DELETE omits it and also bypasses the service layer entirely, hard-coding the URL. The cold page correctly routes its delete through the service (cold-transfer/page.tsx:429). Identity is passed as a query param instead, so the server cannot authenticate the caller.

**Failure scenario.** Once the transfer-in DELETE route requires the bearer token, the request returns 401. The code throws `Delete failed: 401` and toasts "Failed to delete transfer IN" with no indication that the session/token is the cause — while the identical delete on the cold page succeeds, making the failure look route-specific rather than auth-specific.

**Fix.** Route through the service like the cold page does — add a `deleteTransferIn(id, userEmail)` method to InterunitApiService using fetchJSON (which applies getAuthHeaders) and call it here, dropping the inline URL and the ad-hoc error parsing at :299-303.


## "Pending" stat card counts only the requests on the current page, not all pending requests

**legacy_frontend/app/[company]/transfer/page.tsx:352** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: plausible

**Problem.** `requests` holds only the current page (per_page=15, line 99) while the neighbouring cards use server totals (`totalRecords`, `transfersTotal`, `transferInsTotal`). The KPI is therefore bounded by 15 and changes as the user pages, which reads as "pending requests dropped". The comparison is also case-sensitive against the raw status, whereas the rest of the file lowercases before comparing (`req.status.toLowerCase() !== 'pending'`, line 615), so a backend value of `pending` counts as 0.

**Failure scenario.** 40 requests exist, 22 Pending. Page 1 loads 15 requests of which 6 are Pending → the card reads "6". User clicks Next → the card changes to "9". Nobody can see that 22 requests await approval. If any row carries status `pending` (lowercase — the status map at line 314 exists precisely because case varies), it is excluded entirely.

**Fix.** Take the pending count from the server (GET /interunit/stats/summary already returns `request_status` as a status→count map via `InterunitApiService.getStatsSummary()`), and compare case-insensitively.


## In-Transit count fetch swallows non-2xx and network errors and sends no auth header

**legacy_frontend/app/[company]/transfer/page.tsx:356** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** Both the non-2xx branch and the catch are silent, and unlike every other call in the file this `fetch` omits the `Authorization: Bearer` header that `getAuthHeaders()` (interunitApiService.ts:80-90) supplies. The KPI simply stays at its previous value (0 on first load) with no error state, so a 401/500 is indistinguishable from "nothing in transit". `data?.total` is also consumed without checking the response shape.

**Failure scenario.** The pending-stock endpoint starts requiring auth (or returns 500). The In Transit card renders "0", the Pending Transfers workflow looks empty, and boxes sitting in transit are never chased — with nothing in the UI or console indicating the request failed.

**Fix.** Route this through `InterunitApiService`/`fetchJSON` so it carries auth and throws on non-2xx, and hold an error state on the card (e.g. "—" plus a retry) instead of silently keeping a stale/zero count.


## Section headers/record counts show the server total while the table shows a client-filtered subset (count never matches rows)

**legacy_frontend/app/[company]/transfer/page.tsx:705** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** All three counts are the unfiltered server `total`, but the lists underneath render `filteredTransfers` / `filteredRequests` / `filteredTransferIns` — after the cold-exclusion, warehouse and search filters. The cold-transfer page got this right (`count={transferInsClientTotal}`, line 940; `{coldOutClientTotal} record...`, line 675); the main transfer page did not.

**Failure scenario.** User picks warehouse A185. Header reads "452 records", the table shows 11 rows, the pager is hidden (filter mode forces total_pages=1). The user reasonably concludes 441 records failed to load. Same on Transfer IN, where `isColdDestTransferIn` (line 242) additionally removes cold-destination GRNs from the rows but not from the count.

**Fix.** Bind the header counts to the rendered arrays (`filteredTransfers.length`, `filteredRequests.length`, `filteredTransferIns.length`), and show the server total separately as "of N total" when a filter is active.


## "All Transfers" tab ignores the warehouse filter but is silently switched into 500-row no-pagination mode by it

**legacy_frontend/app/[company]/transfer/page.tsx:1369** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** The All Transfers tab has no filter UI and renders `transfers` unfiltered, yet it shares `warehouseFilter` with the Transfer Out tab. Because `getUserDefaultWarehouses` sets a non-"all" warehouse on mount for most users (lines 42-52), `transferOutFilterActive` is permanently true, so this tab always fetches 500 rows in one shot, forces `total_pages` to 1 and hides the pager — while displaying none of the filtering that justified the bulk fetch.

**Failure scenario.** A user whose default warehouse is A185 opens the All Transfers tab: 500 rows of every warehouse render in one DOM table (no virtualization), the pagination bar is gone, and the 452-row table happens to fit — but the moment the table exceeds 500 headers the remaining rows are unreachable from this tab, with no pager and no filter to narrow the set.

**Fix.** Decouple the tabs: give the details tab its own `detailsPage` state and always fetch it with server pagination (per_page=15), independent of `warehouseFilter`; or apply `warehouseMatches` to the rows the tab renders so the bulk fetch is actually used for filtering.


## Article search has no AbortController - a slow earlier response overwrites results for the current query

**legacy_frontend/app/[company]/transfer/request/page.tsx:183** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** The 300ms debounce reduces the number of requests but does not order them. Nothing aborts the previous fetch and nothing checks that the resolved response still corresponds to the current `searchQuery`, so a slower earlier request can resolve last and replace the newer result set (and force the dropdown open again). `searchTotal` is set from the stale payload too, so the 'Showing X of Y' footer describes a different query.

**Failure scenario.** User types 'ca' (pauses, request A fires against a broad match), then 'cashew w320' (request B). B returns in 200ms, A in 2s. The dropdown ends up showing every 'ca' article, and clicking one auto-fills the wrong material type / category into the request line, which is then submitted.

**Fix.** Keep an AbortController ref, abort it at the start of each search, and ignore any response whose query no longer equals the current input.


## Article quick-search is capped at 200 results with no pagination, so matching items are unreachable

**legacy_frontend/app/[company]/transfer/request/page.tsx:192** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: unverified

**Problem.** The limit is hard-coded and there is no page/offset control, no infinite scroll, and no 'load more'. The footer honestly reports 'Showing 200 of N' but nothing lets the user reach items 201..N - the only workaround is to guess a narrower query. The result list is also the only path that auto-fills material type / category / sub-category, so an unreachable item cannot be requested via search at all.

**Failure scenario.** Operator types 'BOX' with 850 matching articles. The dropdown lists the first 200 in server order and the footer reads 'Showing 200 of 850'. The article the operator needs ranks 300th and can never be selected from search.

**Fix.** Add offset/page state driven by the dropdown's scroll (or a 'Load more' row) that appends the next slice, resetting to page 0 on every query change.


## loadTransferDetails has no cancellation — an out-of-order search response replaces the newer transfer

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:284** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: plausible

**Problem.** No AbortController and no request-sequence guard; the function also fires a second dependent request (getPendingByTransferOut) and a chain of cold lookups, so two overlapping searches interleave their setState calls. The cold page is worse (L344) because it also awaits per-item cold-storage lookups in a loop before setting pending state.

**Failure scenario.** Operator types TRANS-A, hits Enter, immediately corrects to TRANS-B and hits Enter. B resolves first (small transfer), then A resolves and calls setTransferData(A) — the header/route card shows A while transferNumber reads B; then B's pending lookup lands and sets pendingHeaderId to B's GRN. Every subsequent acknowledge posts A's box ids into B's pending header.

**Fix.** Keep a `const reqId = ++loadSeqRef.current` at entry and ignore every setState when `reqId !== loadSeqRef.current`; pass an AbortSignal through the service.


## Silent catch on the pending-GRN lookup drops all restored acknowledgements without telling the operator

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:494** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** A failure of getPendingByTransferOut leaves pendingHeaderId, pendingGrnNumber, inwardTransactionNo, linesMatchMap, linesIssueMap and the restored weights all at their initial values, with no user-visible signal. Because inwardTransactionNo stays null, the 'Generate QR ID's' gate at L2284 (`qrsGeneratedNow = !!inwardTransactionNo`) re-enables and will mint a brand-new id series for boxes that already have acknowledged rows.

**Failure scenario.** Operator resumes GRN-20260817101500 with 30 of 40 boxes already acknowledged; the pending lookup times out. The screen shows 0/40 resolved with no error. The operator clicks 'Generate QR ID's', gets new box ids 44556677-1…40, re-acknowledges everything, and interunit_transfer_in_boxes ends with 70 rows (30 old ids + 40 new) for a 40-box transfer.

**Fix.** Surface the failure (toast + a 'could not load saved progress — reload before acknowledging' banner) and block acknowledge/Generate-QR until the pending state is known.


## scan_source / scanned_by are silently dropped by the backend model — STBR audit records every scan as 'manual'

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:606** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** PendingBoxAcknowledge (interunit_models.py L425-437) declares no scan_source/scanned_by and sets no model_config, so Pydantic v2's default extra='ignore' drops them. acknowledge_pending_box then does `scan_source = getattr(data, "scan_source", None) or "manual"` and `scanned_by = getattr(data, "scanned_by", None)` (interunit_tools.py L2572-2573), so the reconciliation audit always stores scan_source='manual', scanned_by=NULL. Sent from regular L606-607 and L741-742, cold L755-756 and L890-891.

**Failure scenario.** Operator QR-scans 40 cartons; every row lands in transfer_box_reconciliation with scan_source 'manual' and scanned_by NULL, and interunit_transfer_in_boxes.scan_source is 'manual' too. The reconciliation report (GET /transfer-in/{id}/reconciliation, typed in interunitApiService.ts with scan_source/scanned_by columns) can never distinguish a scanned override from a hand-clicked one, and 'who scanned this box' is unanswerable during a dispute.

**Fix.** Add `scan_source: Optional[str] = None` and `scanned_by: Optional[str] = None` to PendingBoxAcknowledge (and to the batch item type). Set model_config = ConfigDict(extra='forbid') on request models so future FE-only fields fail loudly instead of vanishing.


## Apply-to-all issue fires N sequential acknowledges and N sequential print dialogs

**legacy_frontend/app/[company]/transfer/transferIn/page.tsx:890** &nbsp;|&nbsp; perf &nbsp;|&nbsp; verdict: confirmed

**Problem.** handleSubmitIssue with 'apply to all' loops one HTTP POST per target (acknowledgeBatch exists and is used elsewhere) and then opens one hidden iframe running window.print() per target, each awaited. There is no progress indicator and no partial-failure handling: a throw mid-loop leaves some rows issued server-side while linesIssueMap is never updated (it is only committed after the loop, L915).

**Failure scenario.** An article has 120 pending cartons; the operator ticks 'Apply same correction to all 119 other pending boxes' and submits. The UI freezes through 119 sequential POSTs, then 119 sequential print dialogs. If POST #60 fails, the catch at L940 shows one toast, the first 59 rows are flagged in the DB but none are flagged locally, so the screen still offers them as un-issued and re-issuing double-writes.

**Fix.** Replace the acknowledge loop with a single acknowledgeBatch call (checking its `conflicts`), and emit one print job containing all labels (handleBulkPrintQR already does this).


## MaterialTypeDropdown's RM/PM/FG fallback is dead code — the API layer swallows errors and returns empty options, leaving the dropdown empty AND disabled

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:56** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** dropdownApi.fetchDropdown catches every network/HTTP error and returns a well-formed empty envelope (lib/api.ts:661-681, `options: { material_types: [], ... }`) instead of throwing. `[]` is truthy and passes Array.isArray, so the success branch runs, the else-branch fallback never fires, the component's own catch never fires, and errorState stays null. The SearchableSelect is then disabled because options.length === 0 — with no message. Same code at directtransferform:57-70/99.

**Failure scenario.** The IMS API is down or the company param is rejected. Operator opens Transfer OUT: Material Type is greyed out and empty, Item Category is disabled because material_type is falsy, and no error is shown anywhere. The form is unusable and the operator has nothing to report but 'it's stuck'.

**Fix.** Treat an empty material_types array as a failure (`if (Array.isArray(mt) && mt.length > 0) { ... } else { fallback + errorState }`), or make fetchDropdown rethrow so the component's catch runs.


## transferform back-dates the transfer to the request date, defeating its own 'always today' reset

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:526** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** The mount effect at lines 419-424 exists specifically to force requestDate to today ('localStorage restore may have cached an old date'), but the loadRequestDetails effect resolves afterwards and replaces it with the originating request's date. That value is submitted as header.stock_trf_date (line 1660), and the backend stamps the in-transit ledger from it: dispatched_at is taken from header.stock_trf_date (legacy_backend/services/ims_service/pending_stock_tools.py:1276-1280). The sibling form deliberately uses today's date for the same field.

**Failure scenario.** Request REQ dated 02-08-2026 is approved and finally transferred on 17-08-2026. The transfer header and every pending_transfer_stock row are stamped 02-08-2026, so the goods appear to have been in transit for 15 days, ageing/pending reports mis-bucket them, and any date-range dispatch report for August 17 misses the shipment entirely.

**Fix.** Keep today's date for the transfer: `requestDate: currentDate` in formDataToSet (matching directtransferform:539), and surface the request's own date read-only if it needs to be visible.


## Item quick-search has no request-sequence guard and swallows failures — stale results can overwrite newer ones

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:801** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** The 300 ms debounce cancels pending timers but not in-flight requests: there is no AbortController and no request-id/latest-query check before applying the response, so a slow earlier response resolves after a faster later one and replaces the correct list. The catch also empties the list without setting any error state, so a failed search is indistinguishable from 'no matches' (the 'No items found' panel at line 2271 only renders when itemSearchOpen is already true). Identical code at directtransferform:1101-1116.

**Failure scenario.** Operator types 'alm' (request A fires), pauses 400 ms, then types 'ond' → 'almond' (request B fires). B returns in 120 ms and the dropdown shows almond items; A returns 900 ms later and replaces them with the broader 'alm' list. The operator clicks the top row and auto-fills ALMOND OIL instead of ALMOND WHOLE — a wrong article on the transfer.

**Fix.** Keep an AbortController (or a monotonically increasing request id) per articleId, abort/ignore superseded requests, and set a visible error state in the catch.


## Quick-search 'id' is a row ordinal, not a SKU id — both forms store it as sku_id and transferform writes it into the box payload as box_id

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:828** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** categorial_global_search synthesises `id = idx + 1 + offset` — the position of the row in the current page of results, not any database key (legacy_backend/services/ims_service/interunit_tools.py:3926-3934). Both forms assign it to article.sku_id (transferform:828, directtransferform:1131), render it as 'SKU: {n}' (transferform:2203, directtransferform:2556), use it for loadedItems matching (transferform:926, 957) and — in transferform — persist it as the box_id of every DIRECT box row that reaches interunit_transfer_boxes. The dropdown path by contrast fetches a real sku via dropdownApi.fetchSkuId (line 216), so the same field carries two incompatible meanings depending on how the operator picked the item.

**Failure scenario.** Operator quick-searches 'almond', clicks the 3rd result, and the badge reads 'SKU: 3'. Adding 20 boxes writes 20 interunit_transfer_boxes rows with box_id '3'. Any later attempt to trace those boxes by box_id, or to reconcile them against the source stock sheet, matches nothing (or matches an unrelated box labelled '3').

**Fix.** Have categorial-search return the real all_sku key (or omit id), and until then do not populate sku_id/box_id from `item.id` — resolve the SKU via dropdownApi.fetchSkuId after a quick-search selection.


## transferform never resets the article after 'Add to Articles List' — a second click silently duplicates every box

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:920** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** handleAddArticleToList appends qty new rows and leaves the article form fully populated, with no dedupe key for DIRECT entries anywhere (the duplicate check at line 1022 only applies to manual box fetch, and the QR check at line 1136 only to scanned boxes). The button (line 2488-2495) sits at the bottom of a long form, and the sibling form clears the article precisely to prevent this.

**Failure scenario.** Operator adds ALMOND WHOLE qty 20, scrolls down, is unsure whether the click registered, and clicks 'Add to Articles List' again. 40 box rows now exist for 20 physical boxes; loadedItems.scanned_count is also incremented twice (lines 923-937), so the 'Pending' badge reads 0 while 20 boxes are still on the floor.

**Fix.** Reset the article to blank values after a successful add, as directtransferform does, and/or block a repeat add of an identical (sku, lot, qty) tuple.


## 'Go to box # / lot' never works on desktop — refs are only registered inside the md:hidden mobile list

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:2694** &nbsp;|&nbsp; ux &nbsp;|&nbsp; verdict: confirmed

**Problem.** BoxScrollContainer's goTo resolves a box number/lot to a DOM node from refsMap and calls scrollIntoView + focuses its first input (components/modules/inward/BoxScrollContainer.tsx:116-133). Only the mobile card list registers refs; the desktop table rows do not. Because `md:hidden` hides via CSS rather than unmounting, refsMap IS populated with display:none nodes, so the lookup succeeds, the 'Not found' branch never fires, and scrollIntoView/focus on a hidden element are no-ops. Same structure at directtransferform:3051 vs 3181.

**Failure scenario.** On a desktop workstation with 120 scanned boxes, the operator types '87' and clicks Go to fix box 87's weight. Nothing happens — no scroll, no highlight, no error — and they must scroll the 300 px pane manually through 120 rows.

**Fix.** Also call `registerRef(index + 1, el)` on the desktop `<tr>` (a ref callback on the row element), or render one list and switch layout with CSS only inside the row.


## Transfer view page has no error state and a hardcoded localhost API fallback - failures render as 'Transfer not found'

**legacy_frontend/app/[company]/transfer/view/[transferId]/page.tsx:90** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** On any failure `transfer` stays null and the page renders the definitive 'Transfer not found' card (line 210-227) - a 500, a CORS failure or an unset NEXT_PUBLIC_API_URL is presented to the user as 'this transfer does not exist'. There is no AbortController either, so a fast back/forward between two transfer ids can apply the older response over the newer one. The `?? 'http://localhost:8000'` fallback means a misconfigured production build silently issues requests to the operator's own machine (the same fallback exists at request/page.tsx:192 and PendingTransfersModal.tsx:66; DebugColdStorageTransfer.tsx:27 has no fallback at all and builds a literal 'undefined/cold-storage/...' URL).

**Failure scenario.** Operator opens /cfpl/transfer/view/1732 while the API is restarting. The page shows 'Transfer not found' with a Back button; the operator reports the transfer as deleted. Navigating quickly from transfer 1732 to 1733 can also leave 1733's page rendering 1732's data if the first response lands last.

**Fix.** Add an `error` state distinct from 'not found' (use response.status === 404 for the latter), attach an AbortController keyed on transferId, and fail loudly at startup when NEXT_PUBLIC_API_URL is unset instead of defaulting to localhost.


## Transfer Details merges lines across lots but prints only the first line's lot/batch/unit_pack_size next to summed quantities

**legacy_frontend/app/[company]/transfer/view/[transferId]/page.tsx:183** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** Cold transfers create one line per (article, lot) (services/ims_service/cold_transfer_out_tools.py:127-128, 318-321), so merging by description+category+pack_size collapses several lots into a single card. Quantity, net_weight and total_weight are summed, but Lot Number, Batch Number, UOM and Unit Pack Size come from whichever line happened to be first - the card asserts that all the summed quantity belongs to one lot.

**Failure scenario.** Article 'CASHEW W320', lot A (60 boxes, 1,500 kg) and lot B (40 boxes, 1,000 kg), same pack_size. The page shows one card: 'Quantity 100, Net Weight 2,500 kg, Lot Number: A'. Lot B is invisible on the detail screen used to investigate discrepancies.

**Fix.** Include lot_number in the merge key, or render a per-lot breakdown inside the merged card.


## Transfer Details labels the number of merged LINES as 'boxes'

**legacy_frontend/app/[company]/transfer/view/[transferId]/page.tsx:190** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** `_box_count` is incremented once per merged line, not per physical box. This is the exact defect DeliveryChallan.tsx:109-111 records as already fixed there ('Previously box_count counted LINES, so it read 1 once the duplicate lines were collapsed - even though 3 boxes were transferred'); the view page still carries it. The page already has the authoritative `transfer.boxes` array and uses it for the 'Boxes Scanned' KPI two cards away, so the same screen shows two contradictory box numbers.

**Failure scenario.** Cold transfer of 100 boxes of one article recorded as 4 lines (one per lot). The item card badge reads '4 boxes' while the 'Boxes Scanned' KPI directly above reads 100.

**Fix.** Count physical boxes per article from `transfer.boxes` (match on `transfer_line_id`), and label the merged-line count separately (e.g. 'merged from 4 lines') if it is worth showing at all.


## ChallanHoverCard caches a failed item fetch permanently - the card shows 'No item details available' for the rest of the session

**legacy_frontend/components/transfer/ChallanHoverCard.tsx:92** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** The retry guard is `fetched === null`, and the failure path sets `fetched` to `[]`. A transient error therefore turns into a sticky empty state: every subsequent hover skips the fetch and renders 'No item details available' (line 214), which is indistinguishable from a transfer that genuinely has no lines. The caller compounds it - PendingTransfersModal's `fetchLines` returns `{ lines: [] }` for both `!res.ok` and any thrown error (lines 494 and 556-558), so a 500 is converted into a successful empty result before it even reaches here.

**Failure scenario.** The API blips while the user hovers challan TR-20260716141141. The card shows 'No item details available'. Every later hover on that same row - after the API recovers - returns the cached empty array, so the reviewer concludes the transfer has no items.

**Fix.** Track an `error` state separately from `fetched`, leave `fetched` null on failure so the next hover retries, and render a distinct 'Couldn't load items - hover to retry' message.


## Hover card labels every quantity "boxes", including ordered line quantities that are not boxes, and turns a 0 qty into 1

**legacy_frontend/components/transfer/ChallanHoverCard.tsx:184** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** `groupBoxesByItem` produces a genuine box count (`g.qty += 1` per box, line 369) while `groupLinesByItem` produces the ORDERED quantity off `interunit_transfers_lines.qty` — which for warehouse PM lines is a pack count, not boxes (see the ordered-vs-shipped contract documented in the backend). The card renders both with the hardcoded suffix "boxes". Additionally `Number(l.quantity || l.qty || 1)` treats a legitimate qty of 0 as 1, because `0 || 1` is 1.

**Failure scenario.** A box-less Article Entry line ordered as 250 packs renders "250 boxes" in the hover; the operator reconciles against 250 physical cartons that never existed. A line corrected to qty 0 (cancelled item) renders "1 boxes".

**Fix.** Carry a unit on HoverLine (`unit?: 'boxes' | 'qty'`), set it to 'boxes' only in `groupBoxesByItem`, and render `{line.qty} {line.unit ?? ''}`. Replace the `|| 1` fallback with `Number(l.quantity ?? l.qty ?? 0)`.


## Hover card labels unit quantities as "boxes" and substitutes 1 when a line's quantity is missing

**legacy_frontend/components/transfer/ChallanHoverCard.tsx:293** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** groupBoxesByItem increments qty by 1 per physical box (:369) — there, "boxes" is accurate. groupLinesByItem instead takes the LINE's `quantity` (interunit_tools.py:535 returns it as a string of lines.qty, i.e. units) and the shared renderer at :184 still prints "boxes". The same badge therefore means boxes for box-backed transfers and units for line-only transfers. The `|| 1` fallback additionally fabricates a quantity of 1 when the field is absent, and `Number('N/A')` yields NaN, which would render "NaN boxes".

**Failure scenario.** A line-only cold transfer (no scanned boxes) has one line with quantity "198". Hovering the challan renders "198 boxes" for what is 198 units across 100 physical cartons — the identical 100-vs-198 confusion reported for the row badge, now inside the hover. For a request line whose quantity is absent, the hover confidently prints "1 boxes".

**Fix.** Carry a unit label through HoverLine (e.g. `qtyUnit: 'boxes' | l.uom`) and render `{line.qty} {line.qtyUnit}`; set it to 'boxes' in groupBoxesByItem and to the line's uom in groupLinesByItem. Replace `|| 1` with `?? 0` plus a NaN guard: `const n = Number(l.quantity ?? l.qty ?? 0); g.qty += Number.isFinite(n) ? n : 0`.


## Hover card merges boxed and line-only items by article name only, so a second lot of the same article silently disappears from the item list

**legacy_frontend/components/transfer/ChallanHoverCard.tsx:413** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** `groupBoxesByItem`/`groupLinesByItem` both key by `name||lot` (lines 359, 284), but the merge that decides which lines are "already covered by boxes" keys by ARTICLE ONLY. Any line whose article also appears on some box is dropped, even when that line's LOT has no boxes at all — exactly the situation `_apply_box_totals` leaves behind when box→line lot matching fails (cold_transfer_out_tools.py:340-344).

**Failure scenario.** Transfer of article "PRAWN PD 16/20" on two lines: lot CF100326 (100 boxes scanned) and lot CF100415 (98 ordered, no boxes). `boxedArticles` contains PRAWN PD 16/20, so the CF100415 line is filtered out of `unboxedLines` and the hover shows only "PRAWN PD 16/20 — 100 boxes, Lot CF100326". The 98 units the list badge counts into "Qty: 198" are invisible in the hover, making the 198 look inexplicable — which is precisely the reported symptom.

**Fix.** Build `boxedArticles` from the same `article||lot` composite the grouping uses, so only the exact (article, lot) pairs that actually carry boxes suppress their line.


## groupTransferItems drops unboxed lines by article name only — a second lot of an already-boxed article disappears from the hover

**legacy_frontend/components/transfer/ChallanHoverCard.tsx:416** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** Boxed items are grouped by article+lot (`key = ${name}||${lot}`, :359), but the exclusion set that decides which lines are "already represented by a box" is keyed by ARTICLE ONLY. Any line for an article that has boxes under a DIFFERENT lot is treated as already covered and silently dropped from the hover. The transfer detail endpoint returns lot_number on both lines and boxes (interunit_tools.py:542, 627), so the lot is available and simply unused here.

**Failure scenario.** Transfer carries "SHRIMP HLSO 21/25" lot L-8891 as 10 scanned boxes, plus the same article lot L-9004 entered as a line with quantity 50 and no boxes. boxedArticles = {"SHRIMP HLSO 21/25"}, so the L-9004 line is filtered out. The hover shows a single row "SHRIMP HLSO 21/25 — 10 boxes, Lot: L-8891"; the 50 units of lot L-9004 are invisible, and the hover totals disagree with the row's Qty badge.

**Fix.** Key the exclusion set on article+lot, mirroring groupBoxesByItem: `const boxedKeys = new Set(boxes.map(b => `${norm(b.article||b.item_description)}||${norm(b.lot_number)}`))` and filter lines with the same composite key.


## groupTransferItems drops line-only lots for any article that has at least one scanned box

**legacy_frontend/components/transfer/ChallanHoverCard.tsx:417** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** Boxes are grouped by `name||lot` (line 359) but the 'which lines are already covered by boxes' test is by ARTICLE only, ignoring lot. Backend lines are per (article, lot) for cold transfers, so a line for a lot that was never box-scanned is filtered out as soon as any other lot of the same article has boxes. Its quantity, weight and PM count disappear from the hover. The precedence mismatch is a second hazard: the box set is built from `article` while the line key prefers `item_desc_raw`, so the two sides can also fail to match and list the same item twice (once from boxes, once from lines), double-counting its qty and count.

**Failure scenario.** Transfer of 'ALMOND NP' : lot 130273 with 40 scanned boxes and lot 130275 entered as a line for 20 units without a scan. The hover shows a single row '40 boxes'; the 20 line-only units never appear and are excluded from the Total Count meta.

**Fix.** Match on the same composite key the box grouping uses (`article||lot`), and normalise both sides through one helper so precedence can't diverge.


## Physical boxes with a blank article are dropped from the box tally entirely

**legacy_frontend/components/transfer/DeliveryChallan.tsx:115** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: plausible

**Problem.** `_map_box_row` (backend) returns `"article": row.article or ""`, so a box row whose article column is NULL/empty yields `d === ''` and is skipped without any accounting. The box exists physically and is counted in `transfer.boxes.length` on the view page and in `total_boxes` in the pending list, but it never reaches the DC's box totals, so the same transfer reports different box counts on different screens.

**Failure scenario.** Transfer with 100 box rows, 2 of them written by a repair/relabel script with an empty article. The Transfer Details page shows 'Boxes Scanned 100'; the printed DC totals 98 (and 196 if the description also splits into two consolidated rows).

**Fix.** Count unmatched/blank-article boxes into an explicit 'Unidentified' bucket and render it as its own DC row, so total printed boxes always reconciles to `boxes.length`.


## DeliveryChallan looks up warehouse addresses with the raw, un-normalized warehouse string — cold-source challans print with no address

**legacy_frontend/components/transfer/DeliveryChallan.tsx:191** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** `warehouseAddresses` is WAREHOUSE_ADDRESSES, keyed strictly by canonical code (warehouses.ts:233-235: W202, A185, A101, A68, F53, "Savla D-39", "Savla D-514", Rishi, Supreme, Eskimo). The prop is fed the raw header value — dc/[transferId]/page.tsx:66 `transferData.from_warehouse || transferData.from_site` — which for every cold-source transfer is the literal "Cold Storage" (interunit_tools.py:554 maps h.from_site straight through, and the sub-cold lives on from_cold_unit). "Cold Storage" is neither a WAREHOUSES key nor an alias, so the lookup returns undefined and the address falls back to an empty string. Aliases like "savla bond" / "d-39" fail the same way. getDisplayWarehouseName/normalizeWarehouseName exist for exactly this and are not used here.

**Failure scenario.** Print the DC for TRANS202608171318 (Cold Storage -> A185). The FROM block prints "Cold Storage" with a blank address line beneath it, and the gate-pass FROM line reads "Candor Foods - Cold Storage". The A185 side prints correctly. A legally-used delivery challan therefore ships with no dispatch address. The A-68 Count column heuristic at :83 is likewise defeated by aliases.

**Fix.** Normalize before lookup: `const fromCode = normalizeWarehouseName(fromWarehouse)` then key on fromCode (same for toWarehouse), and add a "Cold Storage" entry (or pass from_cold_unit down from dc/[transferId]/page.tsx so the concrete cold unit's address is printed).


## Warehouse chip tooltips claim 'pending boxes' but the backend returns transfer counts

**legacy_frontend/components/transfer/PendingTransfersModal.tsx:378** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** `fromSiteCounts` / `toSiteCounts` come from `filter_options.from_site_counts`, which the backend computes as `COUNT(DISTINCT pts.transfer_out_id)` - a count of TRANSFERS, explicitly documented as such (pending_stock_tools.py:2602-2605: 'Chip counts: number of distinct TRANSFERS per (from_site,to_site) ... Counting per-box rows here would inflate to-chip counts'). Both chip rows (lines 378 and 411) relabel that number as boxes, so the badge next to each warehouse understates in-transit volume by orders of magnitude.

**Failure scenario.** W202 has 3 in-transit inbound transfers carrying 300 boxes. The 'To: W202' chip shows badge '3' and the tooltip reads '3 pending boxes', while the table's own Total boxes row for the same filter reads 300.

**Fix.** Change the tooltip to 'N pending transfer(s)', or have the backend return a separate per-site box count and display that.


## dropdownApi.fetchDropdown returns a fabricated empty response on any error instead of throwing

**legacy_frontend/lib/api.ts:661** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** Every failure path — 4xx/5xx (thrown at line 652-654), network error, JSON parse failure — is converted into a well-formed `DropdownResponse` with empty option arrays. Callers cannot distinguish 'this category legitimately has no sub-categories' from 'the request failed'. The fabricated `options` object also omits `item_ids`, `item_sale_groups` and `item_uoms`, which the type declares as optional (line 30-34) and which the parallel-index contract documented at line 31-32 depends on.

**Failure scenario.** Backend restarts mid-session. Operator on the direct transfer form picks item_category 'NUTS'; fetchDropdown 500s, returns the empty stub. The sub-category select renders with zero options and no error, so the operator believes NUTS has no sub-categories, picks a different category, and files the transfer against the wrong one. Only a console.error records the failure.

**Fix.** Rethrow and let the caller own loading/error state (the hooks at api.ts:54-147 already do exactly this correctly with their own error state); if a soft-fail is required, return a discriminated `{ ok:false, error }` so callers must handle it.


## getAllData() is unpaginated and its `total` counts line rows, not transfers

**legacy_frontend/lib/api/transferDashboardApi.ts:34** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: plausible

**Problem.** No page/per_page/limit is sent or supported: the endpoint (transfer_dashboard_server.py:30-61) SELECTs the full header⋈lines join with no LIMIT and materialises it via `fetchall()`. `total` is `len(records)` — the number of *line* rows after the fan-out — so it is not a transfer count and does not match a distinct-transfer figure. The whole array is then JSON.stringify'd into localStorage by `writeTransferCache` (line 69-81) with only a blanket try/catch for quota.

**Failure scenario.** With ~3,000 transfers averaging 4 lines each, /all-data returns ~12,000 records in a single response; `total` reads 12,000, so any 'N transfers' label sourced from it overstates by ~4x. The serialized payload exceeds the ~5 MB localStorage quota, `writeTransferCache` swallows the QuotaExceededError at line 78-80, and the documented instant-paint cache silently never works — every dashboard visit shows the full skeleton with no diagnostic.

**Fix.** Add page/per_page (or a date-window) parameter, return `total` as `COUNT(DISTINCT h.id)` alongside a separate `line_count`, and have writeTransferCache check payload size before writing (and log, not swallow, a quota failure).


## Item-description dropdown mis-pairs UOM with descriptions and silently truncates at 500 items

**legacy_frontend/lib/hooks/useDropdownData.ts:552** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: plausible

**Problem.** This hook backs the Item Description dropdown in both transfer forms (transferform:198, directtransferform:199), and its `uom` becomes article.unit_pack_size (transferform:209), which drives calculateNetWeight (transferform:732-745, directtransferform:1003-1016). The backend returns strictly parallel arrays (interunit_tools.py:4048-4049), but the FE filters descriptions BEFORE indexing into the unfiltered uom_values, so a single blank/empty particulars value in the slice shifts every later item's UOM by one. Separately, limit=500 is a hard cap while the response's meta.total_item_descriptions is never read, so a sub-category with more than 500 items truncates with no indication, and no `search` param is sent, so SearchableSelect can only filter the truncated slice.

**Failure scenario.** (a) A sub-category slice contains one row with an empty particulars value at position 12. Item #13 onward each receive the previous item's uom: selecting a 0.500 kg item silently fills Unit Pack Size 0.250, and a qty-4 × pack-10 FG line computes 10.000 kg instead of 20.000 kg. (b) Sub-category 'SPICES' holds 640 SKUs; items 501-640 can never be selected from the dropdown and the operator has no way to know they exist.

**Fix.** Zip before filtering (`itemDescs.map((d, i) => ({d, uom: uomValues[i]})).filter(x => x.d)`), and either raise the limit to the reported total or wire the SearchableSelect's search term through to the endpoint's `search` param.


## No AbortSignal plumbing in InterunitApiService — concurrent list fetches can land out of order

**legacy_frontend/lib/interunitApiService.ts:92** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: plausible

**Problem.** `fetchJSON` would forward a `signal` if given one, but not a single exported method accepts or constructs one, so callers have no way to cancel an in-flight request. The consumers fire three of these concurrently keyed on paging/filter state (app/[company]/transfer/page.tsx:97, 118, 140) and again in cold-transfer/page.tsx:122, 143, 173, 201, each doing `setRequests(response.records)` on resolve. There is no request-generation guard either, so a slower earlier response overwrites a faster later one.

**Failure scenario.** Operator on the Transfers tab is on page 3, clicks page 4, then quickly page 5. Request for page 4 (1,100 ms, large per_page) and page 5 (250 ms) are both in flight; page 5 resolves first and renders, then page 4 resolves and calls setTransfers with page 4's rows while the pager still reads 'Page 5'. Rows from page 4 appear twice in the session and page 5's rows vanish until the user re-clicks.

**Fix.** Add an optional `signal?: AbortSignal` to each list method and forward it into fetchJSON's RequestInit, so callers can abort the previous request in a useEffect cleanup (or at minimum add a monotonically increasing request-id guard before every setState).


## getTransferIns/getColdTransferIns omit the backend's `search` param, forcing consumers into per_page=500 client-side filtering

**legacy_frontend/lib/interunitApiService.ts:483** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** Both listing endpoints accept a server-side `search` (interunit_server.py:334 and :357) that this client never exposes. Consumers therefore search client-side, and to do so they request a fixed large page — 500, or 1000 for cold transfer-outs, which is exactly the backend's `le=1000` ceiling (interunit_server.py:271, 330, 353). Anything beyond that slice is invisible to the search, and the truncation is silent: the response still carries a correct `total`, and the UI overrides the pager (`setTotalPages(filtering ? 1 : response.total_pages)`, transfer/page.tsx:103) so the user sees 'Page 1 of 1'.

**Failure scenario.** 3,400 transfer-ins exist. Operator types GRN 'GRN-2025-0041' (the 900th row by created_at DESC). The page fetches per_page=500 sorted created_at DESC, filters those 500 client-side, finds nothing, and renders 'No transfer-ins found' with no indication that only the newest 500 were searched. The record exists and the backend's own `search` would have found it.

**Fix.** Add `search?: string` to both param types and forward it, then have the consumers pass the search box value and page normally instead of fetching a 500/1000-row slab.


## Duplicate /transfer/jobwork route tree and a dead JobworkApiService duplicating the whole dashboard surface

**legacy_frontend/lib/jobworkApiService.ts:67** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** There are three parallel implementations of the same job-work summary: (1) the Summary tab in /transfer/job-work/page.tsx using /job-work/reports/dashboard plus a client-side tree; (2) /transfer/jobwork/dashboard/page.tsx which re-implements KPIs, grouping and filtering client-side over /job-work/list; (3) lib/jobworkApiService.ts wrapping a dedicated backend surface /jobwork/dashboard/{summary,filter-options,group-details,jwo-receipts,export-excel} (jobwork_dashboard_server.py, prefix line 33). A repo-wide grep finds no import of JobworkApiService and no link/router.push to "transfer/jobwork" anywhere in the app — the service is dead code and the dashboard route is unreachable except by typing the URL. Only JobworkApiService sends the Authorization bearer token (line 19); every other job-work fetch in these pages uses a bare fetch.

**Failure scenario.** A maintainer fixes an aggregation bug in the Summary tab; the near-identical /transfer/jobwork/dashboard page (still reachable by URL and by anyone with a bookmark, and holding its own broken date/process logic) keeps reporting the old, different numbers for the same data — and the server-side /jobwork/dashboard aggregates that were built for it (including the working Excel export) are never exercised.

**Fix.** Pick one: either wire /transfer/jobwork/dashboard to JobworkApiService (which fixes the receipts stub and gives it a working Excel export) and link it from the Summary tab, or delete the /transfer/jobwork tree plus lib/jobworkApiService.ts and retire the /jobwork/dashboard backend router.


## buildSummary's total_weight falls back from net to gross per node, so parent totals do not equal the sum of their children

**legacy_frontend/lib/transfer/buildSummary.ts:126** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** `net || gross` is evaluated independently at every level of the tree (L1 group, L2 child, leaf item), so a node whose net sums to exactly 0 silently switches to a GROSS measure while its siblings report NET. Mixing the two inside one column means the L1 value is not the sum of its L2 values, and the sort at :86-90 (`b.total_weight - a.total_weight`) then orders groups by a quantity that is net for some rows and gross for others.

**Failure scenario.** Group "Cold Storage" has two sub-groups. Child X: net 0, gross 100 -> total_weight 100. Child Y: net 50, gross 200 -> total_weight 50. Parent: net 50, gross 300 -> `50 || 300` = 50. The dashboard tree prints a parent of 50 above children of 100 and 50 (sum 150), and X sorts above Y despite carrying less net weight. Users expanding the row see the children exceed the parent.

**Fix.** Pick one measure for total_weight and apply it uniformly (e.g. always net, exposing gross separately as the existing total_gross_weight already does), or compute the fallback once at the top level and pass the chosen measure down through buildSummary.


## create_cold_transfer_out silently under-parks: boxes missing from cold_stocks are written to the challan but never deducted or ledgered

**services/ims_service/cold_transfer_out_tools.py:373** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** park_in_pending skips any box whose (box_id, transaction_no) is not found in <company>_cold_stocks — it logs a warning and `continue`s before both the pending INSERT and _delete_source_row (pending_stock_tools.py:1294-1297, 1388). The caller never compares `parked` against len(payload.boxes) and never raises. Meanwhile the same box was already INSERTed into interunit_transfer_boxes at line 347 and counted by _apply_box_totals. The result is a challan line claiming stock that was neither deducted from source nor placed in the in-transit ledger.

**Failure scenario.** The form's pick-boxes cache is stale and 3 of 100 boxes were already dispatched on an earlier challan. create_cold_transfer_out inserts 100 interunit_transfer_boxes rows, _apply_box_totals sets the line qty to 100, but park_in_pending parks only 97 and returns 97. The API returns boxes_parked=97 with HTTP 201 and no error surface. Those 3 boxes remain absent from cold_stocks (already gone) yet appear on two challans; the receiving site can only ever receive 97, so count_remaining_in_transit will never reconcile against the 100-box document and the transfer cannot complete.

**Fix.** Compare parked against the number of boxes eligible for parking and raise 400 (rolling back) listing the box_ids that had no source row — the dispatch document must not claim stock the ledger could not move.


## edit_cold_transfer_out permits editing a partially-received dispatch and silently drops the already-received boxes

**services/ims_service/cold_transfer_out_tools.py:455** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** Under the bridge invariant a PARTIALLY received transfer deliberately stays in status 'Dispatch' (see interunit_tools.py:2299-2301 and 2889-2891: 'Incomplete receipt — ... stay Pending so the unreceived boxes keep showing on the bridge (Transfer OUT stays Dispatch)'). So this guard does not block editing a transfer that already has a GRN against it. edit then calls restore_to_source (line 462), which can only restore rows still In Transit, wipes all boxes and lines (464-465), and re-parks every payload box. Already-received boxes were consumed from cold_stocks at receive time, so park_in_pending's _find_in_cold_stocks returns None and they are silently skipped (pending_stock_tools.py:1295-1297). delete_cold_transfer_out handles exactly this case correctly by unpicking the transfer-ins first (lines 669-682); edit has no equivalent.

**Failure scenario.** A 100-box cold dispatch has 40 boxes received on GRN-248; the OUT header is still 'Dispatch'. The dispatcher edits the challan to fix a vehicle number and resubmits the same 100 boxes. restore_to_source restores the 60 In-Transit rows to cold_stocks; boxes+lines are deleted and reinserted; park_in_pending re-parks only the 60 it can find in cold_stocks and skips the 40 already sitting in the receiving GRN. boxes_parked returns 60 with no warning, the ledger now shows 60 for a 100-box challan, and the 40 received boxes belong to no dispatch record.

**Fix.** Reject the edit when any interunit_transfer_in_header / cold_transfer_in_headers row exists for this transfer_out_id (mirroring the receiving-aware check the reconcile path already uses), or unpick the transfer-ins first the way delete_cold_transfer_out does.


## TransferInEdit is missing the uppercase validator its sibling create models have, so an edited receiving_warehouse becomes invisible to the list filter

**services/ims_service/interunit_models.py:358** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** `TransferInCreate` (models 404-407) and `PendingTransferInCreate` (models 419-422) both apply `@field_validator("receiving_warehouse", "received_by") -> v.upper()`, so stored values are uppercase. `TransferInEdit` has no such validator, and `edit_transfer_in` writes the raw value (`"receiving_warehouse": data.receiving_warehouse`, interunit_tools.py:3603). Meanwhile `list_transfer_ins` filters with exact equality against an uppercased input: `clauses.append("h.receiving_warehouse = :rw"); params["rw"] = receiving_warehouse.upper()` (interunit_tools.py:3196-3197).

**Failure scenario.** A privileged user corrects GRN-4471's destination via `PUT /interunit/transfer-in/by-transfer-out/1615/edit` with `receiving_warehouse: "Warehouse A68"`. The row is stored mixed-case. From then on `GET /interunit/transfer-in?receiving_warehouse=WAREHOUSE A68` compares `'Warehouse A68' = 'WAREHOUSE A68'` → false, and the receipt disappears from every warehouse-filtered Transfer In list while still counting toward an unfiltered `total`.

**Fix.** Add the same `@field_validator("receiving_warehouse")` uppercase coercion to `TransferInEdit`, and make the filter case-insensitive (`UPPER(h.receiving_warehouse) = :rw`) as `list_cold_transfer_ins` already does at interunit_tools.py:3304.


## GET /interunit/pending-stock returns the entire in-transit ledger with no pagination and no per_page/limit param

**services/ims_service/interunit_server.py:107** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: unverified

**Problem.** No `page`/`per_page`/`limit` param exists, and `list_pending_transfers` (pending_stock_tools.py:2461-2574) has no LIMIT — it returns every row of the combined CTE plus a second unbounded query for chip counts. `total` is just `len(records)` (line 2648), so the client cannot even detect truncation. The only server-side text filter is `transfer_out_challan_no ILIKE :s` (line 2456), so lot/site search must happen client-side over the full payload. The frontend re-fetches this on every filter change (PendingTransfersModal.tsx:88-124 with `search` in the useCallback deps, i.e. on every keystroke).

**Failure scenario.** With ~2 years of dispatch history the in-transit + orphaned Dispatch/Partial population grows unbounded (the orphan branch at pending_stock_tools.py:2562-2567 pulls in every header still in 'Dispatch'/'Partial', which never ages out). Typing 'TRANS2026' into the modal's search box issues one full-table CTE scan per character, each returning the whole record set — the modal freezes and the DB takes repeated sequential scans of pending_transfer_stock joined to interunit_transfers_header.

**Fix.** Add `page`/`per_page` (with a sane `le=`) and a server-side LIMIT/OFFSET plus a real `COUNT(*) OVER ()` total; move the chip-count query behind its own cached endpoint.


## DELETE /interunit/requests/{id} omits the user_role param its sibling delete accepts, so admins/developers are permanently 403'd

**services/ims_service/interunit_server.py:246** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** `_check_delete_permission(user_email)` is called with `user_role` defaulted to `""`, so only the two hard-coded addresses in `AUTHORIZED_DELETE_EMAILS` (line 87) can ever delete a request. The transfer delete at line 305-313 accepts and forwards `user_role`, giving admin/developer a bypass. The frontend compounds this: `InterunitApiService.deleteRequest` sends only `user_email` (interunitApiService.ts:340) and `deleteTransfer` likewise sends only `user_email` (interunitApiService.ts:353) — the role bypass is only ever exercised from PendingTransfersModal.tsx:256-262.

**Failure scenario.** A user with role 'admin' but email `ops@candorfoods.in` clicks Delete on a transfer request: `DELETE /interunit/requests/12?user_email=ops@candorfoods.in` → 403 'You are not authorized to delete records'. The same user cancels a transfer from the Pending modal (which does send `user_role=admin`) → 200. Identical privilege, inconsistent outcome, and the Transfer Out list's own delete button also 403s because it omits `user_role`.

**Fix.** Add `user_role: str = Query("")` to `delete_request_endpoint` and pass it through, and have `InterunitApiService.deleteRequest`/`deleteTransfer` append `user_role` — or, preferably, take both from the JWT per the auth finding above.


## GET /interunit/transfers strips lot_numbers_text via response_model, so the list's search-by-lot can never match

**services/ims_service/interunit_server.py:268** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** list_transfers computes `lot_numbers_text` with a dedicated STRING_AGG subquery (interunit_tools.py:1491-1497) and attaches it to every record (line 1512), but TransferListItem (interunit_models.py:310-315) declares only items_count / boxes_count / total_qty / pending_items. FastAPI serializes through the declared model, so lot_numbers_text (and pending_items' sibling extras) are dropped before the response leaves the server.

**Failure scenario.** An operator types a lot number, e.g. '125859', into the Transfer Out Records search box. legacy_frontend/app/[company]/transfer/page.tsx:253 and cold-transfer/page.tsx:367 both call searchMatch(t, term, [..., "lot_numbers_text"]) — the key is always undefined on the client, so the search returns 'No matching records' even though the backend ran the aggregation to answer exactly that query.

**Fix.** Add `lot_numbers_text: str = ""` to TransferListItem in interunit_models.py (next to items_count/boxes_count), or drop `response_model=TransferListResponse` from the endpoint. The former is preferable — it keeps the contract explicit.


## GET /interunit/transfers exposes no `search` param, forcing the UI to fetch 500 rows and filter client-side over a truncated slice

**services/ims_service/interunit_server.py:268** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: unverified

**Problem.** The sibling transfer-in list accepts `search` (line 334, wired into an ILIKE across GRN/challan/receiver/lot/box-id/article at interunit_tools.py:3204-3219) but the transfer-out list offers only exact-match `challan_no`. With no server-side text search, the frontend compensates by fetching `per_page=500` whenever any filter is active and filtering in the browser (transfer/page.tsx:84, 118-123) — i.e. it paginates and searches over a server-truncated slice, and `le=1000` caps the escape hatch.

**Failure scenario.** With 1,200 transfers, the operator types challan 'TRANS202601' into the Transfer Out search box. The FE requests page 1 with per_page=500, so only the 500 most recent transfers (sorted `created_ts desc`) are ever searched; a January 2026 challan sitting at position 800 is never returned and the UI reports 'no results' for a record that plainly exists. Raising per_page past 1000 is rejected by the `le=1000` validator.

**Fix.** Add `search: Optional[str] = Query(None)` to `list_transfers_endpoint` and implement the ILIKE predicate over challan_no / from_site / to_site / vehicle_no / box lot_number in `list_transfers`, mirroring interunit_tools.py:3204-3219, so the FE can drop the 500-row prefetch.


## GET /interunit/transfers/{id} strips grn_records, lot_origin_unit and the per-box source_unit/unit_pack_size that get_transfer computes

**services/ims_service/interunit_server.py:288** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** get_transfer builds three sets of enrichment fields the response model does not declare: `grn_records` (interunit_tools.py:1672, absent from TransferWithLines), `lot_origin_unit` on every line and box (line 1651-1653, absent from TransferLineResponse and BoxResponse), and `source_storage` / `source_unit` / `unit_pack_size` / `rm_pm_fg_type` / `item_category` on boxes (_map_box_row lines 593-598, absent from BoxResponse at interunit_models.py:289-303). Pydantic drops all of them on serialization, so ~40 lines of backend work (including the per-lot dominant-unit CTE at 1612-1645) is computed and thrown away on every request.

**Failure scenario.** PendingTransfersModal.tsx:491 fetches `${apiUrl}/interunit/transfers/${transfer_out_id}` and at line 534 reads `data.grn_records || []` to render the 'GRN' and 'Rcvd boxes' hover chips — the array is always empty, so a transfer whose GRN is already half-received looks untouched in the pending modal. Likewise ChallanHoverCard.groupBoxesByItem reads `b.lot_origin_unit` (line 376) and `b.source_unit || b.source_storage` (line 378) for the 'From' chip and `b.unit_pack_size` (line 352) for the PM 'Total Count' chip; all arrive undefined, so cold boxes fall back to the header-level from_cold_unit and the Count chip disappears for box-backed transfers.

**Fix.** Declare the fields on the models: add `grn_records: List[dict] = []` to TransferWithLines, `lot_origin_unit: Optional[str] = None` to TransferLineResponse and BoxResponse, and `source_storage`, `source_unit`, `unit_pack_size`, `rm_pm_fg_type`, `item_category` (all Optional) to BoxResponse — or remove response_model from the endpoint. Do not 'fix' this by deleting the backend computation; three UI surfaces depend on it.


## Every query in /interunit/box-history swallows exceptions and returns an empty list, reporting 'box not found' when the query actually failed

**services/ims_service/interunit_server.py:631** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** `_scan_table` (line 622-638) and the two ledger blocks (lines 679-680 and 706-707: `except Exception: dispositions = []` / `reconciliations = []`) catch every exception and substitute an empty result. A missing column (`transaction_no` is not present on all four scanned tables), a permission error, or a broken connection is indistinguishable from 'this box is not in that ledger'. The endpoint then reports `summary.in_cold_stocks: 0`, `in_warehouse: 0`, `in_transit: 0` with HTTP 200. Note the DB session is also left in a failed-transaction state after the first swallowed error, so all subsequent scans in the same request fail and return `[]` too.

**Failure scenario.** `GET /interunit/box-history/90671000-1?txn=TR-20260716141141` — the first `_scan_table('cfpl_cold_stocks')` raises because a legacy table lacks `transaction_no`; Postgres aborts the transaction. Every following scan (cdpl_cold_stocks, both boxes_v2 tables, pending_transfer_stock, dispositions, reconciliations) then fails with 'current transaction is aborted' and is silently swallowed. The operator gets a clean 200 saying the box exists nowhere and no event ever touched it — the exact opposite of the truth — while investigating a missing-inventory incident.

**Fix.** Let the exceptions propagate (the endpoint already guards table existence with `to_regclass`), or catch narrowly, `db.rollback()`, log, and return an explicit per-source `{"error": ...}` marker so the response distinguishes 'no rows' from 'lookup failed'.


## GET /interunit/box-history/{box_id} matches dispositions by substring on `notes`, pulling in unrelated boxes and inflating the audit counts

**services/ims_service/interunit_server.py:658** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** Without a `txn` the disposition ledger is matched with an unanchored LIKE against the free-text `notes` column. Box IDs in this system are `<8-digit-base>-<n>` (see interunit_tools.py:3461-3465 and COLD_TRANSFER_DUPBOX_INCIDENT.md), so every id is a strict prefix of nine other ids. The matched rows flow into `dispositions`, into the merged `timeline`, and into `summary.disposition_events` / `summary.active_dispositions` (lines 750-755).

**Failure scenario.** Auditing box `90671000-1` without `?txn=`: a relabel note recorded for box `90671000-12` ('relabelled from 90671000-12') contains the substring `90671000-1`, so that unrelated box's Direct-Out / Job-Work / Transfer-Out events appear in box 90671000-1's dossier and timeline. The endpoint reports `active_dispositions: 2` for a box that has one, leading an auditor to conclude the box left the site twice.

**Fix.** Drop the `notes LIKE` branch (or anchor it on a delimiter, e.g. `notes ~ ('(^|[^0-9-])' || :b || '($|[^0-9-])')`), and keep the `box_id = :b` predicate as the sole match.


## _ensure_interunit_schema marks itself done even when the ALTERs failed, turning a one-off DDL failure into a permanent 500 on the list endpoint

**services/ims_service/interunit_tools.py:112** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: unverified

**Problem.** The `_interunit_schema_ensured = True` assignment sits outside the try/except, so a failed ALTER (permissions, lock timeout, a failed earlier statement poisoning the rest of the block) is swallowed as a warning and never retried for the life of the process. list_transfers then unconditionally selects `h.from_cold_unit` (line 1470) and filters on it (line 1428). If the ADD COLUMN at line 77-80 did not land, every subsequent GET /interunit/transfers raises UndefinedColumn.

**Failure scenario.** Deploy against a database role without ALTER on interunit_transfers_header. First request logs one WARNING and sets the flag; every request thereafter fails with `column h.from_cold_unit does not exist` → HTTP 500. The Transfer Out Records tab is empty for all users, and the only trace of the cause is a single warning line from the first request.

**Fix.** Set the flag only on success (move the assignment inside the try, after db.commit()), and make list_transfers tolerant: `COALESCE(to_jsonb(h)->>'from_cold_unit', NULL)` is not viable, so instead probe the column once via information_schema and omit the projection/filter when absent.


## list_requests returns total_pages: 0 whenever the requested page is past the end, while total is non-zero

**services/ims_service/interunit_tools.py:357** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: unverified

**Problem.** The early-return path hard-codes `total_pages: 0` instead of the computed `(total + per_page - 1)//per_page` used on the normal path (line 397). Any page whose slice is empty — the page after the last one, or a page reached after rows were deleted — reports zero pages while reporting a positive `total`. This backs `GET /interunit/requests` (interunit_server.py:216, response_model=RequestListResponse).

**Failure scenario.** There are 43 requests, per_page=10. The user is on page 5 (records 41-43), deletes those 3 requests, and the list reloads page 5: the backend returns `total: 40, total_pages: 0`. The FE sets `totalPages = response.total_pages` (transfer/page.tsx:103) and `handlePageChange` guards with `page <= totalPages` (line 185), so every pagination button is now dead — the user is stranded on an empty page with no way back to page 1 without a full reload.

**Fix.** Use the same expression on both paths: `"total_pages": (total + per_page - 1) // per_page if total else 0`.


## _fetch_boxes joins pending_transfer_stock on box_id alone — unscoped by transfer and by transaction_no, so it can fan out box rows or attribute another transfer's source unit

**services/ims_service/interunit_tools.py:644** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** pending_transfer_stock is unique on the COMPOSITE key (box_id, transaction_no) — every INSERT in pending_stock_tools.py uses `ON CONFLICT (box_id, transaction_no) DO NOTHING` (lines 1354, 1504, 1568) — so box_id alone is not unique, which is precisely the cross-transaction collision documented in COLD_TRANSFER_DUPBOX_INCIDENT.md (Mode A: `{base}-1` minted in two transactions). This LEFT JOIN matches on box_id only and does not constrain `pts.transfer_out_id = itb.header_id` or `pts.transaction_no = itb.transaction_no`, even though the sibling query get_pending_boxes_by_transfer_out (line 3136-3139) correctly scopes on both. Two In-Transit rows sharing a box_id therefore multiply the box row, and a single foreign match silently labels the box with another dispatch's storage_location/unit.

**Failure scenario.** Transfer X is in transit with box_id '90512000-1' (txn TR-…513) and transfer Y is in transit with the same box_id under txn TR-…751. Opening GET /interunit/transfers/{X} returns 2 rows for that one box: get_transfer's `boxes` array over-reports, the DC's 'No. of Boxes' column (built by counting the boxes array) over-counts, and re-opening the transfer in the edit form re-sends the duplicated box — which the (box_id, transaction_no) guard at cold_transfer_out_tools.py:329 then rejects with HTTP 400, leaving the operator unable to save. In the single-match case the box just shows the wrong 'From' chip (Savla D-39 instead of Rishi).

**Fix.** Scope the join the way get_pending_boxes_by_transfer_out already does:

            LEFT JOIN pending_transfer_stock pts
                   ON pts.box_id = itb.box_id
                  AND COALESCE(pts.transaction_no,'') = COALESCE(itb.transaction_no,'')
                  AND pts.transfer_out_id = itb.header_id
                  AND pts.status = 'In Transit'


## int(line.quantity) raises ValueError on the decimal strings the request model itself produces → unhandled HTTP 500

**services/ims_service/interunit_tools.py:1152** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** TransferLineCreate.quantity is typed `Optional[str]` but interunit_models.py:203-209 installs a before-validator `_coerce_number_to_str` that turns any JSON int/float into `str(v)` precisely so numeric payloads stop 422-ing. A JSON number 2.0 (or any non-integral quantity) therefore arrives as the string "2.0", and `int("2.0")` raises ValueError, which nothing catches — FastAPI turns it into a 500 whose CORS headers are lost, the exact failure mode the TransferHeaderCreate comment at interunit_models.py:161-166 describes. Same call at update_transfer line 1776. Note the neighbouring conversions are safe because they use float().

**Failure scenario.** A transfer form (or any integration) posts lines with `"qty": 2.0` or `"quantity": "1.5"` — legal per the model, which advertises numeric coercion. create_transfer has already allocated a challan number and INSERTed the header row before reaching line 1152; the ValueError aborts the request with a bare 500 and the operator, who has scanned every box, sees 'Failed to fetch' with no field named.

**Fix.** Parse defensively: `qty_i = int(float(line.quantity or 0)) or 1`, or validate/normalize quantity in TransferLineCreate (add a validator that rejects non-integral values with a 422 naming the field).


## Mid-request COMMIT/ROLLBACK inside the schema self-heal breaks create_transfer / update_transfer atomicity

**services/ims_service/interunit_tools.py:1294** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: plausible

**Problem.** get_db (shared/database.py:13-22) owns exactly one transaction per request and commits at the end, so create_transfer/update_transfer are supposed to be atomic. But _ensure_reconciliation_schema (pending_stock_tools.py:363-480) — invoked from park_in_pending line 1394 and from reconcile_transfer_to_order lines 1755/1806, both of which run inside create_transfer (1294/1375) and update_transfer (1921/1992) — calls db.commit() at line 475 on success and db.rollback() at line 479 on failure, on the caller's session. The rollback path discards the caller's uncommitted INSERTs; the commit path splits the write into two transactions so a later failure returns 500 on work that is already durable. _ensure_interunit_schema in this file does the same at line 109.

**Failure scenario.** First transfer submitted after a server restart. The DDL block in _ensure_reconciliation_schema hits a lock timeout on `ALTER TABLE interunit_transfers_header ADD COLUMN IF NOT EXISTS edited_at` → the except branch calls db.rollback(), wiping the header, lines and boxes just inserted by create_transfer. Execution continues; get_db commits an empty transaction; the endpoint returns 201 with an id that no longer exists in interunit_transfers_header, and the operator's scanned consignment is gone. On the success path the mirror case: reconcile commits at line 1992, then the `edited_at` UPDATE at line 1999 fails → 500 to the client for an edit that actually persisted, and the operator re-submits, triggering a second restore_to_source + re-park cycle.

**Fix.** Never commit or roll back a borrowed session inside a helper. Run all schema self-heals on their own short-lived engine connection (or once at startup in main._run_startup_migrations, which is where the vakkal/transfer_out_box_id migrations already live) and delete the commit/rollback calls from _ensure_reconciliation_schema and _ensure_interunit_schema.


## Dispatch status compares box count + LINE count against a qty SUM, marking fully-parked transfers 'Partial'

**services/ims_service/interunit_tools.py:1350** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** `len(uncovered_lines)` is a count of LINE ROWS, not of the units those lines carry, while total_expected sums `qty` across all lines. A single manual line of qty 50 contributes 1 to actual_dispatched and 50 to total_expected. The comment two lines above (1346-1347) claims the opposite: "Everything is now parked (scanned boxes + box-less manual lines), so a mixed / short-scan transfer is still fully dispatched — not Partial." Identical defect in update_transfer at lines 1977-1979.

**Failure scenario.** 100 scanned boxes of article X (line qty 100) plus one manually typed line of article Y, qty 50, which park_lines_in_pending parks in full (50 In-Transit rows). total_expected = 150, actual_dispatched = 100 + 1 = 101 → header written as 'Partial'. The Transfer Out list shows a Partial badge and the status filter `h.status = 'Dispatch'` (line 1417) excludes the transfer entirely from the default Dispatch view.

**Fix.** Compare like with like: `actual_dispatched = len(boxes) + sum(int(l.qty or 0) for l in uncovered_lines)`, or drop the arithmetic and derive the status from the parked-row count returned by park_in_pending + park_lines_in_pending.


## list_transfers re-aggregates the whole lines and boxes tables on every page request

**services/ims_service/interunit_tools.py:1478** &nbsp;|&nbsp; perf &nbsp;|&nbsp; verdict: plausible

**Problem.** None of the three derived tables is correlated to the filtered header set — the WHERE clause references only `h.` columns and sits outside them, so Postgres cannot push the header filter or the LIMIT down. Every request grouping-aggregates all 35,457 rows of interunit_transfers_lines and twice over all 14,362 rows of interunit_transfer_boxes (per _schema_dump.json counts) to return 10 records. The lt subquery is pure waste on top of that, since its output is stripped by the response model (see the lot_numbers_text finding).

**Failure scenario.** The Transfer page's filter mode requests per_page=500 (legacy_frontend/app/[company]/transfer/page.tsx:120, FILTER_FETCH_SIZE) and the Requests/Transfer-In tabs fire in parallel on mount; each transfers request performs three full-table GROUP BYs plus a DISTINCT STRING_AGG. Latency grows linearly with total line/box history rather than with the page size, and it degrades every further transfer as the tables grow.

**Fix.** Correlate the aggregates to the page: select the header ids first (filter + ORDER BY + LIMIT/OFFSET), then join the subqueries with `WHERE header_id = ANY(:ids)`, or replace them with LATERAL subqueries against h.id. Drop the lt subquery unless lot_numbers_text is added to the response model.


## list_transfers boxes_count uses COUNT(DISTINCT box_id) — under-counts legitimately distinct boxes and contradicts the detail endpoint

**services/ims_service/interunit_tools.py:1487** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** A box's identity is (box_id, transaction_no) — create_cold_transfer_out itself dedupes on that composite pair (cold_transfer_out_tools.py:329), so two rows on one header may legitimately share a box_id under different transaction_no. DISTINCT on box_id alone collapses them, so boxes_count is lower than the number of physical boxes. It is also the exact inverse of the _fetch_boxes fan-out (line 645), which inflates the same transfer's box count on the detail endpoint. pending_items at line 1511 is derived from this under-count, so it compounds.

**Failure scenario.** A 100-box cold dispatch includes the documented pair 90671000-1/TR-...751 and 90671000-1/TR-...513. The transfer list shows boxes_count=99 and pending_items = total_qty(100) - 99 = 1, flagging a phantom shortfall; opening the same transfer shows 198 boxes from _fetch_boxes. The operator sees three mutually contradictory counts (99, 100, 198) for one truck.

**Fix.** Use COUNT(DISTINCT (box_id, COALESCE(transaction_no,''))) — or simply COUNT(*), since the boxes subquery is already grouped by header_id and does not fan out — and align it with the detail endpoint once finding #1 is fixed.


## boxes_count uses COUNT(DISTINCT box_id), which under-counts the legitimately non-unique box_ids the create guard allows

**services/ims_service/interunit_tools.py:1487** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** Both dispatch guards key uniqueness on the PAIR (box_id, transaction_no) — cold_transfer_out_tools.py:329-339 and interunit_tools.py:1236-1246 — so two rows with the same box_id and different transaction_no are accepted by design (COLD_TRANSFER_DUPBOX_INCIDENT.md documents that this is real data: one migration minted the same `{base}-n` under two transaction_nos). COUNT(DISTINCT box_id) collapses those into one, so the list's box count is lower than the number of boxes actually dispatched, and `pending_items = total_qty − boxes_count` (line 1511) is correspondingly inflated.

**Failure scenario.** A cold dispatch of 67 boxes from lot 125859 where two boxes carry box_id '90671000-1' under transactions TR-…513 and TR-…751 (the exact live shape in the incident doc). interunit_transfer_boxes holds 67 rows, pending_transfer_stock holds 67 In-Transit rows, but the Transfer Out Records list reports 66 boxes and one extra pending item.

**Fix.** Count the rows, matching the definition every other surface uses (pending_stock_tools.py:2498, 2537 both use plain COUNT(*)): `COUNT(*) AS boxes_count`. If de-duplication is genuinely wanted, it must use the same composite key as the write guards: `COUNT(DISTINCT (COALESCE(box_id,''), COALESCE(transaction_no,'')))`.


## All four list endpoints paginate with an unstable ORDER BY (no unique tiebreaker), so rows repeat or vanish across page boundaries

**services/ims_service/interunit_tools.py:1499** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: unverified

**Problem.** `list_transfers` orders by a single non-unique column (`sort_by` ∈ {challan_no, stock_trf_date, from_site, to_site, status, created_ts}) with LIMIT/OFFSET and no `, h.id` tiebreaker. Postgres gives no ordering guarantee among ties, and the plan can differ between the OFFSET 0 and OFFSET 15 executions. The same pattern exists at interunit_tools.py:351 (`ORDER BY r.created_ts DESC` in list_requests), 3258 (`ORDER BY h.{sort_by}` in list_transfer_ins) and 3381 (list_cold_transfer_ins). Sorting by `stock_trf_date` (a DATE) or `status` makes ties the common case, not the edge case.

**Failure scenario.** The user sorts Transfer Out Records by `stock_trf_date desc` with per_page=15 and 40 transfers dated 2026-08-14. Page 1 (OFFSET 0) returns challans A..O; page 2 (OFFSET 15) re-sorts the same tied block and returns C, F, O again while never showing P and Q. The operator sees duplicate challans across pages and two transfers that exist in `total` but appear on no page.

**Fix.** Append the primary key as a final tiebreaker in every paginated query: `ORDER BY h.{sort_by} {direction}, h.id {direction}` (and `r.created_ts DESC, r.id DESC` in list_requests).


## list_transfers computes pending_items by subtracting a box count from a line-quantity sum (qty vs boxes conflation)

**services/ims_service/interunit_tools.py:1511** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** `total_qty` is `COALESCE(SUM(qty),0)` over `interunit_transfers_lines` (line 1481) — a quantity in the line's own UOM (cartons for RM/FG, but pieces/bags for PM lines, whose `uom` can be BAG and whose `unit_pack_size` carries the piece count). `boxes_count` is `COUNT(DISTINCT COALESCE(box_id, id::text))` over `interunit_transfer_boxes` (line 1487) — physical scanned boxes. Subtracting one from the other only makes sense when 1 qty unit == 1 box, which is exactly what `_map_box_row`'s own comment about PM/packaging piece counts (interunit_tools.py:595) says is not true. The result is surfaced through `TransferListItem.pending_items` (interunit_models.py:314).

**Failure scenario.** A PM transfer line orders qty=5000 pouches shipped in 10 cartons; 10 boxes are scanned. `total_qty=5000`, `boxes_count=10`, so `pending_items = 4990`. The Transfer Out row shows 4,990 items still pending on a fully-scanned, fully-dispatched transfer, and any 'incomplete dispatch' badge driven off `pending_items` fires permanently.

**Fix.** Compute the shortfall against a comparable base — either carton-equivalent qty (only summing lines whose uom is a box/carton unit) or against the ordered box count from `interunit_transfer_boxes` — mirroring the branch logic already used for `unallocated_boxes` in pending_stock_tools.py:2497-2506.


## _fetch_transfer_in_boxes lot fallback uses a NULL-unsafe transaction_no comparison — dead for ~40% of rows

**services/ims_service/interunit_tools.py:2168** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** `ob.transaction_no = itb.transaction_no` evaluates to NULL (not TRUE) whenever either side is NULL, so the subquery returns nothing and the lot stays blank. That is precisely the population the fallback was written for — the docstring says it exists for 'older/article-level acknowledges' where the IN-box lot is blank, and those are the same rows most likely to carry a NULL transaction_no. COLD_TRANSFER_DUPBOX_INCIDENT.md records 20,577 of 51,188 rows with NULL transaction_no. Everywhere else in this file the comparison is NULL-safe (e.g. `COALESCE(otb.transaction_no,'') = COALESCE(pts.transaction_no,'')` at line 3139). The LIMIT 1 also has no ORDER BY, so with multiple matching OUT boxes the returned lot is nondeterministic between calls.

**Failure scenario.** A legacy article-level acknowledge writes an interunit_transfer_in_boxes row with lot_number='' and transaction_no=NULL; the dispatch's interunit_transfer_boxes row carries lot_number='125859' and transaction_no=NULL. The subquery's NULL=NULL predicate is NULL, no row is returned, and the Transfer-IN detail page shows a blank Lot for that box even though the dispatch knows it. Since list_transfer_ins searches ib.lot_number (line 3214), searching '125859' also misses this receipt on the IN side.

**Fix.** Use COALESCE(ob.transaction_no,'') = COALESCE(itb.transaction_no,'') (or IS NOT DISTINCT FROM) and add ORDER BY ob.id LIMIT 1 for determinism.


## create_transfer_in inserts boxes with no ON CONFLICT while acknowledge enforces a unique index — duplicate box_id 500s or duplicates depending on DB state

**services/ims_service/interunit_tools.py:2271** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** This is the one-shot receipt path and it has no conflict handling, unlike acknowledge_pending_box which UPSERTs on (header_id, box_id) (line 2616). COLD_TRANSFER_DUPBOX_INCIDENT.md lists this precise gap as unfixed deferred item 4 ('create_transfer_in one-shot INSERT — add matching ON CONFLICT'). Behaviour is therefore state-dependent: on a database where uq_transfer_in_boxes_header_box exists, a payload with two boxes sharing a box_id raises an unhandled IntegrityError (500, whole receipt rolled back); where the index does not exist, it inserts duplicate rows that will later block the index creation at line 2542 and break every acknowledge on that table.

**Failure scenario.** A GRN is submitted in one shot with 67 boxes for lot 125859, two of which are the documented cross-transaction collision 90671000-1. With the index present, the INSERT raises 23505 on the second one; get_db rolls back; the operator gets a bare 500 after entering 67 boxes and the pick_from_pending call at line 2262 is reverted too, leaving no trace of what failed.

**Fix.** Add ON CONFLICT DO UPDATE with the same target as acknowledge, and pre-validate the payload for duplicate (box_id, transaction_no) pairs with an actionable 400 naming the colliding box_id — matching the guard cold_transfer_out_tools.py:330-338 already performs.


## PendingBoxAcknowledge has no scan_source/scanned_by field — the STBR audit trail records every scan as 'manual'

**services/ims_service/interunit_tools.py:2652** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** The endpoint binds the request body to PendingBoxAcknowledge (interunit_server.py:470), and that model (interunit_models.py:425-437) declares neither scan_source nor scanned_by. Pydantic v2 silently drops unknown keys, so getattr always returns None: scan_source is hard-wired to 'manual' and scanned_by to NULL on every acknowledge, for both the single and batch endpoints. Meanwhile GET /interunit/transfer-in/{id}/reconciliation exists specifically to report 'source of scan, who scanned, when' and selects scan_source/scanned_by (interunit_server.py:527-543).

**Failure scenario.** Warehouse receives 60 boxes by QR gun; the frontend posts {box_id, transaction_no, scan_source:'qr_scan', scanned_by:'ops@candorfoods.in'}. Pydantic drops both fields. transfer_box_reconciliation records 60 rows with scan_source='manual' and scanned_by=NULL. The reconciliation audit report shows 60 manual overrides by an unknown user for a fully-scanned receipt — the exact audit evidence the STBR feature was built to provide is unusable.

**Fix.** Add `scan_source: Optional[str] = None` and `scanned_by: Optional[str] = None` to PendingBoxAcknowledge (and to whatever model the batch endpoint binds), then drop the getattr defensiveness.


## close_transfer_in_with_shortage reports a shortage of 0 while writing off every outstanding unit

**services/ims_service/interunit_tools.py:2758** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** count_remaining_in_transit deliberately EXCLUDES LINE-% sentinel rows (pending_stock_tools.py:1955), but the DELETE removes ALL In-Transit rows including sentinels. So `shortage` (the number written into the permanent audit note and returned to the client) and `written_off` (the number of rows actually destroyed) measure different populations. For any line-parked transfer — warehouse-source and warehouse->cold dispatches, where every parked row is a LINE- sentinel — shortage is 0 no matter how large the real shortfall.

**Failure scenario.** Warehouse->cold dispatch of 100 units parked as 100 LINE- rows; 0 received. An operator closes with shortage. count_remaining_in_transit returns 0, the DELETE removes 100 rows. condition_remarks is permanently stamped 'Closed with shortage: 0 box(es) written off by ops@candorfoods.in.', the API returns shortage=0 / written_off=100, and both headers flip to 'Received'. The audit record for a 100-unit loss reads as a clean, complete receipt.

**Fix.** Compute the shortage from the same population being deleted — count all In-Transit rows (or return both figures explicitly: real_boxes_short and tracking_rows_cleared) and build the note from the number actually written off.


## generate_transfer_in_qrs box-id base repeats every ~27.8 hours — the same generator the incident blames for cross-transaction collisions

**services/ims_service/interunit_tools.py:3461** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** Taking the last 8 digits of epoch-milliseconds is epoch_ms mod 100,000,000 ms = 100,000 s = 27.78 hours, so `base` cycles roughly every 27.8 hours. Each call then numbers from i=1. Two Transfer-INs generated ~27.8 h apart (or any multiple) mint byte-identical inward_box_id values. COLD_TRANSFER_DUPBOX_INCIDENT.md identifies this exact generator — 'str(int(time.time())*1000)[-8:] ... one base per lot ... numbering {base}-{n} restarting at n=1 each group' — as the root cause of the production collision, and its closing caveat asks for the opposite invariant: 'guarantee box_id global/pile uniqueness at mint'. No uniqueness check is performed against existing inward_box_id values before writing.

**Failure scenario.** Transfer-IN #1 generates QRs at 09:00 Monday with base 34732254 producing 34732254-1..40. Transfer-IN #2 generates at ~12:47 Tuesday, base wraps to 34732254 again, producing 34732254-1..25. Twenty-five printed stickers now carry ids identical to Monday's. Any downstream scan or lookup keyed on inward_box_id alone resolves to the wrong receipt's box, and both sets are indistinguishable on the shop floor.

**Fix.** Mint from a database sequence or include the receipt id / full transaction_no in the box id (e.g. f"{inward_txn_no}-{i}"), and verify uniqueness against existing inward_box_id values before committing.


## GET /transfer-dashboard/all-data has no pagination, no date bound and no filter params — the full header×line cross product every load

**services/ims_service/transfer_dashboard_server.py:30** &nbsp;|&nbsp; perf &nbsp;|&nbsp; verdict: unverified

**Problem.** The endpoint accepts zero query parameters. It executes an unbounded `interunit_transfers_header INNER JOIN interunit_transfers_lines` (line 57-58) with no LIMIT and no date predicate, then issues three more unbounded queries (box counts line 84-88, two full transfer-in header scans lines 98-107, and a full issue scan line 112-125), and serialises everything into one JSON body. All filtering — date range, warehouse, category, status, search — is done client-side (dashboard/page.tsx:242-253).

**Failure scenario.** With 4,000 transfers averaging 4 lines, one dashboard open transfers ~16,000 fully-denormalised records (each carrying the repeated header fields plus `issue_details` arrays) in a single response, and the payload grows without bound as history accumulates. There is no way for the client to ask for 'last 30 days' — the date pickers filter a payload that was already fully downloaded.

**Fix.** Accept `from_date`/`to_date` (defaulting to a bounded window such as 90 days) and `page`/`per_page`, push the warehouse/status/category filters into the WHERE clause, and return the per-transfer aggregates separately from the line rows.


## test_transfer_steps_e2e.py exercises interunit_tools._validate_cold_boxes_in_stock, which no longer exists — the suite errors out and the cold-box existence guard is gone with it

**test_transfer_steps_e2e.py:241** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: unverified

**Problem.** `grep -rn "_validate_cold_boxes_in_stock" services/` returns nothing: the function was removed from interunit_tools.py, but five tests (lines 239-271) plus the ALL list (371-375) still call it through the module alias `I`. Running `python test_transfer_steps_e2e.py` therefore dies with AttributeError at the first of them, so the four preceding Step-1/Step-2 groups are the only ones that ever report and Steps 3-4 never run. Beyond the broken suite, the behaviour the tests describe — rejecting a cold OUT whose box exists neither in <company>_cold_stocks nor In Transit — has no implementation left: create_cold_transfer_out validates only that from_warehouse is a cold site (line 220) and that (box_id, transaction_no) pairs are unique within the payload (line 329).

**Failure scenario.** CI (or a developer) runs the dependency-free suite as documented in the module docstring; it aborts at test_validate_noop_for_warehouse_source with AttributeError, so test_close_writes_off_and_marks_both_received and the other Step-4 write-off gates are silently never executed and a regression in close_transfer_in_with_shortage would ship unnoticed. Operationally, a cold dispatch whose box_id/transaction_no no longer exists in cold_stocks is now accepted; park_in_pending finds no source row to deduct and the transfer records stock that was never removed from the cold sheet.

**Fix.** Decide which side is authoritative and make them agree: either restore the guard in the cold OUT path (call it from create_cold_transfer_out/edit_cold_transfer_out before the box INSERT loop) and keep the tests, or delete tests 239-271 and their ALL entries. Do not leave the file in a state where `python test_transfer_steps_e2e.py` cannot complete.



# LOW (23)

## group-details returns every JWO in a group unpaginated, and turnaround/filters_applied are hardcoded placeholders

**legacy_backend/services/ims_service/jobwork_dashboard_server.py:441** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** No LIMIT/OFFSET on the expansion query — a group such as a high-volume vendor returns its entire JWO history in one array. The ORDER BY is also on the text job_work_date column (VARCHAR, DD-MM-YYYY) so the 'newest first' ordering is lexicographic and wrong. Separately the response hardcodes `"turnaround_days": None` (line 462), `"avg_turnaround_days": 0` (line 259) and `"filters_applied": 0` (line 286), all of which are declared as real fields the UI renders (legacy_frontend/types/jobwork.ts lines 78, 97, 106).

**Failure scenario.** Expanding a vendor with 900 JWOs ships 900 objects in one response with no way to page. Every row's Turnaround column renders '-' and the group row renders '-' for Avg TAT regardless of the data, and the header badge always says 0 filters applied even with five filters active.

**Fix.** Add page/per_page + a COUNT to group-details, order by a parsed date, and either compute turnaround from MAX(ir.receipt_date) - job_work_date (the lateral already selects last_receipt_date at line 213 and discards it) or drop the fields from the contract.


## in_transit_by_lot box_count counts synthetic per-unit rows as physical boxes

**legacy_backend/services/ims_service/pending_stock_tools.py:2808** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: plausible

**Problem.** park_lines_in_pending inserts one row per UNIT with box_id 'LINE-<line_id>-<n>' and no_of_cartons hardcoded to 1 (lines 1479-1503) for box-less transfers. Every other consumer of pending_transfer_stock filters those out (count_remaining_in_transit line 1955, the shortfall math at line 2501), but this dashboard-overlay aggregate does not, so both box_count and cartons treat units as boxes.

**Failure scenario.** A box-less transfer moves 300 units of lot 185900 in 25 physical cartons. The cold-storage dashboard's in-transit badge for that lot reads '300 boxes / 300 cartons' instead of 25, next to a stock figure counted in cartons.

**Fix.** Add `AND COALESCE(box_id,'') NOT LIKE 'LINE-%'` to the box_count (or return it as a separate unit_count), consistent with count_remaining_in_transit.


## Cold OUT box payload hardcodes no_of_cartons: 1 and sends an empty cold_storage_data despite the comment claiming metadata is folded in

**legacy_frontend/app/[company]/cold-transfer/coldtransferform/page.tsx:2818** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** The comment at lines 2782-2783 states 'the cold-storage metadata is folded into cold_storage_data so the destination cold receive (if any) can rehydrate it', but an empty object is sent and `unit` is nulled even though `record.unit` was displayed in the picker (line 777) and captured at selection time. In practice the backend ignores all three: `_box_for_park` (cold_transfer_out_tools.py:38-49) drops `no_of_cartons` and `cold_storage_data`, and `park_in_pending` re-reads the true `no_of_cartons` and rebuilds `cold_storage_data` from the source row (pending_stock_tools.py:1298-1302). So the fields are dead weight — but they encode the same false 1-carton-per-box assumption that produces finding #4, and a future reader of this payload will take `no_of_cartons: 1` as authoritative.

**Failure scenario.** A maintainer wiring a cold-to-cold destination receive trusts the declared contract and reads `no_of_cartons` off the OUT payload, getting 1 for a row that physically holds 2 cartons — halving the received carton count. Meanwhile `unit: null` means any consumer that does not go back to the source row loses the sub-cold attribution (Savla D-39 vs D-514) that the picker had already resolved.

**Fix.** Either populate the fields honestly from the selected record (`no_of_cartons` from the pile row, `unit` from `record.unit`, `cold_storage_data` from the record) or delete them from the payload and the ColdOutBoxInput model, and correct the misleading comment at lines 2782-2783.


## innercoldtransfer hardcodes navigation back to /transfer, stranding users of the /cold-transfer copy of the route

**legacy_frontend/app/[company]/cold-transfer/innercoldtransfer/page.tsx:537** &nbsp;|&nbsp; ux &nbsp;|&nbsp; verdict: confirmed

**Problem.** The file is deployed byte-identically at two routes (`app/[company]/transfer/innercoldtransfer/page.tsx` and `app/[company]/cold-transfer/innercoldtransfer/page.tsx` — verified identical by diff), but both the Back button and the post-submit redirect hardcode `/${company}/transfer`. For the /cold-transfer instance every exit path dumps the user into the unrelated warehouse transfer list. The sibling cold OUT form correctly returns to `/${company}/cold-transfer` (coldtransferform:2885, 2929, 3831, 3864).

**Failure scenario.** Operator opens /cfpl/cold-transfer/innercoldtransfer from the cold-transfer dashboard, submits an inner transfer, and is redirected to /cfpl/transfer — a different module's list that does not contain the ICT challan they just created. They cannot see or verify their submission and must navigate back to /cfpl/cold-transfer manually; the same happens on Back, discarding any in-progress entries.

**Fix.** Derive the return path from the current pathname (e.g. `usePathname().split('/innercoldtransfer')[0]`) or delete one of the two duplicate routes and redirect the other, so the exit target always matches the section the user entered from.


## Cold-transfer page's "Transfers Out" stat card shows the count of ALL transfers, not cold ones

**legacy_frontend/app/[company]/cold-transfer/page.tsx:186** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: confirmed

**Problem.** `total` comes from the unfiltered server COUNT on GET /interunit/transfers (interunit_tools.py:1454-1457). The page then filters `coldOutRaw` down to cold-related rows (`isColdRelated`, line 361) for the table and correctly shows `coldOutClientTotal` in the section header (line 675), but the KPI keeps the whole-table count.

**Failure scenario.** 452 transfers exist, 130 of them cold. The Cold Transfer page's "Transfers Out" card reads 452 while the list directly beneath it says "130 records" — the KPI is describing the warehouse transfers this page explicitly excludes.

**Fix.** Set the card from the cold-filtered count (`coldOutClientTotal`), the same value the section header already uses.


## Status hover caches receipts per row forever, showing stale "no receipts" after an inward receipt is recorded

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:153** &nbsp;|&nbsp; fetching &nbsp;|&nbsp; verdict: confirmed

**Problem.** fetched.current is a per-instance ref that is only reset on error. React keeps the StatusReceiveHover instance alive across loadRecords() refreshes because the parent <tr> key (rec.id) is unchanged, so the cached receipt roll-up is never invalidated after a mutation. handleSubmitMaterialIn (line 1041) refreshes the records list but nothing resets these hovers.

**Failure scenario.** User hovers JB202605131331 → "Receipts (0) — No receipts recorded yet". They switch to Material In, record a 1,200 kg FG receipt, return to Records (the list reloads and the status badge correctly flips to "Partial Return"), hover the same row → still "Receipts (0) / No receipts recorded yet".

**Fix.** Key the cache by a value that changes on mutation (e.g. include rec.status/receipt_count in the ref reset condition) or lift the cache into the parent and clear it whenever loadRecords runs.


## Reports tab shows "Loading Reports..." indefinitely after a failed dashboard request

**legacy_frontend/app/[company]/transfer/job-work/page.tsx:3337** &nbsp;|&nbsp; ux &nbsp;|&nbsp; verdict: confirmed

**Problem.** The final else branch is reached whenever rptData is null, which includes the error path: loadReportWithParams toasts and leaves rptData null in its catch (line 1239-1241) while rptLoading is set false in finally. The placeholder claims a load is in progress and offers no retry.

**Failure scenario.** The /job-work/reports/dashboard query times out. The toast disappears after a few seconds and the user is left on a card reading "Loading Reports... Data will appear once loaded." forever, with no spinner and no way to retry except switching tabs.

**Fix.** Track an rptError state and render an error card with a Retry button when the fetch fails, reserving the placeholder for the genuine pre-fetch state.


## Section header and stat cards report server totals that do not describe the rows actually rendered

**legacy_frontend/app/[company]/transfer/page.tsx:705** &nbsp;|&nbsp; ux &nbsp;|&nbsp; verdict: unverified

**Problem.** transfersTotal is `response.total` from the server (:126) — an unfiltered count over interunit_transfers_header (interunit_tools.py:1454-1457) that ignores the cold exclusion, the warehouse filter and the search box all applied client-side. Same mismatch for `totalRecords` (:501, :534) and `transferInsTotal` (:504, :966). On cold-transfer/page.tsx it is worse: loadColdOut writes the ALL-transfers total into the shared transfersTotal at :186, so the cold page's "Transfers Out" card counts non-cold transfers too.

**Failure scenario.** With warehouseFilter = A185 and a search of "A185", the Transfer Out card reads "Transfers Out: 3,412" and the section header "3412 records" while exactly 7 rows are displayed. On /cold-transfer the card shows every transfer in the system (e.g. 3,412) while the cold list header correctly shows 210 — two counts for one list, three screens apart.

**Fix.** Show the count that matches the rows: `{filteredTransfers.length} record...` (the cold page already does this for its list at :675/:940). Keep the raw server total only on a card explicitly labelled as an all-time total, and give the cold page its own total state instead of reusing transfersTotal.


## Request number is regenerated on every render from the client clock

**legacy_frontend/app/[company]/transfer/request/page.tsx:102** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** `requestNo` is computed in the render body with no useState/useMemo, so it changes on every re-render (every keystroke in any form field). The value displayed at line 444 and the value submitted at line 375 are therefore a moving target tied to the last render before submit, and the number is second-precision client time, so two operators submitting inside the same second produce the same request_no. Unlike the challan number, which the backend re-mints on collision (`_allocate_challan_no`, interunit_tools.py:511-524), request_no is taken from the client.

**Failure scenario.** Two warehouse clerks submit requests at 14:32:07; both payloads carry REQ20260817143207. The user-visible 'Request No' on the form also ticks forward while typing, so the number a user writes down before submitting is not the one that gets sent.

**Fix.** Generate it once with `useState(() => ...)` (or let the backend allocate it and read `response.request_no`), matching the challan-number approach.


## Article form resets to quantity 1 / pack size 1 after the first Add, unlike its initial blank state

**legacy_frontend/app/[company]/transfer/request/page.tsx:311** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** After adding an article the form pre-fills quantity and pack size with '1' while the first article starts blank, and `netWeight` is reset to '0' without recalculation. A second article added without touching those fields is submitted as qty 1 / pack_size 1 / net_weight 0; the backend then derives the weight itself (`line_net_weight` only honours a provided value when it is > 0, interunit_tools.py:867-875) and books a 1 kg line the operator never entered.

**Failure scenario.** Clerk adds article 1 correctly, selects article 2 from the search box (which fills type/category/description only), and clicks Add Article without noticing the pre-filled quantity. The request is created with a second line of qty 1, pack size 1, net weight 1.000 kg.

**Fix.** Reset to the same blank defaults as the initial state and require quantity/pack size in `handleAddArticle`'s validation (it currently only checks material type and description).


## Request-line fields the form reads (sku_id, batch_number) are never returned by the backend request mapper

**legacy_frontend/app/[company]/transfer/transferform/page.tsx:2568** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: confirmed

**Problem.** loadedItems are the raw request lines from GET /interunit/requests/{id} (RequestLineResponse, interunit_models.py:94-108). The 'Items from Request' panel reads sku_id and batch_number, which that model does not contain, and the scanned-count bookkeeping matches on `String(it.sku_id) === String(article.sku_id)` (lines 925-928) and `String(it.sku_id) === String(boxToRemove.skuId)` (lines 956-959), which can never be true for request-loaded items — the logic silently degrades to exact item_description equality, and description text coming from the box tables need not match the uppercased text the backend stored for the request line.

**Failure scenario.** Operator opens a 3-item request. Every item card shows 'SKU ID: N/A' and no Batch row. If a scanned box's description differs in case or spacing from the request line's stored (uppercased) description, its scan never increments that item's Scanned/Pending counters, so the panel keeps showing the full quantity as pending even after all boxes are scanned.

**Fix.** Add sku_id/batch_number to RequestLineResponse and _map_line_row, or drop those UI fields and match on a case-insensitive trimmed description (`(a||'').trim().toUpperCase() === (b||'').trim().toUpperCase()`).


## Hover card labels every line quantity as 'boxes' regardless of the line's UOM

**legacy_frontend/components/transfer/ChallanHoverCard.tsx:184** &nbsp;|&nbsp; ux &nbsp;|&nbsp; verdict: confirmed

**Problem.** For line-only items `groupLinesByItem` puts `Number(l.quantity)` into `qty` (line 293), which is the line's quantity in its own UOM (BOX / CARTON / BAG, and pieces for some PM lines - the backend returns `uom` on every line but it is never read here). Rendering it with a hardcoded 'boxes' suffix mislabels the unit; the same number is a true box count only in the `groupBoxesByItem` path.

**Failure scenario.** A PM line 'POUCH 100ML' with quantity 25,000 PCS renders in the hover as '25,000 boxes', suggesting 25,000 physical cartons are in transit.

**Fix.** Carry `uom` through HoverLine and render `{qty} {uom || 'boxes'}`; keep the hardcoded 'boxes' only for the box-derived path.


## Print is triggered on a fixed 500ms timer that can fire before the logo image loads

**legacy_frontend/components/transfer/DeliveryChallan.tsx:54** &nbsp;|&nbsp; ux &nbsp;|&nbsp; verdict: confirmed

**Problem.** The print dialog is opened on a fixed delay with no wait for the two `/candor-logo.jpg` images (lines 166 and 447) or for fonts. On a cold cache or a slow link the browser snapshots the document before the images decode, producing a challan and gate pass with missing letterheads. The effect also logs the entire item payload to the console on every mount (lines 40-52), which ships transfer contents to the browser console in production.

**Failure scenario.** First print of the day on a warehouse terminal: the logo request takes 700ms, the print dialog opens at 500ms, and the operator prints a DC with an empty box where the company letterhead belongs.

**Fix.** Wait for `document.fonts.ready` and for the images' load events (or an `onLoad` counter) before calling `window.print()`, and drop the payload console.logs.


## Gate Pass totals row values sit under the wrong column headers

**legacy_frontend/components/transfer/DeliveryChallan.tsx:537** &nbsp;|&nbsp; ux &nbsp;|&nbsp; verdict: confirmed

**Problem.** The Gate Pass header row is S.No | Item Description | Vakkal | Boxes | Qty | Net Wt | [Count] (lines 493-499), and the table uses `tableLayout: 'fixed'` with a colgroup, so cells land in fixed positions. The totals row puts Total Qty under Vakkal and Total Kg under Qty, so on the printed page each total sits above/below a column of unrelated numbers. Only the inline labels save it from being misread.

**Failure scenario.** Security compares the 'Total Kg: 2,480.5' cell against the column it appears under (Qty, values 60/40) and queries the paperwork.

**Fix.** Place each total in the cell of its own column (blank cells elsewhere) as the DC totals row at line 342-367 already does.


## fetchSkuId never returns `uom` despite the declared return type and its own comment

**legacy_frontend/lib/api.ts:737** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: plausible

**Problem.** The function hand-copies fields from the response into a new object and omits `uom`, so the declared optional `uom?: string | null` is always `undefined` regardless of what the server sent. TypeScript reports no error because the field is optional. The adjacent comment documents the exact decimal-string contract this drops.

**Failure scenario.** The transfer forms (app/[company]/transfer/directtransferform/page.tsx:217, 919 and cold-transfer/coldtransferform/page.tsx:217, 1421) call fetchSkuId to auto-fill an article. When any of them starts reading `skuResponse.uom` to pre-fill the UOM/pack-size field, it silently reads `undefined` and the field stays blank — a bug that looks like a backend omission but is a client-side field drop.

**Fix.** Add `uom: data.uom` to the returned object (and consider `return data` with a narrow type rather than hand-copying, which is what caused the omission).


## InterUnitAPI.list declares a response shape the backend never returns and swallows all errors into empty data in production

**legacy_frontend/lib/api/interunit.ts:221** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** Three contract breaks against GET /interunit/transfers: the array is `records` not `items`, the page-count field is `total_pages` not `pages`, and the row fields declared here (lines_count, qty_total, from_site, to_site — :8-22) are not what the endpoint emits for the list (it returns items_count, boxes_count, total_qty, from_warehouse, to_warehouse — interunit_tools.py:554-555, 1508-1512). Any consumer of `.items` gets undefined and any `.length` on it throws. Additionally apiCall's catch (:275-293) converts every failure — including 4xx/5xx already thrown at :271 — into empty data whenever NODE_ENV !== 'development', so production failures render as "no data" instead of an error.

**Failure scenario.** A future caller writes `const { items, pages } = await interUnitAPI.list(filters, 1, 20)` — TypeScript accepts it because the interface lies — and `items.map(...)` throws "Cannot read properties of undefined (reading 'map')" at runtime, or the pager renders 0 pages against a populated table. If instead the API 500s in production, apiCall returns {items: [], total: 0, pages: 0} and the screen shows a clean empty state for a server outage.

**Fix.** Correct the interface to `{ records: ...[]; total: number; page: number; per_page: number; total_pages: number }` and align InterUnitListItemEnhanced with what the endpoint returns (items_count, boxes_count, total_qty, from_warehouse, to_warehouse, from_cold_unit, lot_numbers_text). Remove the production error-swallowing at :281-292 and let callers handle the rejection. This file appears otherwise unused by the transfer pages — deleting it is also an option.


## SecureApiClient.get() applies baseURL twice — a path-prefixed API URL duplicates the prefix

**legacy_frontend/lib/auth/secureApiClient.ts:83** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: plausible

**Problem.** `get()` builds an absolute URL from baseURL + endpoint, then hands `url.pathname + url.search` to `makeRequest`, which prepends `this.baseURL` a second time. This is only correct while baseURL has an empty path. `.env` currently sets `NEXT_PUBLIC_API_URL=http://localhost:8000`, so it happens to work today; any deployment behind a path prefix breaks. Note `post`/`put`/`patch`/`delete` pass the raw endpoint and are unaffected, so the bug would hit GETs only.

**Failure scenario.** Production sets `NEXT_PUBLIC_API_URL=https://ims.candorfoods.in/api`. `secureApiClient.get('/api/inward/list', {page:1})` → `new URL('https://ims.candorfoods.in/api/api/inward/list?page=1')` → pathname `/api/api/inward/list` → makeRequest fetches `https://ims.candorfoods.in/api/api/api/inward/list?page=1` → 404, retried 3× by retryRequest, then surfaced as a generic APIError. Every list screen breaks while every create/update still works, which misdirects the investigation.

**Fix.** Build the query string without `new URL`, or pass the absolute `url.toString()` to a makeRequest variant that does not re-prepend baseURL.


## delete_cold_transfer_in counts re-parked boxes that ON CONFLICT silently discarded

**services/ims_service/cold_transfer_in_tools.py:545** &nbsp;|&nbsp; correctness &nbsp;|&nbsp; verdict: confirmed

**Problem.** The INSERT ends with ON CONFLICT (box_id, transaction_no) DO NOTHING (line 522), so it may write zero rows, but reparked_n is incremented unconditionally. The returned `reparked` figure is therefore the number of boxes ATTEMPTED, not restored. This is not hypothetical: because the pending row for a received box is frequently never deleted (see the unscoped lookup at line 625 and the box-id regeneration noted at lines 590-593), the conflicting row usually already exists.

**Failure scenario.** A 100-box cold receipt is deleted. All 100 pending rows still exist In Transit (they were never matched at receive time), so every INSERT hits the conflict and writes nothing. The API returns {'boxes_reversed': 100, 'reparked': 100}. The operator believes 100 boxes were restored to the in-transit ledger; in reality zero rows were written by this call, and any box whose pending row had genuinely been consumed is now missing from both cold_stocks and pending_transfer_stock.

**Fix.** Use `result = db.execute(...); reparked_n += result.rowcount` so the response reflects rows actually inserted, and log/return the box_ids that conflicted.


## Request lines drop total_weight: the column is selected, discarded by the mapper, and absent from RequestLineResponse

**services/ims_service/interunit_tools.py:129** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** `create_request` computes and stores `total_weight` per line (interunit_tools.py:256-292), and every read query selects it (`_fetch_lines` interunit_tools.py:171, `list_requests` line 372). But `_map_line_row` never copies it into the dict, and `RequestLineResponse` (interunit_models.py:94-108) declares no `total_weight` field — unlike its transfer counterpart `TransferLineResponse` (interunit_models.py:281), which has it. So `GET /interunit/requests` and `GET /interunit/requests/{id}` can never expose the stored total weight.

**Failure scenario.** A request line is created with `total_weight: 1250.5` kg. Approving it opens the transfer form pre-filled from `GET /interunit/requests/{id}` (transferform/page.tsx:606 spreads `request.lines`), where `total_weight` is `undefined` and falls back to the article default of 0 (transferform/page.tsx:699). The approver must re-enter the gross weight by hand, and the requested-vs-transferred weight comparison is impossible.

**Fix.** Add `"total_weight": str(row.total_weight) if row.total_weight is not None else "0"` to `_map_line_row` and `total_weight: str = "0"` to `RequestLineResponse`.


## list_requests reports total_pages = 0 whenever the page is empty, even when total > 0

**services/ims_service/interunit_tools.py:363** &nbsp;|&nbsp; pagination &nbsp;|&nbsp; verdict: confirmed

**Problem.** The early return hard-codes total_pages to 0 instead of computing it from `total` the way the normal return at line 397 does. Any request for a page past the end — or a page that happens to be empty — reports 0 pages while simultaneously reporting a non-zero total, so the two fields contradict each other. list_transfers does not have this bug (line 1520 always computes it).

**Failure scenario.** 25 requests exist, per_page=10. The user is on page 3 and deletes the last three rows so page 3 is now empty; the refetch returns total=22, total_pages=0. A pager driven by total_pages renders zero page buttons and offers no way back to page 1, stranding the user on an empty screen with '22 records' in the header.

**Fix.** Return `(total + per_page - 1) // per_page if total else 0` in the empty branch too, or delete the early return and let the normal return handle the empty case.


## /interunit/transfers date filters accept only DD-MM-YYYY while sibling endpoints in the same router document YYYY-MM-DD

**services/ims_service/interunit_tools.py:1438** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: unverified

**Problem.** _convert_date (lines 122-126) parses '%d-%m-%Y' only and raises HTTPException(400, 'Invalid date format. Use DD-MM-YYYY') on anything else — including the ISO format that interunit_server.py:112-113 documents for the sibling pending-stock endpoint ('YYYY-MM-DD') and that HTML <input type="date"> emits natively. The /transfers query params at interunit_server.py:275-276 carry no description at all, so a caller has no way to learn the required format except by triggering the 400. (The range itself is correct: stock_trf_date is a DATE column so >= / <= are properly inclusive on both ends.)

**Failure scenario.** A date-range picker bound to a native date input sends from_date=2026-08-01&to_date=2026-08-17. _convert_date raises 400 'Invalid date format. Use DD-MM-YYYY' before any row is read; the list goes empty and the toast blames the date rather than the format. cold_transfer_out_tools._parse_trf_date (lines 195-207) already accepts both formats — the two halves of the same module disagree.

**Fix.** Reuse the dual-format parse: try '%d-%m-%Y' then '%Y-%m-%d' in _convert_date, and add `description="DD-MM-YYYY or YYYY-MM-DD"` to the from_date/to_date Query declarations at interunit_server.py:275-276.


## categorial-search returns a synthetic positional id rather than a stable SKU identifier

**services/ims_service/interunit_tools.py:3928** &nbsp;|&nbsp; payload-contract &nbsp;|&nbsp; verdict: plausible

**Problem.** `CategorialSearchItem.id: int` (interunit_models.py:504-505) is typed as a normal entity id, but the value is the row's ordinal position within the current page (`idx + 1 + offset`) — the underlying query selects `DISTINCT ON (UPPER(particulars), UPPER(item_type))` and never projects the table's own id. The same id therefore denotes a different SKU for every different `search` string, and two different searches at the same offset collide.

**Failure scenario.** `GET /interunit/categorial-search?search=dates&offset=0` returns `{id: 1, item_description: 'FRESHO KIMIA DATES 500 GM'}`. The user retypes the query as `search=raisin` and the very same `id: 1` now denotes 'BLACK RAISIN 1KG'. Any client that caches or keys selections by `id` (a React `key`, a selected-item set, a dedupe map) silently binds the wrong article to a transfer line.

**Fix.** Project the real primary key from `_CATEGORIAL_TABLE` (e.g. `MIN(id)` inside the DISTINCT ON subquery) and return it as `id`, or rename the field to `row_index` so no consumer treats it as an identity.


## list_pending_transfers' orphan branch reports a carton/qty sum as total_boxes, and box counts as total_cartons

**services/ims_service/pending_stock_tools.py:2537** &nbsp;|&nbsp; aggregation &nbsp;|&nbsp; verdict: unverified

**Problem.** The two branches of the UNION ALL feeding `/interunit/pending-stock` define `total_boxes` and `total_cartons` differently. The tracked branch uses `COUNT(*)` of pending rows for `total_boxes` and `SUM(pts.no_of_cartons)` for `total_cartons` (lines 2485-2487). The orphan branch (2537-2544) uses the *same* expression for both — box count when boxes exist, otherwise `SUM(l.qty)` (a carton/piece quantity) for both fields. So for a line-only warehouse transfer, `total_boxes` is a carton count masquerading as a box count.

**Failure scenario.** A legacy article-only transfer with one line of qty=300 cartons and zero rows in `interunit_transfer_boxes` reports `total_boxes: 300, total_cartons: 300`. PendingTransfersModal.tsx:170 sums `r.total_boxes` into the modal footer 'Boxes' total, so the pending-stock footer claims 300 boxes in transit for a dispatch that has no box records at all, and mixes that figure with genuine box counts from the tracked branch.

**Fix.** Emit `total_boxes` as 0 (or NULL) in the orphan branch when `interunit_transfer_boxes` is empty, keeping the `SUM(l.qty)` fallback for `total_cartons` only, and label the two distinctly in the response.

