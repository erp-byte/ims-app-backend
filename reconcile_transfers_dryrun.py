"""
Reconcile 12 transfer transactions against cold stocks and pending_transfer_stock.

Rules:
  Dispatch transfers: if box matches in cold (box_id + txn_no + lot), DELETE from cold.
                      If not already in pending, INSERT into pending (In Transit).
  Received transfers: DELETE matching boxes from cold tables AND pending_transfer_stock.

Run with DRY_RUN=True first to preview, then set DRY_RUN=False to commit.
"""

import psycopg2
import json
from datetime import datetime

DRY_RUN = False  # set to False to actually commit

conn = psycopg2.connect(
    host='wms-postgres-db.cpis084golp7.ap-south-1.rds.amazonaws.com',
    port=5432, dbname='warehouse_db', user='wmsadmin', password='Candorfoods'
)
cur = conn.cursor()

TRANS_IDS = [
    'TRANS202605231823','TRANS202605221736','TRANS202605251050',
    'TRANS202605261429','TRANS202605261427','TRANS202605261548',
    'TRANS202604181202','TRANS202605271219','TRANS202605271221',
    'TRANS202605271224','TRANS202605271228','TRANS202605271230',
]

# ── load headers ──
cur.execute('''
    SELECT id, challan_no, from_site, to_site, status, created_by, created_ts
    FROM interunit_transfers_header WHERE challan_no = ANY(%s)
''', (TRANS_IDS,))
headers = {
    r[1]: {'id':r[0],'from_site':r[2],'to_site':r[3],'status':r[4],'created_by':r[5],'created_ts':r[6]}
    for r in cur.fetchall()
}

# ── load all boxes ──
cur.execute('''
    SELECT h.id, h.challan_no, h.status,
           b.box_id, b.transaction_no, b.article, b.lot_number, b.net_weight, b.gross_weight
    FROM interunit_transfer_boxes b
    JOIN interunit_transfers_header h ON h.id = b.header_id
    WHERE h.challan_no = ANY(%s)
''', (TRANS_IDS,))
all_boxes = cur.fetchall()


def find_cold_strict(box_id, tno):
    """Match by box_id + transaction_no ONLY — no article-unsafe fallback."""
    for table in ('cfpl_cold_stocks', 'cdpl_cold_stocks'):
        cur.execute(
            f'''SELECT id, item_description, lot_no, weight_kg, no_of_cartons,
                       inward_dt, unit, inward_no, item_mark, vakkal, group_name,
                       item_subgroup, storage_location, exporter, last_purchase_rate,
                       value, total_inventory_kgs, spl_remarks, inward_transaction_no
                FROM {table} WHERE box_id=%s AND transaction_no=%s LIMIT 1''',
            (box_id, tno)
        )
        row = cur.fetchone()
        if row:
            return table, row
    return None, None


def in_pending(box_id, tno):
    cur.execute(
        'SELECT id, transfer_out_challan_no FROM pending_transfer_stock WHERE box_id=%s AND transaction_no=%s LIMIT 1',
        (box_id, tno)
    )
    return cur.fetchone()


def cold_row_to_json(row):
    cols = ['item_description','lot_no','weight_kg','no_of_cartons',
            'inward_dt','unit','inward_no','item_mark','vakkal','group_name',
            'item_subgroup','storage_location','exporter','last_purchase_rate',
            'value','total_inventory_kgs','spl_remarks','inward_transaction_no']
    # row[0]=id, row[1:] = above cols offset by 1 (id is index 0 in select)
    # select order: id,item_description,lot_no,...
    d = {}
    for i, col in enumerate(cols):
        val = row[i + 1]
        if hasattr(val, 'isoformat'):
            val = val.isoformat()
        d[col] = val
    return d


print(f'=== {"DRY RUN" if DRY_RUN else "LIVE EXECUTION"} — Transfer Reconciliation ===')
print(f'    Timestamp: {datetime.now()}')
print()

totals = dict(dc=0, pi=0, ao=0, ns=0, rc=0, rp=0, rk=0)

for challan in sorted(headers):
    h = headers[challan]
    transfer_id = h['id']
    boxes = [b for b in all_boxes if b[1] == challan]
    is_received = h['status'].lower() == 'received'
    to_wh = h['to_site']
    to_storage = 'cold' if any(x in (h['to_site'] or '').lower() for x in ['cold','rishi','savla']) else 'warehouse'

    cold_deducts = []       # (table, cold_id, box_id, tno, item, wt)
    pending_inserts = []    # (box_id, tno, article, lot, net_wt, gross_wt, cold_table, cold_row)
    pending_deletes = []    # (pending_id, box_id, tno)

    for b in boxes:
        bid, tno = b[3], b[4]
        if not bid or not tno or tno == 'DIRECT':
            continue

        ct, cr = find_cold_strict(bid, tno)
        pr = in_pending(bid, tno)

        if is_received:
            if cr:
                cold_deducts.append((ct, cr[0], bid, tno, cr[1], cr[3]))
                totals['rc'] += 1
            if pr:
                pending_deletes.append((pr[0], bid, tno))
                totals['rp'] += 1
            if not cr and not pr:
                totals['rk'] += 1
        else:
            if cr:
                cold_deducts.append((ct, cr[0], bid, tno, cr[1], cr[3]))
                totals['dc'] += 1
                if not pr:
                    pending_inserts.append((bid, tno, b[5], b[6], b[7], b[8], ct, cr))
                    totals['pi'] += 1
                else:
                    totals['ao'] += 1
            else:
                if pr:
                    totals['ao'] += 1
                else:
                    totals['ns'] += 1

    # ── report ──
    by_tbl = {}
    for r in cold_deducts: by_tbl.setdefault(r[0], []).append(r)
    print(f'{challan} ({h["status"]}) | {h["from_site"]} -> {h["to_site"]} | {len(boxes)} boxes')
    for tbl, rows in by_tbl.items():
        wt = sum(float(r[5] or 0) for r in rows)
        items = list(dict.fromkeys(r[4] for r in rows))
        print(f'  DELETE {tbl}: {len(rows)} rows | {items[:3]} | total={wt:.1f}kg')
    if pending_inserts:
        print(f'  INSERT pending_transfer_stock: {len(pending_inserts)} new rows')
    if pending_deletes:
        challs = set(r[2] for r in pending_deletes)
        print(f'  DELETE pending_transfer_stock: {len(pending_deletes)} rows')
    no_action = len(boxes) - len(cold_deducts) - len(pending_inserts) - len(pending_deletes)
    if not cold_deducts and not pending_inserts and not pending_deletes:
        print(f'  Already clean — no action needed')
    print()

    if not DRY_RUN:
        # ── EXECUTE COLD DELETES ──
        for (tbl, cold_id, bid, tno, item, wt) in cold_deducts:
            cur.execute(f'DELETE FROM {tbl} WHERE id = %s', (cold_id,))

        # ── EXECUTE PENDING INSERTS (Dispatch only) ──
        now = datetime.now()
        for (bid, tno, article, lot, net_wt, gross_wt, src_table, cr) in pending_inserts:
            from_company = 'cfpl' if src_table.startswith('cfpl') else 'cdpl'
            dest_table = f'{from_company}_bulk_entry_boxes' if to_storage == 'warehouse' else f'{from_company}_cold_stocks'
            cold_data = cold_row_to_json(cr)
            item_description = cr[1] or article or ''
            lot_no = cr[2] or lot or None
            weight_kg = float(cr[3] or net_wt or 0)
            no_of_cartons = int(cr[4] or 1)

            cur.execute('''
                INSERT INTO pending_transfer_stock
                    (transfer_type, transfer_out_id, transfer_out_challan_no,
                     box_id, transaction_no,
                     from_company, to_company, from_site, to_site,
                     from_storage_type, to_storage_type,
                     source_table, source_row_id, destination_table,
                     item_description, lot_no, weight_kg, no_of_cartons,
                     cold_storage_data, net_weight, gross_weight, article,
                     status, dispatched_at, dispatched_by)
                VALUES
                    ('INTERUNIT', %s, %s, %s, %s,
                     %s, %s, %s, %s, 'cold', %s,
                     %s, %s, %s,
                     %s, %s, %s, %s,
                     CAST(%s AS JSONB), %s, %s, %s,
                     'In Transit', %s, %s)
                ON CONFLICT (box_id, transaction_no) DO NOTHING
            ''', (
                transfer_id, challan,
                bid, tno,
                from_company, from_company, h['from_site'], h['to_site'], to_storage,
                src_table, cr[0], dest_table,
                item_description, lot_no, weight_kg, no_of_cartons,
                json.dumps(cold_data) if cold_data else None,
                float(net_wt or 0), float(gross_wt or 0) if gross_wt else None, article or item_description,
                now, h['created_by'] or 'reconcile-script',
            ))

        # ── EXECUTE PENDING DELETES (Received only) ──
        for (pend_id, bid, tno) in pending_deletes:
            cur.execute('DELETE FROM pending_transfer_stock WHERE id = %s', (pend_id,))

        conn.commit()
        print(f'  Committed.')

print('=== TOTALS ===')
print(f'  Dispatch: cold_deduct={totals["dc"]} new_pending_insert={totals["pi"]} already_ok={totals["ao"]} settled_elsewhere={totals["ns"]}')
print(f'  Received: cold_deduct={totals["rc"]} pending_delete={totals["rp"]} already_clean={totals["rk"]}')
print()
if DRY_RUN:
    print('DRY RUN complete — no changes made. Set DRY_RUN=False in this script to execute.')
else:
    print('LIVE EXECUTION complete — all changes committed.')

conn.close()
