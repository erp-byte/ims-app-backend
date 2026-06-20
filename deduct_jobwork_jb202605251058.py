"""
One-off cleanup: delete cold storage rows for job-work material-out JB202605251058 (header_id=35).

DBs were re-opened from 24-May snapshot; this material-out happened between then and now (27-May),
so its 300 boxes are still sitting in cold_stocks and need to be removed.

Run with DRY_RUN=True first to preview, then set DRY_RUN=False to commit.
"""

import os
import psycopg2

DRY_RUN = False  # set to False to actually commit

# DB credentials come from the DATABASE_URL env var (see .env) — never hardcode them.
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

HEADER_ID = 35
CHALLAN_NO = 'JB202605251058'

cur.execute(
    "SELECT id, challan_no, from_warehouse, status FROM jb_materialout_header WHERE id = %s",
    (HEADER_ID,)
)
hdr = cur.fetchone()
if not hdr:
    print(f"ERROR: header_id={HEADER_ID} not found")
    conn.close()
    exit(1)

print(f'=== {"DRY RUN" if DRY_RUN else "LIVE EXECUTION"} — {CHALLAN_NO} cleanup ===')
print(f'  Header: id={hdr[0]}  challan={hdr[1]}  from_warehouse={hdr[2]}  status={hdr[3]}')
print()

cur.execute("""
    SELECT box_id, transaction_no
    FROM jb_materialout_lines
    WHERE header_id = %s AND box_id <> '' AND transaction_no <> ''
""", (HEADER_ID,))
lines = cur.fetchall()
print(f'  Line items with box_id+tno: {len(lines)}')

deleted = {'cfpl_cold_stocks': 0, 'cdpl_cold_stocks': 0}
not_found = []

for box_id, tno in lines:
    matched = False
    for table in ('cfpl_cold_stocks', 'cdpl_cold_stocks'):
        cur.execute(
            f"SELECT id FROM {table} WHERE box_id=%s AND transaction_no=%s LIMIT 1",
            (box_id, tno)
        )
        if cur.fetchone():
            if not DRY_RUN:
                cur.execute(
                    f"DELETE FROM {table} WHERE box_id=%s AND transaction_no=%s",
                    (box_id, tno)
                )
            deleted[table] += 1
            matched = True
            break
    if not matched:
        not_found.append((box_id, tno))

print()
print(f'=== SUMMARY ===')
print(f'  Delete from cfpl_cold_stocks : {deleted["cfpl_cold_stocks"]}')
print(f'  Delete from cdpl_cold_stocks : {deleted["cdpl_cold_stocks"]}')
print(f'  Not found in either table    : {len(not_found)}')
if not_found[:5]:
    print(f'    e.g. {not_found[:5]}')
print()

if DRY_RUN:
    conn.rollback()
    print('DRY RUN — no changes made. Set DRY_RUN=False to execute.')
else:
    conn.commit()
    print('COMMITTED — cold storage rows deleted.')

conn.close()
