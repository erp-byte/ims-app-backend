import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import openpyxl
from collections import Counter, defaultdict

wb = openpyxl.load_workbook("Interunit_Transfer_Reconciliation.xlsx", read_only=True, data_only=True)
ws = wb["Reconciliation (OUT to IN)"]
rows = ws.iter_rows(values_only=True)
hdr = list(next(rows))
data = [r for r in rows]

C_TOUT, C_TID, C_FLOW = 1, 2, 3
C_BOXSTATUS, C_OVERALL, C_GRN = 14, 17, 18
C_SUMK, C_SUMV = 19, 20

print(f"TOTAL DATA ROWS: {len(data)}")

# Side summary block
print("\n===== RECONCILIATION SUMMARY (built into sheet) =====")
for r in data:
    k, v = r[C_SUMK], r[C_SUMV]
    if k not in (None, "", "-"):
        print(f"  {k}: {v}")

# Box Status distribution
print("\n===== Box Status [14] =====")
for k, v in Counter(str(r[C_BOXSTATUS]) for r in data).most_common():
    print(f"  {k!r}: {v}")

# Overall Status distribution
print("\n===== Overall Status [17] =====")
for k, v in Counter(str(r[C_OVERALL]) for r in data).most_common():
    print(f"  {k!r}: {v}")

# GRN Status distribution
print("\n===== GRN Status [18] (header-level) =====")
for k, v in Counter(str(r[C_GRN]) for r in data).most_common():
    print(f"  {k!r}: {v}")

# THE LEAKAGE: GRN=Received but box NOT received
print("\n===== CROSS-TAB: GRN Status x box-received =====")
def is_received_box(r):
    s = str(r[C_OVERALL]).upper()
    return "NOT RECEIVED" not in s and "MISSING" not in s
ct = Counter()
for r in data:
    grn = str(r[C_GRN]).strip()
    ct[(grn, "box_received" if is_received_box(r) else "box_MISSING")] += 1
for k, v in sorted(ct.items()):
    print(f"  {k}: {v}")

# Per-transfer leakage: header GRN Received but has missing boxes
print("\n===== TRANSFERS marked Received-GRN but with MISSING boxes =====")
per = defaultdict(lambda: {"total": 0, "missing": 0, "flow": "", "grn": ""})
for r in data:
    tid = r[C_TID]
    if tid is None:
        continue
    p = per[(r[C_TOUT], tid)]
    p["total"] += 1
    p["flow"] = r[C_FLOW]
    p["grn"] = str(r[C_GRN]).strip()
    if not is_received_box(r):
        p["missing"] += 1
leak = [(k, v) for k, v in per.items()
        if v["grn"].lower().startswith("received") and v["missing"] > 0]
print(f"  Transfers with leakage: {len(leak)}")
tot_missing = sum(v["missing"] for _, v in leak)
tot_boxes = sum(v["total"] for _, v in leak)
print(f"  Boxes missing across them: {tot_missing} / {tot_boxes}")
print("\n  Top 25 by missing-box count:")
print(f"  {'TransferOut':24} {'TID':6} {'flow':28} {'miss/total'}")
for (tout, tid), v in sorted(leak, key=lambda x: -x[1]["missing"])[:25]:
    print(f"  {str(tout):24} {str(tid):6} {str(v['flow'])[:28]:28} {v['missing']}/{v['total']}")

wb.close()
