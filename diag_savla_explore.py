"""READ-ONLY exploration for Savla/Rishi reconciliation. No writes."""
import io, sys, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import psycopg2

# load .env
env = {}
with open(".env", encoding="utf-8") as f:
    for ln in f:
        ln = ln.strip()
        if ln and not ln.startswith("#") and "=" in ln:
            k, v = ln.split("=", 1)
            env[k.strip()] = v.strip()

conn = psycopg2.connect(
    host=env["DB_HOST"], port=int(env.get("DB_PORT", "5432")),
    dbname=env["DB_NAME"], user=env["DB_USER"], password=env["DB_PASSWORD"],
)
cur = conn.cursor()


def q(sql, args=None):
    cur.execute(sql, args or ())
    return cur.fetchall()


print("=" * 80)
print("COLD STOCKS — storage_location distribution (both companies)")
for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
    print(f"\n--- {tbl} ---")
    try:
        total = q(f"SELECT count(*) FROM {tbl}")[0][0]
        print(f"  total rows: {total}")
        print("  storage_location:")
        for loc, n, cart, kg in q(
            f"SELECT storage_location, count(*), COALESCE(sum(no_of_cartons),0), "
            f"COALESCE(sum(total_inventory_kgs),0) FROM {tbl} "
            f"GROUP BY storage_location ORDER BY count(*) DESC"
        ):
            print(f"    {str(loc)!r:30} rows={n:6}  cartons={float(cart):10.1f}  kg={float(kg):12.1f}")
    except Exception as e:
        print(f"  ERROR: {e}")
        conn.rollback()

print("\n" + "=" * 80)
print("COLD STOCKS — unit distribution for Savla/Rishi rows")
for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
    print(f"\n--- {tbl} (storage_location ILIKE savla/rishi) ---")
    try:
        for unit, n, cart, kg in q(
            f"SELECT unit, count(*), COALESCE(sum(no_of_cartons),0), "
            f"COALESCE(sum(total_inventory_kgs),0) FROM {tbl} "
            f"WHERE storage_location ILIKE '%savla%' OR storage_location ILIKE '%rishi%' "
            f"GROUP BY unit ORDER BY count(*) DESC"
        ):
            print(f"    unit={str(unit)!r:14} rows={n:6}  cartons={float(cart):10.1f}  kg={float(kg):12.1f}")
    except Exception as e:
        print(f"  ERROR: {e}")
        conn.rollback()

print("\n" + "=" * 80)
print("cold_stock_disposition ledger")
try:
    print("  total:", q("SELECT count(*) FROM cold_stock_disposition")[0][0])
    print("  by type (reverted=false):")
    for t, n in q("SELECT disposition_type, count(*) FROM cold_stock_disposition "
                  "WHERE reverted=false GROUP BY disposition_type ORDER BY count(*) DESC"):
        print(f"    {str(t)!r:24} {n}")
    rng = q("SELECT min(disposed_at), max(disposed_at) FROM cold_stock_disposition")[0]
    print(f"  disposed_at range: {rng[0]}  ->  {rng[1]}")
except Exception as e:
    print(f"  ERROR: {e}"); conn.rollback()

print("\n" + "=" * 80)
print("Direct Out tables")
for tbl in ("cfpl_cold_storage_direct_out", "cdpl_cold_storage_direct_out"):
    try:
        n = q(f"SELECT count(*) FROM {tbl}")[0][0]
        rng = q(f"SELECT min(entry_date), max(entry_date) FROM {tbl}")[0]
        print(f"  {tbl}: {n} rows, entry_date {rng[0]} -> {rng[1]}")
    except Exception as e:
        print(f"  {tbl}: ERROR {e}"); conn.rollback()

print("\n" + "=" * 80)
print("Jobwork material-out")
try:
    n = q("SELECT count(*) FROM jb_materialout_header WHERE type='OUT'")[0][0]
    print(f"  jb_materialout_header type=OUT: {n}")
    nl = q("SELECT count(*) FROM jb_materialout_lines")[0][0]
    print(f"  jb_materialout_lines: {nl}")
    print("  sample from_warehouse values:")
    for w, n in q("SELECT from_warehouse, count(*) FROM jb_materialout_header GROUP BY from_warehouse ORDER BY count(*) DESC LIMIT 15"):
        print(f"    {str(w)!r:30} {n}")
except Exception as e:
    print(f"  ERROR: {e}"); conn.rollback()

print("\n" + "=" * 80)
print("Transfer boxes — from_site distribution (header)")
try:
    for fs, n in q("SELECT from_site, count(*) FROM interunit_transfers_header GROUP BY from_site ORDER BY count(*) DESC LIMIT 20"):
        print(f"    {str(fs)!r:30} {n}")
except Exception as e:
    print(f"  ERROR: {e}"); conn.rollback()

conn.close()
print("\nDONE (read-only).")
