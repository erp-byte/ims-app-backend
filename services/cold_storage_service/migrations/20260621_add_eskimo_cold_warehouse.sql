-- 20260621_add_eskimo_cold_warehouse.sql
-- Adds the "Eskimo" cold-storage warehouse (CDPL → cdpl_cold_stocks).
--
-- Companion code changes (must ship together):
--   backend/shared/canonicalize.py                      CANONICAL_WAREHOUSES + WAREHOUSE_ALIASES
--   backend/services/ims_service/cold_transfer_in_tools.py   COLD_DESTINATIONS + CDPL_COLD_DESTS
--   backend/services/ims_service/pending_stock_tools.py      COLD_STORAGE_SITE_NAMES
--   backend/services/ims_service/interunit_tools.py          _COLD_DEST_LOWER + _COLD_UNIT_ALIASES
--   backend/services/ims_service/inward_tools.py             _WAREHOUSE_ALIASES + _COLD_UNIT_MAP + _is_cold_warehouse
--   backend/services/ims_service/rtv_tools.py                _RTV_COLD_UNIT_MAP
--   backend/services/ims_service/job_work_server.py          _resolve_cold_table (→ cdpl_cold_stocks)
--   frontend/lib/constants/warehouses.ts                     WAREHOUSES + WAREHOUSE_ALIASES + WAREHOUSE_DISPLAY_NAMES
--
-- Eskimo is a CDPL cold warehouse: its on-hand inventory lives in cdpl_cold_stocks.
--
-- What this migration does:
--   1. Extends canonical_warehouse_fn() so unit/storage_location values like
--      'eskimo' / 'eskimo cold' canonicalize to 'Eskimo' on the cold dashboards.
--   2. Registers 'Eskimo' in the warehouse_sites master so it appears in the
--      interunit transfer source/destination dropdown (/dropdowns/warehouse-sites).
--   3. Backfills canonical_warehouse for any pre-existing Eskimo rows (no-op if none).
--
-- Idempotent: safe to re-run.

-- ---------------------------------------------------------------------------
-- 1. canonical_warehouse_fn() — mirror of 20260525 with the Eskimo branch added
--    to BOTH the unit and the storage_location lookups.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION canonical_warehouse_fn(p_unit TEXT, p_storage_location TEXT)
RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
  k TEXT;
BEGIN
  -- Try unit first
  IF p_unit IS NOT NULL AND length(trim(p_unit)) > 0 THEN
    k := regexp_replace(lower(trim(p_unit)), '_', ' ', 'g');
    IF k IN ('savla d-39','savla d39','d-39','d39','savla bond','old savla','savla d-39 cold','savla d39 cold') THEN
      RETURN 'Savla D-39';
    ELSIF k IN ('savla d-514','savla d514','d-514','d514','new savla','savla d-514 cold') THEN
      RETURN 'Savla D-514';
    ELSIF k IN ('rishi','rishi cold','rishi cold storage') THEN
      RETURN 'Rishi';
    ELSIF k IN ('supreme','supreme cold','supreme cold storage') THEN
      RETURN 'Supreme';
    ELSIF k IN ('eskimo','eskimo cold','eskimo cold storage') THEN
      RETURN 'Eskimo';
    ELSIF k IN ('w202','warehouse w202') THEN RETURN 'W202';
    ELSIF k IN ('a101','warehouse a101') THEN RETURN 'A101';
    ELSIF k IN ('a185','warehouse a185') THEN RETURN 'A185';
    ELSIF k IN ('a68','warehouse a68') THEN RETURN 'A68';
    ELSIF k IN ('f53','warehouse f53') THEN RETURN 'F53';
    ELSIF k IN ('dev int','dev_int') THEN RETURN 'Dev Int';
    END IF;
  END IF;
  -- Fallback: storage_location
  IF p_storage_location IS NOT NULL AND length(trim(p_storage_location)) > 0 THEN
    k := regexp_replace(lower(trim(p_storage_location)), '_', ' ', 'g');
    IF k IN ('savla d-39','savla d39','d-39','d39','savla bond','old savla','savla d-39 cold','savla d39 cold') THEN
      RETURN 'Savla D-39';
    ELSIF k IN ('savla d-514','savla d514','d-514','d514','new savla','savla d-514 cold') THEN
      RETURN 'Savla D-514';
    ELSIF k IN ('rishi','rishi cold','rishi cold storage') THEN
      RETURN 'Rishi';
    ELSIF k IN ('supreme','supreme cold','supreme cold storage') THEN
      RETURN 'Supreme';
    ELSIF k IN ('eskimo','eskimo cold','eskimo cold storage') THEN
      RETURN 'Eskimo';
    ELSIF k IN ('w202','warehouse w202') THEN RETURN 'W202';
    ELSIF k IN ('a101','warehouse a101') THEN RETURN 'A101';
    ELSIF k IN ('a185','warehouse a185') THEN RETURN 'A185';
    ELSIF k IN ('a68','warehouse a68') THEN RETURN 'A68';
    ELSIF k IN ('f53','warehouse f53') THEN RETURN 'F53';
    ELSIF k IN ('dev int','dev_int') THEN RETURN 'Dev Int';
    END IF;
  END IF;
  RETURN NULL;  -- "Other" bucket on the dashboard
END;
$$;

-- The existing *_cold_stocks BEFORE INSERT/UPDATE trigger (sync_canonical_cold_stock,
-- from 20260525) already calls canonical_warehouse_fn(), so new Eskimo rows are
-- canonicalized automatically — no trigger change needed.

-- ---------------------------------------------------------------------------
-- 2. Register Eskimo in the warehouse_sites master (idempotent).
--    NOTE: columns assumed to be (site_code, site_name, is_active) per
--    WarehouseSiteResponse. If warehouse_sites has additional NOT NULL columns
--    without defaults, extend this INSERT accordingly before applying.
-- ---------------------------------------------------------------------------
INSERT INTO warehouse_sites (site_code, site_name, is_active)
SELECT 'ESKIMO', 'Eskimo', true
WHERE NOT EXISTS (
  SELECT 1 FROM warehouse_sites WHERE lower(trim(site_name)) = 'eskimo'
);

-- ---------------------------------------------------------------------------
-- 3. Backfill canonical_warehouse for any pre-existing Eskimo rows (no-op if none).
-- ---------------------------------------------------------------------------
UPDATE cdpl_cold_stocks
   SET canonical_warehouse = canonical_warehouse_fn(unit, storage_location)
 WHERE (lower(coalesce(unit, '')) LIKE 'eskimo%' OR lower(coalesce(storage_location, '')) LIKE 'eskimo%')
   AND canonical_warehouse IS DISTINCT FROM canonical_warehouse_fn(unit, storage_location);

UPDATE cfpl_cold_stocks
   SET canonical_warehouse = canonical_warehouse_fn(unit, storage_location)
 WHERE (lower(coalesce(unit, '')) LIKE 'eskimo%' OR lower(coalesce(storage_location, '')) LIKE 'eskimo%')
   AND canonical_warehouse IS DISTINCT FROM canonical_warehouse_fn(unit, storage_location);
