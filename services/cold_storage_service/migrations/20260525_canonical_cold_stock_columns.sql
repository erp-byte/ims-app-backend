-- 20260525_canonical_cold_stock_columns.sql
-- Adds materialized canonical columns to cfpl_cold_stocks / cdpl_cold_stocks
-- so dashboards can GROUP BY a single normalized field per dimension.
--
-- Columns added:
--   canonical_warehouse  TEXT   (one of: Savla D-39, Savla D-514, Rishi,
--                                Supreme, W202, A101, A185, A68, F53, Dev Int,
--                                or NULL for "Other"/unrecognized)
--   canonical_group      TEXT   (title-cased from all_sku.item_group, falling
--                                back to title-cased group_name)
--   canonical_subgroup   TEXT   (title-cased from all_sku.sub_group, falling
--                                back to title-cased item_subgroup)
--
-- Backfill is run by `backend/services/cold_storage_service/dashboard_server.py
-- backfill_canonical_columns()` — call once after deploying this migration.
--
-- An AFTER INSERT/UPDATE trigger keeps the columns fresh going forward.

-- ---------------------------------------------------------------------------
-- Helper function — folds free text the same way the Python canonical_warehouse
-- helper does, then maps known aliases to canonical names.
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

-- ---------------------------------------------------------------------------
-- Title-case helper — collapses 'DATES'/'dates'/'Dates' → 'Dates'
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION title_fold(p TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE
AS $$
  SELECT CASE
    WHEN p IS NULL OR length(trim(p)) = 0 THEN NULL
    ELSE initcap(lower(trim(p)))
  END;
$$;

-- ---------------------------------------------------------------------------
-- Apply to cfpl_cold_stocks and cdpl_cold_stocks
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['cfpl_cold_stocks', 'cdpl_cold_stocks'] LOOP
    -- Add columns if missing
    EXECUTE format(
      'ALTER TABLE %I ADD COLUMN IF NOT EXISTS canonical_warehouse TEXT', tbl
    );
    EXECUTE format(
      'ALTER TABLE %I ADD COLUMN IF NOT EXISTS canonical_group TEXT', tbl
    );
    EXECUTE format(
      'ALTER TABLE %I ADD COLUMN IF NOT EXISTS canonical_subgroup TEXT', tbl
    );

    -- Backfill
    EXECUTE format(
      'UPDATE %I SET '
      '  canonical_warehouse = canonical_warehouse_fn(unit, storage_location), '
      '  canonical_group = COALESCE('
      '    (SELECT title_fold(s.item_group) FROM all_sku s WHERE lower(s.particulars) = lower(%I.item_description) LIMIT 1), '
      '    title_fold(%I.group_name)'
      '  ), '
      '  canonical_subgroup = COALESCE('
      '    (SELECT title_fold(s.sub_group) FROM all_sku s WHERE lower(s.particulars) = lower(%I.item_description) LIMIT 1), '
      '    title_fold(%I.item_subgroup)'
      '  )',
      tbl, tbl, tbl, tbl, tbl
    );

    -- Helpful indexes for grouping
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON %I (canonical_warehouse)',
      tbl || '_canon_wh_idx', tbl
    );
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON %I (canonical_warehouse, canonical_group, canonical_subgroup)',
      tbl || '_canon_wgs_idx', tbl
    );
  END LOOP;
END;
$$;

-- ---------------------------------------------------------------------------
-- Trigger: keep canonical columns in sync on INSERT/UPDATE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION sync_canonical_cold_stock()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.canonical_warehouse := canonical_warehouse_fn(NEW.unit, NEW.storage_location);
  NEW.canonical_group := COALESCE(
    (SELECT title_fold(s.item_group) FROM all_sku s WHERE lower(s.particulars) = lower(NEW.item_description) LIMIT 1),
    title_fold(NEW.group_name)
  );
  NEW.canonical_subgroup := COALESCE(
    (SELECT title_fold(s.sub_group) FROM all_sku s WHERE lower(s.particulars) = lower(NEW.item_description) LIMIT 1),
    title_fold(NEW.item_subgroup)
  );
  RETURN NEW;
END;
$$;

DO $$
DECLARE
  tbl TEXT;
  trig_name TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['cfpl_cold_stocks', 'cdpl_cold_stocks'] LOOP
    trig_name := tbl || '_sync_canonical';
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', trig_name, tbl);
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE INSERT OR UPDATE OF unit, storage_location, item_description, group_name, item_subgroup '
      'ON %I FOR EACH ROW EXECUTE FUNCTION sync_canonical_cold_stock()',
      trig_name, tbl
    );
  END LOOP;
END;
$$;
