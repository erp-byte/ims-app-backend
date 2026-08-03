-- Inward GRN: give each article a stable per-transaction identity (line_number)
-- so two articles with the SAME name but different grade/rate can coexist.
--
-- PROBLEM (root cause)
--   Articles were identified by their NAME. The tables enforced:
--       {prefix}_articles_v2  UNIQUE (transaction_no, item_description)
--       {prefix}_boxes_v2     UNIQUE (transaction_no, article_description, box_number)
--   and every box<->article link was the string  b.article_description = a.item_description.
--   So adding a 2nd article "cashew 320" (grade W320) under a 1st "cashew 320" (M320):
--     * the 2nd article row was silently dropped (ON CONFLICT (transaction_no,
--       item_description) DO NOTHING),
--     * its boxes collided with article 1's boxes on (article_description, box_number)
--       and were dropped / overwritten -> identical box_ids, entry not saved as typed.
--
-- FIX
--   Add an integer  line_number  (1-based position of the article within the
--   transaction) to BOTH tables and re-key identity to:
--       {prefix}_articles_v2  UNIQUE (transaction_no, line_number)
--       {prefix}_boxes_v2     UNIQUE (transaction_no, line_number, box_number)
--   article_description / item_description STAY on the rows (display + free-text
--   search + back-compat); they are no longer the identity.
--
-- BACKFILL is loss-free because existing data is unique-by-name per transaction
-- (that was the old constraint): each article gets line_number = its ordinal by id,
-- and each box inherits its article's line_number by matching the (now unique)
-- article_description. Orphan boxes (boxes whose description matches no article row)
-- are numbered after the real articles so they never collide.
--
-- HOW TO RUN
--   Runs inside ONE transaction (atomic: either the whole re-key lands or nothing).
--   Not CONCURRENTLY — the unique-index builds take a brief write lock
--   (cfpl_boxes_v2 is the big one, ~150k rows, a few seconds). Run in a quiet window
--   via psql:   psql "$DATABASE_URL" -f 2026-07-31_inward_article_line_number.sql
--   Re-runnable: every step is guarded (IF NOT EXISTS / signature-matched drops), so
--   re-applying is a no-op.
--
-- ROLLBACK (if ever needed): drop the new unique indexes, recreate the old ones,
-- then  ALTER TABLE ... DROP COLUMN line_number;  (the column is additive, so
-- leaving it in place is harmless).

BEGIN;

-- 1) Add the column (nullable; backfilled below). Additive + idempotent.
ALTER TABLE cfpl_articles_v2 ADD COLUMN IF NOT EXISTS line_number integer;
ALTER TABLE cdpl_articles_v2 ADD COLUMN IF NOT EXISTS line_number integer;
ALTER TABLE cfpl_boxes_v2    ADD COLUMN IF NOT EXISTS line_number integer;
ALTER TABLE cdpl_boxes_v2    ADD COLUMN IF NOT EXISTS line_number integer;

-- 2) Backfill articles: line_number = ordinal within the transaction (stable by id).
UPDATE cfpl_articles_v2 a
SET line_number = r.rn
FROM (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY transaction_no ORDER BY id) AS rn
    FROM cfpl_articles_v2
) r
WHERE a.id = r.id AND a.line_number IS NULL;

UPDATE cdpl_articles_v2 a
SET line_number = r.rn
FROM (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY transaction_no ORDER BY id) AS rn
    FROM cdpl_articles_v2
) r
WHERE a.id = r.id AND a.line_number IS NULL;

-- 3) Backfill boxes: inherit the parent article's line_number by matching
--    (transaction_no, article_description = item_description). Unique in old data.
UPDATE cfpl_boxes_v2 b
SET line_number = a.line_number
FROM cfpl_articles_v2 a
WHERE a.transaction_no = b.transaction_no
  AND a.item_description = b.article_description
  AND b.line_number IS NULL;

UPDATE cdpl_boxes_v2 b
SET line_number = a.line_number
FROM cdpl_articles_v2 a
WHERE a.transaction_no = b.transaction_no
  AND a.item_description = b.article_description
  AND b.line_number IS NULL;

-- 3b) Orphan boxes (no matching article row — e.g. transactions whose article rows
--     were never written and are synthesized-from-boxes on read). Give each distinct
--     article_description its own line_number, continuing AFTER the real article lines
--     so it can never collide with a backfilled article line.
WITH base AS (
    SELECT transaction_no, COALESCE(MAX(line_number), 0) AS max_ln
    FROM cfpl_articles_v2 GROUP BY transaction_no
),
orphan AS (
    SELECT b.transaction_no, b.article_description,
           DENSE_RANK() OVER (PARTITION BY b.transaction_no
                              ORDER BY b.article_description) AS rn
    FROM (SELECT DISTINCT transaction_no, article_description
          FROM cfpl_boxes_v2 WHERE line_number IS NULL) b
)
UPDATE cfpl_boxes_v2 x
SET line_number = COALESCE(base.max_ln, 0) + orphan.rn
FROM orphan
LEFT JOIN base ON base.transaction_no = orphan.transaction_no
WHERE x.transaction_no = orphan.transaction_no
  AND x.article_description = orphan.article_description
  AND x.line_number IS NULL;

WITH base AS (
    SELECT transaction_no, COALESCE(MAX(line_number), 0) AS max_ln
    FROM cdpl_articles_v2 GROUP BY transaction_no
),
orphan AS (
    SELECT b.transaction_no, b.article_description,
           DENSE_RANK() OVER (PARTITION BY b.transaction_no
                              ORDER BY b.article_description) AS rn
    FROM (SELECT DISTINCT transaction_no, article_description
          FROM cdpl_boxes_v2 WHERE line_number IS NULL) b
)
UPDATE cdpl_boxes_v2 x
SET line_number = COALESCE(base.max_ln, 0) + orphan.rn
FROM orphan
LEFT JOIN base ON base.transaction_no = orphan.transaction_no
WHERE x.transaction_no = orphan.transaction_no
  AND x.article_description = orphan.article_description
  AND x.line_number IS NULL;

-- 4) Drop the OLD name-based unique constraints/indexes (whichever form exists),
--    matched by their column signature so we don't depend on a specific name.
DO $$
DECLARE
    r record;
    -- sorted column signatures of the old unique keys
    art_sig text[] := ARRAY['item_description','transaction_no'];
    box_sig text[] := ARRAY['article_description','box_number','transaction_no'];
BEGIN
    -- 4a) unique CONSTRAINTS
    FOR r IN
        SELECT rel.relname AS tbl, con.conname AS name
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        WHERE con.contype = 'u'
          AND rel.relname IN ('cfpl_articles_v2','cdpl_articles_v2',
                              'cfpl_boxes_v2','cdpl_boxes_v2')
          AND (
              SELECT array_agg(att.attname ORDER BY att.attname)
              FROM unnest(con.conkey) AS k(attnum)
              JOIN pg_attribute att
                ON att.attrelid = con.conrelid AND att.attnum = k.attnum
          ) = CASE WHEN rel.relname LIKE '%_articles_v2' THEN art_sig ELSE box_sig END
    LOOP
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', r.tbl, r.name);
        RAISE NOTICE 'dropped unique constraint %.%', r.tbl, r.name;
    END LOOP;

    -- 4b) standalone unique INDEXES (not backing a constraint)
    FOR r IN
        SELECT t.relname AS tbl, i.indexrelid::regclass::text AS name
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        WHERE ix.indisunique
          AND t.relname IN ('cfpl_articles_v2','cdpl_articles_v2',
                            'cfpl_boxes_v2','cdpl_boxes_v2')
          AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = ix.indexrelid)
          AND (
              SELECT array_agg(att.attname ORDER BY att.attname)
              FROM unnest(ix.indkey) AS k(attnum)
              JOIN pg_attribute att
                ON att.attrelid = ix.indrelid AND att.attnum = k.attnum
          ) = CASE WHEN t.relname LIKE '%_articles_v2' THEN art_sig ELSE box_sig END
    LOOP
        EXECUTE format('DROP INDEX %s', r.name);
        RAISE NOTICE 'dropped unique index % on %', r.name, r.tbl;
    END LOOP;
END $$;

-- 5) Add the NEW line_number-based unique keys. ON CONFLICT in inward_tools.py
--    infers these. Rows with a NULL line_number (unreached legacy insert paths)
--    are treated as distinct by Postgres and simply bypass the constraint.
CREATE UNIQUE INDEX IF NOT EXISTS uq_cfpl_articles_v2_txn_line
    ON cfpl_articles_v2 (transaction_no, line_number);
CREATE UNIQUE INDEX IF NOT EXISTS uq_cdpl_articles_v2_txn_line
    ON cdpl_articles_v2 (transaction_no, line_number);
CREATE UNIQUE INDEX IF NOT EXISTS uq_cfpl_boxes_v2_txn_line_box
    ON cfpl_boxes_v2 (transaction_no, line_number, box_number);
CREATE UNIQUE INDEX IF NOT EXISTS uq_cdpl_boxes_v2_txn_line_box
    ON cdpl_boxes_v2 (transaction_no, line_number, box_number);

-- 5b) Safety net for legacy insert paths that don't set line_number (e.g. the
--     pending-stock "restore to source" reconciliation, which used a bare
--     ON CONFLICT DO NOTHING against the OLD (transaction_no, article_description,
--     box_number) key). A PARTIAL unique index on exactly those columns for the
--     NULL-line_number rows keeps that idempotency without blocking same-name
--     articles (whose boxes always carry a line_number and so are excluded here).
CREATE UNIQUE INDEX IF NOT EXISTS uq_cfpl_boxes_v2_txn_desc_box_nolinenull
    ON cfpl_boxes_v2 (transaction_no, article_description, box_number)
    WHERE line_number IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_cdpl_boxes_v2_txn_desc_box_nolinenull
    ON cdpl_boxes_v2 (transaction_no, article_description, box_number)
    WHERE line_number IS NULL;

COMMIT;

-- 6) Sanity checks (read-only; run after COMMIT). All should return 0 rows.
--   -- articles still missing a line_number:
--   SELECT 'cfpl_art' t, count(*) FROM cfpl_articles_v2 WHERE line_number IS NULL
--   UNION ALL SELECT 'cdpl_art', count(*) FROM cdpl_articles_v2 WHERE line_number IS NULL
--   UNION ALL SELECT 'cfpl_box', count(*) FROM cfpl_boxes_v2 WHERE line_number IS NULL
--   UNION ALL SELECT 'cdpl_box', count(*) FROM cdpl_boxes_v2 WHERE line_number IS NULL;
--   -- duplicate identities that should now be impossible:
--   SELECT transaction_no, line_number, count(*) FROM cfpl_articles_v2
--     GROUP BY 1,2 HAVING count(*) > 1;
