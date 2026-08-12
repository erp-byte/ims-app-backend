-- OPT-IN step 5 -- DESTRUCTIVE. This file DELETES rows.
--
-- PREREQUISITES, in order:
--   1. 2026-08-12_backfill_box_line_numbers.sql has COMMITTED.
--   2. 2026-08-12_cleanup_review_ONLY.sql has been run and read.
--   Skipping (1) means "unset" still matches all 121,853 rows, not the ~1,871 leftovers.
--
-- WHY THESE ROWS EXIST
--   The repair deliberately leaves ~1,871 cfpl rows unset: each is the duplicate side of
--   a (transaction_no, line_number, box_number) clash. A NULL line_number slips past
--   cfpl_boxes_v2_txn_line_box_uq (NULLs are distinct in Postgres), so the app inserted a
--   second row instead of updating the first.
--
-- WHAT THIS DELETES -- measured on the post-repair state (dry run, 2026-08-12):
--     1,857  never printed AND completely empty   -> DELETED here
--         7  PRINTED (box_id NOT NULL)            -> kept, a physical label exists
--         7  carrying a weight or a count         -> kept
--   Only the first group goes. Every deleted row is copied to
--   _repair_20260812_deleted_boxes first, so this is reversible (undo at the bottom).
--
-- RUN -- works in psql, pgAdmin, DBeaver or any plain SQL client.
--   psql:  psql -v ON_ERROR_STOP=1 "$DATABASE_URL" -f 2026-08-12_cleanup_duplicate_phantom_boxes.sql
--   GUI :  execute the whole file. Everything is inside BEGIN/COMMIT, so an error
--          anywhere aborts the transaction and nothing is deleted.
--
-- (No psql \-meta-commands: they are a syntax error in every non-psql client.)

BEGIN;

-- Snapshot first -- full row copies, so the delete can be undone.
CREATE TABLE IF NOT EXISTS _repair_20260812_deleted_boxes
    (LIKE cfpl_boxes_v2 INCLUDING DEFAULTS);

INSERT INTO _repair_20260812_deleted_boxes
SELECT * FROM cfpl_boxes_v2
WHERE (line_number IS NULL OR line_number = 0)
  AND box_id IS NULL
  AND COALESCE(net_weight, 0)   = 0
  AND COALESCE(gross_weight, 0) = 0
  AND count IS NULL;

DELETE FROM cfpl_boxes_v2
WHERE (line_number IS NULL OR line_number = 0)
  AND box_id IS NULL
  AND COALESCE(net_weight, 0)   = 0
  AND COALESCE(gross_weight, 0) = 0
  AND count IS NULL;

-- Aggregates again -- the deletions change the box counts for those articles.
UPDATE cfpl_articles_v2 a
SET quantity_units = s.cnt, net_weight = s.net, total_weight = s.gross
FROM (SELECT transaction_no, line_number, COUNT(*) AS cnt,
             COALESCE(SUM(net_weight), 0) AS net,
             COALESCE(SUM(gross_weight), 0) AS gross
      FROM cfpl_boxes_v2 WHERE line_number IS NOT NULL
      GROUP BY transaction_no, line_number) s
WHERE a.transaction_no = s.transaction_no AND a.line_number = s.line_number;

COMMIT;

-- ===========================================================================
-- AFTER: only the deliberately-kept rows remain unset. Expect 14.
-- ===========================================================================
SELECT COUNT(*) AS still_unset_expect_14
FROM cfpl_boxes_v2 WHERE line_number IS NULL OR line_number = 0;

SELECT COUNT(*) AS rows_deleted_expect_1857
FROM _repair_20260812_deleted_boxes;

-- To undo the deletion:
--   INSERT INTO cfpl_boxes_v2 SELECT * FROM _repair_20260812_deleted_boxes;
