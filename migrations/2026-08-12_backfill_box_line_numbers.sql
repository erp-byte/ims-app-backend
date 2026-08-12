-- Repair: box rows whose line_number never got set.
--
-- WHY
--   Boxes join to their article by line_number. The 2026-07-31 re-key added the column
--   and its unique keys DID land (as cfpl/cdpl_boxes_v2_txn_line_box_uq on
--   (transaction_no, line_number, box_number)), but the BACKFILL never ran. Measured
--   2026-08-12:
--       cfpl_boxes_v2   121,853 / 128,707 rows with line_number NULL or 0
--       cdpl_boxes_v2    21,411 /  21,411 rows  (and all 50 cdpl articles too)
--   Postgres treats NULLs as distinct, so every one of those rows slips straight past
--   the unique key -- which is why duplicates accumulated on top of the mismatch.
--
--   Consequences seen in the app: the inward EDIT and REVIEW screens group boxes by
--   line_number and so render none of them, while the VIEW screen renders a flat list
--   and looks fine. The article's quantity_units / net_weight also stop tracking its
--   boxes, because the compute-on-read overlay keys off line_number too.
--
--   A 0 counts as unset here, not as a real line: real lines are 1-based, but the UI
--   normalises a missing line to 0 and posts it back, and upsert_box only skips the
--   column on `is None` -- so 0 gets written and permanently un-matches the row.
--
-- SAFETY
--   Steps 1-4 run in ONE transaction and are re-runnable (every one is guarded by
--   "still unset"). Step 0 snapshots every row they touch. Step 5 is a SEPARATE,
--   OPT-IN cleanup of duplicates -- read its notes before running it.
--   Nothing here deletes anything except step 5.
--
--   Deploy the get_inward read-side fix as well. Without it the app keeps writing 0
--   back and the drift returns.
--
-- RUN -- works in psql, pgAdmin, DBeaver or any plain SQL client.
--   psql:  psql -v ON_ERROR_STOP=1 "$DATABASE_URL" -f 2026-08-12_backfill_box_line_numbers.sql
--   GUI :  execute the whole file. Steps 0-4 are wrapped in BEGIN/COMMIT, so if any
--          statement errors the transaction aborts and COMMIT rolls the lot back --
--          the repair is all-or-nothing either way.
--
-- (No psql \-meta-commands are used here, deliberately: they are a syntax error in
--  every non-psql client.)

-- ===========================================================================
-- BEFORE: what we are about to change
-- ===========================================================================
SELECT 'cfpl_articles' AS t,
       COUNT(*) FILTER (WHERE line_number IS NULL OR line_number = 0) AS unset,
       COUNT(*) AS total FROM cfpl_articles_v2
UNION ALL SELECT 'cdpl_articles',
       COUNT(*) FILTER (WHERE line_number IS NULL OR line_number = 0),
       COUNT(*) FROM cdpl_articles_v2
UNION ALL SELECT 'cfpl_boxes',
       COUNT(*) FILTER (WHERE line_number IS NULL OR line_number = 0),
       COUNT(*) FROM cfpl_boxes_v2
UNION ALL SELECT 'cdpl_boxes',
       COUNT(*) FILTER (WHERE line_number IS NULL OR line_number = 0),
       COUNT(*) FROM cdpl_boxes_v2;

BEGIN;

-- ===========================================================================
-- 0) Snapshot every row the repair can touch, so any step is reversible.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS _repair_20260812_box_line_backup (
    company        text,
    tbl            text,
    row_id         bigint,
    transaction_no text,
    article_desc   text,
    box_number     integer,
    old_line       integer,
    box_id         text,
    captured_at    timestamptz DEFAULT now()
);

INSERT INTO _repair_20260812_box_line_backup
    (company, tbl, row_id, transaction_no, article_desc, box_number, old_line, box_id)
SELECT 'CFPL', 'cfpl_boxes_v2', id, transaction_no, article_description,
       box_number, line_number, box_id
FROM cfpl_boxes_v2 WHERE line_number IS NULL OR line_number = 0;

INSERT INTO _repair_20260812_box_line_backup
    (company, tbl, row_id, transaction_no, article_desc, box_number, old_line, box_id)
SELECT 'CDPL', 'cdpl_boxes_v2', id, transaction_no, article_description,
       box_number, line_number, box_id
FROM cdpl_boxes_v2 WHERE line_number IS NULL OR line_number = 0;

INSERT INTO _repair_20260812_box_line_backup
    (company, tbl, row_id, transaction_no, article_desc, box_number, old_line, box_id)
SELECT 'CDPL', 'cdpl_articles_v2', id, transaction_no, item_description,
       NULL, line_number, NULL
FROM cdpl_articles_v2 WHERE line_number IS NULL OR line_number = 0;

-- ===========================================================================
-- 1) Articles first -- a box can only inherit a line that exists.
--    cfpl articles are already complete (0 unset); cdpl has all 50 unset.
--    Ordinal by id within the transaction, matching _assign_line_numbers().
-- ===========================================================================
UPDATE cdpl_articles_v2 a
SET line_number = r.rn
FROM (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY transaction_no ORDER BY id) AS rn
    FROM cdpl_articles_v2
    WHERE line_number IS NULL OR line_number = 0
) r
WHERE a.id = r.id AND (a.line_number IS NULL OR a.line_number = 0);

UPDATE cfpl_articles_v2 a
SET line_number = r.rn
FROM (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY transaction_no ORDER BY id) AS rn
    FROM cfpl_articles_v2
    WHERE line_number IS NULL OR line_number = 0
) r
WHERE a.id = r.id AND (a.line_number IS NULL OR a.line_number = 0);

-- ===========================================================================
-- 2) Boxes inherit their article's line by name.
--
--    Two guards, both required:
--      NOT EXISTS  - a row already sits on (transaction_no, target_line, box_number)
--      rn = 1      - two unset rows can share a box_number and would otherwise
--                    collide with EACH OTHER on the way in
--    Printed rows (box_id NOT NULL) win the tie, then lowest id. Rows that lose stay
--    unset and are reported in step 5 -- they are duplicates, not data to reassign.
--
--    Safe by name: 0 (transaction_no, item_description) pairs have >1 article row in
--    either company, so the mapping is one-to-one and never guesses.
-- ===========================================================================
WITH cand AS (
    SELECT b.id,
           a.line_number AS target_line,
           ROW_NUMBER() OVER (
               PARTITION BY b.transaction_no, a.line_number, b.box_number
               ORDER BY (b.box_id IS NULL), b.id
           ) AS rn
    FROM cfpl_boxes_v2 b
    JOIN cfpl_articles_v2 a
      ON a.transaction_no   = b.transaction_no
     AND a.item_description = b.article_description
     AND a.line_number IS NOT NULL AND a.line_number <> 0
    WHERE (b.line_number IS NULL OR b.line_number = 0)
      AND NOT EXISTS (
            SELECT 1 FROM cfpl_boxes_v2 b2
            WHERE b2.transaction_no = b.transaction_no
              AND b2.line_number    = a.line_number
              AND b2.box_number     = b.box_number
          )
)
UPDATE cfpl_boxes_v2 x
SET line_number = c.target_line
FROM cand c
WHERE x.id = c.id AND c.rn = 1;

WITH cand AS (
    SELECT b.id,
           a.line_number AS target_line,
           ROW_NUMBER() OVER (
               PARTITION BY b.transaction_no, a.line_number, b.box_number
               ORDER BY (b.box_id IS NULL), b.id
           ) AS rn
    FROM cdpl_boxes_v2 b
    JOIN cdpl_articles_v2 a
      ON a.transaction_no   = b.transaction_no
     AND a.item_description = b.article_description
     AND a.line_number IS NOT NULL AND a.line_number <> 0
    WHERE (b.line_number IS NULL OR b.line_number = 0)
      AND NOT EXISTS (
            SELECT 1 FROM cdpl_boxes_v2 b2
            WHERE b2.transaction_no = b.transaction_no
              AND b2.line_number    = a.line_number
              AND b2.box_number     = b.box_number
          )
)
UPDATE cdpl_boxes_v2 x
SET line_number = c.target_line
FROM cand c
WHERE x.id = c.id AND c.rn = 1;

-- ===========================================================================
-- 3) Orphans: boxes whose article_description matches no article row
--    (cfpl 4,115  cdpl 398). They are real rows and must stay reachable, so give
--    each distinct description its own line numbered AFTER everything already used
--    in that transaction -- articles and boxes alike -- so it cannot collide.
--    _surface_orphan_box_articles() then presents them as their own article group.
-- ===========================================================================
WITH used AS (
    SELECT transaction_no, MAX(line_number) AS mx FROM (
        SELECT transaction_no, line_number FROM cfpl_articles_v2
        UNION ALL
        SELECT transaction_no, line_number FROM cfpl_boxes_v2
    ) u WHERE line_number IS NOT NULL GROUP BY transaction_no
),
orphan AS (
    SELECT b.transaction_no, b.article_description,
           DENSE_RANK() OVER (PARTITION BY b.transaction_no
                              ORDER BY b.article_description) AS rn
    FROM (SELECT DISTINCT b.transaction_no, b.article_description
          FROM cfpl_boxes_v2 b
          WHERE (b.line_number IS NULL OR b.line_number = 0)
            AND NOT EXISTS (SELECT 1 FROM cfpl_articles_v2 a
                            WHERE a.transaction_no   = b.transaction_no
                              AND a.item_description = b.article_description)) b
)
UPDATE cfpl_boxes_v2 x
SET line_number = COALESCE(used.mx, 0) + orphan.rn
FROM orphan LEFT JOIN used ON used.transaction_no = orphan.transaction_no
WHERE x.transaction_no      = orphan.transaction_no
  AND x.article_description = orphan.article_description
  AND (x.line_number IS NULL OR x.line_number = 0);

WITH used AS (
    SELECT transaction_no, MAX(line_number) AS mx FROM (
        SELECT transaction_no, line_number FROM cdpl_articles_v2
        UNION ALL
        SELECT transaction_no, line_number FROM cdpl_boxes_v2
    ) u WHERE line_number IS NOT NULL GROUP BY transaction_no
),
orphan AS (
    SELECT b.transaction_no, b.article_description,
           DENSE_RANK() OVER (PARTITION BY b.transaction_no
                              ORDER BY b.article_description) AS rn
    FROM (SELECT DISTINCT b.transaction_no, b.article_description
          FROM cdpl_boxes_v2 b
          WHERE (b.line_number IS NULL OR b.line_number = 0)
            AND NOT EXISTS (SELECT 1 FROM cdpl_articles_v2 a
                            WHERE a.transaction_no   = b.transaction_no
                              AND a.item_description = b.article_description)) b
)
UPDATE cdpl_boxes_v2 x
SET line_number = COALESCE(used.mx, 0) + orphan.rn
FROM orphan LEFT JOIN used ON used.transaction_no = orphan.transaction_no
WHERE x.transaction_no      = orphan.transaction_no
  AND x.article_description = orphan.article_description
  AND (x.line_number IS NULL OR x.line_number = 0);

-- ===========================================================================
-- 4) Re-point the article aggregates at their boxes. quantity_units is strictly
--    COUNT(boxes) (the locked decision in recalc_article_aggregates), so this
--    clears the drift the screens were showing (e.g. qty 1440 over 1153 boxes).
--    Only touches articles that actually have boxes -- a legacy boxless article
--    is left alone rather than zeroed.
-- ===========================================================================
UPDATE cfpl_articles_v2 a
SET quantity_units = s.cnt, net_weight = s.net, total_weight = s.gross
FROM (SELECT transaction_no, line_number, COUNT(*) AS cnt,
             COALESCE(SUM(net_weight), 0) AS net,
             COALESCE(SUM(gross_weight), 0) AS gross
      FROM cfpl_boxes_v2 WHERE line_number IS NOT NULL
      GROUP BY transaction_no, line_number) s
WHERE a.transaction_no = s.transaction_no AND a.line_number = s.line_number;

UPDATE cdpl_articles_v2 a
SET quantity_units = s.cnt, net_weight = s.net, total_weight = s.gross
FROM (SELECT transaction_no, line_number, COUNT(*) AS cnt,
             COALESCE(SUM(net_weight), 0) AS net,
             COALESCE(SUM(gross_weight), 0) AS gross
      FROM cdpl_boxes_v2 WHERE line_number IS NOT NULL
      GROUP BY transaction_no, line_number) s
WHERE a.transaction_no = s.transaction_no AND a.line_number = s.line_number;

COMMIT;

-- ===========================================================================
-- AFTER: everything below should be 0 except the duplicates step 5 handles.
-- ===========================================================================
SELECT 'cfpl_boxes still unset' AS check, COUNT(*) AS n
FROM cfpl_boxes_v2 WHERE line_number IS NULL OR line_number = 0
UNION ALL
SELECT 'cdpl_boxes still unset', COUNT(*)
FROM cdpl_boxes_v2 WHERE line_number IS NULL OR line_number = 0
UNION ALL
SELECT 'cfpl_articles still unset', COUNT(*)
FROM cfpl_articles_v2 WHERE line_number IS NULL OR line_number = 0
UNION ALL
SELECT 'cdpl_articles still unset', COUNT(*)
FROM cdpl_articles_v2 WHERE line_number IS NULL OR line_number = 0;
