-- READ-ONLY review. Run this BEFORE 2026-08-12_cleanup_duplicate_phantom_boxes.sql.
-- Contains no BEGIN, no DELETE, no UPDATE -- executing the whole file is safe.
--
-- Run it AFTER 2026-08-12_backfill_box_line_numbers.sql has committed; before that, the
-- unset set is all 121,853 rows rather than the ~1,871 genuine leftovers.
--
-- Expected (dry run, 2026-08-12):
--     3_EMPTY_phantom_delete   1,857   <- what the cleanup removes
--     1_PRINTED_keep               7   <- physical label exists
--     2_HAS_DATA_keep              7   <- carries weight or count
--   The 14 "keep" rows are listed in full by the second query, for a human decision.

-- 1) How the leftovers split, and how many transactions they touch.
SELECT CASE
         WHEN box_id IS NOT NULL                            THEN '1_PRINTED_keep'
         WHEN COALESCE(net_weight, 0) <> 0
           OR COALESCE(gross_weight, 0) <> 0
           OR count IS NOT NULL                             THEN '2_HAS_DATA_keep'
         ELSE                                                    '3_EMPTY_phantom_delete'
       END                            AS disposition,
       COUNT(*)                       AS rows,
       COUNT(DISTINCT transaction_no) AS txns
FROM cfpl_boxes_v2
WHERE line_number IS NULL OR line_number = 0
GROUP BY 1
ORDER BY 1;

-- 2) Every row the cleanup will KEEP, in full. Decide on each one.
--    For each, the row it collides with is shown alongside so you can tell a genuine
--    duplicate from a distinct box that merely shares a box_number.
SELECT b.transaction_no,
       b.box_number,
       b.box_id                AS leftover_box_id,
       b.net_weight            AS leftover_net,
       b.lot_number            AS leftover_lot,
       a.line_number           AS would_move_to_line,
       b2.box_id               AS existing_box_id,
       b2.net_weight           AS existing_net,
       b2.lot_number           AS existing_lot,
       LEFT(b.article_description, 44) AS article
FROM cfpl_boxes_v2 b
LEFT JOIN cfpl_articles_v2 a
       ON a.transaction_no   = b.transaction_no
      AND a.item_description = b.article_description
      AND a.line_number IS NOT NULL AND a.line_number <> 0
LEFT JOIN cfpl_boxes_v2 b2
       ON b2.transaction_no = b.transaction_no
      AND b2.line_number    = a.line_number
      AND b2.box_number     = b.box_number
WHERE (b.line_number IS NULL OR b.line_number = 0)
  AND (b.box_id IS NOT NULL
       OR COALESCE(b.net_weight, 0)   <> 0
       OR COALESCE(b.gross_weight, 0) <> 0
       OR b.count IS NOT NULL)
ORDER BY b.transaction_no, b.box_number;

-- 3) Exactly what would be deleted, summarised per transaction.
SELECT transaction_no, COUNT(*) AS phantoms_to_delete
FROM cfpl_boxes_v2
WHERE (line_number IS NULL OR line_number = 0)
  AND box_id IS NULL
  AND COALESCE(net_weight, 0)   = 0
  AND COALESCE(gross_weight, 0) = 0
  AND count IS NULL
GROUP BY transaction_no
ORDER BY COUNT(*) DESC;
