-- Inward free-text search: trigram indexes on the box tables.
-- APPLIED to warehouse_db on 2026-07-25; all 12 built valid, ~17 MB total.
--
-- The search rewrite in inward_tools.build_search_conditions took a search from
-- ~39s to ~1.1s with no schema change. What was left of that 1.1s was almost
-- entirely the ILIKE '%term%' scan over ~147k box rows (the transaction and
-- article tables are ~1.2k rows each and are not worth indexing).
--
-- Measured end-to-end on GET /inward/records, CFPL, page 1, 20 per page:
--   term        before rewrite   after rewrite   after these indexes
--   'SAVLA'          39.0s           1.8s              0.18s
--   'BOX'            26.2s           2.0s              0.16s
--   '123'            25.1s           1.2s              0.54s
--   'BX' (2 chars)      -               -              1.32s
--
-- A trigram index cannot serve terms shorter than 3 characters, so 1-2 character
-- searches still fall back to a scan. That is the pre-existing behaviour, not a
-- regression — debounce to 3+ characters in the UI if it shows.
--
-- Cost: 12 GIN indexes on tables written in bulk during inward. To roll back,
-- DROP INDEX CONCURRENTLY each ix_*_trgm below.
--
-- HOW TO RUN: CONCURRENTLY means no write downtime, but it also means these
-- cannot run inside a transaction block. pgAdmin / DBeaver wrap a multi-statement
-- script in one transaction and will fail with SQLSTATE 25001 — run it through
-- psql, or execute the statements one at a time with auto-commit on.
-- Build time is ~5s for cfpl_boxes_v2, under 1s for the rest.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cfpl_boxes_v2_article_trgm
    ON cfpl_boxes_v2 USING gin (article_description gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cfpl_boxes_v2_lot_trgm
    ON cfpl_boxes_v2 USING gin (lot_number gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cfpl_boxes_v2_boxid_trgm
    ON cfpl_boxes_v2 USING gin (box_id gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cfpl_be_boxes_article_trgm
    ON cfpl_bulk_entry_boxes USING gin (article_description gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cfpl_be_boxes_lot_trgm
    ON cfpl_bulk_entry_boxes USING gin (lot_number gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cfpl_be_boxes_boxid_trgm
    ON cfpl_bulk_entry_boxes USING gin (box_id gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cdpl_boxes_v2_article_trgm
    ON cdpl_boxes_v2 USING gin (article_description gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cdpl_boxes_v2_lot_trgm
    ON cdpl_boxes_v2 USING gin (lot_number gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cdpl_boxes_v2_boxid_trgm
    ON cdpl_boxes_v2 USING gin (box_id gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cdpl_be_boxes_article_trgm
    ON cdpl_bulk_entry_boxes USING gin (article_description gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cdpl_be_boxes_lot_trgm
    ON cdpl_bulk_entry_boxes USING gin (lot_number gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_cdpl_be_boxes_boxid_trgm
    ON cdpl_bulk_entry_boxes USING gin (box_id gin_trgm_ops);
