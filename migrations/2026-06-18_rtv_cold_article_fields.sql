-- 2026-06-18 RTV: per-article cold fields. lot_number already exists on *_rtv_boxes;
-- add it to *_rtv_lines (article-level default) and add item_mark/spl_remarks/vakkal
-- to both lines and boxes so RTV cold lots are cross-linkable like inward.

ALTER TABLE cfpl_rtv_lines
  ADD COLUMN IF NOT EXISTS lot_number  varchar,
  ADD COLUMN IF NOT EXISTS item_mark   varchar,
  ADD COLUMN IF NOT EXISTS spl_remarks varchar,
  ADD COLUMN IF NOT EXISTS vakkal      varchar;

ALTER TABLE cdpl_rtv_lines
  ADD COLUMN IF NOT EXISTS lot_number  varchar,
  ADD COLUMN IF NOT EXISTS item_mark   varchar,
  ADD COLUMN IF NOT EXISTS spl_remarks varchar,
  ADD COLUMN IF NOT EXISTS vakkal      varchar;

ALTER TABLE cfpl_rtv_boxes
  ADD COLUMN IF NOT EXISTS item_mark   varchar,
  ADD COLUMN IF NOT EXISTS spl_remarks varchar,
  ADD COLUMN IF NOT EXISTS vakkal      varchar;

ALTER TABLE cdpl_rtv_boxes
  ADD COLUMN IF NOT EXISTS item_mark   varchar,
  ADD COLUMN IF NOT EXISTS spl_remarks varchar,
  ADD COLUMN IF NOT EXISTS vakkal      varchar;
