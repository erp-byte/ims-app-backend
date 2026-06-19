-- 2026-06-18 RTV: persist a manual Sales POC email.
-- When the Sales POC dropdown selection is "Other", the FE sends a free-text
-- name (sales_poc) plus a manually entered email (sales_poc_email). The email
-- is added to the CC of every RTV notification. Nullable; legacy rows stay NULL.
ALTER TABLE cfpl_rtv_header
  ADD COLUMN IF NOT EXISTS sales_poc_email varchar;

ALTER TABLE cdpl_rtv_header
  ADD COLUMN IF NOT EXISTS sales_poc_email varchar;
