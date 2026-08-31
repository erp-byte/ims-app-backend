-- 2026-08-29 RTV: persist a free-text Location and POC contact detail on the header.
-- `location` is where the return is being collected from / delivered to; `poc_contact`
-- is a free-text contact detail for the POC (usually a phone number, but not validated
-- and NOT an email -- it is display-only and is never added to any mail CC).
-- Both nullable; legacy rows stay NULL.
ALTER TABLE cfpl_rtv_header
  ADD COLUMN IF NOT EXISTS location    varchar,
  ADD COLUMN IF NOT EXISTS poc_contact varchar;

ALTER TABLE cdpl_rtv_header
  ADD COLUMN IF NOT EXISTS location    varchar,
  ADD COLUMN IF NOT EXISTS poc_contact varchar;
