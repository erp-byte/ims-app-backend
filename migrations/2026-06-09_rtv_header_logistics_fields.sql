-- 2026-06-09 RTV: persist logistics header fields (previously sent by FE, dropped by BE).
ALTER TABLE cfpl_rtv_header
  ADD COLUMN IF NOT EXISTS vehicle_number   varchar,
  ADD COLUMN IF NOT EXISTS transporter_name varchar,
  ADD COLUMN IF NOT EXISTS driver_name      varchar,
  ADD COLUMN IF NOT EXISTS inward_manager   varchar;

ALTER TABLE cdpl_rtv_header
  ADD COLUMN IF NOT EXISTS vehicle_number   varchar,
  ADD COLUMN IF NOT EXISTS transporter_name varchar,
  ADD COLUMN IF NOT EXISTS driver_name      varchar,
  ADD COLUMN IF NOT EXISTS inward_manager   varchar;
