-- Packing Details (QR / encrypted batch tokens).
-- Mirrored in main.py::_run_startup_migrations so the schema self-heals at boot.
-- batch_code + article_name are the business handles; `details` is a free-form
-- JSON body. The QR encodes an AES-256-GCM token of batch_code (not the id).
--
-- `details` is JSON (not JSONB) on purpose: JSONB canonicalises object keys
-- (reorders them by length, then bytewise), which would silently discard the
-- user's block ordering on every round-trip. JSON stores the text verbatim, so
-- the order the user builds their blocks in is preserved. Nothing queries
-- `details` with jsonb operators or a GIN index, so no functionality is lost.
CREATE TABLE IF NOT EXISTS packing_details (
    id           SERIAL PRIMARY KEY,
    batch_code   VARCHAR(255) NOT NULL,
    article_name VARCHAR(255) NOT NULL,
    details      JSON NOT NULL DEFAULT '{}'::json,
    created_by   VARCHAR(255),
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- Self-heal older deployments where `details` was created as JSONB: convert it
-- to JSON in place so key order is preserved from here on (idempotent — only
-- rewrites while the column is still jsonb).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'packing_details'
          AND column_name = 'details'
          AND data_type  = 'jsonb'
    ) THEN
        ALTER TABLE packing_details ALTER COLUMN details DROP DEFAULT;
        ALTER TABLE packing_details ALTER COLUMN details TYPE JSON USING details::text::json;
        ALTER TABLE packing_details ALTER COLUMN details SET DEFAULT '{}'::json;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_packing_details_batch   ON packing_details(batch_code);
CREATE INDEX IF NOT EXISTS idx_packing_details_article ON packing_details(article_name);
