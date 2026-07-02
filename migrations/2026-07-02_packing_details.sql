-- Packing Details (QR / encrypted batch tokens).
-- Mirrored in main.py::_run_startup_migrations so the schema self-heals at boot.
-- batch_code + article_name are the business handles; `details` is a free-form
-- JSONB body. The QR encodes an AES-256-GCM token of batch_code (not the id).
CREATE TABLE IF NOT EXISTS packing_details (
    id           SERIAL PRIMARY KEY,
    batch_code   VARCHAR(255) NOT NULL,
    article_name VARCHAR(255) NOT NULL,
    details      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by   VARCHAR(255),
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_packing_details_batch   ON packing_details(batch_code);
CREATE INDEX IF NOT EXISTS idx_packing_details_article ON packing_details(article_name);
