-- fdk (the Shoonya volume-spike detector): the Supabase database behind FDK_SUPABASE_DSN.
-- NOT the IMS database, and NOT run by main.py's start-up migrations.
--
-- Recorded after the fact. These statements were run by hand on 2026-11-02 when the detector
-- moved from Fyers to Shoonya. SymbolManager._ensure_tables() and _link_symbols_to_instruments()
-- now do the schema part on every start-up, so this file is the record of what changed, not
-- something to run again.

-- 1. One row per company: the Shoonya token the feed subscribes by, keyed by ISIN.
CREATE TABLE IF NOT EXISTS instruments (
    isin           TEXT PRIMARY KEY,
    company_name   TEXT NOT NULL,
    series         TEXT NOT NULL,              -- EQ, BE, BZ
    verified_on    DATE NOT NULL,              -- last seen in Shoonya's symbol file
    shoonya_symbol TEXT NOT NULL UNIQUE,       -- 'TCS-EQ'
    shoonya_token  TEXT NOT NULL UNIQUE        -- subscribed as NSE|<token>
);
-- Filled from https://api.shoonya.com/NSE_symbols.txt.zip (token, trading symbol, series) plus
-- the ISIN and company name of each monitored symbol (699 rows on 2026-11-02).

-- 2. Symbols point at their instrument.
ALTER TABLE stock_symbols ADD COLUMN IF NOT EXISTS isin TEXT REFERENCES instruments (isin);
CREATE INDEX IF NOT EXISTS idx_stock_symbols_isin ON stock_symbols (isin);

-- 3. Shoonya names: the Fyers 'NSE:' prefix went, and the large-cap detector label became shoonya.
UPDATE stock_symbols   SET symbol = substr(symbol, 5) WHERE symbol LIKE 'NSE:%';
UPDATE sector_mappings SET symbol = substr(symbol, 5) WHERE symbol LIKE 'NSE:%';
UPDATE stock_symbols   SET detector = 'shoonya' WHERE detector = 'fyers';

-- 4. Link every symbol to the instrument of the same name.
UPDATE stock_symbols s SET isin = i.isin
FROM instruments i
WHERE s.isin IS NULL AND i.shoonya_symbol = s.symbol;

-- 5. Sessions: shoonya_tokens replaced fyers_tokens (TokenManager creates it on start-up).
-- DROP TABLE IF EXISTS fyers_tokens;   -- optional clean-up, never run automatically
