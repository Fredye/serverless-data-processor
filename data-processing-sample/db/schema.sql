-- Schema for the data-processing-sample PostgreSQL persistence layer.
-- Create the table before deploying the Lambda, e.g.:
--   psql "$DATABASE_URL" -f db/schema.sql

CREATE TABLE IF NOT EXISTS processed_transactions (
    event_id       TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    line_id        TEXT NOT NULL,
    store_id       TEXT NOT NULL,
    quantity       NUMERIC NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC NOT NULL,
    gross_amount   NUMERIC NOT NULL,
    currency_code  TEXT NOT NULL,
    source_system  TEXT NOT NULL,
    trace_id       TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_processed_transactions_store_id
    ON processed_transactions (store_id);
