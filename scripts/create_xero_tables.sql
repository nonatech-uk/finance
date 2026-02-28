-- Xero integration tables

-- Maps finance category paths to Xero account codes
CREATE TABLE IF NOT EXISTS xero_account_mapping (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category_path TEXT NOT NULL UNIQUE,
    xero_account_code TEXT NOT NULL,
    xero_account_name TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Tracks which transactions have been pushed to Xero
CREATE TABLE IF NOT EXISTS xero_sync_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_transaction_id UUID NOT NULL REFERENCES raw_transaction(id),
    xero_transaction_id TEXT NOT NULL,
    synced_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (raw_transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_xero_sync_log_txn ON xero_sync_log(raw_transaction_id);
