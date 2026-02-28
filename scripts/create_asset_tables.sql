-- Other Assets: manual valuations for non-stock assets (property, vehicles, etc.)

CREATE TABLE IF NOT EXISTS asset_holding (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    asset_type TEXT NOT NULL DEFAULT 'other',
    currency CHAR(3) NOT NULL DEFAULT 'GBP',
    scope TEXT NOT NULL DEFAULT 'personal',
    is_active BOOLEAN NOT NULL DEFAULT true,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS asset_valuation (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    holding_id UUID NOT NULL REFERENCES asset_holding(id),
    valuation_date DATE NOT NULL,
    gross_value NUMERIC(18,4) NOT NULL,
    tax_payable NUMERIC(18,4) NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_asset_valuation_holding ON asset_valuation(holding_id);
CREATE INDEX IF NOT EXISTS idx_asset_valuation_date ON asset_valuation(valuation_date);
