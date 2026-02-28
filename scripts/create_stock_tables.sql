-- Stock module tables
-- Run: psql -h 192.168.128.9 -U finance -d finance -f scripts/create_stock_tables.sql

BEGIN;

-- 1. Holdings (ticker-level)
CREATE TABLE public.stock_holding (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    symbol text NOT NULL,
    name text NOT NULL,
    country char(2) DEFAULT 'US' NOT NULL,
    currency char(3) DEFAULT 'USD' NOT NULL,
    scope text DEFAULT 'personal' NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT stock_holding_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX uq_stock_holding_symbol ON public.stock_holding USING btree (symbol);
ALTER TABLE public.stock_holding OWNER TO finance;

-- 2. Trades (immutable buy/sell records)
CREATE TABLE public.stock_trade (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    holding_id uuid NOT NULL,
    trade_type text NOT NULL,
    trade_date date NOT NULL,
    quantity numeric(18,6) NOT NULL,
    price_per_share numeric(18,4) NOT NULL,
    total_cost numeric(18,4) NOT NULL,
    fees numeric(18,4) DEFAULT 0 NOT NULL,
    currency char(3) NOT NULL,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT stock_trade_pkey PRIMARY KEY (id),
    CONSTRAINT stock_trade_holding_fk FOREIGN KEY (holding_id) REFERENCES public.stock_holding(id),
    CONSTRAINT stock_trade_type_check CHECK (trade_type IN ('buy', 'sell'))
);
CREATE INDEX idx_stock_trade_holding ON public.stock_trade USING btree (holding_id);
CREATE INDEX idx_stock_trade_date ON public.stock_trade USING btree (trade_date);
ALTER TABLE public.stock_trade OWNER TO finance;

-- 3. Dividends (schema only — API/UI deferred)
CREATE TABLE public.stock_dividend (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    holding_id uuid NOT NULL,
    ex_date date NOT NULL,
    pay_date date,
    amount_per_share numeric(18,6) NOT NULL,
    total_amount numeric(18,4) NOT NULL,
    currency char(3) NOT NULL,
    withholding_tax numeric(18,4) DEFAULT 0 NOT NULL,
    withholding_rate numeric(5,4) DEFAULT 0 NOT NULL,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT stock_dividend_pkey PRIMARY KEY (id),
    CONSTRAINT stock_dividend_holding_fk FOREIGN KEY (holding_id) REFERENCES public.stock_holding(id)
);
CREATE INDEX idx_stock_dividend_holding ON public.stock_dividend USING btree (holding_id);
CREATE INDEX idx_stock_dividend_ex_date ON public.stock_dividend USING btree (ex_date);
ALTER TABLE public.stock_dividend OWNER TO finance;

-- 4. Cached daily close prices
CREATE TABLE public.stock_price (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    holding_id uuid NOT NULL,
    price_date date NOT NULL,
    close_price numeric(18,4) NOT NULL,
    currency char(3) NOT NULL,
    source text DEFAULT 'yahoo' NOT NULL,
    fetched_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT stock_price_pkey PRIMARY KEY (id),
    CONSTRAINT stock_price_holding_fk FOREIGN KEY (holding_id) REFERENCES public.stock_holding(id)
);
CREATE UNIQUE INDEX uq_stock_price_holding_date ON public.stock_price USING btree (holding_id, price_date);
CREATE INDEX idx_stock_price_date ON public.stock_price USING btree (price_date DESC);
ALTER TABLE public.stock_price OWNER TO finance;

-- 5. Tax year income (shared with future UK tax module)
CREATE TABLE public.tax_year_income (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tax_year text NOT NULL,
    gross_income numeric(18,4) NOT NULL,
    personal_allowance numeric(18,4) DEFAULT 12570 NOT NULL,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tax_year_income_pkey PRIMARY KEY (id),
    CONSTRAINT tax_year_income_year_check CHECK (tax_year ~ '^\d{4}/\d{2}$')
);
CREATE UNIQUE INDEX uq_tax_year_income_year ON public.tax_year_income USING btree (tax_year);
ALTER TABLE public.tax_year_income OWNER TO finance;

COMMIT;
