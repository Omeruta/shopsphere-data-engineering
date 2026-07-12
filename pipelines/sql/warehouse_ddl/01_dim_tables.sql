-- Dimension tables. Surrogate keys (BIGSERIAL) are the primary keys used
-- by fact tables; natural keys from source systems are kept as UNIQUE
-- columns so upserts can match on them during reruns.

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL UNIQUE,
    first_name VARCHAR(80) NOT NULL,
    last_name VARCHAR(80) NOT NULL,
    email VARCHAR(160) NOT NULL,
    phone VARCHAR(40),
    city VARCHAR(80) NOT NULL,
    state VARCHAR(80) NOT NULL,
    country VARCHAR(80) NOT NULL,
    source_created_at TIMESTAMPTZ NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE analytics.dim_customer IS
    'Grain: one row per customer_id. Upserted on customer_id. Source: postgres.customers.';

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key BIGSERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL UNIQUE,
    product_name VARCHAR(160) NOT NULL,
    category VARCHAR(80) NOT NULL,
    brand VARCHAR(80),
    unit_price NUMERIC(12, 2) NOT NULL,
    cost_price NUMERIC(12, 2) NOT NULL,
    stock_quantity INTEGER NOT NULL,
    source_created_at TIMESTAMPTZ NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE analytics.dim_product IS
    'Grain: one row per product_id. Upserted on product_id. Source: postgres.products.';

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INTEGER PRIMARY KEY,          -- YYYYMMDD, e.g. 20260711
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,          -- 1=Monday .. 7=Sunday (ISO)
    day_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
COMMENT ON TABLE analytics.dim_date IS
    'Grain: one row per calendar date. Statically populated, not upserted by the pipeline.';

-- Populate dim_date once, covering all source data (2025) plus surrounding
-- years for headroom. Safe to re-run: ON CONFLICT DO NOTHING skips dates
-- that already exist instead of erroring.
INSERT INTO analytics.dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day, day_of_week, day_name, week_of_year, is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    TO_CHAR(d, 'FMMonth'),
    EXTRACT(DAY FROM d)::INTEGER,
    EXTRACT(ISODOW FROM d)::INTEGER,
    TO_CHAR(d, 'FMDay'),
    EXTRACT(WEEK FROM d)::INTEGER,
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2024-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;
