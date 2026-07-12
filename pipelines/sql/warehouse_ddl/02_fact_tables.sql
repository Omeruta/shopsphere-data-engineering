-- Fact tables. Each references dimension surrogate keys, not natural keys.
-- Every table has a UNIQUE constraint on its natural key so loads can
-- upsert (ON CONFLICT DO UPDATE) and reruns never create duplicates.

CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_key BIGSERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL UNIQUE,
    customer_key BIGINT REFERENCES analytics.dim_customer (customer_key),
    order_date_key INTEGER REFERENCES analytics.dim_date (date_key),
    order_status VARCHAR(30) NOT NULL,
    currency CHAR(3) NOT NULL,
    subtotal NUMERIC(12, 2) NOT NULL,
    shipping_fee NUMERIC(12, 2) NOT NULL,
    discount_amount NUMERIC(12, 2) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE analytics.fact_orders IS
    'Grain: one row per order_id. Upserted on order_id. Source: postgres.orders.';

CREATE TABLE IF NOT EXISTS analytics.fact_order_items (
    order_item_key BIGSERIAL PRIMARY KEY,
    order_item_id INTEGER NOT NULL UNIQUE,
    order_key BIGINT REFERENCES analytics.fact_orders (order_key),
    product_key BIGINT REFERENCES analytics.dim_product (product_key),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    discount_amount NUMERIC(12, 2) NOT NULL,
    line_total NUMERIC(12, 2) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE analytics.fact_order_items IS
    'Grain: one row per order_item_id (one line item within one order). Source: postgres.order_items.';

CREATE TABLE IF NOT EXISTS analytics.fact_payments (
    payment_key BIGSERIAL PRIMARY KEY,
    payment_id INTEGER NOT NULL UNIQUE,
    order_key BIGINT REFERENCES analytics.fact_orders (order_key),
    payment_method VARCHAR(40) NOT NULL,
    payment_status VARCHAR(30) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    transaction_reference VARCHAR(80) NOT NULL,
    paid_date_key INTEGER REFERENCES analytics.dim_date (date_key),
    paid_at TIMESTAMPTZ,
    source_updated_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE analytics.fact_payments IS
    'Grain: one row per payment_id (one payment attempt on one order). Source: postgres.payments.';

CREATE TABLE IF NOT EXISTS analytics.fact_customer_events (
    event_key BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(40) NOT NULL,
    event_index INTEGER NOT NULL,          -- position of this event within its session's events array
    customer_key BIGINT REFERENCES analytics.dim_customer (customer_key),  -- nullable: some sessions are anonymous
    event_type VARCHAR(40) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    product_key BIGINT REFERENCES analytics.dim_product (product_key),    -- nullable: not every event references a product
    search_term VARCHAR(160),
    quantity INTEGER,
    page_url VARCHAR(200),
    browser VARCHAR(40),
    device_type VARCHAR(20),
    device_os VARCHAR(20),
    location_city VARCHAR(80),
    location_state VARCHAR(80),
    location_country VARCHAR(80),
    session_started_at TIMESTAMPTZ NOT NULL,
    session_ended_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, event_index)
);
COMMENT ON TABLE analytics.fact_customer_events IS
    'Grain: one row per event within a session (flattened from the events array). '
    'Deduplication key is (session_id, event_index) rather than event_time, because '
    'source data can contain duplicate event_time values within the same session.';

CREATE TABLE IF NOT EXISTS analytics.fact_product_reviews (
    review_key BIGSERIAL PRIMARY KEY,
    review_id VARCHAR(40) NOT NULL UNIQUE,
    product_key BIGINT REFERENCES analytics.dim_product (product_key),   -- nullable: source has orphan product_ids
    customer_key BIGINT REFERENCES analytics.dim_customer (customer_key),
    rating SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    title VARCHAR(200),
    review_text TEXT,
    verified_purchase BOOLEAN NOT NULL,
    review_date_key INTEGER REFERENCES analytics.dim_date (date_key),
    created_at TIMESTAMPTZ NOT NULL,
    helpful_votes INTEGER NOT NULL DEFAULT 0,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE analytics.fact_product_reviews IS
    'Grain: one row per review_id. Rows with rating outside 1-5 are rejected by the '
    'transform layer before load (see data quality checks), not just by this CHECK constraint.';

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON analytics.fact_orders (customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON analytics.fact_orders (order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_order ON analytics.fact_order_items (order_key);
CREATE INDEX IF NOT EXISTS idx_fact_payments_order ON analytics.fact_payments (order_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer_events_session ON analytics.fact_customer_events (session_id);
CREATE INDEX IF NOT EXISTS idx_fact_customer_events_customer ON analytics.fact_customer_events (customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_product_reviews_product ON analytics.fact_product_reviews (product_key);
