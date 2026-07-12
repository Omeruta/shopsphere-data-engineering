-- Carrier dimension and shipment fact tables, sourced from the SwiftDrop
-- Logistics API. delivery_address is a nested object in the API response;
-- flattened here since its fields are fixed and known. events is a nested
-- array; flattened into its own fact table, same pattern as
-- fact_customer_events.

CREATE TABLE IF NOT EXISTS analytics.dim_carrier (
    carrier_key BIGSERIAL PRIMARY KEY,
    carrier_id VARCHAR(40) NOT NULL UNIQUE,
    carrier_name VARCHAR(120) NOT NULL,
    service_level VARCHAR(40) NOT NULL,
    support_phone VARCHAR(40),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE analytics.dim_carrier IS
    'Grain: one row per carrier_id. Upserted on carrier_id. Source: SwiftDrop /api/v1/carriers.';

CREATE TABLE IF NOT EXISTS analytics.fact_shipments (
    shipment_key BIGSERIAL PRIMARY KEY,
    shipment_id VARCHAR(40) NOT NULL UNIQUE,
    order_key BIGINT REFERENCES analytics.fact_orders (order_key),
    carrier_key BIGINT REFERENCES analytics.dim_carrier (carrier_key),
    tracking_number VARCHAR(60) NOT NULL,
    shipment_status VARCHAR(30) NOT NULL,
    shipped_at TIMESTAMPTZ,
    estimated_delivery_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    delivery_street VARCHAR(200),
    delivery_city VARCHAR(80),
    delivery_state VARCHAR(80),
    delivery_country VARCHAR(80),
    delivery_postal_code VARCHAR(20),
    source_updated_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE analytics.fact_shipments IS
    'Grain: one row per shipment_id. Upserted on shipment_id. order_key is resolved by '
    'matching the source order_id against fact_orders.order_id at load time. Source: '
    'SwiftDrop /api/v1/shipments.';

CREATE TABLE IF NOT EXISTS analytics.fact_shipment_events (
    shipment_event_key BIGSERIAL PRIMARY KEY,
    shipment_id VARCHAR(40) NOT NULL,
    event_index INTEGER NOT NULL,          -- position of this event within the shipment's events array
    event_type VARCHAR(40) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    location VARCHAR(120),
    notes TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (shipment_id, event_index)
);
COMMENT ON TABLE analytics.fact_shipment_events IS
    'Grain: one row per event within a shipment (flattened from the events array). '
    'Deduplication key is (shipment_id, event_index), matching the same pattern used '
    'for fact_customer_events.';

CREATE INDEX IF NOT EXISTS idx_fact_shipments_order ON analytics.fact_shipments (order_key);
CREATE INDEX IF NOT EXISTS idx_fact_shipments_carrier ON analytics.fact_shipments (carrier_key);
CREATE INDEX IF NOT EXISTS idx_fact_shipments_status ON analytics.fact_shipments (shipment_status);
CREATE INDEX IF NOT EXISTS idx_fact_shipment_events_shipment ON analytics.fact_shipment_events (shipment_id);
