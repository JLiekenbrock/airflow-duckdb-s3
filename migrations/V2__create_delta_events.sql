CREATE TABLE IF NOT EXISTS analytics.delta_events
(
    tenant_id UInt64 DEFAULT 0,
    event_id UInt64,
    event_date Date,
    event_type LowCardinality(String),
    user_id UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, event_date, event_id);

ALTER TABLE analytics.delta_events
    ADD COLUMN IF NOT EXISTS tenant_id UInt64 DEFAULT 0 FIRST;
