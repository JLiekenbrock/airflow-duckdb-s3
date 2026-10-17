CREATE TABLE IF NOT EXISTS analytics.nyc_taxi_trips
(
    trip_id UInt64,
    source_month Date,
    source_row_number UInt64,
    pickup_date Date,
    vendor_id Nullable(UInt16),
    pickup_at DateTime64(6, 'America/New_York'),
    dropoff_at Nullable(DateTime64(6, 'America/New_York')),
    passenger_count Nullable(UInt16),
    trip_distance Nullable(Float64),
    rate_code_id Nullable(UInt16),
    store_and_fwd_flag Nullable(String),
    pickup_location_id Nullable(UInt16),
    dropoff_location_id Nullable(UInt16),
    payment_type Nullable(UInt16),
    fare_amount Nullable(Decimal(18, 2)),
    extra Nullable(Decimal(18, 2)),
    mta_tax Nullable(Decimal(18, 2)),
    tip_amount Nullable(Decimal(18, 2)),
    tolls_amount Nullable(Decimal(18, 2)),
    improvement_surcharge Nullable(Decimal(18, 2)),
    total_amount Nullable(Decimal(18, 2)),
    congestion_surcharge Nullable(Decimal(18, 2)),
    airport_fee Nullable(Decimal(18, 2)),
    cbd_congestion_fee Nullable(Decimal(18, 2))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_date)
ORDER BY (pickup_date, trip_id);

DROP VIEW IF EXISTS analytics.stg_delta_events;
DROP TABLE IF EXISTS analytics.delta_events;
