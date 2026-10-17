{{ config(materialized='view') }}

{% set source = var('delta_s3_path', none) or env_var('DELTA_S3_PATH') %}

select
    CAST(trip_id AS Nullable(UInt64)) as trip_id,
    CAST(source_month AS Nullable(Date)) as source_month,
    CAST(source_row_number AS Nullable(UInt64)) as source_row_number,
    CAST(pickup_date AS Nullable(Date)) as pickup_date,
    CAST(vendor_id AS Nullable(UInt16)) as vendor_id,
    CAST(pickup_at AS Nullable(DateTime64(6, 'America/New_York'))) as pickup_at,
    CAST(dropoff_at AS Nullable(DateTime64(6, 'America/New_York'))) as dropoff_at,
    CAST(passenger_count AS Nullable(UInt16)) as passenger_count,
    CAST(trip_distance AS Nullable(Float64)) as trip_distance,
    CAST(rate_code_id AS Nullable(UInt16)) as rate_code_id,
    CAST(store_and_fwd_flag AS Nullable(String)) as store_and_fwd_flag,
    CAST(pickup_location_id AS Nullable(UInt16)) as pickup_location_id,
    CAST(dropoff_location_id AS Nullable(UInt16)) as dropoff_location_id,
    CAST(payment_type AS Nullable(UInt16)) as payment_type,
    CAST(fare_amount AS Nullable(Decimal(18, 2))) as fare_amount,
    CAST(extra AS Nullable(Decimal(18, 2))) as extra,
    CAST(mta_tax AS Nullable(Decimal(18, 2))) as mta_tax,
    CAST(tip_amount AS Nullable(Decimal(18, 2))) as tip_amount,
    CAST(tolls_amount AS Nullable(Decimal(18, 2))) as tolls_amount,
    CAST(improvement_surcharge AS Nullable(Decimal(18, 2))) as improvement_surcharge,
    CAST(total_amount AS Nullable(Decimal(18, 2))) as total_amount,
    CAST(congestion_surcharge AS Nullable(Decimal(18, 2))) as congestion_surcharge,
    CAST(airport_fee AS Nullable(Decimal(18, 2))) as airport_fee,
    CAST(cbd_congestion_fee AS Nullable(Decimal(18, 2))) as cbd_congestion_fee
from deltaLake(
    '{{ source | replace("'", "''") }}',
    '{{ env_var("AWS_ACCESS_KEY_ID", "") }}',
    '{{ env_var("AWS_SECRET_ACCESS_KEY", "") }}'
)
where pickup_date >= toDate('{{ var("start_date", "2025-01-01") }}')
  and pickup_date < toDate('{{ var("end_date", "2025-02-01") }}')
