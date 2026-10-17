{{ config(
    materialized='incremental',
    alias=var('case', 'delete_insert'),
    incremental_strategy=var('strategy', 'delete+insert'),
    engine='MergeTree()',
    order_by='(pickup_date, trip_id)',
    partition_by='toYYYYMM(pickup_date)',
    unique_key=(none if var('strategy', 'delete+insert') == 'insert_overwrite' else 'trip_id')
) }}

select * replace (
    assumeNotNull(trip_id) as trip_id,
    assumeNotNull(source_month) as source_month,
    assumeNotNull(source_row_number) as source_row_number,
    assumeNotNull(pickup_date) as pickup_date,
    assumeNotNull(pickup_at) as pickup_at,
    total_amount + toDecimal64({{ var('revision', 0) }}, 2) as total_amount
)
from {{ ref('stg_nyc_taxi_trips') }}
where pickup_date >= toDate('{{ var("start_date", "2025-01-01") }}')
  and pickup_date < toDate('{{ var("end_date", "2025-02-01") }}')
