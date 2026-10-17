{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='MergeTree()',
    order_by='(pickup_date, trip_id)',
    partition_by='toYYYYMM(pickup_date)',
    unique_key='trip_id'
) }}

select * replace (
    assumeNotNull(trip_id) as trip_id,
    assumeNotNull(source_month) as source_month,
    assumeNotNull(source_row_number) as source_row_number,
    assumeNotNull(pickup_date) as pickup_date,
    assumeNotNull(pickup_at) as pickup_at
)
from {{ ref('stg_nyc_taxi_trips') }}
