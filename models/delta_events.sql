{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='(event_date, event_id)',
        partition_by='toYYYYMM(event_date)',
        unique_key='event_id',
        incremental_strategy='delete+insert',
    )
}}

{% set start_date = var('start_date', '1970-01-01') %}
{% set end_date = var('end_date', '9999-12-31') %}

select
    event_id,
    event_date,
    event_type,
    user_id
from deltaLake(
    '{{ env_var("DELTA_S3_PATH") }}',
    '{{ env_var("AWS_ACCESS_KEY_ID", "") }}',
    '{{ env_var("AWS_SECRET_ACCESS_KEY", "") }}'
)
where event_date >= toDate('{{ start_date }}')
  and event_date < toDate('{{ end_date }}')
