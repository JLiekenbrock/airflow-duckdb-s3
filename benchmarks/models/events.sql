{{ config(
    materialized='incremental',
    alias=var('case', 'delete_insert'),
    incremental_strategy=var('strategy', 'delete+insert'),
    engine='MergeTree()',
    order_by='(tenant_id, event_date, event_id)',
    partition_by='toYYYYMM(event_date)',
    unique_key=(none if var('strategy', 'delete+insert') == 'insert_overwrite' else ['tenant_id', 'event_id'])
) }}

select
    assumeNotNull(toUInt64(tenant_id)) as tenant_id,
    assumeNotNull(toUInt64(event_id)) as event_id,
    assumeNotNull(toDate(event_date)) as event_date,
    assumeNotNull(toLowCardinality(toString(event_type))) as event_type,
    assumeNotNull(toUInt64(user_id)) + toUInt64({{ var('revision', 0) }}) as user_id
from deltaLake(
    '{{ env_var("BENCHMARK_SOURCE") | replace("'", "''") }}',
    '{{ env_var("AWS_ACCESS_KEY_ID") }}',
    '{{ env_var("AWS_SECRET_ACCESS_KEY") }}'
)
where event_date >= toDate('{{ var("start_date", "2025-01-01") }}')
  and event_date < toDate('{{ var("end_date", "2025-02-01") }}')
