{{ config(materialized='view', alias='stg_delta_events_' ~ var('batch_id', 'default')) }}

{% set start_date = var('start_date', '1970-01-01') %}
{% set end_date = var('end_date', '9999-12-31') %}
{% set tenant_ids = var('tenant_ids', range(1, 801) | list) %}

select
    toUInt64(tenant_id) as tenant_id,
    toUInt64(event_id) as event_id,
    toDate(event_date) as event_date,
    toLowCardinality(toString(event_type)) as event_type,
    toUInt64(user_id) as user_id
from deltaLake(
    '{{ env_var("DELTA_S3_PATH") }}',
    '{{ env_var("AWS_ACCESS_KEY_ID", "") }}',
    '{{ env_var("AWS_SECRET_ACCESS_KEY", "") }}'
)
where event_date >= toDate('{{ start_date }}')
  and event_date < toDate('{{ end_date }}')
  and tenant_id in ({{ tenant_ids | join(', ') }})
