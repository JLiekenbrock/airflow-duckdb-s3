{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='(tenant_id, event_date, event_id)',
        partition_by='toYYYYMM(event_date)',
        unique_key=['tenant_id', 'event_id'],
        incremental_strategy='delete+insert',
    )
}}

select
    assumeNotNull(tenant_id) as tenant_id,
    assumeNotNull(event_id) as event_id,
    assumeNotNull(event_date) as event_date,
    event_type,
    user_id
from {{ ref('stg_delta_events') }}
