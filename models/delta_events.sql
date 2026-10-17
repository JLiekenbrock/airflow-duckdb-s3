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
    tenant_id,
    event_id,
    event_date,
    event_type,
    user_id
from {{ ref('stg_delta_events') }}
