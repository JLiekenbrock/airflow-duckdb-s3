{{ config(order_by='(event_date, event_id)') }}

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
