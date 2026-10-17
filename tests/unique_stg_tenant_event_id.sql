select tenant_id, event_id
from {{ ref('stg_delta_events') }}
group by tenant_id, event_id
having count() > 1
