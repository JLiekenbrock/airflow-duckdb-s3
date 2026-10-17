select tenant_id, event_id
from {{ ref('delta_events') }}
group by tenant_id, event_id
having count() > 1
