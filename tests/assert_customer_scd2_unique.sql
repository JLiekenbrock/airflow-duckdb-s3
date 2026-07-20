select mandant, customer_id, valid_from, count(*) as version_count
from {{ ref('customer') }}
group by 1, 2, 3
having count(*) > 1

