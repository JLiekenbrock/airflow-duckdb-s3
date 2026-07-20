select mandant, account_id, valid_from, count(*) as version_count
from {{ ref('account') }}
group by 1, 2, 3
having count(*) > 1

