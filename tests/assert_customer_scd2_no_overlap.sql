select older.mandant, older.customer_id, older.valid_from
from {{ ref('customer') }} older
join {{ ref('customer') }} newer
  on older.mandant = newer.mandant
 and older.customer_id = newer.customer_id
 and older.valid_from < newer.valid_from
 and coalesce(older.valid_to, timestamp '9999-12-31') >= newer.valid_from

