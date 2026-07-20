select older.mandant, older.account_id, older.valid_from
from {{ ref('account') }} older
join {{ ref('account') }} newer
  on older.mandant = newer.mandant
 and older.account_id = newer.account_id
 and older.valid_from < newer.valid_from
 and coalesce(older.valid_to, timestamp '9999-12-31') >= newer.valid_from

