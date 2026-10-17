select trip_id
from {{ ref('stg_nyc_taxi_trips') }}
where pickup_date != toDate(pickup_at)
   or toStartOfMonth(pickup_date) != source_month
