select *
from {{ ref('stg_shipments') }}
where actual_delivery_date is not null
  and actual_delivery_date < ship_date
