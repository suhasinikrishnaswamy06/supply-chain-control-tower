select *
from {{ ref('stg_shipments') }}
where shipped_qty < 0
