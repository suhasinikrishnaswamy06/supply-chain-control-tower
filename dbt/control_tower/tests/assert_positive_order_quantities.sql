select *
from {{ ref('stg_orders') }}
where order_qty <= 0
