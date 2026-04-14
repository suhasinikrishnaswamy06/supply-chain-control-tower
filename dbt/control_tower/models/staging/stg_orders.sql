select
    cast(order_id as string) as order_id,
    cast(order_date as date) as order_date,
    cast(warehouse_id as string) as warehouse_id,
    cast(sku_id as string) as sku_id,
    cast(order_qty as int64) as order_qty,
    cast(promised_date as date) as promised_date
from {{ source('raw', 'orders') }}
