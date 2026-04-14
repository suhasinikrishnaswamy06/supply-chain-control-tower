select
    cast(shipment_id as string) as shipment_id,
    cast(order_id as string) as order_id,
    cast(carrier_name as string) as carrier_name,
    cast(warehouse_id as string) as warehouse_id,
    cast(shipment_status as string) as shipment_status,
    cast(ship_date as date) as ship_date,
    cast(actual_delivery_date as date) as actual_delivery_date,
    cast(shipped_qty as int64) as shipped_qty,
    cast(promised_date as date) as promised_date
from {{ source('raw', 'shipments') }}
