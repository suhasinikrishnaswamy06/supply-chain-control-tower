select
    cast(snapshot_date as date) as snapshot_date,
    cast(warehouse_id as string) as warehouse_id,
    cast(sku_id as string) as sku_id,
    cast(on_hand_qty as int64) as on_hand_qty,
    cast(reorder_point as int64) as reorder_point
from {{ source('raw', 'inventory_snapshots') }}
