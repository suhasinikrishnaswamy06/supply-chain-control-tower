with inventory as (
    select * from {{ ref('stg_inventory_snapshots') }}
)
select
    snapshot_date as metric_date,
    warehouse_id,
    sum(on_hand_qty) as total_on_hand_units,
    sum(case when on_hand_qty <= reorder_point then on_hand_qty else 0 end) as inventory_at_risk_units,
    count(*) as sku_count,
    countif(on_hand_qty <= reorder_point) as inventory_below_reorder_sku_count,
    case
        when count(*) = 0 then 0
        else round(countif(on_hand_qty <= reorder_point) / count(*), 4)
    end as inventory_risk_rate
from inventory
group by snapshot_date, warehouse_id
