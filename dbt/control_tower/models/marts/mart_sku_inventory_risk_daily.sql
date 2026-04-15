with inventory as (
    select * from {{ ref('stg_inventory_snapshots') }}
),
skus as (
    select * from {{ ref('dim_sku') }}
),
orders as (
    select
        warehouse_id,
        sku_id,
        sum(order_qty) as ordered_units
    from {{ ref('stg_orders') }}
    group by warehouse_id, sku_id
)
select
    i.snapshot_date as metric_date,
    i.warehouse_id,
    i.sku_id,
    s.sku_name,
    s.product_family,
    i.on_hand_qty,
    i.reorder_point,
    coalesce(o.ordered_units, 0) as ordered_units,
    greatest(coalesce(o.ordered_units, 0) - i.on_hand_qty, 0) as demand_gap_units,
    case when i.on_hand_qty <= i.reorder_point then 1 else 0 end as is_inventory_at_risk,
    round(safe_divide(i.on_hand_qty, nullif(i.reorder_point, 0)), 4) as stock_cover_ratio
from inventory i
left join skus s
    on i.sku_id = s.sku_id
left join orders o
    on i.warehouse_id = o.warehouse_id
    and i.sku_id = o.sku_id
