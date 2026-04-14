with shipments as (
    select * from {{ ref('stg_shipments') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
inventory as (
    select * from {{ ref('stg_inventory_snapshots') }}
),
warehouse_events as (
    select * from {{ ref('stg_warehouse_events') }}
),
shipment_metrics as (
    select
        warehouse_id,
        countif(actual_delivery_date is not null) as delivered_shipments,
        countif(actual_delivery_date is not null and actual_delivery_date <= promised_date) as on_time_shipments,
        countif(actual_delivery_date is not null and actual_delivery_date > promised_date) as late_shipments
    from shipments
    group by warehouse_id
),
backlog_metrics as (
    select
        o.warehouse_id,
        sum(greatest(o.order_qty - coalesce(s.shipped_qty, 0), 0)) as backlog_units
    from orders o
    left join shipments s
        on o.order_id = s.order_id
    group by o.warehouse_id
),
inventory_metrics as (
    select
        warehouse_id,
        sum(case when on_hand_qty <= reorder_point then on_hand_qty else 0 end) as inventory_at_risk_units,
        countif(on_hand_qty <= reorder_point) as inventory_below_reorder_sku_count
    from inventory
    group by warehouse_id
),
warehouse_metrics as (
    select
        warehouse_id,
        countif(actual_pick_delay_minutes > pick_sla_minutes) as warehouse_pick_delay_events
    from warehouse_events
    group by warehouse_id
)
select
    current_date() as metric_date,
    coalesce(sm.warehouse_id, bm.warehouse_id, im.warehouse_id, wm.warehouse_id) as warehouse_id,
    coalesce(sm.delivered_shipments, 0) as delivered_shipments,
    coalesce(sm.on_time_shipments, 0) as on_time_shipments,
    coalesce(sm.late_shipments, 0) as late_shipments,
    case
        when coalesce(sm.delivered_shipments, 0) = 0 then 0
        else round(sm.late_shipments / sm.delivered_shipments, 4)
    end as late_shipment_rate,
    coalesce(bm.backlog_units, 0) as backlog_units,
    coalesce(im.inventory_at_risk_units, 0) as inventory_at_risk_units,
    coalesce(im.inventory_below_reorder_sku_count, 0) as inventory_below_reorder_sku_count,
    coalesce(wm.warehouse_pick_delay_events, 0) as warehouse_pick_delay_events
from shipment_metrics sm
full outer join backlog_metrics bm
    on sm.warehouse_id = bm.warehouse_id
full outer join inventory_metrics im
    on coalesce(sm.warehouse_id, bm.warehouse_id) = im.warehouse_id
full outer join warehouse_metrics wm
    on coalesce(sm.warehouse_id, bm.warehouse_id, im.warehouse_id) = wm.warehouse_id
