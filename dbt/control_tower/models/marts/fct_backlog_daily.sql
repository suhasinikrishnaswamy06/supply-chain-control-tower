with orders as (
    select * from {{ ref('stg_orders') }}
),
shipments as (
    select * from {{ ref('stg_shipments') }}
),
order_backlog as (
    select
        o.order_id,
        o.warehouse_id,
        o.order_qty,
        coalesce(s.shipped_qty, 0) as shipped_qty,
        greatest(o.order_qty - coalesce(s.shipped_qty, 0), 0) as backlog_units
    from orders o
    left join shipments s
        on o.order_id = s.order_id
)
select
    current_date() as metric_date,
    warehouse_id,
    countif(backlog_units > 0) as backlog_orders,
    sum(backlog_units) as backlog_units,
    sum(order_qty) as total_order_units,
    case
        when sum(order_qty) = 0 then 0
        else round(sum(backlog_units) / sum(order_qty), 4)
    end as backlog_rate
from order_backlog
group by warehouse_id
