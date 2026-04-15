with otif as (
    select * from {{ ref('fct_otif_daily') }}
),
backlog as (
    select * from {{ ref('fct_backlog_daily') }}
),
inventory_risk as (
    select * from {{ ref('fct_inventory_risk_daily') }}
),
warehouse_dim as (
    select * from {{ ref('dim_warehouse') }}
),
warehouse_events as (
    select
        current_date() as metric_date,
        warehouse_id,
        countif(actual_pick_delay_minutes > pick_sla_minutes) as warehouse_pick_delay_events,
        avg(actual_pick_delay_minutes) as avg_pick_delay_minutes
    from {{ ref('stg_warehouse_events') }}
    group by warehouse_id
)
select
    coalesce(o.metric_date, b.metric_date, i.metric_date, w.metric_date) as metric_date,
    coalesce(o.warehouse_id, b.warehouse_id, i.warehouse_id, w.warehouse_id) as warehouse_id,
    wd.warehouse_city,
    wd.country_code,
    wd.country_name,
    coalesce(o.delivered_shipments, 0) as delivered_shipments,
    coalesce(o.on_time_shipments, 0) as on_time_shipments,
    coalesce(o.late_shipments, 0) as late_shipments,
    coalesce(o.otif_rate, 0) as otif_rate,
    coalesce(b.backlog_orders, 0) as backlog_orders,
    coalesce(b.backlog_units, 0) as backlog_units,
    coalesce(b.backlog_rate, 0) as backlog_rate,
    coalesce(i.total_on_hand_units, 0) as total_on_hand_units,
    coalesce(i.inventory_at_risk_units, 0) as inventory_at_risk_units,
    coalesce(i.inventory_risk_rate, 0) as inventory_risk_rate,
    coalesce(w.warehouse_pick_delay_events, 0) as warehouse_pick_delay_events,
    round(coalesce(w.avg_pick_delay_minutes, 0), 2) as avg_pick_delay_minutes
from otif o
full outer join backlog b
    on o.metric_date = b.metric_date
    and o.warehouse_id = b.warehouse_id
full outer join inventory_risk i
    on coalesce(o.metric_date, b.metric_date) = i.metric_date
    and coalesce(o.warehouse_id, b.warehouse_id) = i.warehouse_id
full outer join warehouse_events w
    on coalesce(o.metric_date, b.metric_date, i.metric_date) = w.metric_date
    and coalesce(o.warehouse_id, b.warehouse_id, i.warehouse_id) = w.warehouse_id
left join warehouse_dim wd
    on coalesce(o.warehouse_id, b.warehouse_id, i.warehouse_id, w.warehouse_id) = wd.warehouse_id
