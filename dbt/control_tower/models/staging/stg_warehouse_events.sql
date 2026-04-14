select
    cast(event_id as string) as event_id,
    cast(order_id as string) as order_id,
    cast(warehouse_id as string) as warehouse_id,
    cast(event_type as string) as event_type,
    cast(event_ts as timestamp) as event_ts,
    cast(pick_sla_minutes as int64) as pick_sla_minutes,
    cast(actual_pick_delay_minutes as int64) as actual_pick_delay_minutes
from {{ source('raw', 'warehouse_events') }}
