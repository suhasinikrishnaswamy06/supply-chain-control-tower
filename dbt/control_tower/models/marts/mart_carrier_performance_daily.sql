with shipments as (
    select * from {{ ref('stg_shipments') }}
),
carriers as (
    select * from {{ ref('dim_carrier') }}
)
select
    current_date() as metric_date,
    s.carrier_name,
    c.carrier_mode,
    c.carrier_segment,
    count(*) as shipment_records,
    countif(actual_delivery_date is not null) as delivered_shipments,
    countif(actual_delivery_date is not null and actual_delivery_date <= promised_date) as on_time_shipments,
    countif(actual_delivery_date is not null and actual_delivery_date > promised_date) as late_shipments,
    sum(shipped_qty) as shipped_units,
    case
        when countif(actual_delivery_date is not null) = 0 then 0
        else round(
            countif(actual_delivery_date is not null and actual_delivery_date <= promised_date)
            / countif(actual_delivery_date is not null),
            4
        )
    end as on_time_rate
from shipments s
left join carriers c
    on s.carrier_name = c.carrier_name
group by s.carrier_name, c.carrier_mode, c.carrier_segment
