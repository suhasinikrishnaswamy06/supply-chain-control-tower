with shipments as (
    select * from {{ ref('stg_shipments') }}
)
select
    current_date() as metric_date,
    warehouse_id,
    count(*) as shipment_records,
    countif(actual_delivery_date is not null) as delivered_shipments,
    countif(shipped_qty > 0) as shipped_orders,
    countif(actual_delivery_date is not null and actual_delivery_date <= promised_date) as on_time_shipments,
    countif(actual_delivery_date is not null and actual_delivery_date > promised_date) as late_shipments,
    countif(actual_delivery_date is not null and shipped_qty > 0) as in_full_shipments,
    case
        when countif(actual_delivery_date is not null) = 0 then 0
        else round(
            countif(actual_delivery_date is not null and actual_delivery_date <= promised_date)
            / countif(actual_delivery_date is not null),
            4
        )
    end as on_time_rate,
    case
        when count(*) = 0 then 0
        else round(countif(shipped_qty > 0) / count(*), 4)
    end as in_full_rate,
    case
        when countif(actual_delivery_date is not null) = 0 then 0
        else round(
            countif(
                actual_delivery_date is not null
                and actual_delivery_date <= promised_date
                and shipped_qty > 0
            ) / countif(actual_delivery_date is not null),
            4
        )
    end as otif_rate
from shipments
group by warehouse_id
