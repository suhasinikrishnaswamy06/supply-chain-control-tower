with warehouse_ids as (
    select distinct warehouse_id from {{ ref('stg_orders') }}
    union distinct
    select distinct warehouse_id from {{ ref('stg_shipments') }}
    union distinct
    select distinct warehouse_id from {{ ref('stg_inventory_snapshots') }}
    union distinct
    select distinct warehouse_id from {{ ref('stg_warehouse_events') }}
)
select
    warehouse_id,
    split(warehouse_id, '_')[safe_offset(1)] as city_code,
    split(warehouse_id, '_')[safe_offset(2)] as site_number,
    case split(warehouse_id, '_')[safe_offset(1)]
        when 'TOR' then 'Toronto'
        when 'DAL' then 'Dallas'
        when 'ATL' then 'Atlanta'
        else split(warehouse_id, '_')[safe_offset(1)]
    end as warehouse_city,
    case split(warehouse_id, '_')[safe_offset(1)]
        when 'TOR' then 'CA'
        else 'US'
    end as country_code,
    case split(warehouse_id, '_')[safe_offset(1)]
        when 'TOR' then 'Canada'
        else 'United States'
    end as country_name
from warehouse_ids
