with sku_ids as (
    select distinct sku_id from {{ ref('stg_orders') }}
    union distinct
    select distinct sku_id from {{ ref('stg_inventory_snapshots') }}
)
select
    sku_id,
    split(sku_id, '_')[safe_offset(1)] as sku_category_code,
    split(sku_id, '_')[safe_offset(2)] as sku_variant_code,
    case split(sku_id, '_')[safe_offset(1)]
        when 'DRILL' then 'Power Drill'
        when 'SAW' then 'Circular Saw'
        when 'BATT' then 'Battery Pack'
        when 'TOOL' then 'Tool Kit'
        else split(sku_id, '_')[safe_offset(1)]
    end as sku_name,
    case split(sku_id, '_')[safe_offset(1)]
        when 'DRILL' then 'Power Tools'
        when 'SAW' then 'Power Tools'
        when 'BATT' then 'Accessories'
        when 'TOOL' then 'Hand Tools'
        else 'Other'
    end as product_family
from sku_ids
