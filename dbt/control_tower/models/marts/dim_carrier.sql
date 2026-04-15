select distinct
    carrier_name,
    case
        when carrier_name in ('DHL', 'UPS', 'FEDEX') then 'Parcel'
        when carrier_name = 'XPO' then 'LTL'
        else 'Other'
    end as carrier_mode,
    case
        when carrier_name in ('DHL', 'UPS', 'FEDEX') then 'Small Parcel Network'
        when carrier_name = 'XPO' then 'Freight Network'
        else 'Other'
    end as carrier_segment
from {{ ref('stg_shipments') }}
