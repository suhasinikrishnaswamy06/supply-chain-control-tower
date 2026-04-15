select *
from {{ ref('control_tower_executive_dashboard') }}
where shipment_records < 0
   or delivered_shipments < 0
   or on_time_shipments < 0
   or late_shipments < 0
   or backlog_orders < 0
   or backlog_units < 0
   or total_on_hand_units < 0
   or inventory_at_risk_units < 0
   or inventory_below_reorder_sku_count < 0
   or warehouse_pick_delay_events < 0
