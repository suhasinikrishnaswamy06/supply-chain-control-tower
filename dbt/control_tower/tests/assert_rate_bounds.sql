select *
from {{ ref('control_tower_executive_dashboard') }}
where on_time_rate < 0 or on_time_rate > 1
   or in_full_rate < 0 or in_full_rate > 1
   or otif_rate < 0 or otif_rate > 1
   or backlog_rate < 0 or backlog_rate > 1
   or inventory_risk_rate < 0 or inventory_risk_rate > 1
