# Architecture Overview

## Use Case

The control tower is intended for supply chain, transportation, and warehouse leaders who want a near-real-time operational view of fulfillment performance.

## Source Domains

- `orders`: customer order line commitments
- `shipments`: shipment execution and promised dates
- `inventory_snapshots`: daily stock position by warehouse and SKU
- `warehouse_events`: operational scans and task events

## Medallion-Style Flow

### Bronze

Raw source extracts land as CSV files and can later be replaced by API pulls, Pub/Sub messages, or source-system dumps into GCS.

### Silver

dbt staging models standardize column names, timestamps, and status logic.

### Gold

Curated marts produce control tower metrics for daily operational monitoring.

## Control Tower Questions

The mart is designed to answer:

- Which sites are most delayed today?
- Which shipments missed promise date?
- Which SKUs are below reorder threshold?
- Which warehouses are accumulating unpicked demand?

## Core Metric Definitions

- `late_shipment_rate`: late shipments divided by all delivered shipments
- `backlog_units`: order quantity not yet shipped for open orders
- `inventory_at_risk_units`: inventory units where on hand is at or below reorder point
- `warehouse_pick_delay_events`: warehouse picks started after SLA threshold

## Suggested GCP Deployment

- `Cloud Storage`: raw landing
- `BigQuery`: bronze, silver, and mart datasets
- `Cloud Composer`: managed Airflow
- `dbt Core`: transformations executed from Composer or CI
- `Looker Studio`: operational dashboard

## Future Enhancements

- Add lane and carrier dimensions
- Add weather and holiday enrichment
- Add event-driven delay alerts
- Add forecasting and ETA prediction outputs
