# Supply Chain Control Tower

`Supply Chain Control Tower` is a portfolio project that simulates a modern logistics analytics platform for operations teams. The goal is to unify shipment execution, inventory position, warehouse throughput, and service-level metrics into a decision-ready data model.

This project is intentionally designed to feel like a production-minded data engineering repository, not a notebook demo. It uses:

- `GCP` as the target cloud platform
- `Airflow` for orchestration
- `dbt` for transformations and tests
- `SQL` for dimensional and metric modeling
- `Python` for synthetic data generation and ingestion utilities

## Business Problem

Operations leaders often need a single place to answer questions like:

- Which shipments are at risk of being delivered late?
- Which warehouses are building backlog?
- Where are we likely to stock out in the next few days?
- Which carriers or lanes are underperforming?

In many teams, those answers are spread across raw ERP, WMS, and TMS extracts. This project models a control tower layer that standardizes those signals into curated marts.

## Scope

The starter version of the project includes:

- Synthetic logistics source data for orders, shipments, inventory snapshots, and warehouse events
- A Python utility to generate realistic CSV source files
- An Airflow DAG that stages the end-to-end flow
- A dbt project with staging and mart models
- Core business metrics including `OTIF proxy`, `late shipments`, `backlog units`, and `inventory at risk`

## Target Architecture

See [docs/architecture.md](C:/Users/suhas/OneDrive/Documents/New%20project/docs/architecture.md) for a fuller walkthrough.

High-level flow:

1. Python generates or ingests raw operational data
2. Raw files land in a bronze-style layer
3. Airflow orchestrates ingestion and transformations
4. dbt builds staging and curated mart tables
5. BI tools or dashboards query the control tower mart

## Repository Layout

- `airflow/`: orchestration assets
- `data/raw/`: synthetic source files
- `dbt/control_tower/`: dbt project
- `docs/`: architecture and project notes
- `src/`: Python utilities

## KPIs Modeled

- `on_time_shipments`
- `late_shipments`
- `late_shipment_rate`
- `backlog_units`
- `inventory_at_risk_units`
- `inventory_below_reorder_sku_count`
- `warehouse_pick_delay_events`

## Quick Start

1. Create a Python environment and install dependencies from `requirements.txt`
2. Generate source data:

```powershell
python -m src.data.generate_sample_data
```

3. Review the generated files in `data/raw/`
4. Point your Airflow and dbt local setup at the generated data or your warehouse tables
5. Run the DAG and build dbt models

## Portfolio Positioning

This project is meant to demonstrate:

- Strong logistics and supply chain domain context
- Data platform design thinking
- Production-style orchestration and transformation structure
- Business-facing metric design
- Clear repo organization and documentation

## Next Steps

Good follow-up enhancements:

- Add BigQuery landing and curated datasets
- Add dbt tests for freshness and uniqueness
- Add anomaly detection on delays and backlog
- Add dashboard assets in Looker Studio or Streamlit
- Add carrier- and lane-level drilldowns
