# Airflow + BigQuery Setup

## Purpose

This setup turns the portfolio project into an orchestrated ingestion pipeline:

- local synthetic data generation
- raw load into BigQuery
- dbt build execution

`GCS` is intentionally deferred so the project remains runnable without enabling billing-heavy services. The orchestration code is structured so a Cloud Storage landing step can be added later.

## Required Environment Variables

Set these variables in your Airflow environment or local shell before running the DAG:

```powershell
$env:CONTROL_TOWER_GCP_PROJECT="control-tower-493404"
$env:CONTROL_TOWER_RAW_DATASET="raw"
$env:CONTROL_TOWER_BQ_LOCATION="US"
$env:DBT_PROFILES_DIR="C:\Users\suhas\.dbt"
```

## Prerequisites

- Service account credentials that can access BigQuery
- A dbt profile configured for BigQuery
- Airflow installed in the same Python environment or available in Composer

## Local Execution Notes

- The DAG uses `PythonOperator` so it can run locally without adding Airflow provider dependencies
- Raw CSV files are loaded directly from the repo's `data/raw` folder into BigQuery
- `dbt build` runs from the repo's `dbt/control_tower` directory

## Suggested Production Evolution

- Add `GCS` as a landing zone between source generation and BigQuery raw
- Land files in dated partition paths
- Add load audit logging and row-count validation
- Add retry and alerting for ingestion failures
