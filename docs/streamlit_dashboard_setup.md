# Streamlit Dashboard Setup

## Purpose

The Streamlit app provides a lightweight operational UI on top of the curated control tower marts in BigQuery.

## What It Shows

- executive KPI overview
- warehouse-level performance
- carrier performance
- SKU-level inventory risk

## Prerequisites

- BigQuery curated models already built by dbt
- Google credentials available through `GOOGLE_APPLICATION_CREDENTIALS`
- Python dependencies installed from `requirements.txt`

## Configuration

Create a local config file from the template:

```powershell
Copy-Item .env.streamlit.example .env.streamlit
```

Then update `.env.streamlit` with your real values:

```text
CONTROL_TOWER_GCP_PROJECT=control-tower-493404
CONTROL_TOWER_CURATED_DATASET=control_tower_dev
GOOGLE_APPLICATION_CREDENTIALS=C:/Users/suhas/keys/dbt-control-tower-sa.json
```

## Run The App

From the repo root:

```powershell
streamlit run src/dashboard/streamlit_app.py
```

Then open the local URL shown by Streamlit, usually:

- `http://localhost:8501`

## Notes

- The app reads directly from BigQuery marts instead of local CSV files
- If the app cannot connect, verify `.env.streamlit` and confirm the dbt models exist in the configured dataset
