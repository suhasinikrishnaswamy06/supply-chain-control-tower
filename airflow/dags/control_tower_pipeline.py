from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.orchestration.gcp_ingestion import (
    ensure_bigquery_dataset,
    generate_source_data,
    load_raw_tables_from_local_files,
    run_dbt_build,
    validate_raw_files,
)


GCP_PROJECT_ID = os.environ.get("CONTROL_TOWER_GCP_PROJECT", "control-tower-493404")
RAW_DATASET = os.environ.get("CONTROL_TOWER_RAW_DATASET", "raw")
BQ_LOCATION = os.environ.get("CONTROL_TOWER_BQ_LOCATION", "US")


with DAG(
    dag_id="control_tower_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "logistics", "control_tower"],
) as dag:
    generate_data = PythonOperator(
        task_id="generate_source_data",
        python_callable=generate_source_data,
    )

    validate_files = PythonOperator(
        task_id="validate_raw_files",
        python_callable=validate_raw_files,
    )

    ensure_dataset = PythonOperator(
        task_id="ensure_bigquery_raw_dataset",
        python_callable=ensure_bigquery_dataset,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "dataset_name": RAW_DATASET,
            "location": BQ_LOCATION,
        },
    )

    load_raw_tables = PythonOperator(
        task_id="load_raw_tables_from_local_files",
        python_callable=load_raw_tables_from_local_files,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "dataset_name": RAW_DATASET,
            "location": BQ_LOCATION,
        },
    )

    build_dbt_models = PythonOperator(
        task_id="build_dbt_models",
        python_callable=run_dbt_build,
    )

    generate_data >> validate_files >> ensure_dataset >> load_raw_tables >> build_dbt_models
