from __future__ import annotations

from datetime import datetime
from pathlib import Path
import subprocess
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def generate_source_data() -> None:
    subprocess.run(
        [sys.executable, "-m", "src.data.generate_sample_data"],
        cwd=PROJECT_ROOT,
        check=True,
    )


def validate_raw_files() -> None:
    required_files = [
        PROJECT_ROOT / "data" / "raw" / "orders.csv",
        PROJECT_ROOT / "data" / "raw" / "shipments.csv",
        PROJECT_ROOT / "data" / "raw" / "inventory_snapshots.csv",
        PROJECT_ROOT / "data" / "raw" / "warehouse_events.csv",
    ]
    missing = [str(path) for path in required_files if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required raw files: {missing}")


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

    generate_data >> validate_files
