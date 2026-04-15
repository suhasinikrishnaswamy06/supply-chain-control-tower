from __future__ import annotations

from pathlib import Path
import os
import shutil
import subprocess
import sys

from google.cloud import bigquery


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_FILE_TABLES = {
    "orders.csv": "orders",
    "shipments.csv": "shipments",
    "inventory_snapshots.csv": "inventory_snapshots",
    "warehouse_events.csv": "warehouse_events",
}


def generate_source_data() -> None:
    subprocess.run(
        [sys.executable, "-m", "src.data.generate_sample_data"],
        cwd=PROJECT_ROOT,
        check=True,
    )


def validate_raw_files() -> None:
    missing = [name for name in RAW_FILE_TABLES if not (RAW_DIR / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing required raw files: {missing}")


def ensure_bigquery_dataset(project_id: str, dataset_name: str, location: str = "US") -> None:
    client = bigquery.Client(project=project_id)
    dataset_id = f"{project_id}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)
    print(f"Ensured dataset exists: {dataset_id} in {location}")


def load_raw_tables_from_local_files(
    project_id: str,
    dataset_name: str,
    location: str = "US",
) -> None:
    client = bigquery.Client(project=project_id)

    for file_name, table_name in RAW_FILE_TABLES.items():
        local_path = RAW_DIR / file_name
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(local_path, "rb") as source_file:
            load_job = client.load_table_from_file(
                source_file,
                table_id,
                job_config=job_config,
                location=location,
            )
        load_job.result()
        print(f"Loaded {local_path} into {table_id}")


def load_raw_tables_from_gcs(
    project_id: str,
    dataset_name: str,
    bucket_name: str,
    prefix: str = "control_tower/raw",
    location: str = "US",
) -> None:
    client = bigquery.Client(project=project_id)

    for file_name, table_name in RAW_FILE_TABLES.items():
        uri = f"gs://{bucket_name}/{prefix}/{file_name}"
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config,
            location=location,
        )
        load_job.result()
        print(f"Loaded {uri} into {table_id}")


def run_dbt_build(
    project_dir: Path | None = None,
    profiles_dir: Path | None = None,
) -> None:
    dbt_project_dir = project_dir or PROJECT_ROOT / "dbt" / "control_tower"
    dbt_profiles_dir = profiles_dir or Path(os.environ.get("DBT_PROFILES_DIR", Path.home() / ".dbt"))
    dbt_executable = shutil.which("dbt")

    if dbt_executable:
        command = [
            dbt_executable,
            "build",
            "--project-dir",
            str(dbt_project_dir),
            "--profiles-dir",
            str(dbt_profiles_dir),
        ]
    else:
        command = [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "build",
            "--project-dir",
            str(dbt_project_dir),
            "--profiles-dir",
            str(dbt_profiles_dir),
        ]

    subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
    )
