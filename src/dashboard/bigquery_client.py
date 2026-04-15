from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env.streamlit", override=False)


DEFAULT_PROJECT = os.environ.get("CONTROL_TOWER_GCP_PROJECT", "control-tower-493404")
DEFAULT_DATASET = os.environ.get("CONTROL_TOWER_CURATED_DATASET", "control_tower_dev")
DEFAULT_CREDENTIAL_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")


def get_bigquery_client() -> bigquery.Client:
    return bigquery.Client(project=DEFAULT_PROJECT)


def run_query(query: str) -> pd.DataFrame:
    client = get_bigquery_client()
    return client.query(query).result().to_dataframe()


def table_name(model_name: str) -> str:
    return f"`{DEFAULT_PROJECT}.{DEFAULT_DATASET}.{model_name}`"


def get_dashboard_config() -> dict[str, str | None]:
    return {
        "project": DEFAULT_PROJECT,
        "dataset": DEFAULT_DATASET,
        "credentials_path": DEFAULT_CREDENTIAL_PATH,
    }


def get_credentials_help_text() -> str:
    return (
        "Create a `.env.streamlit` file from `.env.streamlit.example` and set "
        "`GOOGLE_APPLICATION_CREDENTIALS` to your local service-account JSON path."
    )
