# Airflow on Docker Desktop

## Purpose

This setup runs the control tower Airflow services inside Linux containers so you can use the Airflow web UI on Windows through Docker Desktop.

## Files

- `docker-compose.airflow.yml`
- `docker/airflow/Dockerfile`
- `.env.airflow.example`

## One-Time Setup

1. Copy `.env.airflow.example` to `.env.airflow`
2. Update these values in `.env.airflow`:

```text
HOST_GCP_KEY_DIR=C:/Users/suhas/keys
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/keys/dbt-control-tower-sa.json
```

Use the real filename of your service-account JSON inside `C:/Users/suhas/keys`.
Docker uses the repo-owned profile at `docker/airflow/profiles.yml`, so your Windows `C:\Users\suhas\.dbt\profiles.yml` remains unchanged.

## Start Airflow

From the repo root:

```powershell
Copy-Item .env.airflow.example .env.airflow
docker compose -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.airflow.yml up -d
```

## Open the UI

Open [http://localhost:8080](http://localhost:8080)

Default credentials come from `.env.airflow`:

- username: `airflow`
- password: `airflow`

## What Runs

- `postgres`: Airflow metadata database
- `airflow-init`: database migration and admin-user creation
- `airflow-webserver`: Airflow UI
- `airflow-scheduler`: DAG scheduling

## Useful Commands

View logs:

```powershell
docker compose -f docker-compose.airflow.yml logs -f airflow-webserver
docker compose -f docker-compose.airflow.yml logs -f airflow-scheduler
```

Stop services:

```powershell
docker compose -f docker-compose.airflow.yml down
```

Rebuild after dependency changes:

```powershell
docker compose -f docker-compose.airflow.yml build
docker compose -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.airflow.yml up -d
```

Trigger the DAG from inside the scheduler container:

```powershell
docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags trigger control_tower_pipeline
```

## Notes

- The project repo is mounted into `/opt/control_tower`, so code changes on Windows appear immediately inside the containers.
- The Docker-specific dbt profile is mounted into `/opt/airflow/.dbt/profiles.yml`.
- Your host key directory is mounted into `/opt/airflow/keys`.
- If Docker Desktop cannot read a Windows path, check Docker file-sharing permissions.
