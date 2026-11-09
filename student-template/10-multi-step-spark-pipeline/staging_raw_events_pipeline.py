"""Lab 10 - Multi-step Spark pipeline on CDE (Workflow Orchestrator, Airflow 3.3.2).

ingest -> validate -> transform -> publish, every step a CDE Spark job.

Changes from the CDE version:
  - CDW is not supported from Workflow Orchestrator, so the CDW row-count
    check is now a Spark job (validate_job) that fails when no rows landed.
  - The Spark jobs read and write Parquet on Object Store, so no Hive
    database (or Hue step) is needed.
  - New CDE provider import path and an explicit connection_id.

Create the CDE jobs ingest_job, validate_job, transform_job and publish_job
from the scripts in spark/10-multi-step-spark-pipeline/ first.
"""
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
CDE_CONN_ID = "cde_runtime_api"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=f"staging_raw_events_pipeline_{STUDENT}",
    default_args=default_args,
    description="Ingest, validate, transform and publish raw events",
    schedule="@daily",
    start_date=datetime(2026, 8, 27),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),   # four Spark jobs can exceed the 1-hour default
    tags=["training", "cde", "raw_events"],
) as dag:

    ingest = CdeRunJobOperator(task_id="ingest_raw_data", job_name="ingest_job", connection_id=CDE_CONN_ID)
    validate = CdeRunJobOperator(task_id="validate_row_count", job_name="validate_job", connection_id=CDE_CONN_ID)
    transform = CdeRunJobOperator(task_id="transform_data", job_name="transform_job", connection_id=CDE_CONN_ID)
    publish = CdeRunJobOperator(task_id="publish_to_curated", job_name="publish_job", connection_id=CDE_CONN_ID)

    ingest >> validate >> transform >> publish
