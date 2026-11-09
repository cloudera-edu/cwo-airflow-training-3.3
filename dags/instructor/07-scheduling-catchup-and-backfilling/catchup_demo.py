"""Lab 07 - Scheduling and catchup behaviour (Workflow Orchestrator, Airflow 3.3.2).

Two DAGs, both on a 5-minute schedule with a fixed start_date in the past:
  - catchup_demo_<student>     catchup=True, with an end_date one hour after start_date
                               -> catches up every 5-minute slot in that hour (about 13 runs)
  - no_catchup_demo_<student>  catchup=False, no end_date
                               -> skips the backlog, runs only the latest slot, then every 5 min

Dates are fixed (not datetime.now()), so the DAG version stays stable and the
lab works on any day without editing.

Airflow 3 change: catchup now defaults to False (it was True in Airflow 2),
so set it explicitly. Part C of the lab starts a backfill from the UI
(Trigger > Backfill), which the scheduler runs.
"""
import re
from pathlib import Path
from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

START = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
END = datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)   # one-hour backlog for the catchup DAG


def process_slot(**context):
    print(f"run_id:       {context['run_id']}")
    print(f"logical date: {context['logical_date']}")


with DAG(
    dag_id=f"catchup_demo_{STUDENT}",
    start_date=START,
    end_date=END,
    schedule="*/5 * * * *",   # every 5 minutes
    catchup=True,
    max_active_runs=3,        # keep the catchup from flooding the shared pool
    tags=["training", "scheduling"],
):
    PythonOperator(task_id="process_slot", python_callable=process_slot)


with DAG(
    dag_id=f"no_catchup_demo_{STUDENT}",
    start_date=START,
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=3,
    tags=["training", "scheduling"],
):
    PythonOperator(task_id="process_slot", python_callable=process_slot)
