"""Lab 04 - Branching logic (Workflow Orchestrator, Airflow 3.3.2), solution."""
import re
from pathlib import Path
import random
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())


def check_data_quality(**kwargs):
    # Simulate: 70% chance of passing
    quality_score = random.uniform(0, 100)
    print(f"Quality score: {quality_score}")
    if quality_score >= 30:
        return "load_data"
    return "send_alert"


def extract_data_fn(**kwargs):
    print("extracting data...")


def load_data_fn(**kwargs):
    print("loading data...")


def cleanup_fn(**kwargs):
    print("running cleanup...")


with DAG(
    dag_id=f"branching_exercise_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "branching", "v2"],
) as dag:

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_fn,
    )

    quality_check = BranchPythonOperator(
        task_id="quality_check",
        python_callable=check_data_quality,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data_fn,
    )

    send_alert = BashOperator(
        task_id="send_alert",
        bash_command='echo "ALERT: data quality check failed"',
    )

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup_fn,
        trigger_rule="none_failed_min_one_success",
    )

    extract_data >> quality_check >> [load_data, send_alert]
    [load_data, send_alert] >> cleanup
