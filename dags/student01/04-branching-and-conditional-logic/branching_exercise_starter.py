"""Lab 04 - Branching logic, starter template (Workflow Orchestrator, Airflow 3.3.2)."""
import re
from pathlib import Path
import random
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator  # noqa: F401  (you will need these)
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator  # noqa: F401

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


with DAG(
    dag_id=f"branching_exercise_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "branching"],
) as dag:

    # TODO: Define tasks:
    # 1. extract_data (PythonOperator)
    # 2. quality_check (BranchPythonOperator)
    # 3. load_data (PythonOperator)
    # 4. send_alert (BashOperator)
    # 5. cleanup (PythonOperator with trigger_rule='none_failed_min_one_success')

    # TODO: Set dependencies:
    # extract_data >> quality_check >> [load_data, send_alert]
    # [load_data, send_alert] >> cleanup

    pass  # remove once you have defined the tasks above
