"""Lab 11 - Debugging a broken DAG, part 2 solution (Workflow Orchestrator, Airflow 3.3.2).

The task needs nothing beyond the standard library, so a plain PythonOperator
does the job. If a task really needs extra packages, add them through a
custom image rather than re-enabling PythonVirtualenvOperator.
"""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())


def summarise():
    import statistics

    print(statistics.mean([3, 5, 8]))


with DAG(
    dag_id=f"debug_disabled_operator_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "debug"],
) as dag:

    PythonOperator(task_id="summarise", python_callable=summarise)
