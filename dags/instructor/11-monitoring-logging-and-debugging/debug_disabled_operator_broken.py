"""Lab 11 - Debugging a broken DAG, part 2 (Workflow Orchestrator only).

This DAG never runs: PythonVirtualenvOperator is on Workflow Orchestrator's
disabled-operators list, so the DAG processor marks the file as failed.
Find the message under DAG import errors in the UI, then fix it in
debug_disabled_operator_solution.py.
"""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonVirtualenvOperator

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

    PythonVirtualenvOperator(
        task_id="summarise",
        python_callable=summarise,
        requirements=[],
    )
