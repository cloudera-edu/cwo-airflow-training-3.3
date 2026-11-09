"""Lab 03 - DAG file structure (Workflow Orchestrator, Airflow 3.3.2)."""
# 1. Import statements
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG                                            # Airflow 3: DAG comes from airflow.sdk
from airflow.providers.standard.operators.python import PythonOperator  # operators live in the standard provider

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

# 2. Default arguments
default_args = {
    "owner": STUDENT,
    "retries": 2,
}

# 3. DAG definition (context manager style)
with DAG(
    dag_id=f"my_first_dag_{STUDENT}",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # 4. Task definitions
    def my_task_function():
        print("Task executed!")

    task1 = PythonOperator(
        task_id="first_task",
        python_callable=my_task_function,
    )

    # 5. Dependencies
    task1  # single task, no dependencies needed
