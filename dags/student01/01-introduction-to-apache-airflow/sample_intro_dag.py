"""Lab 01 - Exploring Airflow concepts (Workflow Orchestrator, Airflow 3.3.2)."""
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

default_args = {
    "owner": "training",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id=f"sample_intro_dag_{STUDENT}",
    default_args=default_args,
    description="A sample DAG for training",
    schedule="@daily",                 # Airflow 3: schedule= replaces schedule_interval=
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "intro"],
)


def print_hello():
    print("Hello from Airflow!")
    return "Hello complete"


task_hello = PythonOperator(
    task_id="say_hello",
    python_callable=print_hello,
    dag=dag,
)

task_date = BashOperator(
    task_id="print_date",
    bash_command="date",
    dag=dag,
)

task_hello >> task_date
