"""Lab 03 - Build and deploy your first DAG (Workflow Orchestrator, Airflow 3.3.2).

Deploy by committing this file under your student folder in the course Git
repository and pushing it. Workflow Orchestrator picks it up on the next Git sync.
"""
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
    "owner": STUDENT,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id=f"training_first_dag_{STUDENT}",
    default_args=default_args,
    description="My first Workflow Orchestrator DAG",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training"],
) as dag:

    def extract():
        print("Extracting data from source...")
        data = {"records": 100, "source": "api"}
        print(f'Extracted {data["records"]} records')
        return data

    def transform():
        print("Transforming data...")
        print("Applied cleaning rules")
        print("Applied business logic")

    def load():
        print("Loading data to destination...")
        print("Data loaded successfully")

    task_extract = PythonOperator(task_id="extract_data", python_callable=extract)
    task_transform = PythonOperator(task_id="transform_data", python_callable=transform)
    task_load = PythonOperator(task_id="load_data", python_callable=load)

    task_notify = BashOperator(
        task_id="send_notification",
        bash_command='echo "Pipeline completed at $(date)"',
    )

    task_extract >> task_transform >> task_load >> task_notify
