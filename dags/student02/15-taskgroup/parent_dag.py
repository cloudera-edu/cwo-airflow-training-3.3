"""Lab 15 - TaskGroup (Workflow Orchestrator, Airflow 3.3.2).

Change: TaskGroup now imported from airflow.sdk (airflow.utils.task_group is deprecated).
"""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG, TaskGroup, task

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"parent_dag_{STUDENT}",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["training", "taskgroup"],
):

    @task
    def start():
        print("pipeline starting")

    @task
    def extract():
        print("extracting...")

    @task
    def transform():
        print("transforming...")

    @task
    def load():
        print("loading...")

    @task
    def end():
        print("pipeline finished")

    with TaskGroup(group_id="etl") as etl:
        extract() >> transform() >> load()

    start() >> etl >> end()
